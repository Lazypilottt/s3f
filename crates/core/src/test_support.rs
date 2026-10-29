//! In-process fake S3 server for tests.
//!
//! Unlike a generic HTTP mock, this serves every connection on its own task
//! and implements the S3 semantics the transfer code relies on:
//! `ListObjectsV2` paging with `prefix`, `start-after` and continuation
//! tokens, ranged `GET`s, and `If-Match`. It can add per-request latency and
//! cap bandwidth per connection, which is how real S3 behaves over a WAN.

#![allow(
    clippy::arithmetic_side_effects,
    clippy::as_conversions,
    clippy::cast_precision_loss,
    clippy::float_arithmetic,
    reason = "test-only server; overflow and precision are irrelevant here"
)]

use core::time::Duration;
use std::{
    collections::{BTreeMap, HashSet},
    fmt::Write as _,
    ops::Bound,
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::{TcpListener, TcpStream},
};

use crate::{Config, S3Client, auth::CredentialSource};

/// Request counters, readable after a test run.
#[derive(Debug, Default, Clone)]
pub(crate) struct Stats {
    active: usize,
    /// Most requests that were being served at the same time.
    pub peak_active: usize,
    /// Total requests served.
    pub requests: usize,
    /// Number of `GET`s that carried a `Range` header.
    pub ranged_gets: usize,
    /// Responses whose body was cut off on purpose.
    pub cut_bodies: usize,
    /// Request targets that have straggled. Each straggles once, so a
    /// backup copy of the same request answers normally.
    pub straggled: HashSet<String>,
}

/// Behaviour knobs for the fake server.
#[derive(Debug, Clone)]
pub(crate) struct FakeS3 {
    /// Objects by key.
    pub objects: BTreeMap<String, Bytes>,
    /// Keys per `ListObjectsV2` page.
    pub page_size: usize,
    /// Added before each response.
    pub latency: Duration,
    /// Bytes per second per connection, if capped.
    pub conn_rate: Option<u64>,
    /// Whether `start-after` is honoured (some backends ignore it).
    pub honour_start_after: bool,
    /// Every Nth request (1-based) waits `straggler_delay` extra before
    /// answering, like an unlucky request on a busy front end.
    pub straggler_every: Option<usize>,
    /// Extra delay for stragglers.
    pub straggler_delay: Duration,
    /// Every Nth object `GET` with a body sends half of it, then drops the
    /// connection.
    pub cut_body_every: Option<usize>,
}

impl FakeS3 {
    /// A server holding `objects`, with S3-like defaults.
    pub(crate) fn new(objects: impl IntoIterator<Item = (String, Bytes)>) -> Self {
        Self {
            objects: objects.into_iter().collect(),
            page_size: 1000,
            latency: Duration::ZERO,
            conn_rate: None,
            honour_start_after: true,
            straggler_every: None,
            straggler_delay: Duration::ZERO,
            cut_body_every: None,
        }
    }

    /// A server holding empty objects with the given keys.
    pub(crate) fn with_keys(keys: impl IntoIterator<Item = String>) -> Self {
        Self::new(keys.into_iter().map(|k| (k, Bytes::new())))
    }

    /// Start serving on an ephemeral port. Returns a client pointed at it
    /// and the shared stats.
    pub(crate) async fn start(self) -> (S3Client, Arc<Mutex<Stats>>) {
        self.start_with(|_| {}).await
    }

    /// Like [`start`](Self::start), letting the test adjust the client's
    /// config first.
    pub(crate) async fn start_with(
        self, configure: impl FnOnce(&mut Config),
    ) -> (S3Client, Arc<Mutex<Stats>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let stats = Arc::new(Mutex::new(Stats::default()));
        let server = Arc::new(self);

        let s = Arc::clone(&stats);
        tokio::spawn(async move {
            while let Ok((sock, _)) = listener.accept().await {
                tokio::spawn(serve_conn(sock, Arc::clone(&server), Arc::clone(&s)));
            }
        });

        let mut config = Config::with_endpoint(
            "us-east-1",
            CredentialSource::Static {
                access_key: "AKID".into(),
                secret_key: "SECRET".into(),
            },
            format!("http://{addr}"),
        );
        configure(&mut config);
        (S3Client::new(config).await.unwrap(), stats)
    }
}

/// A parsed request: path, decoded query pairs, and the headers we use.
struct Req {
    target: String,
    path: String,
    query: Vec<(String, String)>,
    range: Option<(u64, u64)>,
    if_match: Option<String>,
}

impl Req {
    fn param(&self, name: &str) -> Option<&str> {
        self.query.iter().find(|(k, _)| k == name).map(|(_, v)| v.as_str())
    }
}

fn decode(s: &str) -> String {
    percent_encoding::percent_decode_str(s).decode_utf8_lossy().into_owned()
}

fn parse(head: &str) -> Req {
    let mut lines = head.split("\r\n");
    let target = lines.next().unwrap().split(' ').nth(1).unwrap();
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let query = query
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|p| {
            let (k, v) = p.split_once('=').unwrap_or((p, ""));
            (decode(k), decode(v))
        })
        .collect();

    let mut req = Req {
        target: target.to_owned(),
        path: decode(path),
        query,
        range: None,
        if_match: None,
    };
    for line in lines {
        let Some((name, value)) = line.split_once(':') else { continue };
        let value = value.trim();
        match name.to_ascii_lowercase().as_str() {
            "range" => {
                let spec = value.trim_start_matches("bytes=");
                let (a, b) = spec.split_once('-').unwrap();
                req.range = Some((a.parse().unwrap(), b.parse().unwrap()));
            },
            "if-match" => req.if_match = Some(value.to_owned()),
            _ => {},
        }
    }
    req
}

/// Cheap content tag: length plus a strided sample of the bytes, which is
/// enough to tell test objects apart without hashing megabytes per request.
fn etag_of(body: &Bytes) -> String {
    let stride = (body.len() / 4096).max(1);
    let sample: u64 = body.iter().step_by(stride).map(|b| u64::from(*b)).sum();
    format!("\"{sample:x}-{}\"", body.len())
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;")
}

fn list_body(server: &FakeS3, req: &Req) -> String {
    let prefix = req.param("prefix").unwrap_or_default();
    let delimiter = req.param("delimiter").filter(|d| !d.is_empty());
    let after = req
        .param("continuation-token")
        .or_else(|| req.param("start-after").filter(|_| server.honour_start_after));

    // Walk the sorted map from the later of `prefix` and `after`, rolling
    // keys up into common prefixes when a delimiter is given.
    let lower = after.filter(|a| *a >= prefix).map_or(Bound::Included(prefix), Bound::Excluded);
    let mut entries = server
        .objects
        .range::<str, _>((lower, Bound::Unbounded))
        .take_while(|(k, _)| k.starts_with(prefix))
        // Resuming after a rolled-up prefix skips everything under it.
        .filter(|(k, _)| {
            !(delimiter.is_some_and(|d| after.is_some_and(|a| a.ends_with(d)))
                && after.is_some_and(|a| k.starts_with(a)))
        })
        .map(|(k, body)| {
            let rolled = delimiter.and_then(|d| {
                k[prefix.len()..].find(d).map(|i| k[..prefix.len() + i + d.len()].to_owned())
            });
            rolled.map_or(Ok((k, body)), Err)
        })
        .peekable();

    let mut contents = Vec::new();
    let mut prefixes: Vec<String> = Vec::new();
    let mut last_entry = None;
    while contents.len() + prefixes.len() < server.page_size {
        match entries.next() {
            Some(Ok((k, body))) => {
                last_entry = Some(k.clone());
                contents.push((k, body));
            },
            Some(Err(cp)) => {
                if prefixes.last() != Some(&cp) {
                    last_entry = Some(cp.clone());
                    prefixes.push(cp);
                }
            },
            None => break,
        }
    }
    // Swallow the rest of a common prefix that straddles the page edge.
    while let Some(Err(cp)) = entries.peek()
        && prefixes.last() == Some(cp)
    {
        entries.next();
    }
    let truncated = entries.next().is_some();

    let mut xml = format!("<ListBucketResult><IsTruncated>{truncated}</IsTruncated>");
    if truncated && let Some(last) = &last_entry {
        write!(xml, "<NextContinuationToken>{}</NextContinuationToken>", xml_escape(last)).unwrap();
    }
    for (key, body) in &contents {
        write!(
            xml,
            "<Contents><Key>{}</Key><Size>{}</Size><ETag>{}</ETag><LastModified>2024-01-01T00:00:\
             00.000Z</LastModified></Contents>",
            xml_escape(key),
            body.len(),
            xml_escape(&etag_of(body)),
        )
        .unwrap();
    }
    for cp in &prefixes {
        write!(xml, "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>", xml_escape(cp))
            .unwrap();
    }
    xml.push_str("</ListBucketResult>");
    xml
}

/// Build status line, extra headers and body for one request.
fn respond(server: &FakeS3, req: &Req, stats: &Mutex<Stats>) -> (&'static str, String, Bytes) {
    let mut parts = req.path.trim_start_matches('/').splitn(2, '/');
    let _bucket = parts.next();
    let key = parts.next().unwrap_or_default();

    if key.is_empty() {
        return ("200 OK", String::new(), Bytes::from(list_body(server, req)));
    }

    let Some(body) = server.objects.get(key) else {
        return (
            "404 Not Found",
            String::new(),
            Bytes::from_static(b"<Error><Code>NoSuchKey</Code></Error>"),
        );
    };
    let etag = etag_of(body);
    if req.if_match.as_ref().is_some_and(|m| *m != etag) {
        return ("412 Precondition Failed", String::new(), Bytes::new());
    }
    let headers = format!("ETag: {etag}\r\n");
    match req.range {
        Some((a, b)) => {
            stats.lock().unwrap().ranged_gets += 1;
            let end = usize::try_from(b).unwrap().min(body.len() - 1);
            let slice = body.slice(usize::try_from(a).unwrap()..=end);
            ("206 Partial Content", headers, slice)
        },
        None => ("200 OK", headers, body.clone()),
    }
}

async fn serve_conn(mut sock: TcpStream, server: Arc<FakeS3>, stats: Arc<Mutex<Stats>>) {
    let mut buf = Vec::new();
    loop {
        // Read one request head (GETs only carry no body; PUT bodies are
        // not needed by these tests).
        let head = loop {
            if let Some(end) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                let head = String::from_utf8_lossy(&buf[..end]).into_owned();
                buf.drain(..end + 4);
                break head;
            }
            let mut chunk = [0_u8; 8192];
            match sock.read(&mut chunk).await {
                Ok(0) | Err(_) => return,
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
            }
        };

        let number = {
            let mut s = stats.lock().unwrap();
            s.requests += 1;
            s.active += 1;
            s.peak_active = s.peak_active.max(s.active);
            s.requests
        };

        let req = parse(&head);
        let (status_line, headers, body) = respond(&server, &req, &stats);
        let mut delay = server.latency;
        if server.straggler_every.is_some_and(|n| number % n == 0)
            && stats.lock().unwrap().straggled.insert(format!("{} {:?}", req.target, req.range))
        {
            delay += server.straggler_delay;
        }
        tokio::time::sleep(delay).await;

        let is_object =
            !req.path.trim_start_matches('/').split_once('/').is_none_or(|(_, k)| k.is_empty());
        let cut =
            is_object && body.len() > 1 && server.cut_body_every.is_some_and(|n| number % n == 0);
        let sent = if cut {
            stats.lock().unwrap().cut_bodies += 1;
            &body[..body.len() / 2]
        } else {
            &body[..]
        };

        let head =
            format!("HTTP/1.1 {status_line}\r\nContent-Length: {}\r\n{headers}\r\n", body.len());
        let ok = write_throttled(&mut sock, head.as_bytes(), sent, server.conn_rate).await;
        stats.lock().unwrap().active -= 1;
        if !ok || cut {
            return;
        }
    }
}

/// Write `head` then `body`, pacing the body to `rate` bytes per second.
async fn write_throttled(
    sock: &mut TcpStream, head: &[u8], body: &[u8], rate: Option<u64>,
) -> bool {
    if sock.write_all(head).await.is_err() {
        return false;
    }
    let Some(rate) = rate else {
        return sock.write_all(body).await.is_ok();
    };
    let chunk = 64 * 1024;
    let start = tokio::time::Instant::now();
    let mut sent = 0_u64;
    for piece in body.chunks(chunk) {
        if sock.write_all(piece).await.is_err() {
            return false;
        }
        sent += piece.len() as u64;
        let due = Duration::from_secs_f64(sent as f64 / rate as f64);
        tokio::time::sleep_until(start + due).await;
    }
    true
}
