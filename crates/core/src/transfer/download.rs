//! Download transfer — single GET and Range-based multipart download.
//!
//! Small files are fetched with a single GET and streamed to disk.
//! Large files are split into Range requests that write concurrently
//! to different offsets of the same file via positioned writes (`pwrite`).
//! Files in between are downloaded adaptively: a single GET starts
//! streaming, and if its measured speed says the rest would take too long,
//! the remainder is handed to parallel Range requests while the first
//! stream finishes its own share (see [`download_adaptive`]).
//! A single fd is shared across all part tasks — no per-part open/seek/close.
//!
//! Disk I/O for multipart downloads runs on a **dedicated writer thread**
//! (not tokio's blocking pool). Async download tasks send buffered chunks
//! over a bounded channel; the writer thread drains them with `pwrite`.
//! This eliminates ~10K `spawn_blocking` dispatches per GB and the
//! associated thread-pool scheduling overhead.
//!
//! Temp files use [`tempfile::NamedTempFile`] for crash-safe cleanup:
//! if the process is killed mid-download, the OS reclaims the temp
//! file instead of leaving orphaned `.s3f.part` files on disk.

use core::time::Duration;
use std::{os::unix::fs::FileExt as _, path::Path, sync::Arc, time::Instant};

use bytes::Bytes;
use futures_util::StreamExt as _;
use http::{Method, Uri};
use tokio::{
    fs,
    io::{AsyncSeekExt as _, AsyncWriteExt as _},
    sync::{Semaphore, mpsc},
    task::{self, JoinSet},
};

use crate::{
    auth::Credentials,
    config::Config,
    error::{Error, Result},
    http::{
        HttpClient,
        ObjectKey,
        request::build_signed,
        retry::{backoff, send_with_retry},
    },
    trace::{maybe_debug, maybe_info, maybe_warn},
    transfer::part::Part,
};

/// Stream a response body to `file`. The outer error is a local I/O
/// failure; the inner one means the connection broke and the body can be
/// fetched again.
async fn stream_to_file(
    resp: reqwest::Response, file: &mut fs::File,
) -> Result<core::result::Result<u64, reqwest::Error>> {
    let mut size = 0_u64;
    let mut stream = resp.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = match chunk {
            Ok(chunk) => chunk,
            Err(e) => return Ok(Err(e)),
        };
        size = size.saturating_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX));
        file.write_all(&chunk).await?;
    }
    Ok(Ok(size))
}

/// Download a single object with a plain GET — streams to a temp file,
/// then atomically renames to the final path. The temp file is
/// automatically cleaned up on error or process crash.
///
/// If the connection breaks mid-body, the GET starts over (these objects
/// are small, so resuming isn't worth a second request shape).
///
/// # Errors
///
/// Returns an error if the HTTP request, body streaming, or file I/O fails.
pub(crate) async fn download_single(
    http: &HttpClient, config: &Config, creds: &Credentials, bucket: &str, key: &ObjectKey,
    dest: &Path,
) -> Result<u64> {
    let uri: Uri = format!("{}/{bucket}/{}", config.endpoint_url(), key.encoded()).parse()?;
    let get = async || {
        let req = build_signed(Method::GET, uri.clone(), Bytes::new(), creds, &config.region)?;
        send_with_retry(http, req, &config.retry).await
    };
    let mut resp = get().await?;

    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent).await?;
    }

    let tmp = task::spawn_blocking({
        let dir = dest.parent().unwrap_or_else(|| Path::new(".")).to_owned();
        move || tempfile::NamedTempFile::new_in(dir)
    })
    .await
    .map_err(|e| Error::Internal(e.to_string()))??;

    // reopen() gives an independent fd to the same file. We wrap it in
    // tokio::fs::File for async I/O while the NamedTempFile guard stays
    // alive for cleanup-on-drop.
    let std_file = tmp.reopen()?;
    let mut file = fs::File::from_std(std_file);
    let mut attempt = 0_u32;
    let size = loop {
        match stream_to_file(resp, &mut file).await? {
            Ok(size) => break size,
            Err(e) if attempt < config.retry.max_retries => {
                maybe_warn!(key = %key, attempt, error = %e, "download interrupted, restarting");
                file.set_len(0).await?;
                file.rewind().await?;
                tokio::time::sleep(backoff(&config.retry, attempt)).await;
                attempt = attempt.saturating_add(1);
                resp = get().await?;
            },
            Err(e) => return Err(e.into()),
        }
    };
    file.flush().await?;
    drop(file);

    // Atomic rename — consumes the NamedTempFile so it won't delete-on-drop.
    let dest_owned = dest.to_owned();
    task::spawn_blocking(move || tmp.persist(dest_owned).map_err(|e| Error::Io(e.error)))
        .await
        .map_err(|e| Error::Internal(e.to_string()))??;

    Ok(size)
}

/// A write command sent from async download tasks to the writer thread.
struct WriteCmd {
    buf: Vec<u8>,
    offset: u64,
}

/// Handle to the dedicated writer thread. Async tasks send [`WriteCmd`]s
/// through the bounded channel; the thread does `pwrite` without touching
/// the tokio runtime. Dropped senders signal the thread to flush and exit.
struct DiskWriter {
    tx: std::sync::mpsc::SyncSender<WriteCmd>,
    handle: Option<std::thread::JoinHandle<std::io::Result<()>>>,
}

impl DiskWriter {
    /// Spawn a writer thread for `fd`. `capacity` bounds the channel
    /// (backpressure: async tasks block when the writer can't keep up).
    fn spawn(fd: Arc<std::fs::File>, capacity: usize) -> Self {
        let (tx, rx) = std::sync::mpsc::sync_channel::<WriteCmd>(capacity);
        let handle = std::thread::spawn(move || {
            while let Ok(cmd) = rx.recv() {
                fd.write_all_at(&cmd.buf, cmd.offset)?;
            }
            fd.sync_all()
        });
        Self {
            tx,
            handle: Some(handle),
        }
    }

    /// Wait for the writer thread to finish and propagate any I/O error.
    ///
    /// All other sender clones (in `DownloadCtx`) must be dropped before
    /// calling this so the writer's `recv()` loop can exit.
    fn finish(self) -> Result<()> {
        // Drop our sender — if all other clones are already dropped,
        // this unblocks the writer thread's recv() loop.
        drop(self.tx);
        if let Some(h) = self.handle {
            h.join()
                .map_err(|_| Error::Internal("writer thread panicked".into()))?
                .map_err(Error::Io)?;
        }
        Ok(())
    }

    /// Like [`finish`](Self::finish), for a download that ended with
    /// `result`. A failed write stops the writer thread, which the download
    /// tasks only see as a closed channel; the writer's own error (a full
    /// disk, say) is what actually went wrong, so it takes precedence.
    fn finish_with(self, result: Result<()>) -> Result<()> {
        let written = self.finish();
        match result {
            Ok(()) => written,
            Err(e) => Err(written.err().unwrap_or(e)),
        }
    }
}

/// Context for a multipart download.
struct DownloadCtx {
    bucket: String,
    config: Config,
    creds: Credentials,
    http: HttpClient,
    /// `ETag` every part must still match (adaptive downloads), so a
    /// concurrent overwrite fails the download instead of mixing versions.
    if_match: Option<http::HeaderValue>,
    key: ObjectKey,
    /// Channel to the dedicated writer thread.
    writer_tx: std::sync::mpsc::SyncSender<WriteCmd>,
}

/// Part job metadata for the download scheduler.
struct DownloadPartJob {
    number: u32,
    offset: u64,
    size: u64,
}

/// Download a single object using concurrent Range requests.
///
/// All parts share a single file descriptor and write to their respective
/// offsets via `pwrite` (positioned write). This avoids the overhead of
/// opening/seeking/flushing a separate fd per part. The file is
/// pre-allocated to its full size, then a single `sync_all` is issued
/// after all parts have completed.
///
/// Uses [`tempfile::NamedTempFile`] for crash-safe cleanup.
///
/// # Errors
///
/// Returns an error if any Range request or file I/O fails.
#[expect(clippy::too_many_arguments, reason = "internal fn, context struct would add indirection")]
pub(crate) async fn download_multipart(
    http: &HttpClient, config: &Config, creds: &Credentials, bucket: &str, key: &ObjectKey,
    parts: &[Part], dest: &Path, total_size: u64, concurrency: usize,
) -> Result<u64> {
    assert!(concurrency > 0, "concurrency must be at least 1");

    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent).await?;
    }

    let tmp = task::spawn_blocking({
        let dir = dest.parent().unwrap_or_else(|| Path::new(".")).to_owned();
        move || tempfile::NamedTempFile::new_in(dir)
    })
    .await
    .map_err(|e| Error::Internal(e.to_string()))??;

    // Open a single shared fd via reopen(). Pre-allocate to full size so
    // the filesystem can allocate contiguously.
    let std_file = tmp.reopen()?;
    std_file.set_len(total_size)?;

    // Spawn a dedicated writer thread. Channel capacity = concurrency × 2
    // so each async part task can have one buffer in-flight plus one queued.
    let writer = DiskWriter::spawn(Arc::new(std_file), concurrency.saturating_mul(2).max(2));

    maybe_info!(
        key = %key, parts = parts.len(), concurrency, size = total_size,
        "multipart download started"
    );

    let ctx = Arc::new(DownloadCtx {
        bucket: bucket.to_owned(),
        config: config.clone(),
        creds: creds.clone(),
        http: http.clone(),
        if_match: None,
        key: key.clone(),
        writer_tx: writer.tx.clone(),
    });

    let result = download_all_parts(&ctx, parts, concurrency).await;

    // Drop all sender clones so the writer thread can exit.
    drop(ctx);

    // Wait for the writer to flush + sync_all. On error, the NamedTempFile
    // drops here and deletes the temp file.
    writer.finish_with(result)?;

    let dest = dest.to_owned();
    task::spawn_blocking(move || tmp.persist(dest).map_err(|e| Error::Io(e.error)))
        .await
        .map_err(|e| Error::Internal(e.to_string()))??;
    maybe_info!(key = %key, size = total_size, "multipart download complete");
    Ok(total_size)
}

/// Schedule and download all parts using a producer-consumer pipeline.
async fn download_all_parts(
    ctx: &Arc<DownloadCtx>, parts: &[Part], concurrency: usize,
) -> Result<()> {
    let (tx, rx) = mpsc::channel::<DownloadPartJob>(concurrency);

    let parts_owned: Vec<Part> = parts.to_vec();

    let scheduler_handle = task::spawn(async move {
        for part in &parts_owned {
            let job = DownloadPartJob {
                number: part.number,
                offset: part.offset,
                size: part.size,
            };
            if tx.send(job).await.is_err() {
                break;
            }
        }
    });

    let download_result = run_download_workers(ctx, rx, concurrency).await;
    drop(scheduler_handle.await);
    download_result
}

/// Spawn N download workers, collect results.
async fn run_download_workers(
    ctx: &Arc<DownloadCtx>, mut rx: mpsc::Receiver<DownloadPartJob>, concurrency: usize,
) -> Result<()> {
    let mut set = JoinSet::new();
    let mut channel_open = true;

    loop {
        if set.is_empty() && !channel_open {
            break;
        }

        let has_capacity = channel_open && set.len() < concurrency;

        tokio::select! {
            Some(handle) = set.join_next() => {
                match handle.map_err(|e| Error::Internal(e.to_string()))? {
                    Ok(()) => {},
                    Err(e) => {
                        rx.close();
                        set.abort_all();
                        return Err(e);
                    },
                }
            }
            job = rx.recv(), if has_capacity => {
                match job {
                    Some(job) => {
                        let c = Arc::clone(ctx);
                        set.spawn(async move { download_part(&c, job).await });
                    },
                    None => { channel_open = false; },
                }
            }
            else => break,
        }
    }

    Ok(())
}

/// Write buffer capacity — network chunks (~16 KiB from reqwest) are
/// accumulated here and sent to the writer thread once full.
/// 512 KiB balances syscall amortisation against per-part memory
/// (8 concurrent parts × 512 KiB = 4 MiB channel payload).
const WRITE_BUF_SIZE: usize = 512 * 1024;

/// Download a single Range part and write it to the correct file offset.
///
/// Buffers incoming network chunks into a [`WRITE_BUF_SIZE`] buffer and
/// sends each full buffer to the dedicated writer thread via the channel
/// in [`DownloadCtx::writer_tx`]. No `spawn_blocking` — the async task
/// stays on the tokio runtime for network I/O only.
async fn download_part(ctx: &DownloadCtx, job: DownloadPartJob) -> Result<()> {
    debug_assert!(job.size > 0, "zero-size part would underflow range calculation");
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "offset + size bounded by file size; size > 0 guaranteed by plan_parts"
    )]
    let range_end = job.offset + job.size - 1;

    let uri: Uri =
        format!("{}/{}/{}", ctx.config.endpoint_url(), ctx.bucket, ctx.key.encoded()).parse()?;

    maybe_debug!(
        key = %ctx.key, part = job.number, offset = job.offset, size = job.size,
        "downloading part"
    );

    let tx = ctx.writer_tx.clone();
    let mut write_offset = job.offset;
    // Next byte wanted from the server: everything before it is written or
    // buffered, so a broken connection resumes from here.
    let mut next = job.offset;
    let mut attempt = 0_u32;
    let mut buf = Vec::with_capacity(WRITE_BUF_SIZE);

    while next <= range_end {
        let mut signed =
            build_signed(Method::GET, uri.clone(), Bytes::new(), &ctx.creds, &ctx.config.region)?;
        signed.headers_mut().insert(
            "range",
            format!("bytes={next}-{range_end}")
                .parse()
                .map_err(|e: http::header::InvalidHeaderValue| Error::Internal(e.to_string()))?,
        );
        if let Some(etag) = &ctx.if_match {
            signed.headers_mut().insert(http::header::IF_MATCH, etag.clone());
        }

        let resp = send_with_retry(&ctx.http, signed, &ctx.config.retry).await?;
        let mut stream = resp.bytes_stream();

        let interrupted = loop {
            let chunk = match stream.next().await {
                None => break None,
                Some(Err(e)) => break Some(e),
                Some(Ok(chunk)) => chunk,
            };
            buf.extend_from_slice(&chunk);
            next = next.saturating_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX));

            if buf.len() >= WRITE_BUF_SIZE {
                let data = std::mem::replace(&mut buf, Vec::with_capacity(WRITE_BUF_SIZE));
                let data_len = data.len();
                tx.send(WriteCmd {
                    buf: data,
                    offset: write_offset,
                })
                .map_err(|_| Error::Internal("writer thread exited early".into()))?;
                write_offset =
                    write_offset.saturating_add(u64::try_from(data_len).unwrap_or(u64::MAX));
            }
        };

        match interrupted {
            None if next <= range_end => {
                return Err(Error::Internal(format!(
                    "{}: part {} ended {} bytes short",
                    ctx.key,
                    job.number,
                    range_end.saturating_add(1).saturating_sub(next)
                )));
            },
            None => {},
            Some(e) if attempt < ctx.config.retry.max_retries => {
                maybe_warn!(
                    key = %ctx.key, part = job.number, resume_at = next, attempt, error = %e,
                    "part interrupted, resuming"
                );
                tokio::time::sleep(backoff(&ctx.config.retry, attempt)).await;
                attempt = attempt.saturating_add(1);
            },
            Some(e) => return Err(e.into()),
        }
    }

    // Flush remaining bytes.
    if !buf.is_empty() {
        tx.send(WriteCmd {
            buf,
            offset: write_offset,
        })
        .map_err(|_| Error::Internal("writer thread exited early".into()))?;
    }

    Ok(())
}

/// Bytes streamed before deciding whether to split an adaptive download.
const PROBE_BYTES: u64 = 8 * 1024 * 1024;

/// Smallest file worth downloading adaptively: below this, a probe would
/// leave too little to hand off.
pub(crate) const ADAPTIVE_MIN_SIZE: u64 = 2 * PROBE_BYTES;

/// Aim for each connection's share of a file to take about this long.
const SPLIT_TARGET: Duration = Duration::from_millis(500);

/// Smallest range handed to an extra connection.
const MIN_HELPER_PART: u64 = 8 * 1024 * 1024;

/// Most connections used for one object, the first stream included.
const MAX_STREAMS: u64 = 8;

/// How many connections to use for the rest of an object, given that the
/// first `received` bytes took `elapsed`. `1` means don't split.
pub(crate) fn plan_streams(size: u64, received: u64, elapsed: Duration) -> u64 {
    let remaining = size.saturating_sub(received);
    let elapsed = elapsed.as_nanos().max(1);
    let est = u128::from(remaining)
        .saturating_mul(elapsed)
        .checked_div(u128::from(received))
        .unwrap_or(0);
    let wanted = est.div_ceil(SPLIT_TARGET.as_nanos().max(1));
    let by_size = u128::from(remaining / MIN_HELPER_PART).saturating_add(1);
    let streams = wanted.min(by_size).min(u128::from(MAX_STREAMS)).max(1);
    u64::try_from(streams).unwrap_or(1)
}

/// Download an object of known `size`, splitting it across connections only
/// when the first connection turns out to be slow.
///
/// A plain GET streams the object. After [`PROBE_BYTES`], its throughput
/// predicts how long the rest will take; if that exceeds [`SPLIT_TARGET`],
/// the first stream keeps one share and the remainder is fetched with Range
/// requests on extra connections, each pinned to the same `ETag` with
/// `If-Match`. Extra connections are drawn from `budget` (shared by a whole
/// batch) so many concurrent files can't multiply the connection count
/// without bound. On a fast link nothing is split and this behaves like a
/// single GET.
///
/// Returns the bytes written and the number of connections used.
///
/// # Errors
///
/// Returns an error if any request or file I/O fails, or if the object
/// changes while it is being downloaded.
#[expect(clippy::too_many_arguments, reason = "mirrors download_multipart")]
#[expect(clippy::too_many_lines, reason = "one streaming loop; splitting it obscures the flow")]
pub(crate) async fn download_adaptive(
    http: &HttpClient, config: &Config, creds: &Credentials, bucket: &str, key: &ObjectKey,
    dest: &Path, size: u64, budget: &Semaphore,
) -> Result<(u64, u32)> {
    let uri: Uri = format!("{}/{bucket}/{}", config.endpoint_url(), key.encoded()).parse()?;
    let req = build_signed(Method::GET, uri, Bytes::new(), creds, &config.region)?;
    let resp = send_with_retry(http, req, &config.retry).await?;
    let etag = resp.headers().get(http::header::ETAG).cloned();

    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent).await?;
    }
    let tmp = task::spawn_blocking({
        let dir = dest.parent().unwrap_or_else(|| Path::new(".")).to_owned();
        move || tempfile::NamedTempFile::new_in(dir)
    })
    .await
    .map_err(|e| Error::Internal(e.to_string()))??;

    let fd = Arc::new(tmp.reopen()?);
    fd.set_len(size)?;
    let writer = DiskWriter::spawn(Arc::clone(&fd), 16);
    let ctx = Arc::new(DownloadCtx {
        bucket: bucket.to_owned(),
        config: config.clone(),
        creds: creds.clone(),
        http: http.clone(),
        if_match: etag,
        key: key.clone(),
        writer_tx: writer.tx.clone(),
    });

    let mut helpers: JoinSet<Result<()>> = JoinSet::new();
    let mut helper_count = 0_u32;
    let mut permit = None;
    let mut limit = size;
    let mut received = 0_u64;
    let mut written = 0_u64;
    let mut started: Option<Instant> = None;
    let mut decided = false;
    let mut buf = Vec::with_capacity(WRITE_BUF_SIZE);
    let mut stream = resp.bytes_stream();

    let mut interrupted = None;
    let streamed: Result<()> = async {
        while received < limit {
            let Some(chunk) = stream.next().await else { break };
            let chunk = match chunk {
                Ok(chunk) => chunk,
                Err(e) => {
                    interrupted = Some(e);
                    break;
                },
            };
            let t0 = *started.get_or_insert_with(Instant::now);

            let room = usize::try_from(limit.saturating_sub(received)).unwrap_or(usize::MAX);
            let take = chunk.get(..room.min(chunk.len())).unwrap_or_default();
            buf.extend_from_slice(take);
            received = received.saturating_add(u64::try_from(take.len()).unwrap_or(u64::MAX));

            if buf.len() >= WRITE_BUF_SIZE {
                let data = std::mem::replace(&mut buf, Vec::with_capacity(WRITE_BUF_SIZE));
                ctx.writer_tx
                    .send(WriteCmd {
                        buf: data,
                        offset: written,
                    })
                    .map_err(|_| Error::Internal("writer thread exited early".into()))?;
                written = received;
            }

            if !decided && received >= PROBE_BYTES {
                decided = true;
                let wanted = plan_streams(size, received, t0.elapsed());
                // Take as many extra connections as the batch budget allows.
                let mut extra = wanted.saturating_sub(1);
                while extra > 0 {
                    if let Ok(p) = budget.try_acquire_many(u32::try_from(extra).unwrap_or(1)) {
                        permit = Some(p);
                        break;
                    }
                    extra = extra.saturating_sub(1);
                }
                if extra > 0 {
                    let remaining = size.saturating_sub(received);
                    let share = remaining.checked_div(extra.saturating_add(1)).unwrap_or(0);
                    limit = received.saturating_add(share);
                    let helper_total = size.saturating_sub(limit);
                    let part = helper_total.div_ceil(extra);
                    maybe_debug!(key = %key, streams = extra.saturating_add(1), at = limit, "splitting download");
                    let mut offset = limit;
                    let mut number = 2_u32;
                    while offset < size {
                        let job = DownloadPartJob {
                            number,
                            offset,
                            size: part.min(size.saturating_sub(offset)),
                        };
                        offset = offset.saturating_add(job.size);
                        number = number.saturating_add(1);
                        let c = Arc::clone(&ctx);
                        helpers.spawn(async move { download_part(&c, job).await });
                        helper_count = helper_count.saturating_add(1);
                    }
                }
            }
        }
        if !buf.is_empty() {
            ctx.writer_tx
                .send(WriteCmd {
                    buf: std::mem::take(&mut buf),
                    offset: written,
                })
                .map_err(|_| Error::Internal("writer thread exited early".into()))?;
        }
        Ok(())
    }
    .await;
    // Dropping a partly read body closes that connection, which is the
    // price of handing the tail to other connections.
    drop(stream);

    // A broken first stream hands the rest of its share to a ranged part,
    // which resumes on its own if it breaks too.
    if let (Some(_), Ok(())) = (&interrupted, &streamed)
        && received < limit
    {
        maybe_warn!(key = %key, resume_at = received, error = ?interrupted, "stream interrupted, resuming");
        let job = DownloadPartJob {
            number: helper_count.saturating_add(2),
            offset: received,
            size: limit.saturating_sub(received),
        };
        let c = Arc::clone(&ctx);
        helpers.spawn(async move { download_part(&c, job).await });
        helper_count = helper_count.saturating_add(1);
        limit = received;
    }

    let mut result = streamed;
    while let Some(joined) = helpers.join_next().await {
        let outcome = joined.map_err(|e| Error::Internal(e.to_string())).and_then(|r| r);
        if let Err(e) = outcome
            && result.is_ok()
        {
            helpers.abort_all();
            result = Err(e);
        }
    }
    drop(permit);
    let split = limit < size;
    drop(ctx);

    if result.is_ok() && split && received < limit {
        result = Err(Error::Internal(format!("{key} changed size while downloading")));
    }
    writer.finish_with(result)?;

    // Unsplit downloads take whatever the GET returned, like a plain GET.
    let total = if split { size } else { received };
    if total != size {
        fd.set_len(total)?;
    }
    drop(fd);

    let dest = dest.to_owned();
    task::spawn_blocking(move || tmp.persist(dest).map_err(|e| Error::Io(e.error)))
        .await
        .map_err(|e| Error::Internal(e.to_string()))??;

    let parts = if split { helper_count.saturating_add(1) } else { 1 };
    maybe_info!(key = %key, size = total, parts, "adaptive download complete");
    Ok((total, parts))
}

#[cfg(test)]
mod tests {
    #[test]
    fn tempfile_creates_in_target_dir() {
        let dir = std::env::temp_dir();
        let tmp = tempfile::NamedTempFile::new_in(&dir).unwrap();
        assert!(tmp.path().starts_with(&dir));
    }

    #[test]
    fn writer_failure_reports_the_io_error() {
        // A read-only fd makes every write fail, like a full disk would.
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let fd = std::fs::File::open(tmp.path()).unwrap();
        let writer = super::DiskWriter::spawn(std::sync::Arc::new(fd), 1);
        writer
            .tx
            .send(super::WriteCmd {
                buf: vec![1; 16],
                offset: 0,
            })
            .unwrap();
        let seen_by_tasks = Err(super::Error::Internal("writer thread exited early".into()));
        let err = writer.finish_with(seen_by_tasks).unwrap_err();
        assert!(matches!(err, super::Error::Io(_)), "{err:?}");
    }
}
