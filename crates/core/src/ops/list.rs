//! Paginated object listing — `ListObjectsV2`.
//!
//! Listing is inherently serial in S3: each page of ~1000 keys hands back a
//! continuation token for the next. For large prefixes that makes listing
//! the bottleneck. s3f parallelizes it two ways, picking per directory:
//!
//! - **Hierarchical layouts** (`year=…/month=…/…`) are walked with `delimiter=/`, fanning out to
//!   every subdirectory at once.
//! - **Flat key spaces** (counters, hashes, very wide directories) are cut into ranges at
//!   boundaries estimated from the keys already seen, and each range is listed concurrently with
//!   `start-after`. Ranges that turn out dense keep splitting as workers free up.
//!
//! Neither needs to know the layout up front, and neither affects which
//! keys are returned — only how fast.

use std::{collections::VecDeque, fmt};

use bytes::Bytes;
use tokio::{
    sync::mpsc,
    task::{JoinHandle, JoinSet},
};

use crate::{
    auth::Credentials,
    client::S3Client,
    config::Config,
    error::{Error, Result},
    http::{
        HttpClient,
        request::build_signed,
        response::{self, ListBucketResult, ListObject},
        retry::send_with_retry,
    },
    ops::keyspace,
    trace::{maybe_debug, maybe_warn},
};

/// Default number of concurrent `ListObjectsV2` requests.
const DEFAULT_LIST_CONCURRENCY: usize = 32;

/// Pages each extrapolated range is expected to hold. Each range ends with
/// one page that reads past its end, so wider ranges waste fewer requests;
/// dense ranges still split further once workers free up.
const EXTRAPOLATION_STRIDE: u128 = 4;

/// Smallest range, in estimated pages, worth splitting off a bounded range.
const MIN_SPLIT_PAGES: u128 = 2;

/// Metadata for a single S3 object.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ObjectInfo {
    /// Object key.
    pub key: String,
    /// Object size in bytes.
    pub size: u64,
    /// `ETag` of the object.
    pub etag: String,
    /// Last modified timestamp (ISO 8601).
    pub last_modified: String,
}

impl From<ListObject> for ObjectInfo {
    #[inline]
    fn from(o: ListObject) -> Self {
        Self {
            key: o.key,
            size: o.size,
            etag: o.etag,
            last_modified: o.last_modified,
        }
    }
}

/// A single page of listing results.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ListPage {
    /// Objects in this page.
    pub objects: Vec<ObjectInfo>,
    /// Common prefixes (when a delimiter is used).
    pub common_prefixes: Vec<String>,
}

/// What to list.
#[non_exhaustive]
pub struct ListRequest {
    /// Target S3 bucket.
    pub bucket: String,
    /// Maximum concurrent `ListObjectsV2` requests. `1` lists serially in
    /// key order; anything higher partitions the key space (see the module
    /// docs). Ignored when a delimiter is set.
    pub concurrency: usize,
    /// Key prefix to filter by.
    pub prefix: String,
    /// Delimiter for grouping keys (e.g. `"/"`).
    pub delimiter: Option<String>,
}

impl fmt::Debug for ListRequest {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ListRequest")
            .field("bucket", &self.bucket)
            .field("concurrency", &self.concurrency)
            .field("prefix", &self.prefix)
            .field("delimiter", &self.delimiter)
            .finish()
    }
}

impl ListRequest {
    /// Create a new list request.
    #[inline]
    #[must_use]
    pub fn new(bucket: impl Into<String>, prefix: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            concurrency: DEFAULT_LIST_CONCURRENCY,
            prefix: prefix.into(),
            delimiter: None,
        }
    }

    /// Set the maximum number of concurrent list requests (`1` = serial).
    #[inline]
    #[must_use]
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency.max(1);
        self
    }

    /// Set a delimiter for directory-style grouping.
    #[inline]
    #[must_use]
    pub fn with_delimiter(mut self, delimiter: impl Into<String>) -> Self {
        self.delimiter = Some(delimiter.into());
        self
    }
}

/// Paginated S3 object listing.
///
/// Call [`next_page`](Self::next_page) in a loop to consume pages of up to
/// 1000 keys each. Owns copies of client internals — safe to hold across
/// await boundaries and across FFI.
///
/// With [`ListRequest::concurrency`] above 1 (the default), pages come from
/// several key ranges at once and arrive in no particular order; keys
/// within a page are still sorted. [`collect_all`](Self::collect_all)
/// always returns keys in sorted order.
///
/// # Examples
///
/// ```no_run
/// # use s3f::{Config, S3Client, ListRequest, auth::CredentialSource};
/// # async fn example() -> s3f::error::Result<()> {
/// let client = S3Client::new(Config::new("us-east-1", CredentialSource::Env)).await?;
/// let mut paginator = client.list(ListRequest::new("my-bucket", "data/"));
/// while let Some(page) = paginator.next_page().await? {
///     for obj in &page.objects {
///         println!("{} ({} bytes)", obj.key, obj.size);
///     }
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct ListPaginator {
    concurrency: usize,
    continuation_token: Option<String>,
    done: bool,
    fetcher: PageFetcher,
    /// Receiver and coordinator handle once partitioned listing has started.
    partitioned: Option<Partitioned>,
}

/// Background state for a partitioned listing.
#[derive(Debug)]
struct Partitioned {
    handle: JoinHandle<()>,
    rx: mpsc::Receiver<Result<ListPage>>,
}

impl Drop for Partitioned {
    #[inline]
    fn drop(&mut self) {
        self.handle.abort();
    }
}

impl ListPaginator {
    /// Collect all remaining pages into a single `Vec`, sorted by key.
    ///
    /// Convenience for small-to-medium listings. For millions of keys,
    /// prefer iterating with [`next_page`](Self::next_page).
    ///
    /// # Errors
    ///
    /// Returns an error if any page fetch fails.
    pub async fn collect_all(&mut self) -> Result<Vec<ObjectInfo>> {
        let mut all = Vec::new();
        while let Some(page) = self.next_page().await? {
            all.extend(page.objects);
        }
        if self.is_partitioned() {
            all.sort_unstable_by(|a, b| a.key.cmp(&b.key));
        }
        Ok(all)
    }

    /// Fetch the next page of results (up to 1000 keys).
    ///
    /// Returns `None` when the listing is exhausted.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP request or XML parsing fails.
    pub async fn next_page(&mut self) -> Result<Option<ListPage>> {
        if self.done {
            return Ok(None);
        }
        if self.is_partitioned() {
            return self.next_partitioned_page().await;
        }

        let parsed = self
            .fetcher
            .fetch(
                &self.fetcher.prefix,
                self.fetcher.delimiter.as_deref(),
                None,
                self.continuation_token.as_deref(),
            )
            .await?;

        if parsed.is_truncated {
            self.continuation_token = parsed.next_continuation_token;
        } else {
            self.done = true;
            self.continuation_token = None;
        }

        let objects: Vec<ObjectInfo> = parsed.contents.into_iter().map(ObjectInfo::from).collect();
        let common_prefixes: Vec<String> =
            parsed.common_prefixes.into_iter().map(|cp| cp.prefix).collect();

        maybe_debug!(
            keys = objects.len(),
            prefixes = common_prefixes.len(),
            truncated = !self.done,
            "page received"
        );

        if objects.is_empty() && common_prefixes.is_empty() && self.done {
            return Ok(None);
        }

        Ok(Some(ListPage {
            objects,
            common_prefixes,
        }))
    }

    /// Whether this listing runs as concurrent key ranges.
    const fn is_partitioned(&self) -> bool {
        self.concurrency > 1 && self.fetcher.delimiter.is_none()
    }

    async fn next_partitioned_page(&mut self) -> Result<Option<ListPage>> {
        let state = self.partitioned.get_or_insert_with(|| {
            let capacity = self.concurrency.saturating_mul(2);
            let (tx, rx) = mpsc::channel(capacity);
            let partitioner = Partitioner {
                can_split: true,
                concurrency: self.concurrency,
                fetcher: self.fetcher.clone(),
                queue: VecDeque::from([Task::Dir {
                    prefix: self.fetcher.prefix.clone(),
                    token: None,
                }]),
            };
            let handle = tokio::spawn(partitioner.run(tx));
            Partitioned {
                handle,
                rx,
            }
        });

        match state.rx.recv().await {
            Some(Ok(page)) => Ok(Some(page)),
            Some(Err(e)) => {
                self.done = true;
                self.partitioned = None;
                Err(e)
            },
            None => {
                self.done = true;
                self.partitioned = None;
                Ok(None)
            },
        }
    }
}

/// Everything needed to issue one `ListObjectsV2` request.
#[derive(Debug, Clone)]
struct PageFetcher {
    bucket: String,
    config: Config,
    creds: Credentials,
    /// Caller's delimiter (serial listing only).
    delimiter: Option<String>,
    http: HttpClient,
    /// Caller's prefix.
    prefix: String,
}

impl PageFetcher {
    /// Fetch a single page under `prefix`, starting after `start_after` or
    /// resuming from `token` (the token wins when both are given, as in S3).
    async fn fetch(
        &self, prefix: &str, delimiter: Option<&str>, start_after: Option<&str>,
        token: Option<&str>,
    ) -> Result<ListBucketResult> {
        let encode = |v: &str| {
            percent_encoding::utf8_percent_encode(v, percent_encoding::NON_ALPHANUMERIC).to_string()
        };

        let mut query = format!(
            "{}/{bucket}?list-type=2&prefix={prefix}",
            self.config.endpoint_url(),
            bucket = self.bucket,
            prefix = encode(prefix),
        );

        if let Some(delim) = delimiter {
            query.push_str("&delimiter=");
            query.push_str(&encode(delim));
        }

        if let Some(token) = token {
            query.push_str("&continuation-token=");
            query.push_str(&encode(token));
        } else if let Some(start_after) = start_after {
            query.push_str("&start-after=");
            query.push_str(&encode(start_after));
        }

        let uri: http::Uri = query.parse()?;
        maybe_debug!(%uri, "listing page");

        let req =
            build_signed(http::Method::GET, uri, Bytes::new(), &self.creds, &self.config.region)?;
        let resp = send_with_retry(&self.http, req, &self.config.retry).await?;
        let body = resp.text().await?;

        response::parse_list_objects(&body)
    }
}

/// One unit of listing work.
#[derive(Debug, Clone)]
enum Task {
    /// List a "directory" with `delimiter=/`, fanning out to its children.
    Dir {
        /// Directory prefix.
        prefix: String,
        /// Continuation token once started.
        token: Option<String>,
    },
    /// List a slice of a flat key space, splitting it when there is room.
    Range(KeyRange),
}

/// A slice of the keys under `prefix`: keys `k` with `start_after < k <= end`.
#[derive(Debug, Clone)]
struct KeyRange {
    /// Prefix every key in the range shares.
    prefix: String,
    /// Exclusive lower bound; `None` starts at the beginning of the prefix.
    start_after: Option<String>,
    /// Inclusive upper bound; `None` runs to the end of the prefix.
    end: Option<String>,
    /// Continuation token once the range has been started.
    token: Option<String>,
}

impl KeyRange {
    fn contains(&self, key: &str) -> bool {
        self.start_after.as_deref().is_none_or(|s| key > s)
            && self.end.as_deref().is_none_or(|e| key <= e)
    }
}

/// Split points for the rest of a range, after a page `first..=last`:
/// extrapolated for an open-ended range, interpolated up to `end` for a
/// bounded one.
fn split_points(
    first: &str, last: &str, end: Option<&str>, idle: usize, floor: usize,
    alphabet: &keyspace::Alphabet,
) -> Vec<String> {
    if idle == 0 || first >= last {
        return Vec::new();
    }
    end.map_or_else(
        || keyspace::extrapolate(first, last, idle, EXTRAPOLATION_STRIDE, floor, alphabet),
        |end| keyspace::interpolate(first, last, end, idle, MIN_SPLIT_PAGES, alphabet),
    )
}

/// Coordinates a partitioned listing.
///
/// Work starts as one directory task for the caller's prefix. Directory
/// tasks list with `delimiter=/` and fan out to their subdirectories, which
/// discovers hierarchical layouts (`year=…/month=…/…`) one level per round
/// trip. A directory whose entries turn out flat continues as a key range,
/// and key ranges split themselves into idle slots based on how keys are
/// spread in each page they read (see [`keyspace`]).
///
/// Up to `concurrency` page fetches run at once. Ranges are filtered
/// client-side, so results stay correct even against a backend that
/// ignores `start-after`; splitting is switched off once that is detected,
/// since it would only cause duplicate reads.
struct Partitioner {
    can_split: bool,
    concurrency: usize,
    fetcher: PageFetcher,
    queue: VecDeque<Task>,
}

/// Directories with more subdirectories than this on their first page are
/// listed as one flat key range instead of one task per child.
const MAX_DIR_FANOUT: usize = 128;

impl Partitioner {
    async fn run(mut self, tx: mpsc::Sender<Result<ListPage>>) {
        let mut in_flight = JoinSet::new();

        loop {
            while in_flight.len() < self.concurrency {
                let Some(task) = self.queue.pop_front() else { break };
                let f = self.fetcher.clone();
                in_flight.spawn(async move {
                    let page = match &task {
                        Task::Dir {
                            prefix,
                            token,
                        } => f.fetch(prefix, Some("/"), None, token.as_deref()).await,
                        Task::Range(r) => {
                            f.fetch(&r.prefix, None, r.start_after.as_deref(), r.token.as_deref())
                                .await
                        },
                    };
                    (task, page)
                });
            }

            let Some(joined) = in_flight.join_next().await else { break };
            let busy = in_flight.len().saturating_add(self.queue.len());
            let objects = match joined {
                Ok((
                    Task::Dir {
                        prefix,
                        token,
                    },
                    Ok(page),
                )) => self.on_dir_page(prefix, token.is_none(), page, busy),
                Ok((Task::Range(range), Ok(page))) => Ok(self.on_range_page(range, page, busy)),
                Ok((_, Err(e))) => Err(e),
                Err(e) => Err(Error::Internal(e.to_string())),
            };

            match objects {
                Ok(objects) if objects.is_empty() => {},
                Ok(objects) => {
                    let page = ListPage {
                        objects,
                        common_prefixes: Vec::new(),
                    };
                    if tx.send(Ok(page)).await.is_err() {
                        return;
                    }
                },
                Err(e) => {
                    drop(tx.send(Err(e)).await);
                    return;
                },
            }
        }
    }

    /// Handle a page of a directory listing; returns the objects to emit.
    fn on_dir_page(
        &mut self, prefix: String, first_page: bool, mut page: ListBucketResult, busy: usize,
    ) -> Result<Vec<ObjectInfo>> {
        if first_page && page.common_prefixes.len() > MAX_DIR_FANOUT {
            // Too wide to expand child by child: list it as one key range.
            maybe_debug!(%prefix, children = page.common_prefixes.len(), "wide directory");
            self.queue.push_front(Task::Range(KeyRange {
                prefix,
                start_after: None,
                end: None,
                token: None,
            }));
            return Ok(Vec::new());
        }

        let flat = page.common_prefixes.is_empty();
        for child in core::mem::take(&mut page.common_prefixes) {
            self.queue.push_back(Task::Dir {
                prefix: child.prefix,
                token: None,
            });
        }

        if !page.is_truncated {
            return Ok(page.contents.into_iter().map(ObjectInfo::from).collect());
        }

        if flat && !page.contents.is_empty() {
            // No subdirectories here: continue as a key range, which can
            // split. The page already read counts as its first page.
            let range = KeyRange {
                prefix,
                start_after: None,
                end: None,
                token: None,
            };
            let page = ListBucketResult {
                next_continuation_token: None,
                ..page
            };
            return Ok(self.on_range_page(range, page, busy));
        }

        let token = page.next_continuation_token.ok_or_else(|| {
            Error::Internal("truncated listing without a continuation token".into())
        })?;
        self.queue.push_front(Task::Dir {
            prefix,
            token: Some(token),
        });
        Ok(page.contents.into_iter().map(ObjectInfo::from).collect())
    }

    /// Handle a page of a key range; returns the objects to emit.
    fn on_range_page(
        &mut self, mut range: KeyRange, page: ListBucketResult, busy: usize,
    ) -> Vec<ObjectInfo> {
        let first_key = page.contents.first().map(|o| o.key.clone());
        let last_key = page.contents.last().map(|o| o.key.clone());

        if range.token.is_none()
            && let (Some(start), Some(first)) = (&range.start_after, &first_key)
            && first <= start
        {
            maybe_warn!("backend ignores start-after; no longer splitting key ranges");
            self.can_split = false;
        }

        // A page that reads past this range's end has already covered the
        // start of the next range. If that range hasn't started yet, take
        // it over instead of throwing those keys away and re-reading them.
        while let (Some(end), Some(last)) = (&range.end, &last_key)
            && last > end
            && let Some(pos) = self.queue.iter().position(|t| {
                matches!(t, Task::Range(r)
                    if r.token.is_none() && r.prefix == range.prefix
                        && r.start_after.as_ref() == Some(end))
            })
            && let Some(Task::Range(absorbed)) = self.queue.remove(pos)
        {
            range.end = absorbed.end;
        }

        let past_end =
            range.end.as_deref().is_some_and(|e| last_key.as_deref().is_some_and(|k| k > e));
        let floor = range.prefix.len();
        let alphabet = (self.can_split && page.is_truncated && !past_end).then(|| {
            keyspace::Alphabet::from_keys(page.contents.iter().map(|o| o.key.as_str()), floor)
        });
        let objects: Vec<ObjectInfo> = page
            .contents
            .into_iter()
            .filter(|o| range.contains(&o.key))
            .map(ObjectInfo::from)
            .collect();

        let Some(last) = last_key.filter(|_| page.is_truncated && !past_end) else {
            return objects;
        };

        // Resume with the token when the backend gave one; otherwise fall
        // back to start-after on the last key we saw.
        let mut next = KeyRange {
            token: page.next_continuation_token,
            ..range
        };
        if next.token.is_none() {
            next.start_after = Some(last.clone());
        }

        let idle = self.concurrency.saturating_sub(busy.saturating_add(1));
        if let (Some(alphabet), Some(first)) = (alphabet, first_key.as_deref()) {
            let points = split_points(first, &last, next.end.as_deref(), idle, floor, &alphabet);
            self.push_splits(&mut next, &points);
        }
        self.queue.push_front(Task::Range(next));
        objects
    }

    /// Cut `range` at `points`: it keeps everything up to the first point,
    /// and a new range is queued for each following piece.
    fn push_splits(&mut self, range: &mut KeyRange, points: &[String]) {
        let Some(first_point) = points.first() else { return };
        maybe_debug!(prefix = %range.prefix, splits = points.len(), "splitting key range");
        let tail_end = range.end.replace(first_point.clone());
        let mut lo = first_point.clone();
        for hi in points.iter().skip(1) {
            self.queue.push_back(Task::Range(KeyRange {
                prefix: range.prefix.clone(),
                start_after: Some(core::mem::replace(&mut lo, hi.clone())),
                end: Some(hi.clone()),
                token: None,
            }));
        }
        self.queue.push_back(Task::Range(KeyRange {
            prefix: range.prefix.clone(),
            start_after: Some(lo),
            end: tail_end,
            token: None,
        }));
    }
}

#[expect(clippy::multiple_inherent_impl, reason = "ops extend S3Client from their own modules")]
impl S3Client {
    /// Create a paginator for listing objects under a prefix.
    ///
    /// No network call is made until [`ListPaginator::next_page`] is called.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use s3f::{Config, S3Client, ListRequest, auth::CredentialSource};
    /// # async fn example() -> s3f::error::Result<()> {
    /// let client = S3Client::new(Config::new("us-east-1", CredentialSource::Env)).await?;
    /// let mut paginator = client.list(ListRequest::new("my-bucket", "data/"));
    /// while let Some(page) = paginator.next_page().await? {
    ///     for obj in &page.objects {
    ///         println!("{} ({} bytes)", obj.key, obj.size);
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    #[inline]
    #[must_use]
    pub fn list(&self, req: ListRequest) -> ListPaginator {
        ListPaginator {
            concurrency: req.concurrency.max(1),
            continuation_token: None,
            done: false,
            fetcher: PageFetcher {
                bucket: req.bucket,
                config: self.config.clone(),
                creds: self.creds.clone(),
                delimiter: req.delimiter,
                http: self.http.clone(),
                prefix: req.prefix,
            },
            partitioned: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use core::sync::atomic::{AtomicU32, Ordering};

    use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

    use super::*;
    use crate::{Config, auth::CredentialSource, http::response::ListObject};

    async fn test_client(server: &MockServer) -> S3Client {
        let config = Config::with_endpoint(
            "us-east-1",
            CredentialSource::Static {
                access_key: "AKID".into(),
                secret_key: "SECRET".into(),
            },
            server.uri(),
        );
        S3Client::new(config).await.unwrap()
    }

    fn single_page_xml(keys: &[(&str, u64)]) -> String {
        use core::fmt::Write as _;

        let mut xml = String::from(
            r#"<?xml version="1.0" encoding="UTF-8"?>
            <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Name>bucket</Name><Prefix></Prefix>
            <IsTruncated>false</IsTruncated>"#,
        );
        for (key, size) in keys {
            write!(
                xml,
                r#"<Contents><Key>{key}</Key><Size>{size}</Size>
                <ETag>"etag"</ETag><LastModified>2024-01-01T00:00:00.000Z</LastModified>
                </Contents>"#,
            )
            .unwrap();
        }
        xml.push_str("</ListBucketResult>");
        xml
    }

    #[tokio::test]
    async fn list_single_page() {
        let server = MockServer::start().await;
        let body = single_page_xml(&[("file1.txt", 100), ("file2.txt", 200)]);

        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_string(&body))
            .expect(1)
            .mount(&server)
            .await;

        let client = test_client(&server).await;
        let mut paginator = client.list(ListRequest::new("bucket", ""));

        let page = paginator.next_page().await.unwrap().unwrap();
        assert_eq!(page.objects.len(), 2);
        assert_eq!(page.objects[0].key, "file1.txt");
        assert_eq!(page.objects[0].size, 100);
        assert_eq!(page.objects[1].key, "file2.txt");
        assert_eq!(page.objects[1].size, 200);

        // Second call returns None
        assert!(paginator.next_page().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn list_empty_bucket() {
        let server = MockServer::start().await;
        let body = r#"<?xml version="1.0" encoding="UTF-8"?>
            <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Name>bucket</Name><Prefix></Prefix>
            <IsTruncated>false</IsTruncated>
            </ListBucketResult>"#;

        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_string(body))
            .expect(1)
            .mount(&server)
            .await;

        let client = test_client(&server).await;
        let mut paginator = client.list(ListRequest::new("bucket", ""));

        assert!(paginator.next_page().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn list_multi_page_pagination() {
        let server = MockServer::start().await;
        let call_count = AtomicU32::new(0);

        Mock::given(method("GET"))
            .respond_with(move |_: &wiremock::Request| {
                let n = call_count.fetch_add(1, Ordering::Relaxed);
                if n == 0 {
                    ResponseTemplate::new(200).set_body_string(
                        r#"<?xml version="1.0" encoding="UTF-8"?>
                        <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                        <Name>bucket</Name><Prefix></Prefix>
                        <IsTruncated>true</IsTruncated>
                        <NextContinuationToken>token123</NextContinuationToken>
                        <Contents><Key>page1.txt</Key><Size>10</Size>
                        <ETag>"e1"</ETag><LastModified>2024-01-01T00:00:00.000Z</LastModified>
                        </Contents>
                        </ListBucketResult>"#,
                    )
                } else {
                    ResponseTemplate::new(200).set_body_string(
                        r#"<?xml version="1.0" encoding="UTF-8"?>
                        <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                        <Name>bucket</Name><Prefix></Prefix>
                        <IsTruncated>false</IsTruncated>
                        <Contents><Key>page2.txt</Key><Size>20</Size>
                        <ETag>"e2"</ETag><LastModified>2024-01-02T00:00:00.000Z</LastModified>
                        </Contents>
                        </ListBucketResult>"#,
                    )
                }
            })
            .expect(2)
            .mount(&server)
            .await;

        let client = test_client(&server).await;
        let mut paginator = client.list(ListRequest::new("bucket", ""));

        let page1 = paginator.next_page().await.unwrap().unwrap();
        assert_eq!(page1.objects.len(), 1);
        assert_eq!(page1.objects[0].key, "page1.txt");

        let page2 = paginator.next_page().await.unwrap().unwrap();
        assert_eq!(page2.objects.len(), 1);
        assert_eq!(page2.objects[0].key, "page2.txt");

        assert!(paginator.next_page().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn collect_all_aggregates_pages() {
        let server = MockServer::start().await;
        let call_count = AtomicU32::new(0);

        Mock::given(method("GET"))
            .respond_with(move |_: &wiremock::Request| {
                let n = call_count.fetch_add(1, Ordering::Relaxed);
                if n == 0 {
                    ResponseTemplate::new(200).set_body_string(
                        r#"<?xml version="1.0" encoding="UTF-8"?>
                        <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                        <Name>b</Name><Prefix></Prefix>
                        <IsTruncated>true</IsTruncated>
                        <NextContinuationToken>t</NextContinuationToken>
                        <Contents><Key>a.txt</Key><Size>1</Size>
                        <ETag>"e"</ETag><LastModified>2024-01-01T00:00:00.000Z</LastModified>
                        </Contents>
                        </ListBucketResult>"#,
                    )
                } else {
                    ResponseTemplate::new(200).set_body_string(
                        r#"<?xml version="1.0" encoding="UTF-8"?>
                        <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                        <Name>b</Name><Prefix></Prefix>
                        <IsTruncated>false</IsTruncated>
                        <Contents><Key>b.txt</Key><Size>2</Size>
                        <ETag>"e"</ETag><LastModified>2024-01-02T00:00:00.000Z</LastModified>
                        </Contents>
                        </ListBucketResult>"#,
                    )
                }
            })
            .expect(2)
            .mount(&server)
            .await;

        let client = test_client(&server).await;
        let mut paginator = client.list(ListRequest::new("b", ""));
        let all = paginator.collect_all().await.unwrap();

        assert_eq!(all.len(), 2);
        assert_eq!(all[0].key, "a.txt");
        assert_eq!(all[1].key, "b.txt");
    }

    #[tokio::test]
    async fn list_request_builder() {
        let req = ListRequest::new("bucket", "prefix/").with_delimiter("/");
        assert_eq!(req.bucket, "bucket");
        assert_eq!(req.prefix, "prefix/");
        assert_eq!(req.delimiter.as_deref(), Some("/"));
    }

    #[test]
    fn object_info_from_list_object() {
        let lo = ListObject {
            key: "k".into(),
            size: 42,
            etag: "\"e\"".into(),
            last_modified: "2024-01-01".into(),
        };
        let info = ObjectInfo::from(lo);
        assert_eq!(info.key, "k");
        assert_eq!(info.size, 42);
        assert_eq!(info.etag, "\"e\"");
    }

    use crate::test_support::FakeS3;

    /// List `keys` through the partitioned path and check the result is
    /// exactly the expected, sorted, de-duplicated set under `prefix`.
    async fn assert_lists_exactly(
        keys: Vec<String>, prefix: &str,
    ) -> std::sync::Arc<std::sync::Mutex<crate::test_support::Stats>> {
        let mut expected: Vec<String> =
            keys.iter().filter(|k| k.starts_with(prefix)).cloned().collect();
        expected.sort();
        expected.dedup();

        let mut fake = FakeS3::with_keys(keys);
        fake.page_size = 100;
        fake.latency = core::time::Duration::from_millis(5);
        let (client, stats) = fake.start().await;
        let got: Vec<String> = client
            .list(ListRequest::new("b", prefix).with_concurrency(16))
            .collect_all()
            .await
            .unwrap()
            .into_iter()
            .map(|o| o.key)
            .collect();

        assert_eq!(got.len(), expected.len(), "wrong number of keys");
        assert_eq!(got, expected);
        stats
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn partitioned_sequential_keys() {
        let keys = (0..20_000).map(|i| format!("logs/{i:08}.json")).collect();
        let stats = assert_lists_exactly(keys, "logs/").await;
        let s = stats.lock().unwrap().clone();
        assert!(s.peak_active >= 8, "listing did not run in parallel: {s:?}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn partitioned_hex_keys() {
        // Content-addressed layout: uniformly spread hashes.
        let keys = (0_u64..20_000)
            .map(|i| format!("{:016x}", i.wrapping_mul(0x9E37_79B9_7F4A_7C15)))
            .collect();
        let stats = assert_lists_exactly(keys, "").await;
        let s = stats.lock().unwrap().clone();
        assert!(s.peak_active >= 8, "listing did not run in parallel: {s:?}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn partitioned_skewed_hierarchy() {
        // A few huge "directories" next to many tiny ones.
        let mut keys = Vec::new();
        for d in 0..50 {
            let files = if d % 10 == 0 { 3000 } else { 5 };
            keys.extend((0..files).map(|f| format!("data/dir-{d:03}/part-{f:05}.parquet")));
        }
        assert_lists_exactly(keys, "data/").await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn partitioned_unicode_and_symbols() {
        let mut keys = Vec::new();
        for i in 0..3000 {
            keys.push(format!("données/résumé-{i}.pdf"));
            keys.push(format!("data & stuff/<{i}>.txt"));
            keys.push(format!("日本/{i}.txt"));
            keys.push(format!("z~{i}"));
        }
        assert_lists_exactly(keys, "").await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn partitioned_respects_prefix() {
        let mut keys: Vec<String> = (0..5000).map(|i| format!("a/{i}")).collect();
        keys.extend((0..5000).map(|i| format!("b/{i}")));
        keys.extend((0..5000).map(|i| format!("ab/{i}")));
        assert_lists_exactly(keys, "a/").await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn partitioned_small_and_empty_listings() {
        let stats = assert_lists_exactly(vec!["only".into()], "").await;
        assert_eq!(stats.lock().unwrap().requests, 1);
        assert_lists_exactly(Vec::new(), "").await;
        assert_lists_exactly((0..100).map(|i| format!("{i:03}")).collect(), "").await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn partitioned_survives_backend_ignoring_start_after() {
        let keys: Vec<String> = (0..5000).map(|i| format!("k/{i:06}")).collect();
        let mut fake = FakeS3::with_keys(keys.clone());
        fake.page_size = 100;
        fake.honour_start_after = false;
        let (client, _) = fake.start().await;

        let got: Vec<String> = client
            .list(ListRequest::new("b", "k/"))
            .collect_all()
            .await
            .unwrap()
            .into_iter()
            .map(|o| o.key)
            .collect();
        assert_eq!(got, keys);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn serial_listing_keeps_page_order() {
        let keys: Vec<String> = (0..1000).map(|i| format!("{i:04}")).collect();
        let mut fake = FakeS3::with_keys(keys.clone());
        fake.page_size = 100;
        let (client, stats) = fake.start().await;

        let mut paginator = client.list(ListRequest::new("b", "").with_concurrency(1));
        let mut got = Vec::new();
        while let Some(page) = paginator.next_page().await.unwrap() {
            got.extend(page.objects.into_iter().map(|o| o.key));
        }
        assert_eq!(got, keys);
        assert_eq!(stats.lock().unwrap().requests, 10);
    }

    #[tokio::test]
    async fn partitioned_page_errors_propagate() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(403).set_body_string(
                    "<Error><Code>AccessDenied</Code><Message>nope</Message></Error>",
                ),
            )
            .mount(&server)
            .await;
        let client = test_client(&server).await;
        let mut paginator = client.list(ListRequest::new("b", ""));
        assert!(paginator.next_page().await.is_err());
        assert!(paginator.next_page().await.unwrap().is_none());
    }

    /// Serial vs partitioned listing at S3-like page size and latency.
    /// Run with `cargo test --release -p s3f bench_listing -- --ignored --nocapture`.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "benchmark"]
    #[expect(clippy::print_stderr, reason = "benchmark output")]
    async fn bench_listing() {
        let layouts: [(&str, Vec<String>); 4] = [
            ("counter", (0..200_000).map(|i| format!("logs/{i:09}.json")).collect()),
            (
                "hashes",
                (0_u64..200_000)
                    .map(|i| format!("{:016x}", i.wrapping_mul(0x9E37_79B9_7F4A_7C15)))
                    .collect(),
            ),
            (
                "tree",
                (0..200_000)
                    .map(|i| {
                        format!(
                            "data/year={}/month={:02}/part-{i:06}.parquet",
                            2000 + i % 7,
                            i % 12
                        )
                    })
                    .collect(),
            ),
            ("wide", (0..200_000).map(|i| format!("users/{i:07}/avatar.png")).collect()),
        ];
        for (name, keys) in layouts {
            for c in [1, 16, 32] {
                let mut fake = FakeS3::with_keys(keys.clone());
                fake.latency = core::time::Duration::from_millis(40);
                let (client, stats) = fake.start().await;
                let t = std::time::Instant::now();
                let n = client
                    .list(ListRequest::new("b", "").with_concurrency(c))
                    .collect_all()
                    .await
                    .unwrap()
                    .len();
                let s = stats.lock().unwrap().clone();
                eprintln!(
                    "{name:8} c={c:2} keys={n} requests={:4} elapsed={:?}",
                    s.requests,
                    t.elapsed()
                );
            }
        }
    }
}
