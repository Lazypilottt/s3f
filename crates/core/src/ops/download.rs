//! Batch download operation — S3 prefix to local directory.

use std::{
    fmt,
    path::{Path, PathBuf},
    sync::Arc,
};

use tokio::sync::{Semaphore, mpsc};

use crate::{
    client::S3Client,
    config::TransferConfig,
    error::{Error, Result},
    http::{HttpClient, ObjectKey},
    ops::list::{ListRequest, ObjectInfo},
    trace::{maybe_debug, maybe_info},
    transfer::{download, pool, scheduler},
};

/// Default number of files downloaded in parallel.
const DEFAULT_WORKERS: usize = 32;
/// Default number of parts downloaded concurrently per file.
const DEFAULT_CONCURRENCY: usize = 8;

/// Total concurrent HTTP request budget used by [`tune_parallelism`].
const TOTAL_REQUEST_BUDGET: usize = 256;

/// Extra connections adaptive downloads may open across a whole batch,
/// on top of one per file being downloaded.
const SPLIT_CONNECTION_BUDGET: usize = 64;

/// Callback invoked after each file download completes successfully.
type ProgressCallback = Arc<dyn Fn(&FileDownloadResult) + Send + Sync>;

/// Outcome for a single downloaded file.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct FileDownloadResult {
    /// Local path the file was written to.
    pub dest: PathBuf,
    /// S3 key that was downloaded.
    pub key: String,
    /// Number of parts used (1 = single GET).
    pub parts: u32,
    /// File size in bytes.
    pub size: u64,
}

/// A batch download request.
///
/// Downloads all objects under `prefix` in `bucket` into `dest_dir`,
/// preserving the key structure relative to the prefix.
#[non_exhaustive]
pub struct DownloadRequest {
    /// Source S3 bucket.
    pub bucket: String,
    /// Number of parts downloaded concurrently within a single multipart download.
    pub concurrency_per_file: usize,
    /// Local directory to write files into.
    pub dest_dir: PathBuf,
    /// Callback invoked after each file download completes.
    on_file_complete: Option<ProgressCallback>,
    /// Key prefix in the bucket.
    pub prefix: String,
    /// Number of files downloaded in parallel.
    pub workers: usize,
}

impl fmt::Debug for DownloadRequest {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DownloadRequest")
            .field("bucket", &self.bucket)
            .field("concurrency_per_file", &self.concurrency_per_file)
            .field("dest_dir", &self.dest_dir)
            .field("on_file_complete", &self.on_file_complete.as_ref().map(|_| ".."))
            .field("prefix", &self.prefix)
            .field("workers", &self.workers)
            .finish()
    }
}

impl DownloadRequest {
    /// Create a new download request with sensible defaults.
    #[inline]
    #[must_use]
    pub fn new(
        bucket: impl Into<String>, prefix: impl Into<String>, dest_dir: impl Into<PathBuf>,
    ) -> Self {
        Self {
            bucket: bucket.into(),
            concurrency_per_file: DEFAULT_CONCURRENCY,
            dest_dir: dest_dir.into(),
            on_file_complete: None,
            prefix: prefix.into(),
            workers: DEFAULT_WORKERS,
        }
    }

    /// Auto-tune workers and concurrency based on the object list.
    ///
    /// Call this after listing when using [`S3Client::download_objects`]
    /// to let the heuristic pick optimal parallelism for the workload.
    #[inline]
    #[must_use]
    pub fn auto_tune(mut self, objects: &[ObjectInfo], multipart_threshold: u64) -> Self {
        let config = tune_parallelism(objects, multipart_threshold);
        self.workers = config.workers;
        self.concurrency_per_file = config.concurrency_per_file;
        self
    }

    /// Set a callback that fires after each file download completes.
    #[inline]
    #[must_use]
    pub fn on_file_complete(
        mut self, callback: impl Fn(&FileDownloadResult) + Send + Sync + 'static,
    ) -> Self {
        self.on_file_complete = Some(Arc::new(callback));
        self
    }
}

/// Result of a batch download.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct DownloadResult {
    /// Per-file outcomes.
    pub files: Vec<FileDownloadResult>,
}

#[expect(clippy::multiple_inherent_impl, reason = "ops extend S3Client from their own modules")]
impl S3Client {
    /// Download all objects under a prefix to a local directory.
    ///
    /// Listing and downloading are pipelined: the first page of
    /// `ListObjectsV2` results is used to auto-tune parallelism, then
    /// remaining pages are fetched concurrently with downloads so that
    /// large listings don't block the start of data transfer.
    ///
    /// If `workers` or `concurrency_per_file` were left at their defaults,
    /// they are replaced with auto-tuned values. Explicitly set values are
    /// respected, but a tracing warning is emitted when they differ from
    /// the recommendation.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use s3f::{Config, S3Client, DownloadRequest, auth::CredentialSource};
    /// # async fn example() -> s3f::error::Result<()> {
    /// let client = S3Client::new(Config::new("us-east-1", CredentialSource::Env)).await?;
    /// let result = client.download(DownloadRequest::new("my-bucket", "data/", "./local/")).await?;
    /// for f in &result.files {
    ///     println!("{} -> {}", f.key, f.dest.display());
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if listing, any individual download, or file I/O fails.
    pub async fn download(&self, mut req: DownloadRequest) -> Result<DownloadResult> {
        let mut paginator = self.list(ListRequest::new(&req.bucket, &req.prefix));

        // Fetch the first page to sample the workload for auto-tuning.
        let first_page = paginator.next_page().await?;
        let first_objects = match first_page {
            Some(page) => page.objects,
            None => {
                return Ok(DownloadResult {
                    files: Vec::new(),
                });
            },
        };

        // Auto-tune based on the first page sample.
        let tuned =
            tune_parallelism(&first_objects, self.config.transfer.multipart_download_threshold);
        let user_set_workers = req.workers != DEFAULT_WORKERS;
        let user_set_concurrency = req.concurrency_per_file != DEFAULT_CONCURRENCY;

        if user_set_workers || user_set_concurrency {
            // Respect explicit values, but warn if they differ from the recommendation.
            if req.workers != tuned.workers
                || req.concurrency_per_file != tuned.concurrency_per_file
            {
                maybe_info!(
                    user_workers = req.workers,
                    user_concurrency = req.concurrency_per_file,
                    recommended_workers = tuned.workers,
                    recommended_concurrency = tuned.concurrency_per_file,
                    "using user-specified parallelism; auto-tuned values differ"
                );
            }
        } else {
            req.workers = tuned.workers;
            req.concurrency_per_file = tuned.concurrency_per_file;
        }

        // Buffer enough items to keep the pool fed without over-allocating.
        let channel_cap = req.workers.saturating_mul(2).max(1);
        let (tx, rx) = mpsc::channel::<ObjectInfo>(channel_cap);

        // Spawn the paginator as a background producer. It sends the first
        // page's objects, then continues fetching remaining pages.
        // Returns Ok(()) on success, Err on listing failure.
        let mut paginator_owned = paginator;
        let list_handle = tokio::task::spawn(async move {
            // Send first-page objects.
            for obj in first_objects {
                if tx.send(obj).await.is_err() {
                    return Ok(());
                }
            }
            // Continue with remaining pages.
            loop {
                match paginator_owned.next_page().await {
                    Ok(Some(page)) => {
                        for obj in page.objects {
                            if tx.send(obj).await.is_err() {
                                return Ok(());
                            }
                        }
                    },
                    Ok(None) => return Ok(()),
                    Err(e) => return Err(e),
                }
            }
        });

        let http = self.http.clone();
        let config = self.config.clone();
        let creds = self.creds.clone();
        let bucket = req.bucket.clone();
        let prefix = req.prefix.clone();
        let dest_dir = req.dest_dir.clone();
        let concurrency = req.concurrency_per_file;

        let on_complete: Option<&(dyn Fn(&FileDownloadResult) + Send + Sync)> =
            req.on_file_complete.as_deref();
        let budget = Arc::new(Semaphore::new(SPLIT_CONNECTION_BUDGET));

        let pool_result = pool::run_pool_rx(
            rx,
            req.workers,
            |obj| {
                let http = http.clone();
                let cfg = config.clone();
                let cr = creds.clone();
                let bkt = bucket.clone();
                let pfx = prefix.clone();
                let dest = dest_dir.clone();
                let budget = Arc::clone(&budget);
                async move {
                    download_single_object(
                        &http,
                        &cfg,
                        &cr,
                        &bkt,
                        &pfx,
                        &obj,
                        &dest,
                        concurrency,
                        &budget,
                    )
                    .await
                }
            },
            on_complete,
        )
        .await;

        // Check the listing task first — a listing failure is the root cause
        // when the pool starves because the channel closed early.
        let list_result = list_handle.await.map_err(|e| Error::Internal(e.to_string()))?;
        list_result?;

        let files = pool_result?;

        Ok(DownloadResult {
            files,
        })
    }

    /// Download a pre-listed set of objects to a local directory.
    ///
    /// Use this when you have already listed the objects (e.g. to build a
    /// progress bar) and want to avoid a second `ListObjectsV2` round-trip.
    ///
    /// # Errors
    ///
    /// Returns an error if any individual download or file I/O fails.
    pub async fn download_objects(
        &self, req: DownloadRequest, objects: Vec<ObjectInfo>,
    ) -> Result<DownloadResult> {
        #[cfg(feature = "tracing")]
        let total = objects.len();
        maybe_info!(
            files = total,
            workers = req.workers,
            concurrency = req.concurrency_per_file,
            bucket = %req.bucket,
            prefix = %req.prefix,
            dest = %req.dest_dir.display(),
            "starting download batch"
        );

        if objects.is_empty() {
            return Ok(DownloadResult {
                files: Vec::new(),
            });
        }

        let http = self.http.clone();
        let config = self.config.clone();
        let creds = self.creds.clone();
        let bucket = req.bucket.clone();
        let prefix = req.prefix.clone();
        let dest_dir = req.dest_dir.clone();
        let concurrency = req.concurrency_per_file;

        let on_complete: Option<&(dyn Fn(&FileDownloadResult) + Send + Sync)> =
            req.on_file_complete.as_deref();
        let budget = Arc::new(Semaphore::new(SPLIT_CONNECTION_BUDGET));

        let files = pool::run_pool(
            objects,
            req.workers,
            |obj| {
                let http = http.clone();
                let cfg = config.clone();
                let cr = creds.clone();
                let bkt = bucket.clone();
                let pfx = prefix.clone();
                let dest = dest_dir.clone();
                let budget = Arc::clone(&budget);
                async move {
                    download_single_object(
                        &http,
                        &cfg,
                        &cr,
                        &bkt,
                        &pfx,
                        &obj,
                        &dest,
                        concurrency,
                        &budget,
                    )
                    .await
                }
            },
            on_complete,
        )
        .await?;

        Ok(DownloadResult {
            files,
        })
    }
}

/// Parallelism settings returned by [`tune_parallelism`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct ParallelismConfig {
    /// Number of files downloaded in parallel.
    pub workers: usize,
    /// Number of parts downloaded concurrently within a single multipart download.
    pub concurrency_per_file: usize,
}

/// Analyze a set of objects and return optimal `(workers, concurrency_per_file)`.
///
/// Uses a fixed total request budget (256 concurrent requests) and distributes
/// it based on the median file size relative to the multipart threshold:
///
/// - **Small-files workload** (median ≤ threshold): most files will use a single GET, so maximize
///   worker count and set concurrency to 1.
/// - **Large-files workload** (median > threshold): fewer workers, more per-file concurrency to
///   saturate bandwidth on each large object.
///
/// Returns defaults if the object list is empty.
#[must_use]
pub fn tune_parallelism(objects: &[ObjectInfo], multipart_threshold: u64) -> ParallelismConfig {
    if objects.is_empty() {
        return ParallelismConfig {
            workers: DEFAULT_WORKERS,
            concurrency_per_file: DEFAULT_CONCURRENCY,
        };
    }

    let mut sizes: Vec<u64> = objects.iter().map(|o| o.size).collect();
    let mid = sizes.len() / 2;
    sizes.select_nth_unstable(mid);
    let median = sizes[mid];

    if median <= multipart_threshold {
        // Small-files mode: single GET per file, maximize parallelism.
        let workers = TOTAL_REQUEST_BUDGET.min(objects.len());
        ParallelismConfig {
            workers: workers.max(1),
            concurrency_per_file: 1,
        }
    } else {
        // Large-files mode: fewer workers, more parts per file.
        // Target matches DEFAULT_CONCURRENCY; cap so
        // workers * concurrency ≤ budget.
        let target_concurrency = DEFAULT_CONCURRENCY;
        let workers = (TOTAL_REQUEST_BUDGET / target_concurrency).min(objects.len()).max(1);
        ParallelismConfig {
            workers,
            concurrency_per_file: target_concurrency,
        }
    }
}

/// Download a single object — picks the transfer strategy by size.
///
/// - Small files: one GET.
/// - Up to the multipart threshold: adaptive — one GET that hands its tail to extra connections
///   only if it turns out to be slow.
/// - Above the threshold: Range requests from the start.
///
/// Concurrency for the last case is dynamically scaled per file via
/// [`scheduler::concurrency_for_size`]: small files that exceed the
/// multipart threshold get fewer concurrent Range requests, large files
/// get more to saturate bandwidth.
#[expect(clippy::too_many_arguments, reason = "internal fn, context struct would add indirection")]
async fn download_single_object(
    http: &HttpClient, config: &crate::config::Config, creds: &crate::auth::Credentials,
    bucket: &str, prefix: &str, obj: &ObjectInfo, dest_dir: &Path, _concurrency: usize,
    budget: &Semaphore,
) -> Result<FileDownloadResult> {
    let rel_key = obj.key.strip_prefix(prefix).unwrap_or(&obj.key);
    let dest = dest_dir.join(rel_key);
    let key = ObjectKey::new(&obj.key);

    #[cfg(feature = "tracing")]
    let file_start = std::time::Instant::now();

    let (size, parts) = if obj.size < download::ADAPTIVE_MIN_SIZE {
        maybe_debug!(key = %key, size = obj.size, "single GET");
        let size = download::download_single(http, config, creds, bucket, &key, &dest).await?;
        (size, 1_u32)
    } else if obj.size <= config.transfer.multipart_download_threshold {
        maybe_debug!(key = %key, size = obj.size, "adaptive GET");
        download::download_adaptive(http, config, creds, bucket, &key, &dest, obj.size, budget)
            .await?
    } else {
        let concurrency = scheduler::concurrency_for_size(obj.size);
        maybe_debug!(key = %key, size = obj.size, concurrency, "multipart download");
        let part_size = scheduler::compute_download_part_size(obj.size, concurrency);
        let transfer = TransferConfig {
            part_size,
            ..config.transfer
        };
        let parts_plan = scheduler::plan_parts(obj.size, &transfer);
        let parts_count =
            u32::try_from(parts_plan.len()).map_err(|e| Error::Conversion(e.to_string()))?;
        let size = download::download_multipart(
            http,
            config,
            creds,
            bucket,
            &key,
            &parts_plan,
            &dest,
            obj.size,
            concurrency,
        )
        .await?;
        (size, parts_count)
    };

    #[cfg(feature = "tracing")]
    let elapsed = file_start.elapsed();

    maybe_info!(key = %key, size, parts, ?elapsed, "file downloaded");
    Ok(FileDownloadResult {
        dest,
        key: obj.key.clone(),
        parts,
        size,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const MB: u64 = 1024 * 1024;
    const THRESHOLD: u64 = 50 * MB;

    fn make_objects(sizes: &[u64]) -> Vec<ObjectInfo> {
        sizes
            .iter()
            .enumerate()
            .map(|(i, &size)| {
                ObjectInfo {
                    key: format!("file{i}.bin"),
                    size,
                    etag: String::new(),
                    last_modified: String::new(),
                }
            })
            .collect()
    }

    #[test]
    fn tune_empty_returns_defaults() {
        let cfg = tune_parallelism(&[], THRESHOLD);
        assert_eq!(cfg.workers, DEFAULT_WORKERS);
        assert_eq!(cfg.concurrency_per_file, DEFAULT_CONCURRENCY);
    }

    #[test]
    fn tune_small_files_maximizes_workers() {
        let objects = make_objects(&[MB; 100]);
        let cfg = tune_parallelism(&objects, THRESHOLD);
        assert_eq!(cfg.workers, 100);
        assert_eq!(cfg.concurrency_per_file, 1);
    }

    #[test]
    fn tune_small_files_caps_at_budget() {
        let objects = make_objects(&[MB; 1000]);
        let cfg = tune_parallelism(&objects, THRESHOLD);
        assert_eq!(cfg.workers, TOTAL_REQUEST_BUDGET);
        assert_eq!(cfg.concurrency_per_file, 1);
    }

    #[test]
    fn tune_large_files_uses_concurrency() {
        let objects = make_objects(&[500 * MB; 10]);
        let cfg = tune_parallelism(&objects, THRESHOLD);
        assert_eq!(cfg.concurrency_per_file, DEFAULT_CONCURRENCY);
        assert_eq!(cfg.workers, 10);
    }

    #[test]
    fn tune_large_files_caps_workers() {
        let objects = make_objects(&[500 * MB; 100]);
        let cfg = tune_parallelism(&objects, THRESHOLD);
        assert_eq!(cfg.concurrency_per_file, DEFAULT_CONCURRENCY);
        assert_eq!(cfg.workers, TOTAL_REQUEST_BUDGET / DEFAULT_CONCURRENCY);
    }

    #[test]
    fn tune_single_small_file() {
        let objects = make_objects(&[10 * MB]);
        let cfg = tune_parallelism(&objects, THRESHOLD);
        assert_eq!(cfg.workers, 1);
        assert_eq!(cfg.concurrency_per_file, 1);
    }

    #[test]
    fn tune_single_large_file() {
        let objects = make_objects(&[500 * MB]);
        let cfg = tune_parallelism(&objects, THRESHOLD);
        assert_eq!(cfg.workers, 1);
        assert_eq!(cfg.concurrency_per_file, DEFAULT_CONCURRENCY);
    }

    #[test]
    fn tune_mixed_files_uses_median() {
        // 5 small, 4 large → median is small → small-files mode
        let mut sizes = vec![MB; 5];
        sizes.extend([500 * MB; 4]);
        let objects = make_objects(&sizes);
        let cfg = tune_parallelism(&objects, THRESHOLD);
        assert_eq!(cfg.concurrency_per_file, 1);
    }

    #[test]
    fn auto_tune_applies_config() {
        let objects = make_objects(&[MB; 50]);
        let req =
            DownloadRequest::new("bucket", "prefix/", "/tmp/dest").auto_tune(&objects, THRESHOLD);
        assert_eq!(req.workers, 50);
        assert_eq!(req.concurrency_per_file, 1);
    }

    const MIB: usize = 1024 * 1024;

    #[expect(clippy::arithmetic_side_effects, reason = "test data generator")]
    fn pattern(len: usize, seed: u8) -> bytes::Bytes {
        // Non-repeating at any power-of-two stride, so a part written to
        // the wrong offset can't go unnoticed.
        (0..len)
            .map(|i| u8::try_from((i * 31 + i / 251 + usize::from(seed)) % 251).unwrap())
            .collect::<Vec<u8>>()
            .into()
    }

    async fn download_from(
        fake: crate::test_support::FakeS3,
    ) -> (tempfile::TempDir, core::time::Duration, crate::test_support::Stats) {
        download_from_with(fake, |_| {}).await
    }

    async fn download_from_with(
        fake: crate::test_support::FakeS3, configure: impl FnOnce(&mut crate::Config),
    ) -> (tempfile::TempDir, core::time::Duration, crate::test_support::Stats) {
        let expected = fake.objects.clone();
        let (client, stats) = fake.start_with(configure).await;
        let dir = tempfile::tempdir().unwrap();
        let start = std::time::Instant::now();
        let result = client.download(DownloadRequest::new("b", "", dir.path())).await.unwrap();
        let elapsed = start.elapsed();

        assert_eq!(result.files.len(), expected.len());
        for (key, body) in &expected {
            let got = std::fs::read(dir.path().join(key)).unwrap();
            assert!(
                got == body.as_ref(),
                "{key}: content mismatch ({} vs {} bytes)",
                got.len(),
                body.len()
            );
        }
        let s = stats.lock().unwrap().clone();
        (dir, elapsed, s)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn adaptive_download_splits_on_slow_connections() {
        // 32 MiB at 16 MB/s per connection: ~2.1 s on one stream.
        let mut fake =
            crate::test_support::FakeS3::new([("big.bin".to_owned(), pattern(32 * MIB, 1))]);
        fake.conn_rate = Some(16_000_000);
        let (_dir, elapsed, stats) = download_from(fake).await;
        assert!(stats.ranged_gets >= 1, "did not split: {stats:?}");
        assert!(elapsed < core::time::Duration::from_millis(1500), "too slow: {elapsed:?}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn adaptive_download_stays_single_on_fast_links() {
        let fake = crate::test_support::FakeS3::new([("big.bin".to_owned(), pattern(24 * MIB, 2))]);
        let (_dir, _, stats) = download_from(fake).await;
        assert_eq!(stats.ranged_gets, 0, "split on a fast link: {stats:?}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn mixed_sizes_download_byte_exact() {
        let objects = [
            ("tiny.txt".to_owned(), pattern(10, 3)),
            ("empty".to_owned(), bytes::Bytes::new()),
            ("nested/dir/mid.bin".to_owned(), pattern(5 * MIB, 4)),
            ("nested/odd.bin".to_owned(), pattern(17 * MIB + 12_345, 5)),
            ("nested/slow.bin".to_owned(), pattern(20 * MIB + 7, 6)),
        ];
        let mut fake = crate::test_support::FakeS3::new(objects);
        fake.conn_rate = Some(24_000_000);
        let (_dir, _, stats) = download_from(fake).await;
        assert!(stats.ranged_gets >= 1, "{stats:?}");
    }

    /// Many small objects, a few of whose requests stall for 3 s.
    fn straggling_fake() -> crate::test_support::FakeS3 {
        let objects = (0..400).map(|i| (format!("obj-{i:04}"), pattern(1024, 7)));
        let mut fake = crate::test_support::FakeS3::new(objects);
        fake.latency = core::time::Duration::from_millis(5);
        fake.straggler_every = Some(40);
        fake.straggler_delay = core::time::Duration::from_secs(3);
        fake
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn hedging_cuts_straggler_tail() {
        let (_dir, elapsed, stats) = download_from(straggling_fake()).await;
        assert!(!stats.straggled.is_empty(), "no stragglers injected");
        assert!(elapsed < core::time::Duration::from_millis(2000), "not hedged: {elapsed:?}");
        // 400 GETs and a list page; hedges stay within the 10% budget.
        assert!(stats.requests <= 401 + 10 + 41, "too many hedges: {}", stats.requests);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stragglers_set_the_pace_without_hedging() {
        let (_dir, elapsed, _) =
            download_from_with(straggling_fake(), |c| c.retry.hedge = false).await;
        assert!(elapsed >= core::time::Duration::from_secs(3), "{elapsed:?}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dropped_connections_resume_byte_exact() {
        // Every 3rd object GET loses its connection halfway through the
        // body. Covers single GETs, adaptive streams and ranged parts.
        let objects = [
            ("a/small.bin".to_owned(), pattern(300_000, 11)),
            ("a/small2.bin".to_owned(), pattern(1_000_001, 12)),
            ("b/mid.bin".to_owned(), pattern(20 * MIB + 3, 13)),
            ("b/mid2.bin".to_owned(), pattern(30 * MIB, 14)),
            ("c/tiny".to_owned(), pattern(2, 15)),
        ];
        let mut fake = crate::test_support::FakeS3::new(objects);
        fake.conn_rate = Some(40_000_000);
        fake.cut_body_every = Some(3);
        let (_dir, _, stats) = download_from(fake).await;
        assert!(stats.cut_bodies >= 2, "{stats:?}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn multipart_download_resumes_parts() {
        let mut fake =
            crate::test_support::FakeS3::new([("big".to_owned(), pattern(40 * MIB, 16))]);
        fake.cut_body_every = Some(2);
        let (_dir, _, stats) =
            download_from_with(fake, |c| c.transfer.multipart_download_threshold = 16 * MB).await;
        assert!(stats.cut_bodies >= 1 && stats.ranged_gets >= 2, "{stats:?}");
    }

    /// Small-object download with and without hedging, at S3-like latency
    /// with 1 in 100 requests stalling.
    /// Run with `cargo test --release -p s3f bench_hedging -- --ignored --nocapture`.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "benchmark"]
    #[expect(clippy::print_stderr, reason = "benchmark output")]
    async fn bench_hedging() {
        for stall_ms in [0, 250, 1000] {
            for hedge in [false, true] {
                let objects = (0..2000).map(|i| (format!("obj-{i:05}"), pattern(4096, 3)));
                let mut fake = crate::test_support::FakeS3::new(objects);
                fake.latency = core::time::Duration::from_millis(20);
                fake.straggler_every = Some(100);
                fake.straggler_delay = core::time::Duration::from_millis(stall_ms);
                let (_dir, elapsed, stats) =
                    download_from_with(fake, |c| c.retry.hedge = hedge).await;
                eprintln!(
                    "stall {stall_ms:4} ms  hedge {hedge:5}  elapsed {elapsed:>8.2?}  requests {}",
                    stats.requests
                );
            }
        }
    }

    #[test]
    fn plan_streams_scales_with_measured_speed() {
        use core::time::Duration;
        let mib = 1024 * 1024;
        // Fast: 8 MiB in 5 ms leaves 500 MiB for ~0.3 s — no split.
        assert_eq!(download::plan_streams(508 * mib, 8 * mib, Duration::from_millis(5)), 1);
        // Slow: 8 MiB in 500 ms leaves ~31 s — capped at MAX_STREAMS.
        assert_eq!(download::plan_streams(508 * mib, 8 * mib, Duration::from_millis(500)), 8);
        // Slow but little left: limited by minimum helper size.
        assert_eq!(download::plan_streams(20 * mib, 8 * mib, Duration::from_secs(1)), 2);
    }

    /// Old single-GET path vs adaptive download on one object.
    /// Run with `cargo test --release -p s3f bench_adaptive -- --ignored --nocapture`.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "benchmark"]
    #[expect(clippy::print_stderr, reason = "benchmark output")]
    async fn bench_adaptive_download() {
        let semaphore = Semaphore::new(SPLIT_CONNECTION_BUDGET);
        for (label, rate) in [
            ("unlimited", None),
            ("50 MB/s/conn", Some(50_000_000)),
            ("20 MB/s/conn", Some(20_000_000)),
        ] {
            for size in [64 * MB, 256 * MB] {
                let body = pattern(usize::try_from(size).unwrap(), 9);
                let mut fake = crate::test_support::FakeS3::new([("obj".to_owned(), body)]);
                fake.conn_rate = rate;
                fake.latency = core::time::Duration::from_millis(20);
                let (client, _) = fake.start().await;
                let dir = tempfile::tempdir().unwrap();
                let key = ObjectKey::new("obj");

                let t = std::time::Instant::now();
                download::download_single(
                    &client.http,
                    &client.config,
                    &client.creds,
                    "b",
                    &key,
                    &dir.path().join("a"),
                )
                .await
                .unwrap();
                let single = t.elapsed();

                let t = std::time::Instant::now();
                let (_, parts) = download::download_adaptive(
                    &client.http,
                    &client.config,
                    &client.creds,
                    "b",
                    &key,
                    &dir.path().join("b"),
                    size,
                    &semaphore,
                )
                .await
                .unwrap();
                let adaptive = t.elapsed();
                eprintln!(
                    "{label:14} {:4} MiB  single {single:>8.2?}  adaptive {adaptive:>8.2?} \
                     ({parts} streams)",
                    size / MB
                );
            }
        }
    }
}
