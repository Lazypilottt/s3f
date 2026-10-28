//! Configuration for s3f. Tune it to your workload.
//! Created by Lazypilottt

use core::time::Duration;

use crate::auth::CredentialSource;

/// Everything the client needs to know about connecting to S3.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Config {
    /// Where to get AWS credentials (from env, hardcoded, whatever).
    pub credentials: CredentialSource,
    /// Point to a different S3 endpoint (MinIO, R2, GCS, etc.).
    /// Leave as `None` to use AWS default.
    pub endpoint: Option<String>,
    /// AWS region (e.g. `us-east-1`).
    pub region: String,
    /// The actual endpoint URL (computed once at startup).
    resolved_endpoint: String,
    /// How to handle transient failures.
    pub retry: RetryPolicy,
    /// Knobs for tweaking transfer performance.
    pub transfer: TransferConfig,
}

/// When things fail, retry with smart backoff.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct RetryPolicy {
    /// Start backing off by this much.
    pub base_delay: Duration,
    /// Don't wait more than this between retries.
    pub max_delay: Duration,
    /// Give up after this many attempts.
    pub max_retries: u32,
}

/// Tuning parameters for file transfers (uploads/downloads).
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct TransferConfig {
    /// Switch to range-based multipart downloads above this size (bytes).
    /// Smaller files get a single GET (simpler, faster for small data).
    /// Defaults to 256 MiB.
    pub multipart_download_threshold: u64,
    /// Use multipart uploads for files bigger than this (bytes).
    pub multipart_threshold: u64,
    /// Suggested chunk size (bytes). The scheduler auto-tunes this per file,
    /// but this is the fallback if you call `plan_parts` directly.
    pub part_size: u64,
}

impl Config {
    /// Get the computed S3 endpoint URL.
    #[inline]
    #[must_use]
    pub fn endpoint_url(&self) -> &str {
        &self.resolved_endpoint
    }

    /// Spin up a config with region and credentials.
    ///
    /// Uses sensible defaults for transfer/retry. Tweak the fields if needed.
    #[inline]
    #[must_use]
    pub fn new(region: impl Into<String>, credentials: CredentialSource) -> Self {
        let region = region.into();
        let resolved_endpoint = format!("https://s3.{region}.amazonaws.com");
        Self {
            credentials,
            endpoint: None,
            region,
            resolved_endpoint,
            retry: RetryPolicy::default(),
            transfer: TransferConfig::default(),
        }
    }

    /// Create a new config with a custom endpoint for S3-compatible backends.
    #[inline]
    #[must_use]
    pub fn with_endpoint(
        region: impl Into<String>, credentials: CredentialSource, endpoint: String,
    ) -> Self {
        let region = region.into();
        let resolved_endpoint = endpoint.clone();
        Self {
            credentials,
            endpoint: Some(endpoint),
            region,
            resolved_endpoint,
            retry: RetryPolicy::default(),
            transfer: TransferConfig::default(),
        }
    }
}

impl TransferConfig {
    /// Connection pool size — must be >= peak in-flight requests
    /// (`workers * concurrency_per_file`) to avoid connection churn.
    pub(crate) const MAX_IDLE_CONNECTIONS: usize = 256;
}

impl Default for RetryPolicy {
    #[inline]
    fn default() -> Self {
        Self {
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            max_retries: 5,
        }
    }
}

impl Default for TransferConfig {
    #[inline]
    fn default() -> Self {
        Self {
            multipart_download_threshold: 2 * 1024 * 1024 * 1024,
            multipart_threshold: 50 * 1024 * 1024,
            part_size: 50 * 1024 * 1024,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::CredentialSource;

    #[test]
    fn default_endpoint_uses_region() {
        let cfg = Config::new("eu-west-1", CredentialSource::Env);
        assert_eq!(cfg.endpoint_url(), "https://s3.eu-west-1.amazonaws.com");
        assert_eq!(cfg.region, "eu-west-1");
        assert!(cfg.endpoint.is_none());
    }

    #[test]
    fn custom_endpoint_used_verbatim() {
        let cfg = Config::with_endpoint(
            "us-east-1",
            CredentialSource::Env,
            "http://localhost:9000".into(),
        );
        assert_eq!(cfg.endpoint_url(), "http://localhost:9000");
        assert_eq!(cfg.endpoint.as_deref(), Some("http://localhost:9000"));
    }

    #[test]
    fn retry_policy_defaults() {
        let policy = RetryPolicy::default();
        assert_eq!(policy.max_retries, 5);
        assert_eq!(policy.base_delay, Duration::from_millis(100));
        assert_eq!(policy.max_delay, Duration::from_secs(30));
    }

    #[test]
    fn transfer_config_defaults() {
        let tc = TransferConfig::default();
        assert_eq!(tc.multipart_threshold, 50 * 1024 * 1024);
        assert_eq!(tc.multipart_download_threshold, 2 * 1024 * 1024 * 1024);
        assert_eq!(tc.part_size, 50 * 1024 * 1024);
    }

    #[test]
    fn config_new_uses_default_retry_and_transfer() {
        let cfg = Config::new("us-east-1", CredentialSource::Env);
        assert_eq!(cfg.retry.max_retries, RetryPolicy::default().max_retries);
        assert_eq!(cfg.transfer.part_size, TransferConfig::default().part_size);
    }
}
