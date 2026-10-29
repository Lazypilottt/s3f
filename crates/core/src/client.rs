//! S3 client — the core workhorse. Owns the connection pool and config.
//! Cheap to clone since it just wraps Arc around the HTTP client.
//! Lazypilottt

use core::time::Duration;

use crate::{
    auth::{self, Credentials},
    config::{Config, TransferConfig},
    error::Result,
    http::{HttpClient, dns::SpreadResolver},
};

/// Your gateway to S3. Cheap to clone (shares the connection pool under the hood).
///
/// Don't create a new one for each request — reuse it!
#[derive(Debug, Clone)]
pub struct S3Client {
    pub(crate) config: Config,
    pub(crate) creds: Credentials,
    pub(crate) http: HttpClient,
}

impl S3Client {
    /// Access the client configuration.
    #[inline]
    #[must_use]
    pub const fn config(&self) -> &Config {
        &self.config
    }

    /// Create a new client with the given configuration.
    ///
    /// The connection pool grows on demand — no eager warmup. The first
    /// operation pays ~2 ms per new TCP connection, which is negligible
    /// compared to S3 round-trip latency.
    ///
    /// # Errors
    ///
    /// Returns an error if credential resolution fails (e.g.
    /// [`crate::auth::CredentialSource::Env`] and the env vars are missing)
    /// or if the HTTP client cannot be built.
    #[inline]
    #[expect(clippy::unused_async, reason = "API is async-ready for future credential refresh")]
    pub async fn new(config: Config) -> Result<Self> {
        let creds = auth::resolve(&config.credentials)?;

        // tcp_nodelay: disable Nagle so 256 KiB streaming writes don't
        // interact with TCP delayed-ACK (~40 ms stalls per chunk).
        // http1_only: S3 backends speak HTTP/1.1; skip ALPN negotiation
        // to avoid an extra round trip on every new connection.
        // SpreadResolver: start each new connection on a different S3
        // front-end IP so throughput isn't capped by a single server.
        // read_timeout: a connection that stops delivering data errors out
        // and is retried instead of hanging the transfer.
        let http = reqwest::Client::builder()
            .dns_resolver(SpreadResolver::default())
            .pool_max_idle_per_host(TransferConfig::MAX_IDLE_CONNECTIONS)
            .tcp_keepalive(Duration::from_secs(30))
            .pool_idle_timeout(Duration::from_secs(90))
            .tcp_nodelay(true)
            .http1_only()
            .connect_timeout(Duration::from_secs(10))
            .read_timeout(config.retry.stall_timeout)
            .build()?
            .into();

        Ok(Self {
            config,
            creds,
            http,
        })
    }
}
