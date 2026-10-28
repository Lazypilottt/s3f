//! # s3f
//!
//! S3 operations, fearlessly and blazingly fast.
//!
//! A minimal, high-throughput S3 client built directly on HTTP + `SigV4` signing.
//! Skip the AWS SDK complexity — just `reqwest`, `aws-sigv4`, and `tokio` doing the heavy lifting.
//!
//! ## Quick start
//!
//! ```no_run
//! use s3f::{Config, S3Client, UploadRequest, auth::CredentialSource};
//!
//! # async fn example() -> s3f::error::Result<()> {
//! // Grab credentials from env and spin up a client
//! let client = S3Client::new(Config::new("us-east-1", CredentialSource::Env)).await?;
//!
//! // Upload a whole directory with automatic multipart logic
//! let result = client.upload(
//!     UploadRequest::new(vec!["./data".into()], "my-bucket", "uploads/")
//! ).await?;
//!
//! // See what got uploaded
//! for f in &result.files {
//!     println!("{} → s3://{} ({} parts)", f.source.display(), f.key, f.parts);
//! }
//! # Ok(())
//! # }
//! ```

#![cfg_attr(
    test,
    expect(
        clippy::unwrap_used,
        clippy::indexing_slicing,
        clippy::panic,
        clippy::wildcard_enum_match_arm,
        clippy::expect_used,
        reason = "test code"
    )
)]
#![expect(
    clippy::multiple_crate_versions,
    reason = "transitive deps from aws-sigv4 pull duplicate versions"
)]

pub mod auth;
mod client;
pub mod config;
pub mod error;
mod http;
pub mod ops;
pub(crate) mod trace;
mod transfer;

pub use client::S3Client;
pub use config::Config;
pub use http::ObjectKey;
pub use ops::{
    download::{
        DownloadRequest,
        DownloadResult,
        FileDownloadResult,
        ParallelismConfig,
        tune_parallelism,
    },
    list::{ListPage, ListPaginator, ListRequest, ObjectInfo},
    upload::{FileUploadResult, UploadRequest, UploadResult},
};
