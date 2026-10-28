//! Route handlers for S3 operations.

use std::path::PathBuf;

use axum::{
    Json,
    body::Body,
    extract::{Multipart, Query, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use tokio::fs;
use utoipa::IntoParams;

use crate::{
    error::AppError,
    models::{ObjectInfo, UploadFileResult},
    state::AppState,
};

/// Query parameters for upload.
#[derive(Debug, Deserialize, IntoParams)]
pub struct UploadParams {
    /// Key prefix prepended to each uploaded file name.
    #[serde(default)]
    pub prefix: String,
}

/// Query parameters for download.
#[derive(Debug, Deserialize, IntoParams)]
pub struct DownloadParams {
    /// Full S3 object key to download.
    pub key: String,
}

/// Query parameters for listing.
#[derive(Debug, Deserialize, IntoParams)]
pub struct ListParams {
    /// Key prefix to filter by (e.g. `data/2024/`).
    #[serde(default)]
    pub prefix: String,
    /// Delimiter for directory-style grouping (typically `/`).
    pub delimiter: Option<String>,
}

/// Upload files to S3.
///
/// Accepts multipart form data, stages files to temp storage, then blasts
/// them up to S3 using s3f's parallel multipart engine. Cleans up after itself.
#[utoipa::path(
    post,
    path = "/upload",
    tag = "s3",
    params(UploadParams),
    responses(
        (status = 200, description = "Files uploaded successfully.", body = Vec<UploadFileResult>),
        (status = 500, description = "S3 or I/O error."),
    )
)]
pub async fn upload(
    State(state): State<AppState>, Query(params): Query<UploadParams>, mut multipart: Multipart,
) -> Result<Json<Vec<UploadFileResult>>, AppError> {
    let staging = tempfile::tempdir().map_err(s3f::error::Error::Io)?;
    let mut paths: Vec<PathBuf> = Vec::new();

    while let Some(field) =
        multipart.next_field().await.map_err(|e| s3f::error::Error::Internal(e.to_string()))?
    {
        let name = field.file_name().unwrap_or("unnamed").to_owned();
        let data = field.bytes().await.map_err(|e| s3f::error::Error::Internal(e.to_string()))?;
        let dest = staging.path().join(&name);
        fs::write(&dest, &data).await.map_err(s3f::error::Error::Io)?;
        paths.push(dest);
    }

    // Hand off to s3f — it handles parallelism, multipart, everything
    let req = s3f::UploadRequest::new(paths, &state.bucket, &params.prefix);
    let result = state.client.upload(req).await?;

    Ok(Json(
        result
            .files
            .into_iter()
            .map(|f| {
                UploadFileResult {
                    key: f.key,
                    size: f.size,
                    parts: f.parts,
                    etag: f.etag,
                }
            })
            .collect(),
    ))
}

/// Download a file from S3.
///
/// Takes a full object key, figures out the prefix, and downloads matching files.
/// Returns the exact key you asked for as a downloadable attachment.
#[utoipa::path(
    get,
    path = "/download",
    tag = "s3",
    params(DownloadParams),
    responses(
        (status = 200, description = "File contents.", content_type = "application/octet-stream"),
        (status = 404, description = "Object not found."),
        (status = 500, description = "S3 or I/O error."),
    )
)]
pub async fn download(
    State(state): State<AppState>, Query(params): Query<DownloadParams>,
) -> Result<Response, AppError> {
    let dest_dir = tempfile::tempdir().map_err(s3f::error::Error::Io)?;
    let slash = params.key.rfind('/');
    let prefix = match slash {
        Some(i) => &params.key[..=i],
        None => "",
    };

    // Fire up a s3f download for that prefix
    let req = s3f::DownloadRequest::new(&state.bucket, prefix, dest_dir.path());
    let result = state.client.download(req).await?;

    for f in &result.files {
        if f.key == params.key {
            let body = fs::read(&f.dest).await.map_err(s3f::error::Error::Io)?;
            let filename =
                f.dest.file_name().map_or("download", |n| n.to_str().unwrap_or("download"));
            return Ok((
                [(header::CONTENT_DISPOSITION, format!("attachment; filename=\"{filename}\""))],
                Body::from(body),
            )
                .into_response());
        }
    }

    Ok(StatusCode::NOT_FOUND.into_response())
}

/// List objects under a prefix.
///
/// Fetches metadata for all objects matching the prefix.
/// Use `delimiter=/` for a directory-like tree view.
#[utoipa::path(
    get,
    path = "/list",
    tag = "s3",
    params(ListParams),
    responses(
        (status = 200, description = "Object listing.", body = Vec<ObjectInfo>),
        (status = 500, description = "S3 or I/O error."),
    )
)]
pub async fn list(
    State(state): State<AppState>, Query(params): Query<ListParams>,
) -> Result<Json<Vec<ObjectInfo>>, AppError> {
    let mut req = s3f::ListRequest::new(&state.bucket, &params.prefix);
    if let Some(d) = params.delimiter {
        req = req.with_delimiter(d);
    }

    let mut paginator = state.client.list(req);
    let objects = paginator.collect_all().await?;

    Ok(Json(
        objects
            .into_iter()
            .map(|o| {
                ObjectInfo {
                    key: o.key,
                    size: o.size,
                    etag: o.etag,
                    last_modified: o.last_modified,
                }
            })
            .collect(),
    ))
}
