//! REST API server wrapping s3f — ultra-fast S3 operations via HTTP.
//! Built by Lazypilottt

mod error;
mod models;
mod routes;
mod state;

use axum::{Router, routing::get};
use utoipa::OpenApi;
use utoipa_scalar::{Scalar, Servable};
use crate::state::AppState;

/// s3f API documentation.
#[derive(OpenApi)]
#[openapi(
    info(
        title = "s3f",
        version = "0.1.0", 
        description = "HTTP wrapper around s3f. Heavy lifting (multipart upload/download, \
                        connection pooling, SigV4 signing) happens in the native Rust library."
    ),
    paths(
        routes::upload,
        routes::download,
        routes::list
    ),
    components(
        schemas(models::UploadFileResult, models::ObjectInfo)
    ),
    tags(
        (name = "s3f", description = "S3 operations powered by s3f.")
    )
)]
struct ApiDoc;
//struct ApiDoc seems a lil to obnoxious; 

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load S3 credentials and config from environment
    let state = AppState::from_env().await?;

    // Wire up routes for upload, download, list, and API docs
    let app = Router::new()
        .route("/upload", axum::routing::post(routes::upload))
        .route("/download", get(routes::download))
        .route("/list", get(routes::list))
        .merge(Scalar::with_url("/docs", ApiDoc::openapi()))
        .with_state(state);

    // Start listening on 8080
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
    println!("s3f server is live at http://localhost:8080");
    println!("API docs: http://localhost:8080/docs");
    println!("Built by Lazypilottt");
    axum::serve(listener, app).await?;
    Ok(())
}
