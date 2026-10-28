//! Error handling — maps s3f errors to HTTP responses.
//! Built by Lazypilottt

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};

/// Application error wrapper.
#[derive(Debug)]
pub(crate) struct AppError(s3f::error::Error);

impl From<s3f::error::Error> for AppError {
    #[inline]
    fn from(e: s3f::error::Error) -> Self {
        Self(e)
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        (StatusCode::INTERNAL_SERVER_ERROR, self.0.to_string()).into_response()
    }
}
