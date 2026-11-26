//! Mapping failures to responses.

use axum::Json;
use axum::extract::rejection::{JsonRejection, PathRejection, QueryRejection};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use leaseq_engine::EngineError;
use leaseq_engine::core::QueueError;
use leaseq_proto::{ErrorBody, ErrorDetail};

#[derive(Debug)]
pub(crate) struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl ApiError {
    pub(crate) fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = ErrorBody {
            error: ErrorDetail {
                code: self.code.to_owned(),
                message: self.message,
            },
        };
        (self.status, Json(body)).into_response()
    }
}

impl From<EngineError> for ApiError {
    fn from(error: EngineError) -> Self {
        let (status, code) = match &error {
            EngineError::Queue(QueueError::NotFound(_)) => (StatusCode::NOT_FOUND, "not_found"),
            EngineError::Queue(QueueError::LeaseLost(_)) => (StatusCode::CONFLICT, "lease_lost"),
            EngineError::Queue(QueueError::NotDead(_)) => (StatusCode::CONFLICT, "not_dead"),
            EngineError::Invalid(_) => (StatusCode::BAD_REQUEST, "invalid_request"),
            // Details may include filesystem paths; the engine has logged them.
            EngineError::Storage(_) => {
                return Self::new(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "storage_failure",
                    "storage failure",
                );
            }
            EngineError::Closed => (StatusCode::SERVICE_UNAVAILABLE, "shutting_down"),
        };
        Self::new(status, code, error.to_string())
    }
}

macro_rules! from_rejection {
    ($($rejection:ty),+) => {$(
        impl From<$rejection> for ApiError {
            fn from(rejection: $rejection) -> Self {
                Self::new(rejection.status(), "invalid_request", rejection.body_text())
            }
        }
    )+};
}

from_rejection!(JsonRejection, PathRejection, QueryRejection);
