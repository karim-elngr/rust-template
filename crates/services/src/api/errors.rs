use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use commerce::AppError;

pub struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl ApiError {
    pub(super) fn bad_request(message: &str) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_request",
            message: message.into(),
        }
    }
}

impl From<AppError> for ApiError {
    fn from(error: AppError) -> Self {
        match error {
            AppError::Validation(message) => Self::bad_request(&message),
            AppError::VersionConflict => Self {
                status: StatusCode::CONFLICT,
                code: "version_conflict",
                message: "resource version changed; reload and retry".into(),
            },
            AppError::InvalidState(_) => Self {
                status: StatusCode::CONFLICT,
                code: "invalid_state",
                message: "operation is not allowed in the current state".into(),
            },
            AppError::IdempotencyConflict => Self {
                status: StatusCode::CONFLICT,
                code: "idempotency_conflict",
                message: "idempotency key is already bound to a different request".into(),
            },
            AppError::NotFound => Self {
                status: StatusCode::NOT_FOUND,
                code: "not_found",
                message: "resource not found".into(),
            },
            _ => {
                // Database errors can contain row contents and credentials; never send/log them verbatim.
                tracing::error!(
                    event.name = "request.failed",
                    "application operation failed"
                );
                Self {
                    status: StatusCode::INTERNAL_SERVER_ERROR,
                    code: "internal_error",
                    message: "internal server error".into(),
                }
            }
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(serde_json::json!({"error":{"code":self.code,"message":self.message}})),
        )
            .into_response()
    }
}
