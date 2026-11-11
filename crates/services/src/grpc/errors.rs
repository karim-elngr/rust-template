use commerce::AppError;
use tonic::Status;

pub(super) fn status(error: AppError) -> Status {
    match error {
        AppError::Validation(message) => Status::invalid_argument(message),
        AppError::VersionConflict => Status::aborted("resource version changed; reload and retry"),
        AppError::InvalidState(_) => {
            Status::failed_precondition("operation is not allowed in the current state")
        }
        AppError::IdempotencyConflict => {
            Status::already_exists("idempotency key is already bound to a different request")
        }
        AppError::NotFound => Status::not_found("resource not found"),
        _ => {
            tracing::error!(
                event.name = "request.failed",
                "application operation failed"
            );
            Status::internal("internal server error")
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn grpc_errors_do_not_expose_storage_details() {
        let result = status(AppError::Database(sqlx::Error::Protocol("secret".into())));
        assert_eq!(result.code(), tonic::Code::Internal);
        assert!(!result.message().contains("secret"));
        assert_eq!(
            status(AppError::VersionConflict).code(),
            tonic::Code::Aborted
        );
    }
}
