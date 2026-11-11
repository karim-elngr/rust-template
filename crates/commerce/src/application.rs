use payments::{DemoGateway, PaymentGateway};
use sqlx::PgPool;
use std::sync::Arc;
mod commands;
mod queries;
mod saga;
#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("{0}")]
    Validation(String),
    #[error("version conflict")]
    VersionConflict,
    #[error("{0}")]
    InvalidState(String),
    #[error("idempotency key already used for a different request")]
    IdempotencyConflict,
    #[error("resource not found")]
    NotFound,
    #[error("required database schema migration is not applied")]
    SchemaNotReady,
    #[error("invalid commerce event: {0}")]
    InvalidEvent(String),
    #[error("payment provider outcome is not yet definitive; retry with the persisted operation")]
    RetryableProvider,
    #[error("database operation failed")]
    Database(#[from] sqlx::Error),
    #[error("database migration failed")]
    Migration(#[from] sqlx::migrate::MigrateError),
    #[error("stored data could not be decoded")]
    Serialization(#[from] serde_json::Error),
    #[error("stored aggregate state is inconsistent")]
    CorruptData,
}
impl From<shared_kernel::DomainError> for AppError {
    fn from(e: shared_kernel::DomainError) -> Self {
        Self::Validation(e.0)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyOutcome {
    Applied,
    AlreadyApplied,
    AlreadyFinalized,
}
#[derive(Clone)]
pub struct Store {
    pub(crate) pool: PgPool,
    pub(crate) gateway: Arc<dyn PaymentGateway>,
}
static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../migrations");
impl Store {
    pub fn new(pool: PgPool) -> Self {
        Self::with_gateway(pool, Arc::new(DemoGateway))
    }
    pub fn with_gateway(pool: PgPool, gateway: Arc<dyn PaymentGateway>) -> Self {
        Self { pool, gateway }
    }
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
    pub async fn migrate(&self) -> Result<(), AppError> {
        MIGRATOR.run(&self.pool).await?;
        Ok(())
    }
    pub async fn check_ready(&self) -> Result<(), AppError> {
        let migration = MIGRATOR
            .iter()
            .max_by_key(|m| m.version)
            .ok_or(AppError::SchemaNotReady)?;
        let ready:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM _sqlx_migrations WHERE version=$1 AND checksum=$2 AND success)").bind(migration.version).bind(migration.checksum.as_ref()).fetch_one(&self.pool).await?;
        if !ready {
            return Err(AppError::SchemaNotReady);
        }
        Ok(())
    }
}
