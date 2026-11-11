pub(crate) mod carts;
pub(crate) mod checkouts;
pub(crate) mod events;
pub(crate) mod orders;
pub(crate) mod payments;
pub(crate) mod projection;
use crate::AppError;
use sqlx::{PgConnection, PgPool, Postgres, Transaction};
pub(crate) struct UnitOfWork {
    transaction: Transaction<'static, Postgres>,
}
impl UnitOfWork {
    pub async fn begin(pool: &PgPool) -> Result<Self, AppError> {
        Ok(Self {
            transaction: pool.begin().await?,
        })
    }
    pub fn conn(&mut self) -> &mut PgConnection {
        &mut self.transaction
    }
    pub async fn commit(self) -> Result<(), AppError> {
        self.transaction.commit().await?;
        Ok(())
    }
    pub async fn now(&mut self) -> Result<chrono::DateTime<chrono::Utc>, AppError> {
        Ok(sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(self.conn())
            .await?)
    }
}
