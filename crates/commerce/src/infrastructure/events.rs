use crate::{AppError, CommerceEvent, OutboxMessage, Store};
use sqlx::{PgConnection, PgPool, Row};
use uuid::Uuid;
pub async fn enqueue(
    c: &mut PgConnection,
    event: &CommerceEvent,
    dedup: &str,
) -> Result<(), AppError> {
    event.validate()?;
    sqlx::query("INSERT INTO outbox(id,aggregate_id,checkout_id,event_type,dedup_key,payload) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(dedup_key) DO NOTHING").bind(event.event_id).bind(event.aggregate_id).bind(event.checkout_id).bind(&event.event_type).bind(dedup).bind(serde_json::to_value(event)?).execute(c).await?;
    Ok(())
}
pub async fn verify(c: &mut PgConnection, event: &CommerceEvent) -> Result<(), AppError> {
    let stored: serde_json::Value = sqlx::query_scalar("SELECT payload FROM outbox WHERE id=$1")
        .bind(event.event_id)
        .fetch_optional(c)
        .await?
        .ok_or_else(|| {
            AppError::InvalidEvent("event is not a persisted application event".into())
        })?;
    if stored != serde_json::to_value(event)? {
        return Err(AppError::InvalidEvent(
            "event differs from the persisted envelope".into(),
        ));
    }
    Ok(())
}
pub async fn seen(c: &mut PgConnection, id: Uuid) -> Result<bool, AppError> {
    Ok(sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM inbox WHERE consumer_name='commerce-saga-v1' AND event_id=$1)",
    )
    .bind(id)
    .fetch_one(c)
    .await?)
}
pub async fn record(c: &mut PgConnection, id: Uuid) -> Result<(), AppError> {
    sqlx::query("INSERT INTO inbox(consumer_name,event_id) VALUES('commerce-saga-v1',$1) ON CONFLICT DO NOTHING").bind(id).execute(c).await?;
    Ok(())
}
impl Store {
    pub async fn claim_outbox(
        &self,
        limit: i64,
        lease_seconds: i64,
    ) -> Result<Vec<OutboxMessage>, AppError> {
        if !(1..=1000).contains(&limit) || !(1..=3600).contains(&lease_seconds) {
            return Err(AppError::Validation("invalid outbox claim bounds".into()));
        }
        claim_outbox(&self.pool, limit, lease_seconds).await
    }
    pub async fn mark_published(&self, id: Uuid, token: Uuid) -> Result<bool, AppError> {
        mark_published(&self.pool, id, token).await
    }
    pub async fn release_outbox(
        &self,
        id: Uuid,
        token: Uuid,
        error: &str,
    ) -> Result<bool, AppError> {
        release_outbox(&self.pool, id, token, error).await
    }
}
pub(crate) async fn claim_outbox(
    pool: &PgPool,
    limit: i64,
    lease_seconds: i64,
) -> Result<Vec<OutboxMessage>, AppError> {
    let mut transaction = pool.begin().await?;
    let rows = sqlx::query("WITH candidates AS (SELECT id FROM outbox WHERE published_at IS NULL AND available_at <= clock_timestamp() AND (lease_until IS NULL OR lease_until <= clock_timestamp()) ORDER BY created_at, id LIMIT $1 FOR UPDATE SKIP LOCKED) UPDATE outbox x SET lease_token = $2, lease_until = clock_timestamp() + make_interval(secs => $3::double precision), attempts = x.attempts + 1 FROM candidates c WHERE x.id = c.id RETURNING x.id, x.aggregate_id, x.payload, x.attempts, x.lease_token")
        .bind(limit).bind(Uuid::new_v4()).bind(lease_seconds as f64).fetch_all(&mut *transaction).await?;
    let messages = rows
        .into_iter()
        .map(|row| {
            Ok(OutboxMessage {
                id: row.try_get("id")?,
                aggregate_id: row.try_get("aggregate_id")?,
                payload: row.try_get("payload")?,
                attempts: row.try_get("attempts")?,
                lease_token: row.try_get("lease_token")?,
            })
        })
        .collect::<Result<_, sqlx::Error>>()?;
    transaction.commit().await?;
    Ok(messages)
}

pub(crate) async fn mark_published(pool: &PgPool, id: Uuid, token: Uuid) -> Result<bool, AppError> {
    let result = sqlx::query("UPDATE outbox SET published_at = clock_timestamp(), lease_token = NULL, lease_until = NULL, last_error = NULL WHERE id = $1 AND lease_token = $2 AND lease_until > clock_timestamp() AND published_at IS NULL")
        .bind(id).bind(token).execute(pool).await?;
    Ok(result.rows_affected() == 1)
}

pub(crate) async fn release_outbox(
    pool: &PgPool,
    id: Uuid,
    token: Uuid,
    error: &str,
) -> Result<bool, AppError> {
    let result = sqlx::query("UPDATE outbox SET available_at = clock_timestamp() + make_interval(secs => power(2.0, least(attempts, 8))), lease_token = NULL, lease_until = NULL, last_error = left($3, 512) WHERE id = $1 AND lease_token = $2 AND lease_until > clock_timestamp() AND published_at IS NULL")
        .bind(id).bind(token).bind(error).execute(pool).await?;
    Ok(result.rows_affected() == 1)
}
