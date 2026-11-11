use crate::{AppError, CheckoutReceipt};
use checkout::CheckoutSaga;
use sqlx::{PgConnection, Row};
use uuid::Uuid;
pub async fn load(c: &mut PgConnection, id: Uuid, lock: bool) -> Result<CheckoutSaga, AppError> {
    let q = if lock {
        "SELECT cart_id,order_id,payment_id,status,version,expires_at,data FROM checkouts WHERE id=$1 FOR UPDATE"
    } else {
        "SELECT cart_id,order_id,payment_id,status,version,expires_at,data FROM checkouts WHERE id=$1"
    };
    let row = sqlx::query(q)
        .bind(id)
        .fetch_optional(c)
        .await?
        .ok_or(AppError::NotFound)?;
    let s: CheckoutSaga =
        serde_json::from_value(row.try_get("data")?).map_err(|_| AppError::CorruptData)?;
    s.validate().map_err(|_| AppError::CorruptData)?;
    if s.links().id != id
        || s.links().cart_id != row.try_get::<Uuid, _>("cart_id")?
        || s.links().order_id != row.try_get::<Uuid, _>("order_id")?
        || s.links().payment_id != row.try_get::<Uuid, _>("payment_id")?
        || s.status().as_str() != row.try_get::<String, _>("status")?
        || s.version() != row.try_get::<i64, _>("version")?
        || s.expires_at() != row.try_get::<chrono::DateTime<chrono::Utc>, _>("expires_at")?
    {
        return Err(AppError::CorruptData);
    }
    Ok(s)
}
pub async fn insert(c: &mut PgConnection, s: &CheckoutSaga) -> Result<(), AppError> {
    sqlx::query("INSERT INTO checkouts(id,cart_id,order_id,payment_id,status,version,expires_at,data) VALUES($1,$2,$3,$4,$5,$6,$7,$8)").bind(s.links().id).bind(s.links().cart_id).bind(s.links().order_id).bind(s.links().payment_id).bind(s.status().as_str()).bind(s.version()).bind(s.expires_at()).bind(serde_json::to_value(s)?).execute(c).await?;
    Ok(())
}
pub async fn save(c: &mut PgConnection, s: &CheckoutSaga) -> Result<(), AppError> {
    s.validate()?;
    sqlx::query("UPDATE checkouts SET status=$2,version=$3,expires_at=$4,data=$5 WHERE id=$1")
        .bind(s.links().id)
        .bind(s.status().as_str())
        .bind(s.version())
        .bind(s.expires_at())
        .bind(serde_json::to_value(s)?)
        .execute(c)
        .await?;
    Ok(())
}
pub async fn lock_key(c: &mut PgConnection, key: Uuid) -> Result<(), AppError> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
        .bind(key.to_string())
        .execute(c)
        .await?;
    Ok(())
}
pub async fn receipt(
    c: &mut PgConnection,
    key: Uuid,
) -> Result<Option<(serde_json::Value, CheckoutReceipt)>, AppError> {
    let row = sqlx::query(
        "SELECT canonical_request,receipt FROM checkout_requests WHERE idempotency_key=$1",
    )
    .bind(key)
    .fetch_optional(c)
    .await?;
    row.map(|row| {
        Ok((
            row.try_get("canonical_request")?,
            serde_json::from_value(row.try_get("receipt")?)?,
        ))
    })
    .transpose()
}
pub async fn remember(
    c: &mut PgConnection,
    key: Uuid,
    request: serde_json::Value,
    receipt: &CheckoutReceipt,
) -> Result<(), AppError> {
    sqlx::query("INSERT INTO checkout_requests(idempotency_key,checkout_id,canonical_request,receipt) VALUES($1,$2,$3,$4)").bind(key).bind(receipt.checkout_id).bind(request).bind(serde_json::to_value(receipt)?).execute(c).await?;
    Ok(())
}
pub async fn expired(c: &mut PgConnection, limit: i64) -> Result<Vec<Uuid>, AppError> {
    Ok(sqlx::query_scalar("SELECT id FROM checkouts WHERE status IN('awaiting_authorization','awaiting_capture','awaiting_order_confirmation') AND expires_at<=clock_timestamp() ORDER BY expires_at,id LIMIT $1 FOR UPDATE SKIP LOCKED").bind(limit).fetch_all(c).await?)
}
