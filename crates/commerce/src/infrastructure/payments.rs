use crate::AppError;
use payments::{OperationKind, Payment, PaymentOperation, ProviderOutcome};
use sqlx::{PgConnection, Row};
use uuid::Uuid;
pub async fn load(c: &mut PgConnection, id: Uuid) -> Result<Payment, AppError> {
    let row = sqlx::query("SELECT order_id,checkout_id,data FROM payments WHERE id=$1")
        .bind(id)
        .fetch_optional(c)
        .await?
        .ok_or(AppError::NotFound)?;
    let payment: Payment =
        serde_json::from_value(row.try_get("data")?).map_err(|_| AppError::CorruptData)?;
    if payment.id() != id
        || payment.order_id() != row.try_get::<Uuid, _>("order_id")?
        || payment.checkout_id() != row.try_get::<Uuid, _>("checkout_id")?
    {
        return Err(AppError::CorruptData);
    }
    Ok(payment)
}
pub async fn insert(c: &mut PgConnection, a: &Payment) -> Result<(), AppError> {
    sqlx::query("INSERT INTO payments(id,order_id,checkout_id,data) VALUES($1,$2,$3,$4)")
        .bind(a.id())
        .bind(a.order_id())
        .bind(a.checkout_id())
        .bind(serde_json::to_value(a)?)
        .execute(c)
        .await?;
    Ok(())
}
pub async fn save(c: &mut PgConnection, a: &Payment) -> Result<(), AppError> {
    sqlx::query("UPDATE payments SET data=$2 WHERE id=$1")
        .bind(a.id())
        .bind(serde_json::to_value(a)?)
        .execute(c)
        .await?;
    Ok(())
}
pub async fn ensure_operation(c: &mut PgConnection, op: &PaymentOperation) -> Result<(), AppError> {
    let request = serde_json::to_value(op)?;
    sqlx::query("INSERT INTO payment_operations(operation_key,payment_id,kind,status,request) VALUES($1,$2,$3,'pending',$4) ON CONFLICT DO NOTHING").bind(&op.idempotency_key).bind(op.payment_id).bind(op.kind.as_str()).bind(&request).execute(&mut *c).await?;
    let stored: serde_json::Value = sqlx::query_scalar(
        "SELECT request FROM payment_operations WHERE payment_id=$1 AND kind=$2",
    )
    .bind(op.payment_id)
    .bind(op.kind.as_str())
    .fetch_one(c)
    .await?;
    if stored != request {
        return Err(AppError::InvalidState(
            "provider operation identity changed".into(),
        ));
    }
    Ok(())
}
pub async fn operation(
    c: &mut PgConnection,
    payment_id: Uuid,
    kind: OperationKind,
) -> Result<(PaymentOperation, bool), AppError> {
    let row = sqlx::query(
        "SELECT operation_key,request,status FROM payment_operations WHERE payment_id=$1 AND kind=$2",
    )
    .bind(payment_id)
    .bind(kind.as_str())
    .fetch_optional(c)
    .await?
    .ok_or_else(|| AppError::InvalidEvent("provider operation not persisted".into()))?;
    let operation: PaymentOperation =
        serde_json::from_value(row.try_get("request")?).map_err(|_| AppError::CorruptData)?;
    if operation.payment_id != payment_id
        || operation.kind != kind
        || operation.idempotency_key != row.try_get::<String, _>("operation_key")?
    {
        return Err(AppError::CorruptData);
    }
    Ok((
        operation,
        row.try_get::<String, _>("status")? == "completed",
    ))
}
pub async fn record_attempt(c: &mut PgConnection, op: &PaymentOperation) -> Result<(), AppError> {
    sqlx::query("UPDATE payment_operations SET attempts=attempts+1,last_attempt_at=clock_timestamp() WHERE operation_key=$1 AND status='pending'").bind(&op.idempotency_key).execute(c).await?;
    Ok(())
}
pub async fn record_result(
    c: &mut PgConnection,
    op: &PaymentOperation,
    outcome: &ProviderOutcome,
    completed: bool,
) -> Result<(), AppError> {
    sqlx::query("UPDATE payment_operations SET status=$2,response=$3,completed_at=CASE WHEN $2='completed' THEN clock_timestamp() ELSE completed_at END WHERE operation_key=$1").bind(&op.idempotency_key).bind(if completed{"completed"}else{"pending"}).bind(serde_json::to_value(outcome)?).execute(c).await?;
    Ok(())
}
