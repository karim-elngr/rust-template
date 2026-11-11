use crate::AppError;
use orders::Order;
use sqlx::{PgConnection, Row};
use uuid::Uuid;
pub async fn load(c: &mut PgConnection, id: Uuid) -> Result<Order, AppError> {
    let row = sqlx::query("SELECT checkout_id,customer_id,data FROM orders WHERE id=$1")
        .bind(id)
        .fetch_optional(c)
        .await?
        .ok_or(AppError::NotFound)?;
    let a: Order =
        serde_json::from_value(row.try_get("data")?).map_err(|_| AppError::CorruptData)?;
    a.validate().map_err(|_| AppError::CorruptData)?;
    if a.id() != id
        || a.checkout_id() != row.try_get::<Uuid, _>("checkout_id")?
        || a.customer_id() != row.try_get::<Uuid, _>("customer_id")?
    {
        return Err(AppError::CorruptData);
    }
    Ok(a)
}
pub async fn insert(c: &mut PgConnection, a: &Order) -> Result<(), AppError> {
    sqlx::query("INSERT INTO orders(id,checkout_id,customer_id,data) VALUES($1,$2,$3,$4)")
        .bind(a.id())
        .bind(a.checkout_id())
        .bind(a.customer_id())
        .bind(serde_json::to_value(a)?)
        .execute(c)
        .await?;
    Ok(())
}
pub async fn save(c: &mut PgConnection, a: &Order) -> Result<(), AppError> {
    a.validate()?;
    sqlx::query("UPDATE orders SET data=$2 WHERE id=$1")
        .bind(a.id())
        .bind(serde_json::to_value(a)?)
        .execute(c)
        .await?;
    Ok(())
}
