use crate::AppError;
use cart::Cart;
use sqlx::{PgConnection, Row};
use uuid::Uuid;
pub async fn load(c: &mut PgConnection, id: Uuid, lock: bool) -> Result<Cart, AppError> {
    let query = if lock {
        "SELECT customer_id,version,status,data FROM carts WHERE id=$1 FOR UPDATE"
    } else {
        "SELECT customer_id,version,status,data FROM carts WHERE id=$1"
    };
    let row = sqlx::query(query)
        .bind(id)
        .fetch_optional(c)
        .await?
        .ok_or(AppError::NotFound)?;
    let cart: Cart =
        serde_json::from_value(row.try_get("data")?).map_err(|_| AppError::CorruptData)?;
    cart.validate().map_err(|_| AppError::CorruptData)?;
    if cart.id() != id
        || cart.customer_id() != row.try_get::<Uuid, _>("customer_id")?
        || cart.version() != row.try_get::<i64, _>("version")?
        || cart.status().as_str() != row.try_get::<String, _>("status")?
    {
        return Err(AppError::CorruptData);
    }
    Ok(cart)
}
pub async fn insert(c: &mut PgConnection, a: &Cart) -> Result<(), AppError> {
    sqlx::query("INSERT INTO carts(id,customer_id,version,status,data) VALUES($1,$2,$3,$4,$5)")
        .bind(a.id())
        .bind(a.customer_id())
        .bind(a.version())
        .bind(a.status().as_str())
        .bind(serde_json::to_value(a)?)
        .execute(c)
        .await?;
    Ok(())
}
pub async fn save(c: &mut PgConnection, a: &Cart) -> Result<(), AppError> {
    a.validate()?;
    sqlx::query("UPDATE carts SET version=$2,status=$3,data=$4 WHERE id=$1")
        .bind(a.id())
        .bind(a.version())
        .bind(a.status().as_str())
        .bind(serde_json::to_value(a)?)
        .execute(c)
        .await?;
    Ok(())
}
pub async fn price(c: &mut PgConnection, sku: &str) -> Result<i64, AppError> {
    sqlx::query_scalar(
        "SELECT unit_price_minor FROM catalog WHERE sku=$1 AND active AND currency='USD'",
    )
    .bind(sku)
    .fetch_optional(c)
    .await?
    .ok_or_else(|| AppError::Validation("unknown or unavailable SKU".into()))
}

/// Hold catalog row locks through checkout commit so a quote cannot change after validation.
pub async fn validate_quote(c: &mut PgConnection, cart: &Cart) -> Result<(), AppError> {
    let skus: Vec<&str> = cart.items().iter().map(|line| line.sku.as_str()).collect();
    let rows = sqlx::query("SELECT sku,unit_price_minor,currency,active FROM catalog WHERE sku=ANY($1) ORDER BY sku FOR SHARE")
        .bind(&skus).fetch_all(c).await?;
    let stale =
        || AppError::InvalidState("catalog quote changed; refresh the cart before checkout".into());
    if rows.len() != cart.items().len() {
        return Err(stale());
    }
    for row in rows {
        let sku: String = row.try_get("sku")?;
        let Some(line) = cart.items().iter().find(|line| line.sku == sku) else {
            return Err(stale());
        };
        if !row.try_get::<bool, _>("active")?
            || row.try_get::<String, _>("currency")? != "USD"
            || row.try_get::<i64, _>("unit_price_minor")? != line.unit_price_minor
        {
            return Err(stale());
        }
    }
    Ok(())
}
