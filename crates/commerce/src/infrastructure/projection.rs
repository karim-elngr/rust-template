use crate::{AppError, OrderSummary};
use sqlx::PgConnection;
pub async fn apply(c: &mut PgConnection, s: &OrderSummary) -> Result<(), AppError> {
    sqlx::query("INSERT INTO order_summaries(order_id,version,data) VALUES($1,$2,$3) ON CONFLICT(order_id) DO UPDATE SET version=EXCLUDED.version,data=EXCLUDED.data WHERE order_summaries.version<EXCLUDED.version").bind(s.order_id).bind(s.version).bind(serde_json::to_value(s)?).execute(c).await?;
    Ok(())
}
