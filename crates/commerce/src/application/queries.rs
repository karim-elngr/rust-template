//! Query handlers read sanitized write views or the independently updated projection.
use crate::{
    AppError, CartView, CheckoutView, OrderSummary, OrderView, PaymentView, Store,
    infrastructure::{carts, checkouts, orders, payments},
};
use uuid::Uuid;
impl Store {
    pub async fn get_cart(&self, id: Uuid) -> Result<CartView, AppError> {
        let mut c = self.pool.acquire().await?;
        Ok(CartView::try_from(&carts::load(&mut c, id, false).await?)?)
    }
    pub async fn get_checkout(&self, id: Uuid) -> Result<CheckoutView, AppError> {
        let mut c = self.pool.acquire().await?;
        let s = checkouts::load(&mut c, id, false).await?;
        let p = payments::load(&mut c, s.links().payment_id).await?;
        Ok(CheckoutView::new(&s, &p))
    }
    pub async fn get_order(&self, id: Uuid) -> Result<OrderView, AppError> {
        let mut c = self.pool.acquire().await?;
        let o = orders::load(&mut c, id).await?;
        let s = checkouts::load(&mut c, o.checkout_id(), false).await?;
        Ok(OrderView::new(&o, s.created_at()))
    }
    pub async fn get_payment(&self, id: Uuid) -> Result<PaymentView, AppError> {
        let mut c = self.pool.acquire().await?;
        Ok(PaymentView::from(&payments::load(&mut c, id).await?))
    }
    pub async fn get_order_summary(&self, id: Uuid) -> Result<OrderSummary, AppError> {
        let data: serde_json::Value =
            sqlx::query_scalar("SELECT data FROM order_summaries WHERE order_id=$1")
                .bind(id)
                .fetch_optional(&self.pool)
                .await?
                .ok_or(AppError::NotFound)?;
        Ok(serde_json::from_value(data)?)
    }
}
