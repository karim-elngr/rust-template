use cart::{Cart, CartLine, CartStatus};
use checkout::{CheckoutSaga, CheckoutStatus};
use chrono::{DateTime, Utc};
use orders::{Order, OrderLine, OrderStatus};
use payments::{Payment, PaymentStatus};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CartView {
    pub id: Uuid,
    pub customer_id: Uuid,
    pub version: i64,
    pub status: CartStatus,
    pub items: Vec<CartLine>,
    pub total_minor: i64,
    pub currency: String,
}
impl TryFrom<&Cart> for CartView {
    type Error = shared_kernel::DomainError;
    fn try_from(c: &Cart) -> Result<Self, Self::Error> {
        Ok(Self {
            id: c.id(),
            customer_id: c.customer_id(),
            version: c.version(),
            status: c.status(),
            items: c.items().to_vec(),
            total_minor: c.total_minor()?,
            currency: "USD".into(),
        })
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckoutReceipt {
    pub checkout_id: Uuid,
    pub cart_id: Uuid,
    pub order_id: Uuid,
    pub payment_id: Uuid,
    pub status: CheckoutStatus,
    pub total_minor: i64,
    pub currency: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckoutView {
    pub id: Uuid,
    pub cart_id: Uuid,
    pub order_id: Uuid,
    pub payment_id: Uuid,
    pub status: CheckoutStatus,
    pub total_minor: i64,
    pub currency: String,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}
impl CheckoutView {
    pub(crate) fn new(s: &CheckoutSaga, p: &Payment) -> Self {
        Self {
            id: s.links().id,
            cart_id: s.links().cart_id,
            order_id: s.links().order_id,
            payment_id: s.links().payment_id,
            status: s.status(),
            total_minor: p.amount().amount_minor(),
            currency: p.amount().currency().into(),
            created_at: s.created_at(),
            expires_at: s.expires_at(),
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderView {
    pub id: Uuid,
    pub checkout_id: Uuid,
    pub cart_id: Uuid,
    pub customer_id: Uuid,
    pub payment_id: Uuid,
    pub status: OrderStatus,
    pub items: Vec<OrderLine>,
    pub total_minor: i64,
    pub currency: String,
    pub created_at: DateTime<Utc>,
}
impl OrderView {
    pub(crate) fn new(o: &Order, created_at: DateTime<Utc>) -> Self {
        Self {
            id: o.id(),
            checkout_id: o.checkout_id(),
            cart_id: o.cart_id(),
            customer_id: o.customer_id(),
            payment_id: o.payment_id(),
            status: o.status(),
            items: o.items().to_vec(),
            total_minor: o.total_minor(),
            currency: "USD".into(),
            created_at,
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaymentView {
    pub id: Uuid,
    pub order_id: Uuid,
    pub checkout_id: Uuid,
    pub status: PaymentStatus,
    pub total_minor: i64,
    pub currency: String,
    pub cancellation_requested: bool,
}
impl From<&Payment> for PaymentView {
    fn from(p: &Payment) -> Self {
        Self {
            id: p.id(),
            order_id: p.order_id(),
            checkout_id: p.checkout_id(),
            status: p.status(),
            total_minor: p.amount().amount_minor(),
            currency: p.amount().currency().into(),
            cancellation_requested: p.cancellation_requested(),
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderSummary {
    pub order_id: Uuid,
    pub customer_id: Uuid,
    pub status: OrderStatus,
    pub payment_status: String,
    pub total_minor: i64,
    pub currency: String,
    pub version: i64,
    pub updated_at: DateTime<Utc>,
}
