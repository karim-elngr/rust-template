//! Independent order aggregate. Confirmation is separate from payment capture.
use serde::{Deserialize, Serialize};
use shared_kernel::DomainError;
use uuid::Uuid;
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderStatus {
    PendingPayment,
    Confirmed,
    Cancelled,
}
impl OrderStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PendingPayment => "pending_payment",
            Self::Confirmed => "confirmed",
            Self::Cancelled => "cancelled",
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderLine {
    pub sku: String,
    pub quantity: i32,
    pub unit_price_minor: i64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    id: Uuid,
    checkout_id: Uuid,
    cart_id: Uuid,
    customer_id: Uuid,
    payment_id: Uuid,
    status: OrderStatus,
    items: Vec<OrderLine>,
    total_minor: i64,
}
impl Order {
    pub fn new(
        id: Uuid,
        checkout_id: Uuid,
        cart_id: Uuid,
        customer_id: Uuid,
        payment_id: Uuid,
        items: Vec<OrderLine>,
    ) -> Result<Self, DomainError> {
        let mut order = Self {
            id,
            checkout_id,
            cart_id,
            customer_id,
            payment_id,
            status: OrderStatus::PendingPayment,
            items,
            total_minor: 0,
        };
        order.total_minor = order.sum()?;
        order.validate()?;
        Ok(order)
    }
    pub fn id(&self) -> Uuid {
        self.id
    }
    pub fn checkout_id(&self) -> Uuid {
        self.checkout_id
    }
    pub fn cart_id(&self) -> Uuid {
        self.cart_id
    }
    pub fn customer_id(&self) -> Uuid {
        self.customer_id
    }
    pub fn payment_id(&self) -> Uuid {
        self.payment_id
    }
    pub fn status(&self) -> OrderStatus {
        self.status
    }
    pub fn items(&self) -> &[OrderLine] {
        &self.items
    }
    pub fn total_minor(&self) -> i64 {
        self.total_minor
    }
    pub fn confirm(&mut self) -> Result<(), DomainError> {
        if self.status == OrderStatus::Cancelled {
            return Err(DomainError("cancelled order cannot be confirmed".into()));
        }
        self.status = OrderStatus::Confirmed;
        Ok(())
    }
    pub fn cancel(&mut self) -> Result<(), DomainError> {
        if self.status == OrderStatus::Confirmed {
            return Err(DomainError(
                "confirmed order cancellation requires a separate returns policy".into(),
            ));
        }
        self.status = OrderStatus::Cancelled;
        Ok(())
    }
    fn sum(&self) -> Result<i64, DomainError> {
        self.items.iter().try_fold(0_i64, |sum, line| {
            if line.quantity <= 0 || line.unit_price_minor <= 0 {
                return Err(DomainError("invalid order line".into()));
            }
            line.unit_price_minor
                .checked_mul(i64::from(line.quantity))
                .and_then(|amount| sum.checked_add(amount))
                .ok_or_else(|| DomainError("order amount exceeds supported range".into()))
        })
    }
    pub fn validate(&self) -> Result<(), DomainError> {
        if [
            self.id,
            self.checkout_id,
            self.cart_id,
            self.customer_id,
            self.payment_id,
        ]
        .iter()
        .any(Uuid::is_nil)
            || self.items.is_empty()
            || self.items.len() > 100
            || self.sum()? != self.total_minor
            || self.total_minor <= 0
        {
            return Err(DomainError("invalid order snapshot".into()));
        }
        let mut seen = std::collections::BTreeSet::new();
        for line in &self.items {
            if line.sku.is_empty()
                || line.sku.len() > 64
                || line.sku.chars().any(char::is_whitespace)
                || !seen.insert(&line.sku)
            {
                return Err(DomainError("invalid order SKU".into()));
            }
        }
        Ok(())
    }
}
