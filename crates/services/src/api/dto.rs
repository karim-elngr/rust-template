//! HTTP request validation and explicit public response shapes.
use commerce::{
    AddCartItem, CartView, CheckoutCommand, CheckoutReceipt, CheckoutView, CreateCart,
    OrderSummary, OrderView, PaymentView, RemoveCartItem,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CreateCartBody {
    pub customer_id: Uuid,
}
impl From<CreateCartBody> for CreateCart {
    fn from(body: CreateCartBody) -> Self {
        Self {
            customer_id: body.customer_id,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct AddItemBody {
    pub sku: String,
    pub quantity: i32,
    pub expected_version: i64,
}
impl AddItemBody {
    pub fn command(self) -> Result<AddCartItem, &'static str> {
        if self.expected_version < 0 {
            return Err("expected_version must not be negative");
        }
        if self.quantity <= 0 {
            return Err("quantity must be positive");
        }
        Ok(AddCartItem {
            sku: self.sku,
            quantity: self.quantity,
            expected_version: self.expected_version,
        })
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RemoveItemQuery {
    pub expected_version: i64,
}
impl RemoveItemQuery {
    pub fn command(self, sku: String) -> Result<RemoveCartItem, &'static str> {
        if self.expected_version < 0 {
            return Err("expected_version must not be negative");
        }
        Ok(RemoveCartItem {
            sku,
            expected_version: self.expected_version,
        })
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CheckoutBody {
    pub cart_id: Uuid,
    pub expected_cart_version: i64,
    pub payment_method_token: String,
}
impl CheckoutBody {
    pub fn command(self, key: &str) -> Result<CheckoutCommand, &'static str> {
        if self.expected_cart_version < 0 {
            return Err("expected_cart_version must not be negative");
        }
        Ok(CheckoutCommand {
            idempotency_key: Uuid::parse_str(key).map_err(|_| "Idempotency-Key must be a UUID")?,
            cart_id: self.cart_id,
            expected_cart_version: self.expected_cart_version,
            payment_method_token: self.payment_method_token,
        })
    }
}

#[derive(Serialize)]
pub(super) struct ItemBody {
    pub sku: String,
    pub quantity: i32,
    pub unit_price_minor: i64,
}
#[derive(Serialize)]
pub(super) struct CartBody {
    pub cart_id: Uuid,
    pub customer_id: Uuid,
    pub status: &'static str,
    pub version: i64,
    pub items: Vec<ItemBody>,
    pub total_minor: i64,
    pub currency: String,
}
impl From<CartView> for CartBody {
    fn from(value: CartView) -> Self {
        Self {
            cart_id: value.id,
            customer_id: value.customer_id,
            status: value.status.as_str(),
            version: value.version,
            items: value
                .items
                .into_iter()
                .map(|item| ItemBody {
                    sku: item.sku,
                    quantity: item.quantity,
                    unit_price_minor: item.unit_price_minor,
                })
                .collect(),
            total_minor: value.total_minor,
            currency: value.currency,
        }
    }
}
#[derive(Serialize)]
pub(super) struct ReceiptBody {
    pub checkout_id: Uuid,
    pub cart_id: Uuid,
    pub order_id: Uuid,
    pub payment_id: Uuid,
    pub status: &'static str,
    pub total_minor: i64,
    pub currency: String,
}
impl From<CheckoutReceipt> for ReceiptBody {
    fn from(value: CheckoutReceipt) -> Self {
        Self {
            checkout_id: value.checkout_id,
            cart_id: value.cart_id,
            order_id: value.order_id,
            payment_id: value.payment_id,
            status: value.status.as_str(),
            total_minor: value.total_minor,
            currency: value.currency,
        }
    }
}
#[derive(Serialize)]
pub(super) struct CheckoutViewBody {
    pub checkout_id: Uuid,
    pub cart_id: Uuid,
    pub order_id: Uuid,
    pub payment_id: Uuid,
    pub status: &'static str,
    pub total_minor: i64,
    pub currency: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub expires_at: chrono::DateTime<chrono::Utc>,
}
impl From<CheckoutView> for CheckoutViewBody {
    fn from(value: CheckoutView) -> Self {
        Self {
            checkout_id: value.id,
            cart_id: value.cart_id,
            order_id: value.order_id,
            payment_id: value.payment_id,
            status: value.status.as_str(),
            total_minor: value.total_minor,
            currency: value.currency,
            created_at: value.created_at,
            expires_at: value.expires_at,
        }
    }
}
#[derive(Serialize)]
pub(super) struct OrderBody {
    pub order_id: Uuid,
    pub checkout_id: Uuid,
    pub cart_id: Uuid,
    pub customer_id: Uuid,
    pub payment_id: Uuid,
    pub status: &'static str,
    pub items: Vec<ItemBody>,
    pub total_minor: i64,
    pub currency: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
}
impl From<OrderView> for OrderBody {
    fn from(value: OrderView) -> Self {
        Self {
            order_id: value.id,
            checkout_id: value.checkout_id,
            cart_id: value.cart_id,
            customer_id: value.customer_id,
            payment_id: value.payment_id,
            status: value.status.as_str(),
            items: value
                .items
                .into_iter()
                .map(|item| ItemBody {
                    sku: item.sku,
                    quantity: item.quantity,
                    unit_price_minor: item.unit_price_minor,
                })
                .collect(),
            total_minor: value.total_minor,
            currency: value.currency,
            created_at: value.created_at,
        }
    }
}
#[derive(Serialize)]
pub(super) struct PaymentBody {
    pub payment_id: Uuid,
    pub order_id: Uuid,
    pub checkout_id: Uuid,
    pub status: &'static str,
    pub total_minor: i64,
    pub currency: String,
    pub cancellation_requested: bool,
}
impl From<PaymentView> for PaymentBody {
    fn from(value: PaymentView) -> Self {
        Self {
            payment_id: value.id,
            order_id: value.order_id,
            checkout_id: value.checkout_id,
            status: value.status.as_str(),
            total_minor: value.total_minor,
            currency: value.currency,
            cancellation_requested: value.cancellation_requested,
        }
    }
}
#[derive(Serialize)]
pub(super) struct OrderSummaryBody {
    pub order_id: Uuid,
    pub customer_id: Uuid,
    pub status: &'static str,
    pub payment_status: String,
    pub total_minor: i64,
    pub currency: String,
    pub version: i64,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}
impl From<OrderSummary> for OrderSummaryBody {
    fn from(value: OrderSummary) -> Self {
        Self {
            order_id: value.order_id,
            customer_id: value.customer_id,
            status: value.status.as_str(),
            payment_status: value.payment_status,
            total_minor: value.total_minor,
            currency: value.currency,
            version: value.version,
            updated_at: value.updated_at,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn clients_cannot_supply_the_price_or_checkout_line_items() {
        assert!(serde_json::from_value::<AddItemBody>(serde_json::json!({"sku":"BOOK","quantity":1,"expected_version":0,"unit_price_minor":1})).is_err());
        assert!(serde_json::from_value::<CheckoutBody>(serde_json::json!({"cart_id":Uuid::new_v4(),"expected_cart_version":1,"payment_method_token":"pm_demo_approve","items":[]})).is_err());
    }
    #[test]
    fn malformed_idempotency_and_versions_fail_before_database() {
        let body = CheckoutBody {
            cart_id: Uuid::new_v4(),
            expected_cart_version: 1,
            payment_method_token: "pm_demo_approve".into(),
        };
        assert!(body.command("bad-key").is_err());
        assert!(
            AddItemBody {
                sku: "BOOK".into(),
                quantity: 1,
                expected_version: -1
            }
            .command()
            .is_err()
        );
    }
}
