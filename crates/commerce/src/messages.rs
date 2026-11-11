use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use uuid::Uuid;
pub type TraceContext = BTreeMap<String, String>;
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateCart {
    pub customer_id: Uuid,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AddCartItem {
    pub sku: String,
    pub quantity: i32,
    pub expected_version: i64,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoveCartItem {
    pub sku: String,
    pub expected_version: i64,
}
// No Debug: this command contains an opaque payment-method token.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckoutCommand {
    pub idempotency_key: Uuid,
    pub cart_id: Uuid,
    pub expected_cart_version: i64,
    pub payment_method_token: String,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommerceEvent {
    pub event_id: Uuid,
    pub aggregate_id: Uuid,
    pub checkout_id: Uuid,
    pub event_type: String,
    #[serde(deserialize_with = "version_one")]
    pub schema_version: u16,
    pub data: serde_json::Value,
    pub trace_context: TraceContext,
    pub occurred_at: DateTime<Utc>,
}
fn version_one<'de, D: serde::Deserializer<'de>>(d: D) -> Result<u16, D::Error> {
    let v = u16::deserialize(d)?;
    if v != 1 {
        return Err(serde::de::Error::custom("unsupported event schema"));
    }
    Ok(v)
}
impl CommerceEvent {
    pub fn validate(&self) -> Result<(), shared_kernel::DomainError> {
        if self.schema_version != 1
            || !matches!(
                self.event_type.as_str(),
                "checkout.started"
                    | "payment.authorization_requested"
                    | "payment.capture_requested"
                    | "payment.void_requested"
                    | "payment.refund_requested"
                    | "payment.authorized"
                    | "payment.captured"
                    | "payment.voided"
                    | "payment.refunded"
                    | "payment.failed"
                    | "payment.compensation_failed"
                    | "order.confirmation_requested"
                    | "order.changed"
            )
        {
            return Err(shared_kernel::DomainError(
                "unsupported event type or version".into(),
            ));
        }
        if [self.event_id, self.aggregate_id, self.checkout_id]
            .iter()
            .any(Uuid::is_nil)
            || !self.data.is_object()
            || self.trace_context.len() > 16
            || self
                .trace_context
                .iter()
                .any(|(k, v)| k.len() > 64 || v.len() > 1024)
        {
            return Err(shared_kernel::DomainError("invalid event envelope".into()));
        }
        Ok(())
    }
}
#[derive(Debug, Clone)]
pub struct OutboxMessage {
    pub id: Uuid,
    pub aggregate_id: Uuid,
    pub payload: serde_json::Value,
    pub attempts: i32,
    pub lease_token: Uuid,
}
