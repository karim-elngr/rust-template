use serde::{Deserialize, Serialize};
use shared_kernel::Money;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PaymentStatus {
    Created,
    AuthorizationPending,
    Authorized,
    CapturePending,
    Captured,
    VoidPending,
    Voided,
    RefundPending,
    Refunded,
    Declined,
    Cancelled,
    NeedsAttention,
}

impl PaymentStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::AuthorizationPending => "authorization_pending",
            Self::Authorized => "authorized",
            Self::CapturePending => "capture_pending",
            Self::Captured => "captured",
            Self::VoidPending => "void_pending",
            Self::Voided => "voided",
            Self::RefundPending => "refund_pending",
            Self::Refunded => "refunded",
            Self::Declined => "declined",
            Self::Cancelled => "cancelled",
            Self::NeedsAttention => "needs_attention",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationKind {
    Authorize,
    Capture,
    Void,
    Refund,
}

impl OperationKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Authorize => "authorize",
            Self::Capture => "capture",
            Self::Void => "void",
            Self::Refund => "refund",
        }
    }
}

/// Internal durable provider request. Persist it with the aggregate, never put
/// this complete object in public views, logs, or integration-event envelopes.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PaymentOperation {
    pub payment_id: Uuid,
    pub order_id: Uuid,
    pub checkout_id: Uuid,
    pub kind: OperationKind,
    pub idempotency_key: String,
    pub amount: Money,
    pub payment_method_token: String,
    pub provider_reference: Option<String>,
}

impl std::fmt::Debug for PaymentOperation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PaymentOperation")
            .field("payment_id", &self.payment_id)
            .field("kind", &self.kind)
            .field("amount", &self.amount)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProviderOutcome {
    Succeeded {
        provider_reference: String,
        amount: Money,
    },
    DefinitiveFailure {
        code: String,
    },
    /// The provider may already have performed the operation. Reconcile or
    /// retry with the SAME idempotency key; never infer a definitive decline.
    Retryable {
        code: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PaymentProgress {
    Applied,
    Replay,
    RetryPending,
}

pub(crate) fn operation_key(payment_id: Uuid, kind: OperationKind) -> String {
    format!("payment:{payment_id}:{}:v1", kind.as_str())
}
