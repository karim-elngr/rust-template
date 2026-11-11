use crate::model::operation_key;
use crate::{OperationKind, PaymentOperation, ProviderOutcome};
use async_trait::async_trait;

/// Implementations must bind each key to the full immutable request, reconcile
/// unknown outcomes, and return Retryable for transport failures/timeouts. A
/// definitive failure means that operation did not take effect. Invoke only
/// after committing its intent and closing the database transaction.
#[async_trait]
pub trait PaymentGateway: Send + Sync {
    async fn execute(&self, operation: &PaymentOperation) -> ProviderOutcome;
}

/// Stateless, deterministic teaching adapter. No network, card data, real funds,
/// or claim of a durable remote provider ledger. Real adapters must implement
/// the provider's durable idempotency/reconciliation contract above.
#[derive(Debug, Default, Clone, Copy)]
pub struct DemoGateway;

#[async_trait]
impl PaymentGateway for DemoGateway {
    async fn execute(&self, operation: &PaymentOperation) -> ProviderOutcome {
        let failure = |code: &str| ProviderOutcome::DefinitiveFailure { code: code.into() };
        if operation.payment_id.is_nil()
            || operation.amount.amount_minor() <= 0
            || operation.idempotency_key != operation_key(operation.payment_id, operation.kind)
            || (operation.kind == OperationKind::Authorize)
                != operation.provider_reference.is_none()
        {
            return failure("invalid_operation");
        }
        match operation.payment_method_token.as_str() {
            "pm_demo_decline" => return failure("authorization_declined"),
            "pm_demo_capture_decline" if operation.kind == OperationKind::Capture => {
                return failure("capture_declined");
            }
            "pm_demo_approve" | "pm_demo_capture_decline" => {}
            _ => return failure("unsupported_payment_method"),
        }
        ProviderOutcome::Succeeded {
            provider_reference: format!(
                "demo:{}:{}",
                operation.payment_id,
                operation.kind.as_str()
            ),
            amount: operation.amount.clone(),
        }
    }
}
