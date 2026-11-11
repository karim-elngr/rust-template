use serde::{Deserialize, Serialize};
use shared_kernel::{DomainError, Money};
use uuid::Uuid;

use crate::model::operation_key;
use crate::{OperationKind, PaymentOperation, PaymentProgress, PaymentStatus, ProviderOutcome};

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "PaymentSnapshot")]
pub struct Payment {
    id: Uuid,
    order_id: Uuid,
    checkout_id: Uuid,
    amount: Money,
    payment_method_token: String,
    status: PaymentStatus,
    cancellation_requested: bool,
    pending_operation: Option<PaymentOperation>,
    completed_operations: Vec<CompletedOperation>,
    authorization_reference: Option<String>,
    capture_reference: Option<String>,
    failure_code: Option<String>,
}

impl std::fmt::Debug for Payment {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Payment")
            .field("id", &self.id)
            .field("status", &self.status)
            .field("amount", &self.amount)
            .field("cancellation_requested", &self.cancellation_requested)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CompletedOperation {
    operation: PaymentOperation,
    outcome: ProviderOutcome,
}

impl Payment {
    pub fn new(
        id: Uuid,
        order_id: Uuid,
        checkout_id: Uuid,
        amount: Money,
        payment_method_token: impl Into<String>,
    ) -> Result<Self, DomainError> {
        if id.is_nil()
            || order_id.is_nil()
            || checkout_id.is_nil()
            || id == order_id
            || id == checkout_id
            || order_id == checkout_id
        {
            return Err(DomainError(
                "payment, order and checkout require distinct non-nil identities".into(),
            ));
        }
        if amount.amount_minor() <= 0 {
            return Err(DomainError("payment amount must be positive".into()));
        }
        let payment_method_token = payment_method_token.into();
        if !valid_method_token(&payment_method_token) {
            return Err(DomainError(
                "payment method must be an opaque pm_ reference, never card data".into(),
            ));
        }
        Ok(Self {
            id,
            order_id,
            checkout_id,
            amount,
            payment_method_token,
            status: PaymentStatus::Created,
            cancellation_requested: false,
            pending_operation: None,
            completed_operations: Vec::new(),
            authorization_reference: None,
            capture_reference: None,
            failure_code: None,
        })
    }
    pub fn id(&self) -> Uuid {
        self.id
    }
    pub fn order_id(&self) -> Uuid {
        self.order_id
    }
    pub fn checkout_id(&self) -> Uuid {
        self.checkout_id
    }
    pub fn amount(&self) -> &Money {
        &self.amount
    }
    pub fn status(&self) -> PaymentStatus {
        self.status
    }
    pub fn cancellation_requested(&self) -> bool {
        self.cancellation_requested
    }
    pub fn pending_operation(&self) -> Option<&PaymentOperation> {
        self.pending_operation.as_ref()
    }
    pub fn failure_code(&self) -> Option<&str> {
        self.failure_code.as_deref()
    }

    /// Persist the returned intent and its outbox reference in the same unit of
    /// work. Only then may an application worker call the gateway outside a tx.
    pub fn begin_authorization(&mut self) -> Result<Option<PaymentOperation>, DomainError> {
        if self.has_completed(OperationKind::Authorize) || self.status == PaymentStatus::Cancelled {
            return Ok(None);
        }
        if self.status == PaymentStatus::AuthorizationPending {
            return Ok(self.pending_operation.clone());
        }
        if self.status != PaymentStatus::Created {
            return Err(self.invalid_transition("authorize"));
        }
        self.prepare_operation(OperationKind::Authorize).map(Some)
    }

    /// Full capture is separate from authorization. Partial/multiple captures
    /// require a different model and operation-key sequence, not this method.
    pub fn begin_capture(&mut self) -> Result<Option<PaymentOperation>, DomainError> {
        if self.has_completed(OperationKind::Capture) {
            return Ok(None);
        }
        if self.status == PaymentStatus::CapturePending {
            return Ok(self.pending_operation.clone());
        }
        if self.status != PaymentStatus::Authorized || self.cancellation_requested {
            return Err(self.invalid_transition("capture"));
        }
        self.prepare_operation(OperationKind::Capture).map(Some)
    }

    /// Retain in-flight work until its outcome is known. A later authorization
    /// success schedules void; a later capture success schedules full refund.
    /// The application independently decides whether the checkout may be cancelled.
    pub fn request_cancel(&mut self) -> Result<Option<PaymentOperation>, DomainError> {
        if self.cancellation_requested {
            return Ok(self.pending_operation.clone());
        }
        let mut next = self.clone();
        next.cancellation_requested = true;
        match next.status {
            PaymentStatus::Created | PaymentStatus::Declined => {
                next.status = PaymentStatus::Cancelled
            }
            PaymentStatus::Authorized => {
                next.prepare_operation(OperationKind::Void)?;
            }
            PaymentStatus::Captured => {
                next.prepare_operation(OperationKind::Refund)?;
            }
            PaymentStatus::AuthorizationPending
            | PaymentStatus::CapturePending
            | PaymentStatus::VoidPending
            | PaymentStatus::RefundPending
            | PaymentStatus::Voided
            | PaymentStatus::Refunded
            | PaymentStatus::Cancelled
            | PaymentStatus::NeedsAttention => {}
        }
        let operation = next.pending_operation.clone();
        *self = next;
        Ok(operation)
    }

    /// Apply to the freshly reloaded aggregate under the application's row lock.
    /// A stale worker must not overwrite a cancellation committed during I/O.
    pub fn apply_result(
        &mut self,
        operation: &PaymentOperation,
        outcome: ProviderOutcome,
    ) -> Result<PaymentProgress, DomainError> {
        if let Some(completed) = self
            .completed_operations
            .iter()
            .find(|completed| completed.operation.idempotency_key == operation.idempotency_key)
        {
            if completed.operation != *operation {
                return Err(DomainError(
                    "provider operation key was reused with different input".into(),
                ));
            }
            validate_outcome(&outcome, &self.amount)?;
            if completed.outcome == outcome || matches!(outcome, ProviderOutcome::Retryable { .. })
            {
                return Ok(PaymentProgress::Replay);
            }
            return Err(DomainError(
                "provider returned contradictory definitive outcomes".into(),
            ));
        }
        if self.pending_operation.as_ref() != Some(operation) {
            return Err(DomainError(
                "provider result does not match the durable pending operation".into(),
            ));
        }
        validate_outcome(&outcome, &self.amount)?;
        if matches!(outcome, ProviderOutcome::Retryable { .. }) {
            return Ok(PaymentProgress::RetryPending);
        }
        let mut next = self.clone();
        next.pending_operation = None;
        next.completed_operations.push(CompletedOperation {
            operation: operation.clone(),
            outcome: outcome.clone(),
        });
        match outcome {
            ProviderOutcome::Succeeded {
                provider_reference, ..
            } => match operation.kind {
                OperationKind::Authorize => {
                    next.authorization_reference = Some(provider_reference);
                    next.status = PaymentStatus::Authorized;
                    if next.cancellation_requested {
                        next.prepare_operation(OperationKind::Void)?;
                    }
                }
                OperationKind::Capture => {
                    next.capture_reference = Some(provider_reference);
                    next.status = PaymentStatus::Captured;
                    if next.cancellation_requested {
                        next.prepare_operation(OperationKind::Refund)?;
                    }
                }
                OperationKind::Void => next.status = PaymentStatus::Voided,
                OperationKind::Refund => next.status = PaymentStatus::Refunded,
            },
            ProviderOutcome::DefinitiveFailure { code } => {
                next.failure_code = Some(code);
                match operation.kind {
                    OperationKind::Authorize => {
                        next.status = if next.cancellation_requested {
                            PaymentStatus::Cancelled
                        } else {
                            PaymentStatus::Declined
                        };
                    }
                    OperationKind::Capture => {
                        // A declined capture still leaves the authorization hold.
                        next.cancellation_requested = true;
                        next.prepare_operation(OperationKind::Void)?;
                    }
                    OperationKind::Void | OperationKind::Refund => {
                        next.status = PaymentStatus::NeedsAttention
                    }
                }
            }
            ProviderOutcome::Retryable { .. } => unreachable!("retryable result returned above"),
        }
        *self = next;
        Ok(PaymentProgress::Applied)
    }

    fn has_completed(&self, kind: OperationKind) -> bool {
        self.completed_operations
            .iter()
            .any(|completed| completed.operation.kind == kind)
    }

    fn invalid_transition(&self, action: &str) -> DomainError {
        DomainError(format!(
            "cannot {action} a payment in {} state",
            self.status.as_str()
        ))
    }

    fn prepare_operation(&mut self, kind: OperationKind) -> Result<PaymentOperation, DomainError> {
        let provider_reference = match kind {
            OperationKind::Authorize => None,
            OperationKind::Capture | OperationKind::Void => Some(
                self.authorization_reference
                    .clone()
                    .ok_or_else(|| DomainError("authorization reference missing".into()))?,
            ),
            OperationKind::Refund => Some(
                self.capture_reference
                    .clone()
                    .ok_or_else(|| DomainError("capture reference missing".into()))?,
            ),
        };
        let operation = PaymentOperation {
            payment_id: self.id,
            order_id: self.order_id,
            checkout_id: self.checkout_id,
            kind,
            idempotency_key: operation_key(self.id, kind),
            amount: self.amount.clone(),
            payment_method_token: self.payment_method_token.clone(),
            provider_reference,
        };
        self.status = match kind {
            OperationKind::Authorize => PaymentStatus::AuthorizationPending,
            OperationKind::Capture => PaymentStatus::CapturePending,
            OperationKind::Void => PaymentStatus::VoidPending,
            OperationKind::Refund => PaymentStatus::RefundPending,
        };
        self.pending_operation = Some(operation.clone());
        Ok(operation)
    }
}

fn valid_method_token(token: &str) -> bool {
    token.starts_with("pm_")
        && (4..=128).contains(&token.len())
        && token
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
}

fn validate_outcome(outcome: &ProviderOutcome, expected: &Money) -> Result<(), DomainError> {
    match outcome {
        ProviderOutcome::Succeeded {
            provider_reference,
            amount,
        } => {
            if amount != expected {
                return Err(DomainError(
                    "provider amount or currency differs from the payment".into(),
                ));
            }
            if provider_reference.is_empty()
                || provider_reference.len() > 256
                || !provider_reference
                    .bytes()
                    .all(|byte| byte.is_ascii_graphic())
            {
                return Err(DomainError(
                    "provider reference is missing or malformed".into(),
                ));
            }
        }
        ProviderOutcome::DefinitiveFailure { code } | ProviderOutcome::Retryable { code } => {
            if code.is_empty()
                || code.len() > 64
                || !code
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
            {
                return Err(DomainError(
                    "provider outcome requires a bounded non-sensitive reason code".into(),
                ));
            }
        }
    }
    Ok(())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PaymentSnapshot {
    id: Uuid,
    order_id: Uuid,
    checkout_id: Uuid,
    amount: Money,
    payment_method_token: String,
    status: PaymentStatus,
    cancellation_requested: bool,
    pending_operation: Option<PaymentOperation>,
    completed_operations: Vec<CompletedOperation>,
    authorization_reference: Option<String>,
    capture_reference: Option<String>,
    failure_code: Option<String>,
}

impl TryFrom<PaymentSnapshot> for Payment {
    type Error = DomainError;

    fn try_from(snapshot: PaymentSnapshot) -> Result<Self, Self::Error> {
        if snapshot.completed_operations.len() > 4 {
            return Err(DomainError(
                "payment has too many completed operations".into(),
            ));
        }
        let candidate = Self {
            id: snapshot.id,
            order_id: snapshot.order_id,
            checkout_id: snapshot.checkout_id,
            amount: snapshot.amount,
            payment_method_token: snapshot.payment_method_token,
            status: snapshot.status,
            cancellation_requested: snapshot.cancellation_requested,
            pending_operation: snapshot.pending_operation,
            completed_operations: snapshot.completed_operations,
            authorization_reference: snapshot.authorization_reference,
            capture_reference: snapshot.capture_reference,
            failure_code: snapshot.failure_code,
        };
        let mut validated = Self::new(
            candidate.id,
            candidate.order_id,
            candidate.checkout_id,
            candidate.amount.clone(),
            candidate.payment_method_token.clone(),
        )?;
        // Reconstruct the bounded provider history using the same rules as live
        // commands. This checks correlations, amounts, keys, references, ordering
        // and terminal states rather than trusting serialized private fields.
        for completed in &candidate.completed_operations {
            let expected = match completed.operation.kind {
                OperationKind::Authorize => validated.begin_authorization()?,
                OperationKind::Capture => validated.begin_capture()?,
                OperationKind::Void | OperationKind::Refund => validated.request_cancel()?,
            };
            if expected.as_ref() != Some(&completed.operation)
                || validated.apply_result(&completed.operation, completed.outcome.clone())?
                    != PaymentProgress::Applied
            {
                return Err(DomainError(
                    "payment completion history is inconsistent".into(),
                ));
            }
        }
        if let Some(pending) = &candidate.pending_operation {
            let expected = match pending.kind {
                OperationKind::Authorize => validated.begin_authorization()?,
                OperationKind::Capture => validated.begin_capture()?,
                OperationKind::Void | OperationKind::Refund => validated.request_cancel()?,
            };
            if expected.as_ref() != Some(pending) {
                return Err(DomainError(
                    "payment pending operation is inconsistent".into(),
                ));
            }
        }
        if candidate.cancellation_requested {
            validated.request_cancel()?;
        }
        if candidate != validated {
            return Err(DomainError(
                "payment snapshot violates its lifecycle invariants".into(),
            ));
        }
        Ok(validated)
    }
}
