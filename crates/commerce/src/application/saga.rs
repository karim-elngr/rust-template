//! Durable event handlers. Provider I/O is strictly between committed transactions.
use crate::{
    infrastructure::{UnitOfWork, checkouts, events, orders, payments, projection},
    *,
};
use ::orders::Order;
use ::payments::{OperationKind, Payment, PaymentProgress, PaymentStatus, ProviderOutcome};
use checkout::{CheckoutSaga, CheckoutStatus};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PaymentRequest {
    payment_id: Uuid,
    kind: OperationKind,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfirmationRequest {
    order_id: Uuid,
}

pub(super) async fn emit(
    w: &mut UnitOfWork,
    s: &CheckoutSaga,
    kind: &str,
    aggregate_id: Uuid,
    data: serde_json::Value,
    trace: &TraceContext,
    dedup: &str,
) -> Result<(), AppError> {
    let event = CommerceEvent {
        event_id: Uuid::new_v4(),
        aggregate_id,
        checkout_id: s.links().id,
        event_type: kind.into(),
        schema_version: 1,
        data,
        trace_context: trace.clone(),
        occurred_at: w.now().await?,
    };
    events::enqueue(w.conn(), &event, dedup).await
}
pub(super) async fn emit_summary(
    w: &mut UnitOfWork,
    s: &CheckoutSaga,
    o: &Order,
    p: &Payment,
    trace: &TraceContext,
) -> Result<(), AppError> {
    let summary = OrderSummary {
        order_id: o.id(),
        customer_id: o.customer_id(),
        status: o.status(),
        payment_status: p.status().as_str().into(),
        total_minor: o.total_minor(),
        currency: "USD".into(),
        version: s.version(),
        updated_at: w.now().await?,
    };
    emit(
        w,
        s,
        "order.changed",
        o.id(),
        serde_json::to_value(summary)?,
        trace,
        &format!("order:{}:projection:{}", o.id(), s.version()),
    )
    .await
}
fn requested(kind: OperationKind) -> &'static str {
    match kind {
        OperationKind::Authorize => "payment.authorization_requested",
        OperationKind::Capture => "payment.capture_requested",
        OperationKind::Void => "payment.void_requested",
        OperationKind::Refund => "payment.refund_requested",
    }
}
fn operation_kind(event_type: &str) -> Option<OperationKind> {
    match event_type {
        "payment.authorization_requested" => Some(OperationKind::Authorize),
        "payment.capture_requested" => Some(OperationKind::Capture),
        "payment.void_requested" => Some(OperationKind::Void),
        "payment.refund_requested" => Some(OperationKind::Refund),
        _ => None,
    }
}
pub(super) async fn enqueue_operation(
    w: &mut UnitOfWork,
    p: &Payment,
    trace: &TraceContext,
) -> Result<(), AppError> {
    if let Some(op) = p.pending_operation() {
        payments::ensure_operation(w.conn(), op).await?;
        let event = CommerceEvent {
            event_id: Uuid::new_v4(),
            aggregate_id: p.id(),
            checkout_id: p.checkout_id(),
            event_type: requested(op.kind).into(),
            schema_version: 1,
            data: serde_json::to_value(PaymentRequest {
                payment_id: p.id(),
                kind: op.kind,
            })?,
            trace_context: trace.clone(),
            occurred_at: w.now().await?,
        };
        events::enqueue(
            w.conn(),
            &event,
            &format!("payment-operation:{}", op.idempotency_key),
        )
        .await?;
    }
    Ok(())
}
async fn persist(
    w: &mut UnitOfWork,
    s: &CheckoutSaga,
    o: &Order,
    p: &Payment,
    trace: &TraceContext,
) -> Result<(), AppError> {
    orders::save(w.conn(), o).await?;
    payments::save(w.conn(), p).await?;
    checkouts::save(w.conn(), s).await?;
    enqueue_operation(w, p, trace).await?;
    emit_summary(w, s, o, p, trace).await
}
fn neutral(status: PaymentStatus) -> bool {
    matches!(
        status,
        PaymentStatus::Voided
            | PaymentStatus::Refunded
            | PaymentStatus::Declined
            | PaymentStatus::Cancelled
    )
}
pub(super) async fn cancel_locked(
    w: &mut UnitOfWork,
    s: &mut CheckoutSaga,
    o: &mut Order,
    p: &mut Payment,
    trace: &TraceContext,
) -> Result<(), AppError> {
    s.cancel().map_err(|e| AppError::InvalidState(e.0))?;
    o.cancel().map_err(|e| AppError::InvalidState(e.0))?;
    p.request_cancel()?;
    if neutral(p.status()) && s.status() == CheckoutStatus::Compensating {
        s.compensated()?;
    }
    persist(w, s, o, p, trace).await
}

impl Store {
    pub async fn apply_event(&self, event: &CommerceEvent) -> Result<ApplyOutcome, AppError> {
        self.apply_event_with_context(event, event.trace_context.clone())
            .await
    }
    #[tracing::instrument(name="commerce.apply_event",skip_all,fields(event_id=%event.event_id,checkout_id=%event.checkout_id))]
    pub async fn apply_event_with_context(
        &self,
        event: &CommerceEvent,
        trace: TraceContext,
    ) -> Result<ApplyOutcome, AppError> {
        event.validate().map_err(|e| AppError::InvalidEvent(e.0))?;
        if let Some(kind) = operation_kind(&event.event_type) {
            return self.apply_provider_request(event, kind, trace).await;
        }
        let mut w = UnitOfWork::begin(&self.pool).await?;
        events::verify(w.conn(), event).await?;
        let mut saga = checkouts::load(w.conn(), event.checkout_id, true).await?;
        if events::seen(w.conn(), event.event_id).await? {
            w.commit().await?;
            return Ok(ApplyOutcome::AlreadyApplied);
        }
        if event.event_type == "order.changed" {
            let summary: OrderSummary = serde_json::from_value(event.data.clone())?;
            if summary.order_id != saga.links().order_id
                || summary.version < 0
                || event.aggregate_id != summary.order_id
            {
                return Err(AppError::InvalidEvent(
                    "invalid order projection identity".into(),
                ));
            }
            projection::apply(w.conn(), &summary).await?;
        }
        if event.event_type == "order.confirmation_requested" {
            let request: ConfirmationRequest = serde_json::from_value(event.data.clone())?;
            if request.order_id != saga.links().order_id || event.aggregate_id != request.order_id {
                return Err(AppError::InvalidEvent(
                    "invalid order confirmation identity".into(),
                ));
            }
            let mut order = orders::load(w.conn(), request.order_id).await?;
            let mut payment = payments::load(w.conn(), saga.links().payment_id).await?;
            if saga.is_pending() && saga.expires_at() <= w.now().await? {
                cancel_locked(&mut w, &mut saga, &mut order, &mut payment, &trace).await?;
            }
            if saga.status() == CheckoutStatus::AwaitingOrderConfirmation
                && !saga.cancellation_requested()
            {
                if payment.status() != PaymentStatus::Captured {
                    return Err(AppError::InvalidState(
                        "order confirmation requires a captured payment".into(),
                    ));
                }
                if order.confirm().is_ok() {
                    saga.complete()?;
                } else {
                    saga.fail()?;
                    payment.request_cancel()?;
                }
                persist(&mut w, &saga, &order, &payment, &trace).await?;
            }
        }
        events::record(w.conn(), event.event_id).await?;
        w.commit().await?;
        Ok(ApplyOutcome::Applied)
    }

    async fn apply_provider_request(
        &self,
        event: &CommerceEvent,
        kind: OperationKind,
        trace: TraceContext,
    ) -> Result<ApplyOutcome, AppError> {
        let request: PaymentRequest = serde_json::from_value(event.data.clone())?;
        if request.kind != kind || request.payment_id != event.aggregate_id {
            return Err(AppError::InvalidEvent(
                "invalid provider request identity".into(),
            ));
        }
        // Phase one validates the immutable envelope and the durable operation before I/O.
        let mut w = UnitOfWork::begin(&self.pool).await?;
        events::verify(w.conn(), event).await?;
        let mut saga = checkouts::load(w.conn(), event.checkout_id, true).await?;
        if saga.links().payment_id != request.payment_id {
            return Err(AppError::InvalidEvent(
                "payment does not belong to checkout".into(),
            ));
        }
        if events::seen(w.conn(), event.event_id).await? {
            w.commit().await?;
            return Ok(ApplyOutcome::AlreadyApplied);
        }
        if saga.is_pending() && saga.expires_at() <= w.now().await? {
            let mut order = orders::load(w.conn(), saga.links().order_id).await?;
            let mut payment = payments::load(w.conn(), saga.links().payment_id).await?;
            cancel_locked(&mut w, &mut saga, &mut order, &mut payment, &trace).await?;
        }
        let (operation, completed) =
            payments::operation(w.conn(), request.payment_id, kind).await?;
        if completed {
            events::record(w.conn(), event.event_id).await?;
            w.commit().await?;
            return Ok(ApplyOutcome::AlreadyApplied);
        }
        let payment = payments::load(w.conn(), request.payment_id).await?;
        if payment.checkout_id() != saga.links().id
            || payment.order_id() != saga.links().order_id
            || payment.pending_operation() != Some(&operation)
        {
            return Err(AppError::CorruptData);
        }
        payments::record_attempt(w.conn(), &operation).await?;
        w.commit().await?;
        let outcome = match tokio::time::timeout(
            std::time::Duration::from_secs(5),
            self.gateway.execute(&operation),
        )
        .await
        {
            Ok(outcome) => outcome,
            Err(_) => ProviderOutcome::Retryable {
                code: "provider_timeout".into(),
            },
        };
        // Phase two reloads current state; cancellation can have happened during I/O.
        let mut w = UnitOfWork::begin(&self.pool).await?;
        let mut saga = checkouts::load(w.conn(), event.checkout_id, true).await?;
        if events::seen(w.conn(), event.event_id).await? {
            w.commit().await?;
            return Ok(ApplyOutcome::AlreadyApplied);
        }
        let mut order = orders::load(w.conn(), saga.links().order_id).await?;
        let mut payment = payments::load(w.conn(), saga.links().payment_id).await?;
        if saga.is_pending() && saga.expires_at() <= w.now().await? {
            saga.cancel()?;
            order.cancel()?;
            payment.request_cancel()?;
        }
        let progress = payment.apply_result(&operation, outcome.clone())?;
        if progress == PaymentProgress::RetryPending {
            payments::record_result(w.conn(), &operation, &outcome, false).await?;
            persist(&mut w, &saga, &order, &payment, &trace).await?;
            w.commit().await?;
            return Err(AppError::RetryableProvider);
        }
        if progress == PaymentProgress::Applied {
            let completed_event = match &outcome {
                ProviderOutcome::Succeeded { .. } => match kind {
                    OperationKind::Authorize => {
                        if !saga.cancellation_requested() {
                            payment.begin_capture()?;
                            saga.authorized()?;
                        }
                        "payment.authorized"
                    }
                    OperationKind::Capture => {
                        if !saga.cancellation_requested() {
                            saga.captured()?;
                            emit(
                                &mut w,
                                &saga,
                                "order.confirmation_requested",
                                order.id(),
                                serde_json::to_value(ConfirmationRequest {
                                    order_id: order.id(),
                                })?,
                                &trace,
                                &format!("order:{}:confirm", order.id()),
                            )
                            .await?;
                        }
                        "payment.captured"
                    }
                    OperationKind::Void => {
                        saga.compensated()?;
                        "payment.voided"
                    }
                    OperationKind::Refund => {
                        saga.compensated()?;
                        "payment.refunded"
                    }
                },
                ProviderOutcome::DefinitiveFailure { .. } => {
                    if matches!(kind, OperationKind::Void | OperationKind::Refund) {
                        saga.needs_attention()?;
                        "payment.compensation_failed"
                    } else {
                        if !saga.cancellation_requested() {
                            saga.fail()?;
                        }
                        order.cancel()?;
                        payment.request_cancel()?;
                        if neutral(payment.status()) {
                            saga.compensated()?;
                        }
                        "payment.failed"
                    }
                }
                ProviderOutcome::Retryable { .. } => return Err(AppError::RetryableProvider),
            };
            saga.record_progress()?;
            payments::record_result(w.conn(), &operation, &outcome, true).await?;
            persist(&mut w, &saga, &order, &payment, &trace).await?;
            emit(
                &mut w,
                &saga,
                completed_event,
                payment.id(),
                serde_json::json!({"payment_id":payment.id(),"status":payment.status().as_str()}),
                &trace,
                &format!("payment-result:{}", operation.idempotency_key),
            )
            .await?;
        }
        events::record(w.conn(), event.event_id).await?;
        w.commit().await?;
        Ok(ApplyOutcome::Applied)
    }
}
