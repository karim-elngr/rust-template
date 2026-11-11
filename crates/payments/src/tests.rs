use super::*;
use shared_kernel::Money;
use uuid::Uuid;

#[test]
fn every_reachable_lifecycle_snapshot_restores_without_losing_pending_work() {
    use std::collections::{HashSet, VecDeque};
    let mut seen = HashSet::new();
    let mut queue = VecDeque::from([payment()]);
    while let Some(current) = queue.pop_front() {
        let json = serde_json::to_string(&current).unwrap();
        if !seen.insert(json.clone()) {
            continue;
        }
        assert!(
            seen.len() < 100,
            "lifecycle unexpectedly has an unbounded operation history"
        );
        let restored: Payment = serde_json::from_str(&json).unwrap();
        assert_eq!(restored, current);
        for action in 0..3 {
            let mut next = current.clone();
            let result = match action {
                0 => next.begin_authorization(),
                1 => next.begin_capture(),
                _ => next.request_cancel(),
            };
            if result.is_err() {
                assert_eq!(next, current);
            } else {
                queue.push_back(next);
            }
        }
        if let Some(operation) = current.pending_operation() {
            for outcome in [
                success(operation),
                ProviderOutcome::DefinitiveFailure {
                    code: "declined".into(),
                },
                ProviderOutcome::Retryable {
                    code: "timeout_unknown".into(),
                },
            ] {
                let mut next = current.clone();
                let progress = next.apply_result(operation, outcome).unwrap();
                if progress == PaymentProgress::RetryPending {
                    assert_eq!(next, current);
                }
                queue.push_back(next);
            }
        }
    }
    assert!(
        seen.len() >= 15,
        "expected authorization/capture and compensation branches"
    );
}

fn payment() -> Payment {
    Payment::new(
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        Money::new(1250, "USD").unwrap(),
        "pm_demo_approve",
    )
    .unwrap()
}

fn success(operation: &PaymentOperation) -> ProviderOutcome {
    ProviderOutcome::Succeeded {
        provider_reference: format!(
            "provider:{}:{}",
            operation.payment_id,
            operation.kind.as_str()
        ),
        amount: operation.amount.clone(),
    }
}

fn authorized() -> Payment {
    let mut payment = payment();
    let operation = payment.begin_authorization().unwrap().unwrap();
    payment
        .apply_result(&operation, success(&operation))
        .unwrap();
    payment
}

fn captured() -> Payment {
    let mut payment = authorized();
    let operation = payment.begin_capture().unwrap().unwrap();
    payment
        .apply_result(&operation, success(&operation))
        .unwrap();
    payment
}

#[test]
fn authorize_then_capture_are_separate_durable_operations() {
    let mut payment = payment();
    assert_eq!(payment.status(), PaymentStatus::Created);
    let authorize = payment.begin_authorization().unwrap().unwrap();
    assert_eq!(payment.status(), PaymentStatus::AuthorizationPending);
    assert_eq!(authorize.kind, OperationKind::Authorize);
    assert_eq!(authorize.provider_reference, None);
    assert_eq!(
        payment
            .apply_result(&authorize, success(&authorize))
            .unwrap(),
        PaymentProgress::Applied
    );
    assert_eq!(payment.status(), PaymentStatus::Authorized);
    assert!(payment.pending_operation().is_none());
    let capture = payment.begin_capture().unwrap().unwrap();
    assert_ne!(capture.idempotency_key, authorize.idempotency_key);
    assert_eq!(
        capture.provider_reference,
        Some(format!("provider:{}:authorize", payment.id()))
    );
    payment.apply_result(&capture, success(&capture)).unwrap();
    assert_eq!(payment.status(), PaymentStatus::Captured);
    assert!(payment.pending_operation().is_none());
}

#[test]
fn authorization_decline_never_attempts_capture() {
    let mut payment = payment();
    let operation = payment.begin_authorization().unwrap().unwrap();
    payment
        .apply_result(
            &operation,
            ProviderOutcome::DefinitiveFailure {
                code: "declined".into(),
            },
        )
        .unwrap();
    assert_eq!(payment.status(), PaymentStatus::Declined);
    assert_eq!(payment.failure_code(), Some("declined"));
    assert!(payment.begin_capture().is_err());
    assert!(payment.pending_operation().is_none());
}

#[test]
fn crash_and_unknown_outcome_reuse_exact_operation() {
    let mut payment = payment();
    let authorize = payment.begin_authorization().unwrap().unwrap();
    let mut restored: Payment =
        serde_json::from_str(&serde_json::to_string(&payment).unwrap()).unwrap();
    assert_eq!(restored.pending_operation(), Some(&authorize));
    assert_eq!(
        restored.begin_authorization().unwrap(),
        Some(authorize.clone())
    );
    assert_eq!(
        restored
            .apply_result(
                &authorize,
                ProviderOutcome::Retryable {
                    code: "timeout_unknown".into()
                }
            )
            .unwrap(),
        PaymentProgress::RetryPending
    );
    assert_eq!(restored.status(), PaymentStatus::AuthorizationPending);
    assert_eq!(restored.pending_operation(), Some(&authorize));
    restored
        .apply_result(&authorize, success(&authorize))
        .unwrap();
    assert_eq!(restored.status(), PaymentStatus::Authorized);
}

#[test]
fn capture_before_authorization_is_rejected_without_mutation() {
    let mut payment = payment();
    let before = payment.clone();
    assert!(payment.begin_capture().is_err());
    assert_eq!(payment, before);
}

#[test]
fn cancellation_before_authorization_is_terminal_without_provider_io() {
    let mut payment = payment();
    assert!(payment.request_cancel().unwrap().is_none());
    assert_eq!(payment.status(), PaymentStatus::Cancelled);
    assert!(payment.begin_authorization().unwrap().is_none());
    assert!(payment.pending_operation().is_none());
}

#[test]
fn cancellation_during_authorization_survives_restart_then_voids() {
    let mut payment = payment();
    let authorize = payment.begin_authorization().unwrap().unwrap();
    assert_eq!(payment.request_cancel().unwrap(), Some(authorize.clone()));
    let mut restored: Payment =
        serde_json::from_str(&serde_json::to_string(&payment).unwrap()).unwrap();
    assert!(restored.cancellation_requested());
    restored
        .apply_result(&authorize, success(&authorize))
        .unwrap();
    assert_eq!(restored.status(), PaymentStatus::VoidPending);
    let void = restored.pending_operation().unwrap().clone();
    assert_eq!(void.kind, OperationKind::Void);
    restored.apply_result(&void, success(&void)).unwrap();
    assert_eq!(restored.status(), PaymentStatus::Voided);
    assert!(restored.begin_capture().is_err());
}

#[test]
fn authorization_decline_after_cancellation_leaves_no_hold() {
    let mut payment = payment();
    let authorize = payment.begin_authorization().unwrap().unwrap();
    payment.request_cancel().unwrap();
    payment
        .apply_result(
            &authorize,
            ProviderOutcome::DefinitiveFailure {
                code: "declined".into(),
            },
        )
        .unwrap();
    assert_eq!(payment.status(), PaymentStatus::Cancelled);
    assert!(payment.pending_operation().is_none());
    assert_eq!(
        serde_json::from_str::<Payment>(&serde_json::to_string(&payment).unwrap()).unwrap(),
        payment
    );
}

#[test]
fn cancellation_during_capture_refunds_success_instead_of_reversing_it() {
    let mut payment = authorized();
    let capture = payment.begin_capture().unwrap().unwrap();
    payment.request_cancel().unwrap();
    assert_eq!(payment.status(), PaymentStatus::CapturePending);
    payment
        .apply_result(
            &capture,
            ProviderOutcome::Retryable {
                code: "timeout_unknown".into(),
            },
        )
        .unwrap();
    assert_eq!(payment.pending_operation(), Some(&capture));
    payment.apply_result(&capture, success(&capture)).unwrap();
    let refund = payment.pending_operation().unwrap().clone();
    assert_eq!(payment.status(), PaymentStatus::RefundPending);
    assert_eq!(refund.kind, OperationKind::Refund);
    assert_eq!(
        refund.provider_reference,
        Some(format!("provider:{}:capture", payment.id()))
    );
    payment.apply_result(&refund, success(&refund)).unwrap();
    assert_eq!(payment.status(), PaymentStatus::Refunded);
}

#[test]
fn cancelling_authorized_voids_and_cancelling_captured_refunds() {
    for (mut payment, kind, status) in [
        (authorized(), OperationKind::Void, PaymentStatus::Voided),
        (captured(), OperationKind::Refund, PaymentStatus::Refunded),
    ] {
        let operation = payment.request_cancel().unwrap().unwrap();
        assert_eq!(operation.kind, kind);
        assert_eq!(payment.request_cancel().unwrap(), Some(operation.clone()));
        payment
            .apply_result(&operation, success(&operation))
            .unwrap();
        assert_eq!(payment.status(), status);
        assert!(payment.request_cancel().unwrap().is_none());
    }
}

#[test]
fn definitive_capture_failure_releases_authorization() {
    let mut payment = authorized();
    let capture = payment.begin_capture().unwrap().unwrap();
    payment
        .apply_result(
            &capture,
            ProviderOutcome::DefinitiveFailure {
                code: "capture_declined".into(),
            },
        )
        .unwrap();
    assert_eq!(payment.failure_code(), Some("capture_declined"));
    assert_eq!(payment.status(), PaymentStatus::VoidPending);
    let void = payment.pending_operation().unwrap().clone();
    payment.apply_result(&void, success(&void)).unwrap();
    assert_eq!(payment.status(), PaymentStatus::Voided);
    assert_eq!(payment.failure_code(), Some("capture_declined"));
}

#[test]
fn compensation_failure_requires_attention() {
    for mut payment in [authorized(), captured()] {
        let compensation = payment.request_cancel().unwrap().unwrap();
        payment
            .apply_result(
                &compensation,
                ProviderOutcome::DefinitiveFailure {
                    code: "manual_review".into(),
                },
            )
            .unwrap();
        assert_eq!(payment.status(), PaymentStatus::NeedsAttention);
        assert_eq!(payment.failure_code(), Some("manual_review"));
        assert!(payment.pending_operation().is_none());
        assert!(payment.request_cancel().unwrap().is_none());
        assert_eq!(
            serde_json::from_str::<Payment>(&serde_json::to_string(&payment).unwrap()).unwrap(),
            payment
        );
    }
}

#[test]
fn late_completion_replay_cannot_reverse_advanced_state() {
    let mut payment = payment();
    let authorize = payment.begin_authorization().unwrap().unwrap();
    payment
        .apply_result(&authorize, success(&authorize))
        .unwrap();
    let capture = payment.begin_capture().unwrap().unwrap();
    assert_eq!(payment.begin_capture().unwrap(), Some(capture.clone()));
    let before = payment.clone();
    assert_eq!(
        payment
            .apply_result(&authorize, success(&authorize))
            .unwrap(),
        PaymentProgress::Replay
    );
    assert_eq!(payment, before);
    payment.apply_result(&capture, success(&capture)).unwrap();
    let refund = payment.request_cancel().unwrap().unwrap();
    payment.apply_result(&refund, success(&refund)).unwrap();
    let before = payment.clone();
    assert_eq!(
        payment.apply_result(&capture, success(&capture)).unwrap(),
        PaymentProgress::Replay
    );
    assert_eq!(
        payment
            .apply_result(
                &authorize,
                ProviderOutcome::Retryable {
                    code: "late_timeout".into()
                }
            )
            .unwrap(),
        PaymentProgress::Replay
    );
    assert_eq!(payment, before);
}

#[test]
fn conflicting_completion_and_mismatched_request_leave_state_unchanged() {
    let mut payment = payment();
    let authorize = payment.begin_authorization().unwrap().unwrap();
    let before = payment.clone();
    let wrong_amount = ProviderOutcome::Succeeded {
        provider_reference: "auth".into(),
        amount: Money::new(1251, "USD").unwrap(),
    };
    assert!(payment.apply_result(&authorize, wrong_amount).is_err());
    let wrong_currency = ProviderOutcome::Succeeded {
        provider_reference: "auth".into(),
        amount: Money::new(1250, "EUR").unwrap(),
    };
    assert!(payment.apply_result(&authorize, wrong_currency).is_err());
    let mut forged = authorize.clone();
    forged.checkout_id = Uuid::new_v4();
    assert!(payment.apply_result(&forged, success(&forged)).is_err());
    assert_eq!(payment, before);
    payment
        .apply_result(&authorize, success(&authorize))
        .unwrap();
    let before = payment.clone();
    assert!(
        payment
            .apply_result(
                &authorize,
                ProviderOutcome::DefinitiveFailure {
                    code: "contradiction".into()
                }
            )
            .is_err()
    );
    assert_eq!(payment, before);
}

#[test]
fn deserialization_rejects_impossible_state_and_modified_operation() {
    let mut payment = authorized();
    payment.begin_capture().unwrap();
    let mut value = serde_json::to_value(&payment).unwrap();
    value["status"] = serde_json::json!("refunded");
    assert!(serde_json::from_value::<Payment>(value).is_err());
    let mut value = serde_json::to_value(&payment).unwrap();
    value["pending_operation"]["idempotency_key"] = serde_json::json!("forged-key");
    assert!(serde_json::from_value::<Payment>(value).is_err());
    assert_eq!(
        serde_json::from_value::<Payment>(serde_json::to_value(&payment).unwrap()).unwrap(),
        payment
    );
}

#[test]
fn identifiers_positive_amount_and_opaque_method_are_required() {
    let ids = [Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
    assert!(
        Payment::new(
            ids[0],
            ids[1],
            ids[2],
            Money::new(0, "USD").unwrap(),
            "pm_demo_approve"
        )
        .is_err()
    );
    assert!(
        Payment::new(
            Uuid::nil(),
            ids[1],
            ids[2],
            Money::new(1, "USD").unwrap(),
            "pm_demo_approve"
        )
        .is_err()
    );
    assert!(
        Payment::new(
            ids[0],
            ids[0],
            ids[2],
            Money::new(1, "USD").unwrap(),
            "pm_demo_approve"
        )
        .is_err()
    );
    assert!(
        Payment::new(
            ids[0],
            ids[1],
            ids[2],
            Money::new(1, "USD").unwrap(),
            "4111111111111111"
        )
        .is_err()
    );
}

#[tokio::test]
async fn demo_gateway_is_deterministic_and_exercises_distinct_provider_stages() {
    let gateway = DemoGateway;
    for token in [
        "pm_demo_approve",
        "pm_demo_decline",
        "pm_demo_capture_decline",
    ] {
        let mut payment = Payment::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Money::new(100, "USD").unwrap(),
            token,
        )
        .unwrap();
        let authorize = payment.begin_authorization().unwrap().unwrap();
        let outcome = gateway.execute(&authorize).await;
        assert_eq!(gateway.execute(&authorize).await, outcome);
        payment.apply_result(&authorize, outcome).unwrap();
        if token == "pm_demo_decline" {
            assert_eq!(payment.status(), PaymentStatus::Declined);
            continue;
        }
        assert_eq!(payment.status(), PaymentStatus::Authorized);
        let capture = payment.begin_capture().unwrap().unwrap();
        payment
            .apply_result(&capture, gateway.execute(&capture).await)
            .unwrap();
        if token == "pm_demo_capture_decline" {
            let void = payment.pending_operation().unwrap().clone();
            payment
                .apply_result(&void, gateway.execute(&void).await)
                .unwrap();
            assert_eq!(payment.status(), PaymentStatus::Voided);
        } else {
            assert_eq!(payment.status(), PaymentStatus::Captured);
        }
    }
}
