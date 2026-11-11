use crate::*;
use chrono::Utc;
use uuid::Uuid;
fn saga() -> CheckoutSaga {
    CheckoutSaga::new(
        CheckoutLinks {
            id: Uuid::new_v4(),
            cart_id: Uuid::new_v4(),
            order_id: Uuid::new_v4(),
            payment_id: Uuid::new_v4(),
        },
        Utc::now(),
    )
}
#[test]
fn capture_requires_authorization_and_confirmation_is_separate() {
    let mut s = saga();
    assert!(s.captured().is_err());
    s.authorized().unwrap();
    s.captured().unwrap();
    assert_eq!(s.status(), CheckoutStatus::AwaitingOrderConfirmation);
    s.complete().unwrap();
    assert!(s.cancel().is_err());
}
#[test]
fn cancellation_prevents_late_confirmation() {
    let mut s = saga();
    s.authorized().unwrap();
    s.captured().unwrap();
    s.cancel().unwrap();
    assert!(s.complete().is_err());
    s.compensated().unwrap();
    assert_eq!(s.status(), CheckoutStatus::Cancelled);
}
#[test]
fn failed_checkout_requires_compensation_before_terminal_failure() {
    let mut s = saga();
    s.fail().unwrap();
    assert_eq!(s.status(), CheckoutStatus::Compensating);
    s.compensated().unwrap();
    assert_eq!(s.status(), CheckoutStatus::Failed);
}

#[test]
fn terminal_cancellation_and_attention_cannot_be_reopened_by_failure() {
    let mut s = saga();
    s.cancel().unwrap();
    s.compensated().unwrap();
    assert!(s.fail().is_err());
    let mut s = saga();
    s.cancel().unwrap();
    s.needs_attention().unwrap();
    assert!(s.fail().is_err());
}
