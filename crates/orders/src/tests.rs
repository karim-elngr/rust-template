use crate::*;
use uuid::Uuid;
fn order() -> Order {
    Order::new(
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        vec![OrderLine {
            sku: "BOOK".into(),
            quantity: 2,
            unit_price_minor: 2500,
        }],
    )
    .unwrap()
}
#[test]
fn snapshots_server_prices() {
    assert_eq!(order().total_minor(), 5000);
}
#[test]
fn confirmation_and_cancellation_are_terminal() {
    let mut o = order();
    o.confirm().unwrap();
    o.confirm().unwrap();
    assert!(o.cancel().is_err());
    let mut o = order();
    o.cancel().unwrap();
    assert!(o.confirm().is_err());
}
#[test]
fn rejects_invalid_or_overflowing_snapshot() {
    assert!(
        Order::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            vec![]
        )
        .is_err()
    );
    assert!(
        Order::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            vec![OrderLine {
                sku: "A".into(),
                quantity: 2,
                unit_price_minor: i64::MAX
            }]
        )
        .is_err()
    );
}
