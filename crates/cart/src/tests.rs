use crate::*;
use uuid::Uuid;
#[test]
fn creates_empty_cart_then_adds_priced_lines() {
    let mut c = Cart::new(Uuid::new_v4(), Uuid::new_v4());
    assert!(c.items().is_empty());
    c.add("BOOK".into(), 2, 2500, 0).unwrap();
    assert_eq!(c.total_minor().unwrap(), 5000);
    assert_eq!(c.version(), 1);
}
#[test]
fn rejects_stale_and_invalid_commands_without_mutation() {
    let mut c = Cart::new(Uuid::new_v4(), Uuid::new_v4());
    assert!(c.add("BOOK".into(), 0, 2500, 0).is_err());
    c.add("BOOK".into(), 1, 2500, 0).unwrap();
    assert!(c.add("BOOK".into(), 1, 2500, 0).is_err());
    assert_eq!(c.items()[0].quantity, 1);
    assert!(c.add("BIG".into(), 2, i64::MAX, 1).is_err());
    assert_eq!(c.items().len(), 1);
}
#[test]
fn checkout_freezes_nonempty_cart() {
    let mut c = Cart::new(Uuid::new_v4(), Uuid::new_v4());
    assert!(c.checkout(0).is_err());
    c.add("BOOK".into(), 1, 2500, 0).unwrap();
    c.checkout(1).unwrap();
    assert!(c.remove("BOOK", 2).is_err());
    assert!(c.add("BOOK".into(), 1, 2500, 2).is_err());
}
#[test]
fn remove_is_versioned_and_existing_quantity_is_set() {
    let mut c = Cart::new(Uuid::new_v4(), Uuid::new_v4());
    c.add("BOOK".into(), 1, 2500, 0).unwrap();
    c.add("BOOK".into(), 2, 2500, 1).unwrap();
    assert_eq!(c.items()[0].quantity, 2);
    c.remove("BOOK", 2).unwrap();
    assert!(c.items().is_empty());
    assert_eq!(c.version(), 3);
}
