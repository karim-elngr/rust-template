//! Wire/domain mappings intentionally contain no persistence or business workflow.
use commerce::{
    AddCartItem, CartView, CheckoutCommand, CheckoutReceipt, CheckoutView, CreateCart,
    OrderSummary, OrderView, PaymentView, RemoveCartItem,
};
use contracts::commerce::v1;
use tonic::Status;
use uuid::Uuid;

pub(super) fn id(value: &str) -> Result<Uuid, Status> {
    Uuid::parse_str(value)
        .map_err(|_| Status::invalid_argument("resource identifiers must be UUIDs"))
}
fn version(value: i64) -> Result<i64, Status> {
    if value < 0 {
        Err(Status::invalid_argument(
            "expected_version must not be negative",
        ))
    } else {
        Ok(value)
    }
}
pub(super) fn create_cart(value: v1::CreateCartRequest) -> Result<CreateCart, Status> {
    Ok(CreateCart {
        customer_id: id(&value.customer_id)?,
    })
}
pub(super) fn add_item(value: v1::AddCartItemRequest) -> Result<(Uuid, AddCartItem), Status> {
    if value.quantity <= 0 {
        return Err(Status::invalid_argument("quantity must be positive"));
    }
    Ok((
        id(&value.cart_id)?,
        AddCartItem {
            sku: value.sku,
            quantity: value.quantity,
            expected_version: version(value.expected_version)?,
        },
    ))
}
pub(super) fn remove_item(
    value: v1::RemoveCartItemRequest,
) -> Result<(Uuid, RemoveCartItem), Status> {
    Ok((
        id(&value.cart_id)?,
        RemoveCartItem {
            sku: value.sku,
            expected_version: version(value.expected_version)?,
        },
    ))
}
pub(super) fn checkout(value: v1::CheckoutRequest) -> Result<CheckoutCommand, Status> {
    Ok(CheckoutCommand {
        idempotency_key: id(&value.idempotency_key)?,
        cart_id: id(&value.cart_id)?,
        expected_cart_version: version(value.expected_cart_version)?,
        payment_method_token: value.payment_method_token,
    })
}
pub(super) fn cart(value: CartView) -> v1::CartResponse {
    v1::CartResponse {
        cart_id: value.id.to_string(),
        customer_id: value.customer_id.to_string(),
        status: value.status.as_str().into(),
        version: value.version,
        items: value
            .items
            .into_iter()
            .map(|item| v1::ItemResponse {
                sku: item.sku,
                quantity: item.quantity,
                unit_price_minor: item.unit_price_minor,
            })
            .collect(),
        total_minor: value.total_minor,
        currency: value.currency,
    }
}
pub(super) fn receipt(value: CheckoutReceipt) -> v1::CheckoutResponse {
    v1::CheckoutResponse {
        checkout_id: value.checkout_id.to_string(),
        cart_id: value.cart_id.to_string(),
        order_id: value.order_id.to_string(),
        payment_id: value.payment_id.to_string(),
        status: value.status.as_str().into(),
        total_minor: value.total_minor,
        currency: value.currency,
    }
}
pub(super) fn checkout_view(value: CheckoutView) -> v1::CheckoutViewResponse {
    v1::CheckoutViewResponse {
        checkout_id: value.id.to_string(),
        cart_id: value.cart_id.to_string(),
        order_id: value.order_id.to_string(),
        payment_id: value.payment_id.to_string(),
        status: value.status.as_str().into(),
        total_minor: value.total_minor,
        currency: value.currency,
        created_at: value.created_at.to_rfc3339(),
        expires_at: value.expires_at.to_rfc3339(),
    }
}
pub(super) fn order(value: OrderView) -> v1::OrderResponse {
    v1::OrderResponse {
        order_id: value.id.to_string(),
        customer_id: value.customer_id.to_string(),
        checkout_id: value.checkout_id.to_string(),
        cart_id: value.cart_id.to_string(),
        payment_id: value.payment_id.to_string(),
        status: value.status.as_str().into(),
        total_minor: value.total_minor,
        currency: value.currency,
        items: value
            .items
            .into_iter()
            .map(|item| v1::ItemResponse {
                sku: item.sku,
                quantity: item.quantity,
                unit_price_minor: item.unit_price_minor,
            })
            .collect(),
        created_at: value.created_at.to_rfc3339(),
    }
}
pub(super) fn payment(value: PaymentView) -> v1::PaymentResponse {
    v1::PaymentResponse {
        payment_id: value.id.to_string(),
        order_id: value.order_id.to_string(),
        checkout_id: value.checkout_id.to_string(),
        status: value.status.as_str().into(),
        total_minor: value.total_minor,
        currency: value.currency,
        cancellation_requested: value.cancellation_requested,
    }
}
pub(super) fn order_summary(value: OrderSummary) -> v1::OrderSummaryResponse {
    v1::OrderSummaryResponse {
        order_id: value.order_id.to_string(),
        customer_id: value.customer_id.to_string(),
        status: value.status.as_str().into(),
        payment_status: value.payment_status,
        total_minor: value.total_minor,
        currency: value.currency,
        version: value.version,
        updated_at: value.updated_at.to_rfc3339(),
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nonpositive_cart_quantity_is_rejected_before_database_access() {
        for quantity in [0, -1] {
            let request = v1::AddCartItemRequest {
                cart_id: Uuid::new_v4().to_string(),
                sku: "BOOK".into(),
                quantity,
                expected_version: 0,
            };
            assert!(
                matches!(add_item(request), Err(error) if error.code() == tonic::Code::InvalidArgument)
            );
        }
    }

    #[test]
    fn checkout_requires_cart_identity_version_and_idempotency() {
        let request = v1::CheckoutRequest {
            idempotency_key: Uuid::new_v4().to_string(),
            cart_id: Uuid::new_v4().to_string(),
            expected_cart_version: 2,
            payment_method_token: "pm_demo_approve".into(),
        };
        let mut invalid = request.clone();
        invalid.cart_id = "invalid".into();
        assert!(checkout(invalid).is_err());
        let mut invalid = request.clone();
        invalid.expected_cart_version = -1;
        assert!(checkout(invalid).is_err());
        assert_eq!(checkout(request).unwrap().expected_cart_version, 2);
    }
}
