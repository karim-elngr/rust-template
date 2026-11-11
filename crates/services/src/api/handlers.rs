//! Handlers delegate use cases to the application facade and map public DTOs.
use super::{dto::*, errors::ApiError};
use axum::{
    Json,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
};
use commerce::Store;
use opentelemetry::global;
use platform::capture_trace_context;
use uuid::Uuid;

pub(super) async fn create_cart(
    State(store): State<Store>,
    Json(body): Json<CreateCartBody>,
) -> Result<(StatusCode, Json<CartBody>), ApiError> {
    Ok((
        StatusCode::CREATED,
        Json(store.create_cart(body.into()).await?.into()),
    ))
}
pub(super) async fn add_item(
    State(store): State<Store>,
    Path(id): Path<Uuid>,
    Json(body): Json<AddItemBody>,
) -> Result<Json<CartBody>, ApiError> {
    Ok(Json(
        store
            .add_item(id, body.command().map_err(ApiError::bad_request)?)
            .await?
            .into(),
    ))
}
pub(super) async fn remove_item(
    State(store): State<Store>,
    Path((id, sku)): Path<(Uuid, String)>,
    Query(body): Query<RemoveItemQuery>,
) -> Result<Json<CartBody>, ApiError> {
    Ok(Json(
        store
            .remove_item(id, body.command(sku).map_err(ApiError::bad_request)?)
            .await?
            .into(),
    ))
}
pub(super) async fn get_cart(
    State(store): State<Store>,
    Path(id): Path<Uuid>,
) -> Result<Json<CartBody>, ApiError> {
    Ok(Json(store.get_cart(id).await?.into()))
}
pub(super) async fn checkout(
    State(store): State<Store>,
    headers: HeaderMap,
    Json(body): Json<CheckoutBody>,
) -> Result<(StatusCode, Json<ReceiptBody>), ApiError> {
    let key = headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| ApiError::bad_request("Idempotency-Key must be a UUID"))?;
    let receipt = store
        .checkout(
            body.command(key).map_err(ApiError::bad_request)?,
            capture_trace_context(),
        )
        .await?;
    tracing::info!(event.name="checkout.accepted",checkout_id=%receipt.checkout_id,order_id=%receipt.order_id,"checkout accepted");
    global::meter("commerce")
        .u64_counter("checkout.accepted")
        .build()
        .add(1, &[]);
    Ok((StatusCode::ACCEPTED, Json(receipt.into())))
}
pub(super) async fn get_checkout(
    State(store): State<Store>,
    Path(id): Path<Uuid>,
) -> Result<Json<CheckoutViewBody>, ApiError> {
    Ok(Json(store.get_checkout(id).await?.into()))
}
pub(super) async fn cancel_checkout(
    State(store): State<Store>,
    Path(id): Path<Uuid>,
) -> Result<Json<CheckoutViewBody>, ApiError> {
    Ok(Json(
        store
            .cancel_checkout_with_context(id, capture_trace_context())
            .await?
            .into(),
    ))
}
pub(super) async fn get_order(
    State(store): State<Store>,
    Path(id): Path<Uuid>,
) -> Result<Json<OrderBody>, ApiError> {
    Ok(Json(store.get_order(id).await?.into()))
}
pub(super) async fn get_payment(
    State(store): State<Store>,
    Path(id): Path<Uuid>,
) -> Result<Json<PaymentBody>, ApiError> {
    Ok(Json(store.get_payment(id).await?.into()))
}
pub(super) async fn get_order_summary(
    State(store): State<Store>,
    Path(id): Path<Uuid>,
) -> Result<Json<OrderSummaryBody>, ApiError> {
    Ok(Json(store.get_order_summary(id).await?.into()))
}
