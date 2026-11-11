use super::{handlers, middleware::observe};
use axum::{
    Router,
    extract::DefaultBodyLimit,
    http::StatusCode,
    middleware,
    routing::{delete, get, post, put},
};
use commerce::Store;
use tower::limit::ConcurrencyLimitLayer;
use tower_http::timeout::TimeoutLayer;

pub fn router(store: Store, request_timeout: std::time::Duration) -> Router {
    Router::new()
        .route("/v1/carts", post(handlers::create_cart))
        .route("/v1/carts/{id}", get(handlers::get_cart))
        .route("/v1/carts/{id}/items", put(handlers::add_item))
        .route("/v1/carts/{id}/items/{sku}", delete(handlers::remove_item))
        .route("/v1/checkouts", post(handlers::checkout))
        .route("/v1/checkouts/{id}", get(handlers::get_checkout))
        .route("/v1/checkouts/{id}/cancel", post(handlers::cancel_checkout))
        .route("/v1/orders/{id}", get(handlers::get_order))
        .route("/v1/orders/{id}/summary", get(handlers::get_order_summary))
        .route("/v1/payments/{id}", get(handlers::get_payment))
        .with_state(store)
        .layer(DefaultBodyLimit::max(64 * 1024))
        .layer(ConcurrencyLimitLayer::new(128))
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            request_timeout,
        ))
        .layer(middleware::from_fn(observe))
}
