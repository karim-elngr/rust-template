use super::{errors::ApiError, middleware::observe, router};
use axum::body::Body;
use axum::{
    Router,
    http::{Request, StatusCode},
    middleware,
    response::IntoResponse,
    routing::get,
};
use commerce::{AppError, Store};
use http_body_util::BodyExt;
use tower::ServiceExt;
use uuid::Uuid;

#[tokio::test]
async fn server_span_records_response_status_and_marks_server_failures() {
    use opentelemetry::trace::{Status, TracerProvider};
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
    use tracing::instrument::WithSubscriber;
    use tracing_subscriber::prelude::*;

    for code in [StatusCode::INTERNAL_SERVER_ERROR, StatusCode::BAD_REQUEST] {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let subscriber = tracing_subscriber::registry()
            .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("http-observe-test")));
        let router = Router::new()
            .route("/test", get(move || async move { code }))
            .layer(middleware::from_fn(observe));
        let response = router
            .oneshot(Request::get("/test").body(Body::empty()).unwrap())
            .with_subscriber(subscriber)
            .await
            .unwrap();
        assert_eq!(response.status(), code);
        let spans = exporter.get_finished_spans().unwrap();
        let span = spans
            .iter()
            .find(|span| span.name == "http.request")
            .unwrap();
        assert!(
            span.attributes
                .iter()
                .any(
                    |attribute| attribute.key.as_str() == "http.response.status_code"
                        && attribute.value == opentelemetry::Value::I64(i64::from(code.as_u16()))
                )
        );
        assert_eq!(
            matches!(span.status, Status::Error { .. }),
            code.is_server_error()
        );
    }
}

fn disconnected_app() -> Router {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
        .unwrap();
    router(Store::new(pool), std::time::Duration::from_secs(1))
}

#[tokio::test]
async fn missing_idempotency_key_is_rejected_before_database_access() {
    let body = serde_json::json!({"cart_id":Uuid::new_v4(),"expected_cart_version":1,"payment_method_token":"pm_demo_approve"});
    let response = disconnected_app()
        .oneshot(
            Request::post("/v1/checkouts")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(response.headers().contains_key("x-request-id"));
}

#[tokio::test]
async fn database_error_details_are_not_returned() {
    let response = ApiError::from(AppError::Database(sqlx::Error::Protocol(
        "password=super-secret".into(),
    )))
    .into_response();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    assert!(!String::from_utf8_lossy(&bytes).contains("super-secret"));
}

#[test]
fn public_errors_have_distinct_statuses() {
    assert_eq!(
        ApiError::from(AppError::NotFound).into_response().status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        ApiError::from(AppError::IdempotencyConflict)
            .into_response()
            .status(),
        StatusCode::CONFLICT
    );
}
