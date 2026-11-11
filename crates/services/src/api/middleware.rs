use axum::{
    extract::{MatchedPath, Request},
    http::{HeaderMap, HeaderValue},
    middleware::Next,
    response::Response,
};
use opentelemetry::{KeyValue, global};
use platform::extract_context;
use std::{collections::BTreeMap, time::Instant};
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

pub(super) async fn observe(request: Request, next: Next) -> Response {
    let started = Instant::now();
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map(|p| p.as_str())
        .unwrap_or("unmatched")
        .to_owned();
    let method = request.method().to_string();
    let request_id = Uuid::new_v4().to_string();
    let carrier = trace_headers(request.headers());
    let span = tracing::info_span!(
        "http.request", otel.kind = "server", http.request.method = %method,
        http.route = %route, request_id = %request_id,
        http.response.status_code = tracing::field::Empty,
        otel.status_code = tracing::field::Empty,
    );
    let _ = span.set_parent(extract_context(&carrier));
    async {
        let mut response = next.run(request).await;
        if let Ok(value) = HeaderValue::from_str(&request_id) {
            response.headers_mut().insert("x-request-id", value);
        }
        let status = response.status();
        let current = tracing::Span::current();
        current.record("http.response.status_code", i64::from(status.as_u16()));
        if status.is_server_error() {
            current.record("otel.status_code", "ERROR");
            current.set_status(opentelemetry::trace::Status::error("HTTP server error"));
        }
        let labels = [
            KeyValue::new("http.request.method", method),
            KeyValue::new("http.route", route),
            KeyValue::new("http.response.status_code", i64::from(status.as_u16())),
        ];
        global::meter("services")
            .f64_histogram("http.server.request.duration")
            .with_unit("s")
            .build()
            .record(started.elapsed().as_secs_f64(), &labels);
        response
    }
    .instrument(span)
    .await
}

pub fn trace_headers(headers: &HeaderMap) -> BTreeMap<String, String> {
    ["traceparent", "tracestate"]
        .into_iter()
        .filter_map(|key| {
            headers
                .get(key)
                .and_then(|value| value.to_str().ok())
                .map(|value| (key.into(), value.into()))
        })
        .collect()
}
