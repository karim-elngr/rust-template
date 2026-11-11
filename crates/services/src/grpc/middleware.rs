use platform::extract_context;
use std::collections::BTreeMap;
use tonic::Request;
use tracing_opentelemetry::OpenTelemetrySpanExt;

pub(super) fn server_span<T>(
    request: &Request<T>,
    service: &'static str,
    method: &'static str,
) -> tracing::Span {
    let carrier: BTreeMap<String, String> = ["traceparent", "tracestate"]
        .into_iter()
        .filter_map(|key| {
            request
                .metadata()
                .get(key)
                .and_then(|value| value.to_str().ok())
                .map(|value| (key.into(), value.into()))
        })
        .collect();
    let span = tracing::info_span!(
        "grpc.request",
        otel.kind = "server",
        rpc.system = "grpc",
        rpc.service = service,
        rpc.method = method
    );
    let _ = span.set_parent(extract_context(&carrier));
    span
}
