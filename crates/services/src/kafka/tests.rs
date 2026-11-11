use super::{
    config::client_config,
    decode_event,
    trace::{publisher_headers, trace_carrier},
};
use commerce::CommerceEvent;
use platform::{config::KafkaConfig, extract_context};
use rdkafka::message::{Header, OwnedHeaders};
use std::collections::BTreeMap;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;
#[test]
fn headers_link_consumer_to_publisher_without_mutating_original_event() {
    use opentelemetry::trace::{TraceContextExt, TracerProvider};
    use opentelemetry_sdk::{
        propagation::TraceContextPropagator,
        trace::{InMemorySpanExporter, SdkTracerProvider},
    };
    use tracing_subscriber::prelude::*;
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    let subscriber = tracing_subscriber::registry().with(
        tracing_opentelemetry::layer().with_tracer(provider.tracer("kafka-propagation-test")),
    );
    let original = BTreeMap::from([(
        "traceparent".into(),
        "00-0af7651916cd43dd8448eb211c80319c-1111111111111111-01".into(),
    )]);
    tracing::subscriber::with_default(subscriber, || {
        let publisher = tracing::info_span!("publisher");
        publisher.set_parent(extract_context(&original)).unwrap();
        let headers = publisher.in_scope(publisher_headers);
        let publisher_id = publisher.context().span().span_context().span_id();
        let consumer = tracing::info_span!("consumer");
        consumer
            .set_parent(extract_context(&trace_carrier(Some(&headers), &original)))
            .unwrap();
        drop(consumer);
        drop(publisher);
        let spans = exporter.get_finished_spans().unwrap();
        let consumer = spans.iter().find(|span| span.name == "consumer").unwrap();
        assert_eq!(consumer.parent_span_id, publisher_id);
        assert_eq!(
            consumer.span_context.trace_id().to_string(),
            "0af7651916cd43dd8448eb211c80319c"
        );
    });
    assert_eq!(
        original["traceparent"],
        "00-0af7651916cd43dd8448eb211c80319c-1111111111111111-01"
    );
}

#[test]
fn kafka_trace_headers_override_original_event_context() {
    let original = "00-0af7651916cd43dd8448eb211c80319c-1111111111111111-01";
    let publisher = "00-0af7651916cd43dd8448eb211c80319c-2222222222222222-01";
    let fallback = BTreeMap::from([("traceparent".into(), original.into())]);
    let headers = OwnedHeaders::new()
        .insert(Header {
            key: "traceparent",
            value: Some(publisher),
        })
        .insert(Header {
            key: "tracestate",
            value: Some("vendor=state"),
        })
        .insert(Header {
            key: "authorization",
            value: Some("secret"),
        });
    let carrier = trace_carrier(Some(&headers), &fallback);
    assert_eq!(
        carrier.get("traceparent").map(String::as_str),
        Some(publisher)
    );
    assert_eq!(
        carrier.get("tracestate").map(String::as_str),
        Some("vendor=state")
    );
    assert!(!carrier.contains_key("authorization"));
    assert_eq!(
        fallback.get("traceparent").map(String::as_str),
        Some(original)
    );
}

#[test]
fn absent_kafka_traceparent_uses_envelope_context() {
    let fallback = BTreeMap::from([("traceparent".into(), "original-context".into())]);
    assert_eq!(trace_carrier::<OwnedHeaders>(None, &fallback), fallback);
    assert_eq!(
        trace_carrier(Some(&OwnedHeaders::new()), &fallback),
        fallback
    );
}

fn event() -> CommerceEvent {
    CommerceEvent {
        event_id: Uuid::new_v4(),
        aggregate_id: Uuid::new_v4(),
        checkout_id: Uuid::new_v4(),
        event_type: "checkout.started".into(),
        schema_version: 1,
        data: serde_json::json!({}),
        trace_context: Default::default(),
        occurred_at: chrono::Utc::now(),
    }
}
#[test]
fn broker_messages_require_valid_schema_and_partition_key() {
    let event = event();
    let bytes = serde_json::to_vec(&event).unwrap();
    assert!(
        decode_event(
            Some(&bytes),
            Some(event.aggregate_id.to_string().as_bytes())
        )
        .is_ok()
    );
    assert!(decode_event(Some(&bytes), Some(b"wrong-order")).is_err());
    assert!(decode_event(Some(&bytes), None).is_err());
    assert!(decode_event(None, None).is_err());
    assert!(decode_event(Some(b"not json"), None).is_err());
    assert!(decode_event(Some(&vec![b' '; 65537]), None).is_err());
}

#[test]
fn auto_topic_creation_is_disabled() {
    let config = KafkaConfig {
        brokers: "localhost:9092".into(),
        topic: "commerce.events.v1".into(),
        group_id: "payments-v1".into(),
        security_protocol: "PLAINTEXT".into(),
        sasl_mechanism: None,
        sasl_username: None,
        sasl_password: None,
        ssl_ca_location: None,
    };
    let base = client_config(&config);
    assert_eq!(base.get("allow.auto.create.topics"), Some("false"));
}
