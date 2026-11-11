use super::{config::client_config, trace::publisher_headers};
use anyhow::{Context, Result, bail};
use commerce::{CommerceEvent, Store};
use opentelemetry::global;
use platform::{config::KafkaConfig, extract_context};
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

pub fn producer(config: &KafkaConfig) -> Result<FutureProducer> {
    client_config(config)
        .set("enable.idempotence", "true")
        .set("acks", "all")
        .set("message.timeout.ms", "5000")
        .set("queue.buffering.max.messages", "1000")
        .create()
        .context("could not initialize Kafka producer")
}

/// Claims one row immediately before delivery so a large batch cannot age its leases.
/// Returning true means a row was attempted; false means no eligible work remains.
pub async fn publish_next(
    store: &Store,
    producer: &FutureProducer,
    topic: &str,
    lease_seconds: i64,
) -> Result<bool> {
    let Some(row) = store.claim_outbox(1, lease_seconds).await?.pop() else {
        return Ok(false);
    };
    let event: CommerceEvent =
        serde_json::from_value(row.payload.clone()).context("invalid stored outbox event")?;
    let span = tracing::info_span!("kafka.publish", otel.kind="producer", messaging.system="kafka", messaging.destination.name=topic, event_id=%row.id);
    let _ = span.set_parent(extract_context(&event.trace_context));
    async {
        let payload = serde_json::to_vec(&row.payload)?;
        let key = row.aggregate_id.to_string();
        // Preserve the immutable persisted envelope; current transport context
        // belongs in Kafka headers so the consumer is a child of this send span.
        let headers = publisher_headers();
        match producer.send(FutureRecord::to(topic).key(&key).payload(&payload).headers(headers), Duration::from_secs(1)).await {
            Ok(_) => {
                if !store.mark_published(row.id,row.lease_token).await? {
                    // Delivery happened, but lease ownership changed: a duplicate may follow.
                    tracing::warn!(event.name="outbox.lease_lost", event_id=%row.id, "delivery acknowledged after lease ownership ended");
                }
                global::meter("commerce").u64_counter("outbox.published").build().add(1,&[]);
                Ok(true)
            }
            Err(_) => {
                store.release_outbox(row.id,row.lease_token,"broker delivery failed").await?;
                global::meter("commerce").u64_counter("outbox.delivery_failures").build().add(1,&[]);
                bail!("Kafka delivery failed; outbox retry scheduled")
            }
        }
    }.instrument(span).await
}
