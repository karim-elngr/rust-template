use super::{config::client_config, trace::trace_carrier};
use anyhow::{Context, Result, ensure};
use commerce::{CommerceEvent, Store};
use opentelemetry::{KeyValue, global};
use platform::{config::KafkaConfig, extract_context};
use rdkafka::{
    Message,
    consumer::{CommitMode, Consumer, StreamConsumer},
};
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

pub fn consumer(config: &KafkaConfig) -> Result<StreamConsumer> {
    client_config(config)
        .set("group.id", &config.group_id)
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("auto.offset.reset", "earliest")
        .set("isolation.level", "read_committed")
        .set("max.poll.interval.ms", "300000")
        .set("session.timeout.ms", "10000")
        .set("fetch.message.max.bytes", "1048576")
        .create()
        .context("could not initialize Kafka consumer")
}

pub fn decode_event(payload: Option<&[u8]>, key: Option<&[u8]>) -> Result<CommerceEvent> {
    let bytes = payload.context("commerce event has no payload")?;
    ensure!(bytes.len() <= 64 * 1024, "commerce event exceeds 64 KiB");
    let event: CommerceEvent =
        serde_json::from_slice(bytes).context("invalid commerce event encoding")?;
    event.validate().context("invalid commerce event schema")?;
    ensure!(
        key == Some(event.aggregate_id.to_string().as_bytes()),
        "Kafka record key must equal the aggregate ID"
    );
    Ok(event)
}

/// Stops on a poison message or failed database operation. Continuing and committing
/// a later offset could skip the failed record in the same partition.
pub async fn consume(
    store: Store,
    consumer: Arc<StreamConsumer>,
    topic: &str,
    cancel: CancellationToken,
) -> Result<()> {
    consumer
        .subscribe(&[topic])
        .context("Kafka subscription failed")?;
    loop {
        let message = tokio::select! {
            _ = cancel.cancelled() => break,
            message = consumer.recv() => message.context("Kafka receive failed")?,
        };
        let event = decode_event(message.payload(), message.key())?;
        let partition = message.partition();
        let offset = message.offset();
        let carrier = trace_carrier(message.headers(), &event.trace_context);
        drop(message);
        let span = tracing::info_span!("kafka.process", otel.kind="consumer", messaging.system="kafka", messaging.destination.name=topic, event_id=%event.event_id, partition, offset);
        let _ = span.set_parent(extract_context(&carrier));
        async {
            let outcome = store.apply_event_with_context(&event, platform::capture_trace_context()).await
                .context("event processing failed; offset was not committed")?;
            // Sync commit runs on a dedicated native thread with a 12s wait
            // deadline. A timeout does not undo the durable database effect;
            // failing the process permits safe idempotent redelivery.
            let mut offsets = rdkafka::TopicPartitionList::new();
            offsets.add_partition_offset(
                topic,
                partition,
                rdkafka::Offset::Offset(offset.checked_add(1).context("Kafka offset overflow")?),
            )?;
            let committing = consumer.clone();
            crate::lifecycle::native_task("kafka-offset-commit", Duration::from_secs(12), move || {
                committing.commit(&offsets, CommitMode::Sync).context("Kafka offset commit failed")
            }).await?;
            tracing::info!(event.name="commerce.event.processed", event_id=%event.event_id, ?outcome, "commerce event processed");
            global::meter("commerce")
                .u64_counter("commerce.events.processed")
                .build()
                .add(1, &[KeyValue::new("outcome", format!("{outcome:?}"))]);
            Ok::<_, anyhow::Error>(())
        }.instrument(span).await?;
    }
    consumer.unsubscribe();
    Ok(())
}
