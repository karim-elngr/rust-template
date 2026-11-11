//! JSON logging and OpenTelemetry export on background workers.

use std::{collections::BTreeMap, time::Duration};

use anyhow::{Context as _, Result};
use opentelemetry::{Context, KeyValue, global, trace::TracerProvider};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    Resource,
    metrics::{PeriodicReader, SdkMeterProvider},
    propagation::TraceContextPropagator,
    trace::SdkTracerProvider,
};
use tracing_appender::non_blocking::{ErrorCounter, NonBlockingBuilder, WorkerGuard};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::prelude::*;

/// Retain this handle until all application tasks and spans have ended.
pub struct Telemetry {
    tracer_provider: SdkTracerProvider,
    meter_provider: SdkMeterProvider,
    log_guard: WorkerGuard,
    log_errors: ErrorCounter,
    shutdown_timeout: Duration,
}

impl Telemetry {
    /// Initialize once per process. Exporters and stdout I/O run on dedicated
    /// threads; a full logging queue drops messages rather than blocking requests.
    pub fn init(config: &crate::config::TelemetryConfig, service_name: &str) -> Result<Self> {
        let resource = Resource::builder()
            .with_service_name(service_name.to_owned())
            .with_attributes([
                KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
                KeyValue::new("deployment.environment.name", config.environment.clone()),
            ])
            .build();
        let mut traces = SdkTracerProvider::builder().with_resource(resource.clone());
        let mut metrics = SdkMeterProvider::builder().with_resource(resource);
        if config.enabled {
            let trace_exporter = opentelemetry_otlp::SpanExporter::builder()
                .with_http()
                .with_endpoint(signal_endpoint(config, "traces"))
                .with_timeout(config.export_timeout)
                .build()
                .map_err(|_| {
                    anyhow::anyhow!(
                        "could not initialize OTLP trace exporter; check endpoint and headers"
                    )
                })?;
            let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
                .with_http()
                .with_endpoint(signal_endpoint(config, "metrics"))
                .with_timeout(config.export_timeout)
                .build()
                .map_err(|_| {
                    anyhow::anyhow!(
                        "could not initialize OTLP metric exporter; check endpoint and headers"
                    )
                })?;
            traces = traces.with_batch_exporter(trace_exporter);
            metrics = metrics.with_reader(
                PeriodicReader::builder(metric_exporter)
                    .with_interval(config.metric_export_interval)
                    .build(),
            );
        }
        let tracer_provider = traces.build();
        let meter_provider = metrics.build();
        let (writer, log_guard) = NonBlockingBuilder::default()
            .buffered_lines_limit(config.log_buffer_lines)
            .lossy(true)
            .thread_name("json-stdout")
            .finish(std::io::stdout());
        let log_errors = writer.error_counter();
        let filter = tracing_subscriber::EnvFilter::try_new(&config.log_filter)
            .map_err(|_| anyhow::anyhow!("RUST_LOG must be a valid tracing filter"))?;
        tracing_subscriber::registry()
            .with(filter)
            .with(
                tracing_opentelemetry::layer()
                    .with_tracer(tracer_provider.tracer(service_name.to_owned())),
            )
            .with(
                tracing_subscriber::fmt::layer()
                    .json()
                    .with_ansi(false)
                    .with_current_span(true)
                    .with_span_list(true)
                    .with_writer(writer),
            )
            .try_init()
            .context("could not install the process tracing subscriber")?;
        global::set_text_map_propagator(TraceContextPropagator::new());
        global::set_tracer_provider(tracer_provider.clone());
        global::set_meter_provider(meter_provider.clone());
        Ok(Self {
            tracer_provider,
            meter_provider,
            log_guard,
            log_errors,
            shutdown_timeout: config.shutdown_timeout,
        })
    }

    /// Shutdown flushes pending spans and the final metrics collection. The
    /// deadline includes logger drain. A stuck exporter cannot trap Tokio's
    /// blocking-task pool when the runtime subsequently shuts down.
    pub async fn shutdown(self) -> Result<()> {
        let Self {
            tracer_provider,
            meter_provider,
            log_guard,
            log_errors,
            shutdown_timeout,
        } = self;
        bounded_shutdown(
            move || {
                let trace_result = tracer_provider.shutdown_with_timeout(shutdown_timeout);
                let metric_result = meter_provider.shutdown_with_timeout(shutdown_timeout);
                let dropped = log_errors.dropped_lines();
                if dropped > 0 {
                    tracing::warn!(
                        dropped_log_lines = dropped,
                        "stdout logging queue dropped lines"
                    );
                }
                // This guard must outlive provider shutdown and its final events.
                drop(log_guard);
                trace_result.context("trace exporter shutdown failed")?;
                metric_result.context("metric exporter shutdown failed")?;
                Ok(())
            },
            shutdown_timeout,
        )
        .await
    }
}

pub fn capture_trace_context() -> BTreeMap<String, String> {
    let mut carrier = BTreeMap::new();
    let context = tracing::Span::current().context();
    global::get_text_map_propagator(|propagator| {
        propagator.inject_context(&context, &mut MapInjector(&mut carrier));
    });
    carrier
}

pub fn extract_context(carrier: &BTreeMap<String, String>) -> Context {
    global::get_text_map_propagator(|propagator| propagator.extract(&MapExtractor(carrier)))
}

fn signal_endpoint(config: &crate::config::TelemetryConfig, signal: &str) -> String {
    let explicit = match signal {
        "traces" => config.traces_endpoint.as_ref(),
        "metrics" => config.metrics_endpoint.as_ref(),
        _ => None,
    };
    explicit.cloned().unwrap_or_else(|| {
        let base = config
            .otlp_endpoint
            .as_deref()
            .unwrap_or("http://localhost:4318");
        format!("{}/v1/{signal}", base.trim_end_matches('/'))
    })
}

struct MapInjector<'a>(&'a mut BTreeMap<String, String>);

impl opentelemetry::propagation::Injector for MapInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        self.0.insert(key.to_owned(), value);
    }
}

struct MapExtractor<'a>(&'a BTreeMap<String, String>);

impl opentelemetry::propagation::Extractor for MapExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(String::as_str)
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(String::as_str).collect()
    }
}

async fn bounded_shutdown(
    work: impl FnOnce() -> Result<()> + Send + 'static,
    timeout: Duration,
) -> Result<()> {
    let (sender, receiver) = tokio::sync::oneshot::channel();
    std::thread::Builder::new()
        .name("telemetry-shutdown".to_owned())
        .spawn(move || {
            let _ = sender.send(work());
        })
        .context("could not start telemetry shutdown worker")?;
    tokio::time::timeout(timeout, receiver)
        .await
        .context("telemetry shutdown deadline exceeded; buffered telemetry may be lost")?
        .context("telemetry shutdown worker exited unexpectedly")?
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::trace::{TraceContextExt, TracerProvider};
    use opentelemetry_sdk::{propagation::TraceContextPropagator, trace::SdkTracerProvider};
    use tracing_opentelemetry::OpenTelemetrySpanExt;

    #[test]
    fn outgoing_context_preserves_remote_trace_and_flags() {
        opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
        let carrier = BTreeMap::from([(
            "traceparent".to_owned(),
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_owned(),
        )]);
        let provider = SdkTracerProvider::builder().build();
        let subscriber = tracing_subscriber::registry()
            .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("propagation-test")));
        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!("child");
            span.set_parent(extract_context(&carrier)).unwrap();
            let _entered = span.enter();
            let outbound = capture_trace_context();
            let context = extract_context(&outbound);
            let span_context = context.span().span_context().clone();
            assert_eq!(
                span_context.trace_id().to_string(),
                "0af7651916cd43dd8448eb211c80319c"
            );
            assert!(span_context.is_sampled());
            assert_ne!(span_context.span_id().to_string(), "b7ad6b7169203331");
        });
    }

    #[test]
    fn malformed_remote_context_is_ignored() {
        opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
        let context = extract_context(&BTreeMap::from([(
            "traceparent".to_owned(),
            "invalid".to_owned(),
        )]));
        assert!(!context.span().span_context().is_valid());
    }

    #[tokio::test]
    async fn shutdown_deadline_does_not_wait_for_stuck_worker() {
        let (release, blocked) = std::sync::mpsc::channel::<()>();
        let result = bounded_shutdown(
            move || {
                let _ = blocked.recv();
                Ok(())
            },
            Duration::from_millis(10),
        )
        .await;
        drop(release);
        assert!(result.is_err(), "shutdown must report the missed deadline");
    }

    #[tokio::test]
    async fn shutdown_reports_exporter_failure() {
        let result = bounded_shutdown(
            || anyhow::bail!("test-export-failed"),
            Duration::from_secs(1),
        )
        .await;
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("test-export-failed")
        );
    }

    #[tokio::test]
    async fn shutdown_waits_for_successful_work() {
        let completed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let worker_completed = completed.clone();
        bounded_shutdown(
            move || {
                worker_completed.store(true, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            },
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        assert!(completed.load(std::sync::atomic::Ordering::SeqCst));
    }
}
