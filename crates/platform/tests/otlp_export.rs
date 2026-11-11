//! Exercise the actual HTTP/protobuf exporters without Docker or an external
//! collector, including the final metrics export for a short-lived process.

use std::{
    io::{BufRead, BufReader, Read, Write},
    net::TcpListener,
    time::{Duration, Instant},
};

use opentelemetry_proto::tonic::collector::{
    metrics::v1::ExportMetricsServiceRequest, trace::v1::ExportTraceServiceRequest,
};
use platform::{Telemetry, config::AppConfig};
use prost::Message;

#[tokio::test(flavor = "current_thread")]
async fn shutdown_exports_short_lived_traces_and_metrics_with_resource_identity() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    listener.set_nonblocking(true).unwrap();
    let collector = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut requests = Vec::new();
        while requests.len() < 2 && Instant::now() < deadline {
            let (mut stream, _) = match listener.accept() {
                Ok(connection) => connection,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(5));
                    continue;
                }
                Err(error) => panic!("collector accept: {error}"),
            };
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let mut reader = BufReader::new(&mut stream);
            let mut request_line = String::new();
            reader.read_line(&mut request_line).unwrap();
            let mut content_length = None;
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                if line == "\r\n" || line.is_empty() {
                    break;
                }
                if let Some((name, value)) = line.split_once(':')
                    && name.eq_ignore_ascii_case("content-length")
                {
                    content_length = Some(value.trim().parse::<usize>().unwrap());
                }
            }
            let mut body = vec![0; content_length.expect("OTLP content length")];
            reader.read_exact(&mut body).unwrap();
            drop(reader);
            stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/x-protobuf\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").unwrap();
            requests.push((request_line, body));
        }
        requests
    });

    let endpoint = format!("http://{address}/collector/");
    let cfg = AppConfig::from_lookup("otlp-test", |key| match key {
        "DATABASE_URL" => Some("postgres://localhost/unused".to_owned()),
        "APP_ENV" => Some("test".to_owned()),
        "OTEL_EXPORTER_OTLP_ENDPOINT" => Some(endpoint.clone()),
        "OTEL_EXPORTER_OTLP_TIMEOUT" => Some("1000".to_owned()),
        "OTEL_METRIC_EXPORT_INTERVAL" => Some("3600000".to_owned()),
        _ => None,
    })
    .unwrap();
    let telemetry = Telemetry::init(&cfg.telemetry, "otlp-test").unwrap();
    let counter = opentelemetry::global::meter("test")
        .u64_counter("completed_jobs")
        .build();
    {
        let span = tracing::info_span!("one_job", otel.kind = "internal");
        let _entered = span.enter();
        counter.add(1, &[]);
        tracing::info!(event.name = "job.completed", "job completed");
    }
    telemetry.shutdown().await.unwrap();

    let requests = collector.join().unwrap();
    let trace_bytes = &requests
        .iter()
        .find(|(line, _)| line.starts_with("POST /collector/v1/traces "))
        .expect("trace export uses the appended signal path")
        .1;
    let traces = ExportTraceServiceRequest::decode(trace_bytes.as_slice()).unwrap();
    assert!(
        traces
            .resource_spans
            .iter()
            .flat_map(|resource| &resource.scope_spans)
            .flat_map(|scope| &scope.spans)
            .any(|span| span.name == "one_job")
    );
    assert!(
        traces
            .resource_spans
            .iter()
            .flat_map(|resource| &resource.scope_spans)
            .flat_map(|scope| &scope.spans)
            .flat_map(|span| &span.events)
            .flat_map(|event| &event.attributes)
            .any(|attribute| attribute.key == "event.name"
                && attribute
                    .value
                    .as_ref()
                    .and_then(|value| value.value.as_ref())
                    == Some(
                        &opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                            "job.completed".to_owned()
                        )
                    ))
    );
    let resource = traces.resource_spans[0].resource.as_ref().unwrap();
    assert!(resource.attributes.iter().any(|attribute| {
        attribute.key == "service.name"
            && attribute
                .value
                .as_ref()
                .and_then(|value| value.value.as_ref())
                == Some(
                    &opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                        "otlp-test".to_owned(),
                    ),
                )
    }));

    let metric_bytes = &requests
        .iter()
        .find(|(line, _)| line.starts_with("POST /collector/v1/metrics "))
        .expect("metrics are exported at shutdown before the periodic interval")
        .1;
    let metrics = ExportMetricsServiceRequest::decode(metric_bytes.as_slice()).unwrap();
    assert!(
        metrics
            .resource_metrics
            .iter()
            .flat_map(|resource| &resource.scope_metrics)
            .flat_map(|scope| &scope.metrics)
            .any(|metric| metric.name == "completed_jobs")
    );
}
