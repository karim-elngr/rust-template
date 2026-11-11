use std::{
    process::ExitCode,
    str::FromStr,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use commerce::Store;
use contracts::commerce::v1::{
    cart_service_server::CartServiceServer, checkout_service_server::CheckoutServiceServer,
    order_service_server::OrderServiceServer, payment_service_server::PaymentServiceServer,
};
use platform::{Telemetry, config::AppConfig};
use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    producer::{FutureProducer, Producer},
};
use sqlx::{
    ConnectOptions,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use tokio::{net::TcpListener, task::JoinSet};
use tokio_util::sync::CancellationToken;

use crate::{api, grpc::GrpcCommerce, health::Health, kafka};

const GRPC_SERVICES: [&str; 4] = [
    "commerce.v1.CartService",
    "commerce.v1.CheckoutService",
    "commerce.v1.OrderService",
    "commerce.v1.PaymentService",
];

#[derive(Clone, Copy, Debug)]
pub enum Role {
    HttpApi,
    GrpcApi,
    OutboxPoller,
    KafkaConsumer,
    Cronjob,
    Migrate,
}

impl Role {
    pub fn name(self) -> &'static str {
        match self {
            Self::HttpApi => "http-api",
            Self::GrpcApi => "grpc-api",
            Self::OutboxPoller => "kafka-outbox-poller",
            Self::KafkaConsumer => "kafka-consumer",
            Self::Cronjob => "cronjob",
            Self::Migrate => "migrate",
        }
    }
}

/// Explicit runtime teardown bounds any native call still running after async
/// task cancellation. With default settings the complete shutdown budget is
/// 25s grace + 1s abort join + 2s native disposal + 2s pool closure + 5s telemetry
/// + 2s runtime teardown = 37s, within the example pod's 45s allowance.
pub fn entrypoint(role: Role) -> ExitCode {
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(_) => {
            eprintln!("could not initialize application runtime");
            return ExitCode::FAILURE;
        }
    };
    let result = runtime.block_on(run(role));
    runtime.shutdown_timeout(Duration::from_secs(2));
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

pub async fn run(role: Role) -> Result<()> {
    let config = AppConfig::from_env(role.name())?;
    let telemetry = Telemetry::init(&config.telemetry, role.name())?;
    let cancel = CancellationToken::new();
    let signal_cancel = cancel.clone();
    let signal_task = tokio::spawn(async move {
        let result = platform::shutdown::signal().await;
        if result.is_err() {
            tracing::error!("could not listen for shutdown signal");
        }
        signal_cancel.cancel();
        result
    });
    tracing::info!(service.name = role.name(), "starting service");
    let result = run_inner(role, &config, cancel.clone()).await;
    cancel.cancel();
    let signal_result = finish_signal(signal_task).await;
    let result = result.and(signal_result);
    if result.is_err() {
        tracing::error!(event.name = "process.failed", "service failed");
    }
    tracing::info!(
        service.name = role.name(),
        "service stopped; flushing telemetry"
    );
    let flushed = telemetry.shutdown().await;
    result.and(flushed)
}

fn validate_gateway_composition(role: Role, environment: &str) -> Result<()> {
    if environment == "production" && matches!(role, Role::KafkaConsumer) {
        bail!("replace DemoGateway with company PaymentGateway before production");
    }
    Ok(())
}

async fn connect(role: Role, config: &AppConfig) -> Result<Store> {
    // Store::new currently composes the in-process demo payment provider.
    // Replace this composition and guard together when integrating the company gateway.
    validate_gateway_composition(role, &config.environment)?;
    let options = PgConnectOptions::from_str(&config.database_url)
        .map_err(|_| anyhow!("DATABASE_URL is invalid"))?
        .application_name(&config.service_name)
        .options([("statement_timeout", "10000"), ("lock_timeout", "3000")])
        .disable_statement_logging();
    let pool = PgPoolOptions::new()
        .max_connections(config.db_max_connections)
        .acquire_timeout(Duration::from_secs(5))
        .connect_with(options)
        .await
        .map_err(|_| anyhow!("could not connect to PostgreSQL"))?;
    Ok(Store::new(pool))
}

async fn run_inner(role: Role, config: &AppConfig, cancel: CancellationToken) -> Result<()> {
    let store = tokio::select! {
        result = connect(role, config) => result?,
        _ = cancel.cancelled() => bail!("startup interrupted"),
    };
    let result = run_connected(role, config, store.clone(), cancel).await;
    // Pool closure is bounded independently; dropped transactions roll back.
    let _ = tokio::time::timeout(Duration::from_secs(2), store.pool().close()).await;
    result
}

enum NativeKeepalive {
    Producer(FutureProducer),
    Consumer(Arc<StreamConsumer>),
}

async fn run_connected(
    role: Role,
    config: &AppConfig,
    store: Store,
    cancel: CancellationToken,
) -> Result<()> {
    if matches!(role, Role::Migrate | Role::Cronjob) {
        let job = async {
            if matches!(role, Role::Migrate) {
                store.migrate().await?;
                tracing::info!("migrations applied");
            } else {
                let mut total = 0_u64;
                loop {
                    if cancel.is_cancelled() {
                        bail!("scheduled job interrupted")
                    }
                    let count = store.expire_pending(config.expiry_batch_size).await?;
                    total += count;
                    if count < config.expiry_batch_size as u64 {
                        break;
                    }
                }
                opentelemetry::global::meter("commerce")
                    .u64_counter("checkouts.expired")
                    .build()
                    .add(total, &[]);
                tracing::info!(
                    event.name = "checkouts.expired",
                    count = total,
                    "checkout expiry sweep requested compensation"
                );
            }
            Ok(())
        };
        tokio::pin!(job);
        return tokio::select! {
            result=&mut job=>result,
            _=cancel.cancelled()=>tokio::time::timeout(config.shutdown_timeout,&mut job).await.context("one-off job exceeded shutdown deadline")?,
        };
    }
    let health = Health::new(
        store.clone(),
        cancel.clone(),
        !matches!(role, Role::OutboxPoller | Role::KafkaConsumer),
    );
    let listener = TcpListener::bind(config.health_addr)
        .await
        .context("could not bind health address")?;
    let mut tasks = JoinSet::new();
    let health_cancel = cancel.clone();
    let health_router = health.clone().router();
    tasks.spawn(async move {
        axum::serve(listener, health_router)
            .with_graceful_shutdown(health_cancel.cancelled_owned())
            .await
            .context("health server failed")
    });
    // Retain a native handle while all async tasks exit or are aborted so their
    // cleanup cannot perform the final blocking native-client destruction.
    let mut keepalive = None;
    let started: Result<()> = async {
        match role {
        Role::HttpApi => {
            let listener = TcpListener::bind(config.http_addr)
                .await
                .context("could not bind HTTP address")?;
            let router = api::router(store, config.request_timeout).merge(health.router());
            let shutdown = cancel.clone();
            tasks.spawn(async move {
                axum::serve(listener, router)
                    .with_graceful_shutdown(shutdown.cancelled_owned())
                    .await
                    .context("HTTP server failed")
            });
        }
        Role::GrpcApi => {
            let implementation = GrpcCommerce { store };
            let carts = CartServiceServer::new(implementation.clone())
                .max_decoding_message_size(64 * 1024)
                .max_encoding_message_size(64 * 1024);
            let checkouts = CheckoutServiceServer::new(implementation.clone())
                .max_decoding_message_size(64 * 1024)
                .max_encoding_message_size(64 * 1024);
            let orders = OrderServiceServer::new(implementation.clone())
                .max_decoding_message_size(64 * 1024)
                .max_encoding_message_size(64 * 1024);
            let payments = PaymentServiceServer::new(implementation)
                .max_decoding_message_size(64 * 1024)
                .max_encoding_message_size(64 * 1024);
            let (reporter, health_service) = tonic_health::server::health_reporter();
            let health_check = health.clone();
            let monitor_cancel = cancel.clone();
            tasks.spawn(async move {
                loop {
                    let serving = if health_check.ready().await {
                        tonic_health::ServingStatus::Serving
                    } else {
                        tonic_health::ServingStatus::NotServing
                    };
                    reporter.set_service_status("", serving).await;
                    for service in GRPC_SERVICES {
                        reporter.set_service_status(service, serving).await;
                    }
                    tokio::select! { _=monitor_cancel.cancelled()=>break, _=tokio::time::sleep(Duration::from_secs(1))=>{} }
                }
                reporter.set_service_status("", tonic_health::ServingStatus::NotServing).await;
                for service in GRPC_SERVICES {
                    reporter.set_service_status(service, tonic_health::ServingStatus::NotServing).await;
                }
                Ok(())
            });
            let addr = config.grpc_addr;
            let timeout = config.request_timeout;
            let shutdown = cancel.clone();
            tasks.spawn(async move {
                tonic::transport::Server::builder()
                    .timeout(timeout)
                    .concurrency_limit_per_connection(128)
                    .add_service(health_service)
                    .add_service(carts)
                    .add_service(checkouts)
                    .add_service(orders)
                    .add_service(payments)
                    .serve_with_shutdown(addr, shutdown.cancelled_owned())
                    .await
                    .context("gRPC server failed")
            });
        }
        Role::OutboxPoller => {
            let producer = kafka::producer(&config.kafka)?;
            keepalive = Some(NativeKeepalive::Producer(producer.clone()));
            let checking = producer.clone();
            let topic = config.kafka.topic.clone();
            spawn_broker_monitor(&mut tasks, health.clone(), cancel.clone(), move || {
                checking
                    .client()
                    .fetch_metadata(Some(&topic), Duration::from_secs(2))
                    .map(|metadata| {
                        metadata
                            .topics()
                            .iter()
                            .any(|t| t.name() == topic && t.error().is_none())
                    })
                    .unwrap_or(false)
            });
            let topic = config.kafka.topic.clone();
            let lease = config.outbox_lease_seconds;
            let batch = config.outbox_batch_size;
            let interval = config.poll_interval;
            let shutdown = cancel.clone();
            tasks.spawn(async move {
                loop {
                    for _ in 0..batch {
                        if shutdown.is_cancelled() {
                            return Ok(());
                        }
                        match kafka::publish_next(&store, &producer, &topic, lease).await {
                            Ok(true) => {},
                            Ok(false) => break,
                            Err(_) => {
                                tracing::warn!(event.name="outbox.retry", "outbox publish failed; will retry");
                                break;
                            }
                        }
                    }
                    tokio::select! {_=shutdown.cancelled()=>break,_=tokio::time::sleep(interval)=>{}}
                }
                Ok(())
            });
        }
        Role::KafkaConsumer => {
            let consumer = Arc::new(kafka::consumer(&config.kafka)?);
            keepalive = Some(NativeKeepalive::Consumer(consumer.clone()));
            let checking = consumer.clone();
            let topic = config.kafka.topic.clone();
            spawn_broker_monitor(&mut tasks, health, cancel.clone(), move || {
                checking
                    .fetch_metadata(Some(&topic), Duration::from_secs(2))
                    .map(|metadata| {
                        metadata
                            .topics()
                            .iter()
                            .any(|t| t.name() == topic && t.error().is_none())
                    })
                    .unwrap_or(false)
            });
            let topic = config.kafka.topic.clone();
            let shutdown = cancel.clone();
            tasks.spawn(async move { kafka::consume(store, consumer, &topic, shutdown).await });
        }
        Role::Cronjob | Role::Migrate => unreachable!(),
        }
        Ok(())
    }
    .await;
    let result = match started {
        Ok(()) => supervise(tasks, cancel.clone(), config.shutdown_timeout).await,
        Err(error) => {
            cancel.cancel();
            abort_and_join(&mut tasks).await;
            Err(error)
        }
    };
    let cleanup = if let Some(keepalive) = keepalive {
        native_task("kafka-disposal", Duration::from_secs(2), move || {
            match keepalive {
                NativeKeepalive::Producer(producer) => drop(producer),
                NativeKeepalive::Consumer(consumer) => drop(consumer),
            }
            Ok(())
        })
        .await
    } else {
        Ok(())
    };
    result.and(cleanup)
}

fn spawn_broker_monitor<F>(
    tasks: &mut JoinSet<Result<()>>,
    health: Health,
    cancel: CancellationToken,
    check: F,
) where
    F: Fn() -> bool + Send + Sync + 'static,
{
    let check = Arc::new(check);
    tasks.spawn(async move {
        loop {
            if cancel.is_cancelled() {break;}
            let probe=check.clone();
            let ready=tokio::task::spawn_blocking(move || probe()).await.context("broker readiness task failed")?;
            health.dependency_ready.store(ready,Ordering::Relaxed);
            tokio::select! {_=cancel.cancelled()=>break,_=tokio::time::sleep(Duration::from_secs(5))=>{}}
        }
        Ok(())
    });
}

pub(crate) async fn native_task<T: Send + 'static>(
    name: &'static str,
    timeout: Duration,
    work: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T> {
    let (sender, receiver) = tokio::sync::oneshot::channel();
    std::thread::Builder::new()
        .name(name.to_owned())
        .spawn(move || {
            let _ = sender.send(work());
        })
        .with_context(|| format!("could not start {name} worker"))?;
    tokio::time::timeout(timeout, receiver)
        .await
        .with_context(|| format!("{name} deadline exceeded"))?
        .with_context(|| format!("{name} worker exited unexpectedly"))?
}

async fn finish_signal(task: tokio::task::JoinHandle<std::io::Result<()>>) -> Result<()> {
    task.abort();
    match task.await {
        Ok(result) => result.context("shutdown signal listener failed"),
        Err(error) if error.is_cancelled() => Ok(()),
        Err(error) => Err(error).context("shutdown signal task failed"),
    }
}

async fn abort_and_join(tasks: &mut JoinSet<Result<()>>) {
    tasks.abort_all();
    let _ = tokio::time::timeout(Duration::from_secs(1), async {
        while tasks.join_next().await.is_some() {}
    })
    .await;
}

pub async fn supervise(
    mut tasks: JoinSet<Result<()>>,
    cancel: CancellationToken,
    grace: Duration,
) -> Result<()> {
    let mut first = tokio::select! {
        biased;
        _=cancel.cancelled()=>Ok(()),
        result=tasks.join_next()=>match result {
            Some(Ok(Err(error)))=>Err(error),
            Some(Err(error))=>Err(error.into()),
            _=>Err(anyhow!("a long-running service task stopped unexpectedly")),
        },
    };
    cancel.cancel();
    let drained = tokio::time::timeout(grace, async {
        while let Some(result) = tasks.join_next().await {
            let result = match result {
                Ok(result) => result,
                Err(error) => Err(error.into()),
            };
            if first.is_ok() {
                first = result;
            }
        }
    })
    .await;
    match drained {
        Ok(()) => first,
        Err(_) => {
            abort_and_join(&mut tasks).await;
            bail!("service exceeded graceful shutdown deadline")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn demo_gateway_is_never_composed_for_production_consumer() {
        assert_eq!(
            validate_gateway_composition(Role::KafkaConsumer, "production")
                .unwrap_err()
                .to_string(),
            "replace DemoGateway with company PaymentGateway before production"
        );
        assert!(validate_gateway_composition(Role::KafkaConsumer, "local").is_ok());
        assert!(validate_gateway_composition(Role::HttpApi, "production").is_ok());
    }

    #[tokio::test]
    async fn completed_signal_failure_is_not_reported_as_success() {
        let task = tokio::spawn(async { Err(std::io::Error::other("signal unavailable")) });
        tokio::task::yield_now().await;
        assert!(finish_signal(task).await.is_err());
    }

    #[tokio::test]
    async fn unused_signal_listener_is_cancelled_without_error() {
        let task = tokio::spawn(std::future::pending::<std::io::Result<()>>());
        assert!(finish_signal(task).await.is_ok());
    }

    #[tokio::test]
    async fn native_task_completes_off_the_async_worker_and_reports_its_error() {
        let caller = std::thread::current().id();
        let worker = native_task("test-native", Duration::from_secs(1), || {
            Ok(std::thread::current().id())
        })
        .await
        .unwrap();
        assert_ne!(caller, worker);
        let result: Result<()> = native_task("test-native-error", Duration::from_secs(1), || {
            bail!("native failure")
        })
        .await;
        assert!(result.unwrap_err().to_string().contains("native failure"));
    }

    #[tokio::test]
    async fn native_task_deadline_does_not_wait_for_completion() {
        let (release, blocked) = std::sync::mpsc::channel::<()>();
        let result = native_task("test-native-stuck", Duration::from_millis(10), move || {
            let _ = blocked.recv();
            Ok(())
        })
        .await;
        drop(release);
        assert!(result.unwrap_err().to_string().contains("deadline"));
    }

    #[tokio::test]
    async fn drain_error_does_not_abort_other_inflight_work() {
        let cancel = CancellationToken::new();
        cancel.cancel();
        let completed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let finished = completed.clone();
        let mut tasks = JoinSet::new();
        tasks.spawn(async { bail!("shutdown error") });
        tasks.spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            finished.store(true, Ordering::SeqCst);
            Ok(())
        });
        assert!(
            supervise(tasks, cancel, Duration::from_secs(1))
                .await
                .is_err()
        );
        assert!(completed.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn worker_failure_cancels_siblings_and_returns_failure() {
        let token = CancellationToken::new();
        let sibling = token.clone();
        let mut tasks = JoinSet::new();
        tasks.spawn(async { bail!("worker failed") });
        tasks.spawn(async move {
            sibling.cancelled().await;
            Ok(())
        });
        assert!(
            supervise(tasks, token.clone(), Duration::from_secs(1))
                .await
                .is_err()
        );
        assert!(token.is_cancelled());
    }

    #[tokio::test]
    async fn shutdown_drains_inflight_work_and_times_out_stuck_tasks() {
        let token = CancellationToken::new();
        let sibling = token.clone();
        let mut tasks = JoinSet::new();
        tasks.spawn(async move {
            sibling.cancelled().await;
            Ok(())
        });
        token.cancel();
        assert!(
            supervise(tasks, token, Duration::from_secs(1))
                .await
                .is_ok()
        );
        let token = CancellationToken::new();
        token.cancel();
        let mut tasks = JoinSet::new();
        tasks.spawn(std::future::pending::<Result<()>>());
        assert!(
            supervise(tasks, token, Duration::from_millis(5))
                .await
                .is_err()
        );
    }
}
