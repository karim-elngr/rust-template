use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};
use commerce::{CommerceEvent, Store};
use platform::config::KafkaConfig;
use rdkafka::{
    ClientConfig, Offset, TopicPartitionList,
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    consumer::{Consumer, StreamConsumer},
};
use sqlx::postgres::PgPoolOptions;
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{CmdWaitFor, ExecCommand, IntoContainerPort, WaitFor, wait::LogWaitStrategy},
    runners::AsyncRunner,
};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

fn image_from_reference(reference: &str) -> Result<GenericImage> {
    let (name, tag) = reference
        .rsplit_once(':')
        .context("test image requires an explicit tag or digest")?;
    if name.is_empty()
        || tag.is_empty()
        || tag.contains('/')
        || reference.chars().any(char::is_whitespace)
    {
        bail!("invalid test image reference");
    }
    // GenericImage rejoins these fields with ':', preserving repo@sha256:digest.
    Ok(GenericImage::new(name, tag))
}

fn configured_image(variable: &str, default: &str) -> Result<GenericImage> {
    let reference = match std::env::var(variable) {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => default.into(),
        Err(std::env::VarError::NotUnicode(_)) => bail!("{variable} must contain valid Unicode"),
    };
    image_from_reference(&reference)
}

#[test]
fn container_image_references_preserve_tags_ports_and_digests() {
    for reference in [
        "apache/kafka:4.4.0",
        "registry.example:5000/company/kafka:4.4.0",
        "registry.example/company/kafka@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    ] {
        let image = image_from_reference(reference).expect("valid explicit image reference");
        assert_eq!(
            testcontainers::ContainerRequest::from(image).descriptor(),
            reference
        );
    }
    for invalid in [
        "apache/kafka",
        "apache/kafka:",
        ":4.4.0",
        "registry.example:5000/kafka",
    ] {
        assert!(image_from_reference(invalid).is_err());
    }
}

pub struct Fixture {
    pub store: Store,
    pub config: KafkaConfig,
    _postgres: ContainerAsync<GenericImage>,
    _kafka: ContainerAsync<GenericImage>,
}

impl Fixture {
    pub async fn new() -> Result<Self> {
        let (postgres, kafka) = tokio::try_join!(postgres(), kafka())?;
        let (postgres, store) = postgres;
        let (kafka, brokers) = kafka;
        let config = KafkaConfig {
            brokers,
            topic: format!("commerce-{}", Uuid::new_v4().simple()),
            group_id: format!("payments-{}", Uuid::new_v4().simple()),
            security_protocol: "PLAINTEXT".into(),
            sasl_mechanism: None,
            sasl_username: None,
            sasl_password: None,
            ssl_ca_location: None,
        };
        let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
            .set("bootstrap.servers", &config.brokers)
            .set("socket.timeout.ms", "3000")
            .create()?;
        // A bounded, observable broker request is the readiness condition. It also
        // creates the explicit single-partition topic used for offset assertions.
        tokio::time::timeout(Duration::from_secs(90), async {
            loop {
                let options = AdminOptions::new()
                    .operation_timeout(Some(Duration::from_secs(3)))
                    .request_timeout(Some(Duration::from_secs(3)));
                if let Ok(results) = admin
                    .create_topics(
                        &[NewTopic::new(&config.topic, 1, TopicReplication::Fixed(1))],
                        &options,
                    )
                    .await
                    && results.iter().all(|result| {
                        result.is_ok()
                            || matches!(
                                result,
                                Err((_, rdkafka::types::RDKafkaErrorCode::TopicAlreadyExists))
                            )
                    })
                {
                    return Ok::<(), anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        })
        .await
        .context("Kafka did not become ready within 90 seconds")??;
        Ok(Self {
            store,
            config,
            _postgres: postgres,
            _kafka: kafka,
        })
    }

    pub async fn event(&self, checkout_id: Uuid, event_type: &str) -> Result<CommerceEvent> {
        let payload: serde_json::Value = sqlx::query_scalar(
            "SELECT payload FROM outbox WHERE payload->>'checkout_id'=$1 AND payload->>'event_type'=$2 ORDER BY created_at LIMIT 1"
        ).bind(checkout_id.to_string()).bind(event_type).fetch_one(self.store.pool()).await?;
        Ok(serde_json::from_value(payload)?)
    }

    pub fn poll(&self) -> Result<Running> {
        let cancel = CancellationToken::new();
        let token = cancel.clone();
        let store = self.store.clone();
        let topic = self.config.topic.clone();
        let producer = services::kafka::producer(&self.config)?;
        let task = tokio::spawn(async move {
            loop {
                if token.is_cancelled() {
                    return Ok(());
                }
                while services::kafka::publish_next(&store, &producer, &topic, 30).await? {
                    if token.is_cancelled() {
                        return Ok(());
                    }
                }
                tokio::select! {_=token.cancelled()=>return Ok(()),_=tokio::time::sleep(Duration::from_millis(25))=>{}}
            }
        });
        Ok(Running { cancel, task })
    }

    pub fn consume(&self, consumer: Arc<StreamConsumer>) -> Running {
        let cancel = CancellationToken::new();
        let token = cancel.clone();
        let store = self.store.clone();
        let topic = self.config.topic.clone();
        let task =
            tokio::spawn(
                async move { services::kafka::consume(store, consumer, &topic, token).await },
            );
        Running { cancel, task }
    }
}

async fn postgres() -> Result<(ContainerAsync<GenericImage>, Store)> {
    let container = configured_image("TEST_POSTGRES_IMAGE", "postgres:18.6-bookworm")?
        .with_exposed_port(5432.tcp())
        // The temporary init server logs to stdout; final TCP server readiness is stderr.
        .with_wait_for(WaitFor::log(LogWaitStrategy::stderr(
            "database system is ready to accept connections",
        )))
        .with_env_var("POSTGRES_PASSWORD", "test-only-password")
        .with_env_var("POSTGRES_DB", "commerce")
        .with_startup_timeout(Duration::from_secs(90))
        .start()
        .await?;
    let url = format!(
        "postgres://postgres:test-only-password@{}:{}/commerce",
        container.get_host().await?,
        container.get_host_port_ipv4(5432).await?
    );
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .acquire_timeout(Duration::from_secs(20))
        .connect(&url)
        .await?;
    let store = Store::new(pool);
    store.migrate().await?;
    Ok((container, store))
}

async fn kafka() -> Result<(ContainerAsync<GenericImage>, String)> {
    // The broker must advertise Docker's dynamically allocated external port.
    // Start a waiting shell, obtain that port, then atomically provide its value.
    // The official image's startup script still performs all Kafka configuration.
    let container = configured_image("TEST_KAFKA_IMAGE", "apache/kafka:4.4.0")?
        .with_exposed_port(19092.tcp())
        .with_entrypoint("/bin/sh")
        .with_cmd(["-c", "while [ ! -f /tmp/test-advertised-listeners ]; do sleep 0.1; done; export KAFKA_ADVERTISED_LISTENERS=\"$(cat /tmp/test-advertised-listeners)\"; exec /etc/kafka/docker/run"])
        .with_env_var("KAFKA_NODE_ID", "1")
        .with_env_var("KAFKA_PROCESS_ROLES", "broker,controller")
        .with_env_var("KAFKA_LISTENERS", "INTERNAL://:9092,EXTERNAL://:19092,CONTROLLER://:9093")
        .with_env_var("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT")
        .with_env_var("KAFKA_INTER_BROKER_LISTENER_NAME", "INTERNAL")
        .with_env_var("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
        .with_env_var("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9093")
        .with_env_var("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .with_env_var("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR", "1")
        .with_env_var("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR", "1")
        .with_env_var("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
        .with_env_var("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
        .with_env_var("KAFKA_LOG_DIRS", "/var/lib/kafka/data")
        .with_env_var("KAFKA_HEAP_OPTS", "-Xms256m -Xmx256m")
        .with_env_var("CLUSTER_ID", "MkU3OEVBNTcwNTJENDM2Qk")
        .with_startup_timeout(Duration::from_secs(90))
        .start().await?;
    let brokers = format!(
        "{}:{}",
        container.get_host().await?,
        container.get_host_port_ipv4(19092).await?
    );
    container.exec(ExecCommand::new(["/bin/sh", "-c", "printf '%s' \"$TEST_LISTENERS\" > /tmp/test-advertised-listeners.tmp && mv /tmp/test-advertised-listeners.tmp /tmp/test-advertised-listeners"])
        .with_env_vars([("TEST_LISTENERS", format!("INTERNAL://localhost:9092,EXTERNAL://{brokers}"))])
        .with_cmd_ready_condition(CmdWaitFor::exit_code(0))).await?;
    Ok((container, brokers))
}

pub struct Running {
    pub cancel: CancellationToken,
    pub task: JoinHandle<Result<()>>,
}

impl Running {
    pub async fn stop(mut self) -> Result<()> {
        self.cancel.cancel();
        tokio::time::timeout(Duration::from_secs(10), &mut self.task)
            .await
            .context("task did not shut down")??
    }

    pub async fn expect_failure(mut self) -> Result<anyhow::Error> {
        let result = tokio::time::timeout(Duration::from_secs(45), &mut self.task)
            .await
            .context("consumer did not fail within its deadline")??;
        match result {
            Ok(()) => bail!("consumer unexpectedly accepted a failed record"),
            Err(error) => Ok(error),
        }
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        self.cancel.cancel();
        self.task.abort();
    }
}

pub async fn committed_offset(consumer: &Arc<StreamConsumer>, topic: &str) -> Result<Offset> {
    let mut partitions = TopicPartitionList::new();
    partitions.add_partition(topic, 0);
    let querying = consumer.clone();
    let offsets = tokio::task::spawn_blocking(move || {
        querying.committed_offsets(partitions, Duration::from_secs(5))
    })
    .await??;
    Ok(offsets
        .find_partition(topic, 0)
        .context("missing requested partition")?
        .offset())
}

pub async fn wait_for_offset(
    consumer: &Arc<StreamConsumer>,
    topic: &str,
    expected: i64,
) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(45), async {
        loop {
            if matches!(committed_offset(consumer, topic).await?, Offset::Offset(value) if value >= expected) {
                return Ok::<(), anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }).await.context("consumer did not commit the expected offset")?
}
