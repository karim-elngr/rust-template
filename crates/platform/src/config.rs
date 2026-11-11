//! Validated settings loaded once, before accepting work.

use std::{net::SocketAddr, time::Duration};

use anyhow::{Result, bail, ensure};

/// Configuration deliberately does not implement `Debug`: database and broker URLs
/// may contain credentials.
pub struct AppConfig {
    pub service_name: String,
    pub environment: String,
    pub database_url: String,
    pub db_max_connections: u32,
    pub http_addr: SocketAddr,
    pub grpc_addr: SocketAddr,
    pub health_addr: SocketAddr,
    pub request_timeout: Duration,
    pub shutdown_timeout: Duration,
    pub poll_interval: Duration,
    pub outbox_batch_size: i64,
    pub outbox_lease_seconds: i64,
    pub expiry_batch_size: i64,
    pub api_auth_mode: String,
    pub kafka: KafkaConfig,
    pub telemetry: TelemetryConfig,
}

/// Keep secrets out of derived formatting and structured logs.
pub struct KafkaConfig {
    pub brokers: String,
    pub topic: String,
    pub group_id: String,
    pub security_protocol: String,
    pub sasl_mechanism: Option<String>,
    pub sasl_username: Option<String>,
    pub sasl_password: Option<String>,
    pub ssl_ca_location: Option<String>,
}

#[derive(Clone)]
pub struct TelemetryConfig {
    pub enabled: bool,
    pub environment: String,
    pub otlp_endpoint: Option<String>,
    pub traces_endpoint: Option<String>,
    pub metrics_endpoint: Option<String>,
    pub export_timeout: Duration,
    pub metric_export_interval: Duration,
    pub shutdown_timeout: Duration,
    pub log_buffer_lines: usize,
    pub log_filter: String,
}

impl AppConfig {
    pub fn from_env(service_name: &str) -> anyhow::Result<Self> {
        let invalid = std::cell::RefCell::new(None);
        let result = Self::from_lookup(service_name, |key| match std::env::var(key) {
            Ok(value) => Some(value),
            Err(std::env::VarError::NotPresent) => None,
            Err(std::env::VarError::NotUnicode(_)) => {
                *invalid.borrow_mut() = Some(key.to_owned());
                None
            }
        });
        if let Some(name) = invalid.into_inner() {
            bail!("{name} must contain valid Unicode");
        }
        result
    }

    /// Injectable lookup avoids changing process-wide environment in tests.
    pub fn from_lookup(
        service_name: &str,
        lookup: impl Fn(&str) -> Option<String>,
    ) -> anyhow::Result<Self> {
        ensure!(
            !service_name.trim().is_empty(),
            "service_name must not be empty"
        );
        let env = Settings(lookup);
        let environment = env.value("APP_ENV", "local");
        ensure!(
            matches!(environment.as_str(), "local" | "test" | "production"),
            "APP_ENV must be local, test, or production"
        );
        let database_url = env.required("DATABASE_URL")?;
        let parsed_database = url::Url::parse(&database_url)
            .map_err(|_| anyhow::anyhow!("DATABASE_URL must be a valid PostgreSQL URL"))?;
        ensure!(
            matches!(parsed_database.scheme(), "postgres" | "postgresql"),
            "DATABASE_URL must use postgres or postgresql"
        );
        if environment == "production" {
            let ssl_modes: Vec<_> = parsed_database
                .query_pairs()
                .filter(|(key, _)| key == "sslmode")
                .map(|(_, value)| value.into_owned())
                .collect();
            ensure!(
                ssl_modes == ["verify-full"],
                "DATABASE_URL requires exactly one sslmode=verify-full in production"
            );
        }

        let api_auth_mode = env.value("API_AUTH_MODE", "local-unsafe");
        if matches!(service_name, "http-api" | "grpc-api") {
            ensure!(
                matches!(api_auth_mode.as_str(), "local-unsafe" | "upstream"),
                "API_AUTH_MODE must be local-unsafe or upstream"
            );
            ensure!(
                environment != "production" || api_auth_mode == "upstream",
                "API_AUTH_MODE=upstream is required in production; deploy the company authentication boundary before exposing the API"
            );
        }

        let kafka = KafkaConfig {
            brokers: env.value("KAFKA_BROKERS", "localhost:19092"),
            topic: env.value("KAFKA_TOPIC", "commerce.events.v1"),
            group_id: env.value("KAFKA_GROUP_ID", "commerce-workflows-v1"),
            security_protocol: env.value("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
            sasl_mechanism: env.optional("KAFKA_SASL_MECHANISM"),
            sasl_username: env.optional("KAFKA_SASL_USERNAME"),
            sasl_password: env.optional("KAFKA_SASL_PASSWORD"),
            ssl_ca_location: env.optional("KAFKA_SSL_CA_LOCATION"),
        };
        if matches!(service_name, "kafka-consumer" | "kafka-outbox-poller") {
            ensure!(
                matches!(
                    kafka.security_protocol.as_str(),
                    "PLAINTEXT" | "SSL" | "SASL_PLAINTEXT" | "SASL_SSL"
                ),
                "KAFKA_SECURITY_PROTOCOL is unsupported"
            );
            ensure!(
                environment != "production"
                    || matches!(kafka.security_protocol.as_str(), "SSL" | "SASL_SSL"),
                "KAFKA_SECURITY_PROTOCOL must be SSL or SASL_SSL in production"
            );
            if kafka.security_protocol.starts_with("SASL_") {
                for (name, value) in [
                    ("KAFKA_SASL_MECHANISM", &kafka.sasl_mechanism),
                    ("KAFKA_SASL_USERNAME", &kafka.sasl_username),
                    ("KAFKA_SASL_PASSWORD", &kafka.sasl_password),
                ] {
                    ensure!(
                        value.is_some(),
                        "{name} is required for SASL authentication"
                    );
                }
                ensure!(
                    matches!(
                        kafka.sasl_mechanism.as_deref(),
                        Some("PLAIN" | "SCRAM-SHA-256" | "SCRAM-SHA-512")
                    ),
                    "KAFKA_SASL_MECHANISM must be PLAIN, SCRAM-SHA-256, or SCRAM-SHA-512"
                );
            }
        }

        let otlp_endpoint = env.endpoint("OTEL_EXPORTER_OTLP_ENDPOINT")?;
        let traces_endpoint = env.endpoint("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")?;
        let metrics_endpoint = env.endpoint("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT")?;
        let enabled = env.boolean(
            "OTEL_ENABLED",
            otlp_endpoint.is_some() || traces_endpoint.is_some() || metrics_endpoint.is_some(),
        )?;
        for name in [
            "OTEL_EXPORTER_OTLP_PROTOCOL",
            "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL",
            "OTEL_EXPORTER_OTLP_METRICS_PROTOCOL",
        ] {
            if let Some(protocol) = env.optional(name) {
                ensure!(
                    protocol == "http/protobuf",
                    "{name} must be http/protobuf for this template"
                );
            }
        }
        let log_filter = env.value("RUST_LOG", "info");
        ensure!(
            tracing_subscriber::EnvFilter::try_new(&log_filter).is_ok(),
            "RUST_LOG must be a valid tracing filter"
        );
        let telemetry = TelemetryConfig {
            enabled,
            environment: environment.clone(),
            otlp_endpoint,
            traces_endpoint,
            metrics_endpoint,
            export_timeout: Duration::from_millis(env.number(
                "OTEL_EXPORTER_OTLP_TIMEOUT",
                3000,
                1,
                300_000,
            )?),
            metric_export_interval: Duration::from_millis(env.number(
                "OTEL_METRIC_EXPORT_INTERVAL",
                10_000,
                1,
                3_600_000,
            )?),
            shutdown_timeout: Duration::from_secs(env.number(
                "TELEMETRY_SHUTDOWN_TIMEOUT_SECONDS",
                5,
                1,
                300,
            )?),
            log_buffer_lines: env.number("LOG_BUFFER_LINES", 8192, 1, 1_000_000)? as usize,
            log_filter,
        };
        Ok(Self {
            service_name: service_name.to_owned(),
            environment,
            database_url,
            db_max_connections: env.number("DB_MAX_CONNECTIONS", 10, 1, 1000)? as u32,
            http_addr: env.address("HTTP_ADDR", "127.0.0.1:8080")?,
            grpc_addr: env.address("GRPC_ADDR", "127.0.0.1:50051")?,
            health_addr: env.address("HEALTH_ADDR", "127.0.0.1:9000")?,
            request_timeout: Duration::from_secs(env.number(
                "REQUEST_TIMEOUT_SECONDS",
                10,
                1,
                3600,
            )?),
            shutdown_timeout: Duration::from_secs(env.number(
                "SHUTDOWN_TIMEOUT_SECONDS",
                25,
                1,
                300,
            )?),
            poll_interval: Duration::from_millis(env.number("POLL_INTERVAL_MS", 500, 1, 60_000)?),
            outbox_batch_size: env.number("OUTBOX_BATCH_SIZE", 50, 1, 1000)? as i64,
            outbox_lease_seconds: env.number("OUTBOX_LEASE_SECONDS", 60, 1, 3600)? as i64,
            expiry_batch_size: env.number("EXPIRY_BATCH_SIZE", 100, 1, 1000)? as i64,
            api_auth_mode,
            kafka,
            telemetry,
        })
    }
}

struct Settings<F>(F);

impl<F: Fn(&str) -> Option<String>> Settings<F> {
    fn optional(&self, name: &str) -> Option<String> {
        (self.0)(name).filter(|value| !value.trim().is_empty())
    }

    fn required(&self, name: &str) -> Result<String> {
        self.optional(name)
            .ok_or_else(|| anyhow::anyhow!("{name} is required"))
    }

    fn value(&self, name: &str, default: &str) -> String {
        self.optional(name).unwrap_or_else(|| default.to_owned())
    }

    fn number(&self, name: &str, default: u64, min: u64, max: u64) -> Result<u64> {
        let Some(value) = self.optional(name) else {
            return Ok(default);
        };
        let parsed = value
            .parse::<u64>()
            .map_err(|_| anyhow::anyhow!("{name} must be an integer in {min}..={max}"))?;
        ensure!(
            (min..=max).contains(&parsed),
            "{name} must be in {min}..={max}"
        );
        Ok(parsed)
    }

    fn address(&self, name: &str, default: &str) -> Result<SocketAddr> {
        self.value(name, default)
            .parse()
            .map_err(|_| anyhow::anyhow!("{name} must be an IP address and port"))
    }

    fn boolean(&self, name: &str, default: bool) -> Result<bool> {
        match self.optional(name).as_deref() {
            None => Ok(default),
            Some("true" | "1") => Ok(true),
            Some("false" | "0") => Ok(false),
            _ => bail!("{name} must be true or false"),
        }
    }

    fn endpoint(&self, name: &str) -> Result<Option<String>> {
        let Some(value) = self.optional(name) else {
            return Ok(None);
        };
        let url = url::Url::parse(&value)
            .map_err(|_| anyhow::anyhow!("{name} must be an HTTP or HTTPS URL"))?;
        ensure!(
            matches!(url.scheme(), "http" | "https") && url.host_str().is_some(),
            "{name} must be an HTTP or HTTPS URL"
        );
        ensure!(
            url.query().is_none()
                && url.fragment().is_none()
                && url.username().is_empty()
                && url.password().is_none(),
            "{name} must not contain credentials, query parameters, or a fragment; use OTLP headers for authentication"
        );
        Ok(Some(value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(service: &str, values: &[(&str, &str)]) -> anyhow::Result<AppConfig> {
        AppConfig::from_lookup(service, |key| {
            values
                .iter()
                .find(|(name, _)| *name == key)
                .map(|(_, value)| (*value).to_owned())
        })
    }

    fn error(service: &str, values: &[(&str, &str)]) -> String {
        config(service, values)
            .err()
            .expect("expected invalid configuration")
            .to_string()
    }

    #[test]
    fn local_defaults_bind_loopback_and_bound_work() {
        let cfg = config(
            "http-api",
            &[("DATABASE_URL", "postgres://local@localhost/commerce")],
        )
        .unwrap();
        assert_eq!(cfg.http_addr, "127.0.0.1:8080".parse().unwrap());
        assert_eq!(cfg.grpc_addr, "127.0.0.1:50051".parse().unwrap());
        assert_eq!(cfg.health_addr, "127.0.0.1:9000".parse().unwrap());
        assert_eq!(cfg.request_timeout, Duration::from_secs(10));
        assert_eq!(cfg.db_max_connections, 10);
        assert_eq!(cfg.shutdown_timeout, Duration::from_secs(25));
        assert_eq!(cfg.poll_interval, Duration::from_millis(500));
        assert_eq!(cfg.outbox_batch_size, 50);
        assert_eq!(cfg.outbox_lease_seconds, 60);
        assert_eq!(cfg.expiry_batch_size, 100);
        assert_eq!(cfg.api_auth_mode, "local-unsafe");
        assert!(!cfg.telemetry.enabled);
    }

    #[test]
    fn database_url_is_required() {
        assert!(error("http-api", &[]).contains("DATABASE_URL"));
    }

    #[test]
    fn invalid_config_errors_do_not_echo_secret_input() {
        let message = error(
            "http-api",
            &[("DATABASE_URL", "postgres://alice:secret-password@[")],
        );
        assert!(message.contains("DATABASE_URL"));
        assert!(!message.contains("secret-password"));
    }

    #[test]
    fn zero_or_malformed_work_limits_are_rejected() {
        for (name, value) in [
            ("REQUEST_TIMEOUT_SECONDS", "0"),
            ("SHUTDOWN_TIMEOUT_SECONDS", "-1"),
            ("POLL_INTERVAL_MS", "NaN"),
            ("OUTBOX_BATCH_SIZE", "0"),
            ("OUTBOX_BATCH_SIZE", "1001"),
            ("OUTBOX_LEASE_SECONDS", "0"),
            ("OUTBOX_LEASE_SECONDS", "3601"),
            ("EXPIRY_BATCH_SIZE", "0"),
            ("EXPIRY_BATCH_SIZE", "1001"),
            ("LOG_BUFFER_LINES", "0"),
            ("DB_MAX_CONNECTIONS", "1001"),
            ("HTTP_ADDR", "bad address"),
            ("APP_ENV", "prodution"),
        ] {
            assert!(
                error(
                    "http-api",
                    &[("DATABASE_URL", "postgres://localhost/db"), (name, value)]
                )
                .contains(name),
                "{name}"
            );
        }
    }

    #[test]
    fn production_api_requires_upstream_auth_boundary() {
        let message = error(
            "http-api",
            &[
                (
                    "DATABASE_URL",
                    "postgres://localhost/db?sslmode=verify-full",
                ),
                ("APP_ENV", "production"),
            ],
        );
        assert!(message.contains("API_AUTH_MODE"));
    }

    #[test]
    fn production_api_does_not_require_unused_kafka_credentials() {
        let cfg = config(
            "grpc-api",
            &[
                (
                    "DATABASE_URL",
                    "postgres://localhost/db?sslmode=verify-full",
                ),
                ("APP_ENV", "production"),
                ("API_AUTH_MODE", "upstream"),
            ],
        )
        .unwrap();
        assert_eq!(cfg.api_auth_mode, "upstream");
    }

    #[test]
    fn production_database_requires_unambiguous_verify_full() {
        for database in [
            "postgres://localhost/db",
            "postgres://localhost/db?sslmode=require",
            "postgres://localhost/db?sslmode=verify-full&sslmode=disable",
        ] {
            assert!(
                error(
                    "cronjob",
                    &[("DATABASE_URL", database), ("APP_ENV", "production")]
                )
                .contains("sslmode")
            );
        }
    }

    #[test]
    fn production_worker_requires_encrypted_kafka() {
        let message = error(
            "kafka-consumer",
            &[
                (
                    "DATABASE_URL",
                    "postgres://localhost/db?sslmode=verify-full",
                ),
                ("APP_ENV", "production"),
            ],
        );
        assert!(message.contains("KAFKA_SECURITY_PROTOCOL"));
    }

    #[test]
    fn sasl_requires_complete_credentials_without_leaking_them() {
        let message = error(
            "kafka-consumer",
            &[
                ("DATABASE_URL", "postgres://localhost/db"),
                ("KAFKA_SECURITY_PROTOCOL", "SASL_SSL"),
                ("KAFKA_SASL_USERNAME", "user-secret"),
                ("KAFKA_SASL_PASSWORD", "password-secret"),
            ],
        );
        assert!(message.contains("KAFKA_SASL_MECHANISM"));
        assert!(!message.contains("user-secret"));
        assert!(!message.contains("password-secret"));
    }

    #[test]
    fn explicit_telemetry_settings_are_validated() {
        let cfg = config(
            "cronjob",
            &[
                ("DATABASE_URL", "postgres://localhost/db"),
                (
                    "OTEL_EXPORTER_OTLP_ENDPOINT",
                    "https://collector.example/otlp/",
                ),
                ("OTEL_EXPORTER_OTLP_TIMEOUT", "1500"),
                ("OTEL_METRIC_EXPORT_INTERVAL", "2000"),
            ],
        )
        .unwrap();
        assert!(cfg.telemetry.enabled);
        assert_eq!(cfg.telemetry.export_timeout, Duration::from_millis(1500));
        assert_eq!(
            cfg.telemetry.metric_export_interval,
            Duration::from_millis(2000)
        );
        assert_eq!(cfg.telemetry.shutdown_timeout, Duration::from_secs(5));
    }
}
