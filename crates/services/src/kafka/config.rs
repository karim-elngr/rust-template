use platform::config::KafkaConfig;
use rdkafka::ClientConfig;

pub(super) fn client_config(config: &KafkaConfig) -> ClientConfig {
    let mut result = ClientConfig::new();
    result
        .set("bootstrap.servers", &config.brokers)
        .set("security.protocol", &config.security_protocol)
        .set("socket.timeout.ms", "5000")
        .set("socket.connection.setup.timeout.ms", "5000")
        .set("allow.auto.create.topics", "false");
    for (name, value) in [
        ("sasl.mechanism", &config.sasl_mechanism),
        ("sasl.username", &config.sasl_username),
        ("sasl.password", &config.sasl_password),
        ("ssl.ca.location", &config.ssl_ca_location),
    ] {
        if let Some(value) = value {
            result.set(name, value);
        }
    }
    result
}
