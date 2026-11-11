//! Kafka transport adapters; domain decisions remain in application handlers.
mod config;
mod consumer;
mod producer;
mod trace;

pub use consumer::{consume, consumer, decode_event};
pub use producer::{producer, publish_next};

#[cfg(test)]
mod tests;
