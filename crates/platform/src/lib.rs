//! Shared process configuration, telemetry, and operating-system shutdown signals.

pub mod config;
pub mod shutdown;
pub mod telemetry;

pub use telemetry::{Telemetry, capture_trace_context, extract_context};
