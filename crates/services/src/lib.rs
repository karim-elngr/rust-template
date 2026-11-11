//! Process composition and transport adapters.
pub mod api;
pub mod grpc;
pub mod health;
pub mod kafka;
pub mod lifecycle;

pub use lifecycle::{Role, entrypoint, run};
