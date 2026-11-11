//! HTTP transport: routing, validation, mapping, and observability.
mod dto;
mod errors;
mod handlers;
mod middleware;
mod routes;

pub use middleware::trace_headers;
pub use routes::router;
#[cfg(test)]
mod tests;
