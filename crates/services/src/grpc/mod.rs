//! Domain-specific gRPC services with shared validation and trace middleware.
mod errors;
mod mapping;
mod middleware;
mod services;

#[derive(Clone)]
pub struct GrpcCommerce {
    pub store: commerce::Store,
}
