//! Payment aggregate and provider port. Persistence and orchestration live in
//! the application layer; no database or provider I/O belongs in this domain.

mod aggregate;
mod gateway;
mod model;

pub use aggregate::Payment;
pub use gateway::{DemoGateway, PaymentGateway};
pub use model::{OperationKind, PaymentOperation, PaymentProgress, PaymentStatus, ProviderOutcome};

#[cfg(test)]
mod tests;
