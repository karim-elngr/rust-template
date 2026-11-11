//! Small cross-domain value types; domain events stay with their owning domain.

mod money;
pub use money::Money;

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
#[error("{0}")]
pub struct DomainError(pub String);
