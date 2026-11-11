//! Durable checkout saga state; payment and order remain independent aggregates.
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use shared_kernel::DomainError;
use uuid::Uuid;
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckoutStatus {
    AwaitingAuthorization,
    AwaitingCapture,
    AwaitingOrderConfirmation,
    Completed,
    Compensating,
    Cancelled,
    Failed,
    NeedsAttention,
}
impl CheckoutStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AwaitingAuthorization => "awaiting_authorization",
            Self::AwaitingCapture => "awaiting_capture",
            Self::AwaitingOrderConfirmation => "awaiting_order_confirmation",
            Self::Completed => "completed",
            Self::Compensating => "compensating",
            Self::Cancelled => "cancelled",
            Self::Failed => "failed",
            Self::NeedsAttention => "needs_attention",
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckoutLinks {
    pub id: Uuid,
    pub cart_id: Uuid,
    pub order_id: Uuid,
    pub payment_id: Uuid,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckoutSaga {
    links: CheckoutLinks,
    status: CheckoutStatus,
    version: i64,
    cancellation_requested: bool,
    failed: bool,
    created_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
}
impl CheckoutSaga {
    pub fn new(links: CheckoutLinks, now: DateTime<Utc>) -> Self {
        Self {
            links,
            status: CheckoutStatus::AwaitingAuthorization,
            version: 0,
            cancellation_requested: false,
            failed: false,
            created_at: now,
            expires_at: now + chrono::Duration::minutes(30),
        }
    }
    pub fn links(&self) -> &CheckoutLinks {
        &self.links
    }
    pub fn status(&self) -> CheckoutStatus {
        self.status
    }
    pub fn version(&self) -> i64 {
        self.version
    }
    pub fn cancellation_requested(&self) -> bool {
        self.cancellation_requested
    }
    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }
    pub fn expires_at(&self) -> DateTime<Utc> {
        self.expires_at
    }
    fn bump(&mut self) -> Result<(), DomainError> {
        self.version = self
            .version
            .checked_add(1)
            .ok_or_else(|| DomainError("checkout version exhausted".into()))?;
        Ok(())
    }
    fn advance(&mut self, from: CheckoutStatus, to: CheckoutStatus) -> Result<(), DomainError> {
        if self.status != from || self.cancellation_requested {
            return Err(DomainError("invalid checkout transition".into()));
        }
        self.bump()?;
        self.status = to;
        Ok(())
    }
    pub fn authorized(&mut self) -> Result<(), DomainError> {
        self.advance(
            CheckoutStatus::AwaitingAuthorization,
            CheckoutStatus::AwaitingCapture,
        )
    }
    pub fn captured(&mut self) -> Result<(), DomainError> {
        self.advance(
            CheckoutStatus::AwaitingCapture,
            CheckoutStatus::AwaitingOrderConfirmation,
        )
    }
    pub fn complete(&mut self) -> Result<(), DomainError> {
        self.advance(
            CheckoutStatus::AwaitingOrderConfirmation,
            CheckoutStatus::Completed,
        )
    }
    pub fn cancel(&mut self) -> Result<(), DomainError> {
        if self.status == CheckoutStatus::Completed {
            return Err(DomainError(
                "completed checkout cancellation requires a separate returns policy".into(),
            ));
        }
        if self.cancellation_requested {
            return Ok(());
        }
        self.bump()?;
        self.cancellation_requested = true;
        self.status = CheckoutStatus::Compensating;
        Ok(())
    }
    pub fn fail(&mut self) -> Result<(), DomainError> {
        if matches!(
            self.status,
            CheckoutStatus::Cancelled | CheckoutStatus::NeedsAttention
        ) {
            return Err(DomainError(
                "terminal checkout requires an explicit recovery transition".into(),
            ));
        }
        if self.failed {
            return Ok(());
        }
        if self.status == CheckoutStatus::Completed {
            return Err(DomainError("completed checkout cannot fail".into()));
        }
        self.bump()?;
        self.failed = true;
        self.cancellation_requested = true;
        self.status = CheckoutStatus::Compensating;
        Ok(())
    }
    pub fn compensated(&mut self) -> Result<(), DomainError> {
        if matches!(
            self.status,
            CheckoutStatus::Cancelled | CheckoutStatus::Failed
        ) {
            return Ok(());
        }
        if self.status != CheckoutStatus::Compensating || !self.cancellation_requested {
            return Err(DomainError("checkout is not compensating".into()));
        }
        self.bump()?;
        self.status = if self.failed {
            CheckoutStatus::Failed
        } else {
            CheckoutStatus::Cancelled
        };
        Ok(())
    }
    pub fn needs_attention(&mut self) -> Result<(), DomainError> {
        if self.status != CheckoutStatus::Compensating {
            return Err(DomainError(
                "only failed compensation requires attention".into(),
            ));
        }
        self.bump()?;
        self.status = CheckoutStatus::NeedsAttention;
        self.cancellation_requested = true;
        Ok(())
    }
    pub fn validate(&self) -> Result<(), DomainError> {
        if [
            self.links.id,
            self.links.cart_id,
            self.links.order_id,
            self.links.payment_id,
        ]
        .iter()
        .any(Uuid::is_nil)
            || self.version < 0
            || self.expires_at <= self.created_at
        {
            return Err(DomainError("invalid checkout snapshot".into()));
        }
        if matches!(
            self.status,
            CheckoutStatus::Completed
                | CheckoutStatus::AwaitingAuthorization
                | CheckoutStatus::AwaitingCapture
                | CheckoutStatus::AwaitingOrderConfirmation
        ) && self.cancellation_requested
        {
            return Err(DomainError("invalid checkout cancellation state".into()));
        }
        if matches!(
            self.status,
            CheckoutStatus::Compensating
                | CheckoutStatus::Cancelled
                | CheckoutStatus::Failed
                | CheckoutStatus::NeedsAttention
        ) && !self.cancellation_requested
        {
            return Err(DomainError("missing checkout cancellation state".into()));
        }
        Ok(())
    }
    pub fn record_progress(&mut self) -> Result<(), DomainError> {
        self.bump()
    }
    pub fn is_pending(&self) -> bool {
        matches!(
            self.status,
            CheckoutStatus::AwaitingAuthorization
                | CheckoutStatus::AwaitingCapture
                | CheckoutStatus::AwaitingOrderConfirmation
        )
    }
}
