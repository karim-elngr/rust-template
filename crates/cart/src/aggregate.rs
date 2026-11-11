//! Cart aggregate: only server-priced commands can change an open cart.
use serde::{Deserialize, Serialize};
use shared_kernel::DomainError;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CartStatus {
    Open,
    CheckedOut,
}
impl CartStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::CheckedOut => "checked_out",
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CartLine {
    pub sku: String,
    pub quantity: i32,
    pub unit_price_minor: i64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cart {
    id: Uuid,
    customer_id: Uuid,
    version: i64,
    status: CartStatus,
    items: Vec<CartLine>,
}
impl Cart {
    pub fn new(id: Uuid, customer_id: Uuid) -> Self {
        Self {
            id,
            customer_id,
            version: 0,
            status: CartStatus::Open,
            items: vec![],
        }
    }
    pub fn id(&self) -> Uuid {
        self.id
    }
    pub fn customer_id(&self) -> Uuid {
        self.customer_id
    }
    pub fn version(&self) -> i64 {
        self.version
    }
    pub fn status(&self) -> CartStatus {
        self.status
    }
    pub fn items(&self) -> &[CartLine] {
        &self.items
    }
    pub fn add(
        &mut self,
        sku: String,
        quantity: i32,
        price: i64,
        expected: i64,
    ) -> Result<(), DomainError> {
        self.check_command(expected)?;
        let mut next = self.clone();
        if let Some(line) = next.items.iter_mut().find(|line| line.sku == sku) {
            line.quantity = quantity;
            line.unit_price_minor = price;
        } else {
            next.items.push(CartLine {
                sku,
                quantity,
                unit_price_minor: price,
            });
        }
        next.items.sort_by(|a, b| a.sku.cmp(&b.sku));
        next.version = next
            .version
            .checked_add(1)
            .ok_or_else(|| DomainError("cart version exhausted".into()))?;
        next.validate()?;
        *self = next;
        Ok(())
    }
    pub fn remove(&mut self, sku: &str, expected: i64) -> Result<(), DomainError> {
        self.check_command(expected)?;
        let index = self
            .items
            .iter()
            .position(|line| line.sku == sku)
            .ok_or_else(|| DomainError("item not found".into()))?;
        let version = self
            .version
            .checked_add(1)
            .ok_or_else(|| DomainError("cart version exhausted".into()))?;
        self.items.remove(index);
        self.version = version;
        Ok(())
    }
    pub fn checkout(&mut self, expected: i64) -> Result<(), DomainError> {
        self.check_command(expected)?;
        self.validate()?;
        if self.items.is_empty() {
            return Err(DomainError("cannot checkout an empty cart".into()));
        }
        self.version = self
            .version
            .checked_add(1)
            .ok_or_else(|| DomainError("cart version exhausted".into()))?;
        self.status = CartStatus::CheckedOut;
        Ok(())
    }
    pub fn total_minor(&self) -> Result<i64, DomainError> {
        self.items.iter().try_fold(0_i64, |sum, item| {
            if item.quantity <= 0 || item.unit_price_minor <= 0 {
                return Err(DomainError("quantity and price must be positive".into()));
            }
            item.unit_price_minor
                .checked_mul(i64::from(item.quantity))
                .and_then(|amount| sum.checked_add(amount))
                .ok_or_else(|| DomainError("cart amount exceeds supported range".into()))
        })
    }
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.id.is_nil()
            || self.customer_id.is_nil()
            || self.version < 0
            || self.items.len() > 100
        {
            return Err(DomainError("invalid cart snapshot".into()));
        }
        let mut seen = std::collections::BTreeSet::new();
        for item in &self.items {
            if item.sku.is_empty()
                || item.sku.len() > 64
                || item.sku.chars().any(char::is_whitespace)
                || !seen.insert(&item.sku)
            {
                return Err(DomainError("invalid or duplicate SKU".into()));
            }
        }
        self.total_minor()?;
        if self.status == CartStatus::CheckedOut && self.items.is_empty() {
            return Err(DomainError("checked out cart must contain items".into()));
        }
        Ok(())
    }
    fn check_command(&self, expected: i64) -> Result<(), DomainError> {
        if self.status != CartStatus::Open {
            return Err(DomainError("cart has already been checked out".into()));
        }
        if self.version != expected {
            return Err(DomainError("cart version conflict".into()));
        }
        Ok(())
    }
}
