use crate::DomainError;
use serde::{Deserialize, Serialize};

/// Nonnegative minor currency units. Currency is a three-uppercase-letter code;
/// supported currencies and their minor-unit scale remain catalog/provider policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "MoneyWire")]
pub struct Money {
    amount_minor: i64,
    currency: String,
}

impl Money {
    pub fn new(amount_minor: i64, currency: impl Into<String>) -> Result<Self, DomainError> {
        let currency = currency.into();
        if amount_minor < 0 {
            return Err(DomainError("money cannot be negative".into()));
        }
        if currency.len() != 3 || !currency.bytes().all(|byte| byte.is_ascii_uppercase()) {
            return Err(DomainError(
                "currency must be three uppercase ASCII letters".into(),
            ));
        }
        Ok(Self {
            amount_minor,
            currency,
        })
    }

    pub fn amount_minor(&self) -> i64 {
        self.amount_minor
    }
    pub fn currency(&self) -> &str {
        &self.currency
    }
    pub fn checked_add(&self, other: &Self) -> Result<Self, DomainError> {
        if self.currency != other.currency {
            return Err(DomainError("cannot add different currencies".into()));
        }
        let amount = self
            .amount_minor
            .checked_add(other.amount_minor)
            .ok_or_else(|| DomainError("money addition overflow".into()))?;
        Self::new(amount, self.currency.clone())
    }
    pub fn checked_mul(&self, quantity: u32) -> Result<Self, DomainError> {
        let amount = self
            .amount_minor
            .checked_mul(i64::from(quantity))
            .ok_or_else(|| DomainError("money multiplication overflow".into()))?;
        Self::new(amount, self.currency.clone())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MoneyWire {
    amount_minor: i64,
    currency: String,
}

impl TryFrom<MoneyWire> for Money {
    type Error = DomainError;
    fn try_from(value: MoneyWire) -> Result<Self, Self::Error> {
        Self::new(value.amount_minor, value.currency)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn money_rejects_negative_amount_and_malformed_currency() {
        assert!(Money::new(-1, "USD").is_err());
        for currency in ["usd", "US", "USDD", "12A", "€UR"] {
            assert!(Money::new(1, currency).is_err(), "accepted {currency}");
        }
    }

    #[test]
    fn arithmetic_preserves_currency_and_checks_overflow() {
        let unit = Money::new(125, "USD").unwrap();
        let total = unit.checked_mul(3).unwrap();
        assert_eq!(total.amount_minor(), 375);
        assert_eq!(total.currency(), "USD");
        assert_eq!(unit.checked_add(&total).unwrap().amount_minor(), 500);
        assert!(unit.checked_add(&Money::new(1, "EUR").unwrap()).is_err());
        assert!(
            Money::new(i64::MAX, "USD")
                .unwrap()
                .checked_add(&unit)
                .is_err()
        );
        assert!(Money::new(i64::MAX, "USD").unwrap().checked_mul(2).is_err());
        assert_eq!(unit.checked_mul(0).unwrap(), Money::new(0, "USD").unwrap());
    }

    #[test]
    fn deserialization_cannot_bypass_money_validation() {
        assert!(serde_json::from_str::<Money>(r#"{"amount_minor":-1,"currency":"USD"}"#).is_err());
        assert!(serde_json::from_str::<Money>(r#"{"amount_minor":10,"currency":"usd"}"#).is_err());
        let valid = Money::new(100, "USD").unwrap();
        assert_eq!(
            serde_json::from_str::<Money>(&serde_json::to_string(&valid).unwrap()).unwrap(),
            valid
        );
    }
}
