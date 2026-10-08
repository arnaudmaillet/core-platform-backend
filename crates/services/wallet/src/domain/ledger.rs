//! The ledger's vocabulary: currencies, why a balance moved, and the keys
//! that make a credit happen at most once.

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::domain::AccountId;
use crate::error::WalletError;

/// The two in-app currencies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Currency {
    /// Shown as likes.
    Points,
    Gems,
}

impl Currency {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Points => "points",
            Self::Gems => "gems",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "points" => Some(Self::Points),
            "gems" => Some(Self::Gems),
            _ => None,
        }
    }
}

/// Why a balance moved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionKind {
    /// The hourly reward.
    Claim,
    /// The gems a wallet opens with.
    StarterGift,
    /// Written by a newer version of this service.
    Unknown,
}

impl TransactionKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Claim => "claim",
            Self::StarterGift => "starter_gift",
            Self::Unknown => "unknown",
        }
    }

    pub fn parse(value: &str) -> Self {
        match value {
            "claim" => Self::Claim,
            "starter_gift" => Self::StarterGift,
            _ => Self::Unknown,
        }
    }
}

/// One movement of one balance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Transaction {
    pub id:            Uuid,
    pub account:       AccountId,
    pub currency:      Currency,
    /// Positive: credited; negative: spent.
    pub delta:         i64,
    pub balance_after: i64,
    pub kind:          TransactionKind,
    pub created_at:    DateTime<Utc>,
}

/// Makes a movement happen at most once per account. A client's key is
/// 8–64 characters of `[A-Za-z0-9_-]`; the service's own start with `sys:`,
/// which no client key can.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdempotencyKey(String);

impl IdempotencyKey {
    pub fn from_client(value: &str) -> Result<Self, WalletError> {
        let valid = (8..=64).contains(&value.len())
            && value.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_');
        if valid { Ok(Self(value.to_owned())) } else { Err(WalletError::InvalidIdempotencyKey) }
    }

    /// The service's own key for a one-off movement.
    pub fn system(name: &str) -> Self {
        Self(format!("sys:{name}"))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_keys_are_checked_and_never_collide_with_the_services() {
        assert!(IdempotencyKey::from_client(&Uuid::now_v7().to_string()).is_ok());
        assert!(IdempotencyKey::from_client("short").is_err());
        assert!(IdempotencyKey::from_client("sys:starter-gems").is_err());
        assert!(IdempotencyKey::from_client(&"k".repeat(65)).is_err());
        assert_eq!(IdempotencyKey::system("starter-gems").as_str(), "sys:starter-gems");
    }

    #[test]
    fn names_round_trip() {
        for c in [Currency::Points, Currency::Gems] {
            assert_eq!(Currency::parse(c.as_str()), Some(c));
        }
        for k in [TransactionKind::Claim, TransactionKind::StarterGift] {
            assert_eq!(TransactionKind::parse(k.as_str()), k);
        }
        assert_eq!(TransactionKind::parse("boost_spend"), TransactionKind::Unknown);
    }
}
