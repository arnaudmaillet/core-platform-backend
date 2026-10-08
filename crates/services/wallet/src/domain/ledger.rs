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
    /// A ×100 stake pack bought with gems.
    StakePack,
    /// A country unlocked on the map with gems (`ref_id`: the country).
    CountryUnlock,
    /// Likes staked on a post or a comment (`ref_id`: `post:<id>`).
    Stake,
    /// Written by a newer version of this service.
    Unknown,
}

impl TransactionKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Claim => "claim",
            Self::StarterGift => "starter_gift",
            Self::StakePack => "stake_pack",
            Self::CountryUnlock => "country_unlock",
            Self::Stake => "stake",
            Self::Unknown => "unknown",
        }
    }

    pub fn parse(value: &str) -> Self {
        match value {
            "claim" => Self::Claim,
            "starter_gift" => Self::StarterGift,
            "stake_pack" => Self::StakePack,
            "country_unlock" => Self::CountryUnlock,
            "stake" => Self::Stake,
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
    /// What it was for, when it names something (a country code).
    pub ref_id:        Option<String>,
    pub created_at:    DateTime<Utc>,
}

/// The operations a caller's key is scoped to: a key used for one never
/// answers for another (a claim's key cannot pass as a paid pack).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    Claim,
    StakePack,
    /// A spend asked by another service.
    SpendGems,
    /// A batch of likes.
    Stake,
}

impl Operation {
    fn prefix(self) -> &'static str {
        match self {
            Self::Claim => "claim",
            Self::StakePack => "pack",
            Self::SpendGems => "spend",
            Self::Stake => "stake",
        }
    }
}

/// Makes a movement happen at most once per account. A caller's key is 8–64
/// characters of `[A-Za-z0-9_-]`, stored scoped to its operation
/// (`claim:<key>`); the service's own start with `sys:`. No caller key holds
/// a `:`, so none can collide with another operation's or the service's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdempotencyKey(String);

impl IdempotencyKey {
    pub fn for_operation(operation: Operation, value: &str) -> Result<Self, WalletError> {
        let valid = (8..=64).contains(&value.len())
            && value.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_');
        if valid { Ok(Self(format!("{}:{value}", operation.prefix()))) } else { Err(WalletError::InvalidIdempotencyKey) }
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
    fn caller_keys_are_checked_scoped_and_never_collide() {
        let key = Uuid::now_v7().to_string();
        let claim = IdempotencyKey::for_operation(Operation::Claim, &key).unwrap();
        let pack = IdempotencyKey::for_operation(Operation::StakePack, &key).unwrap();
        assert_ne!(claim, pack, "one key, two operations: two movements");
        assert_eq!(claim.as_str(), format!("claim:{key}"));
        assert!(IdempotencyKey::for_operation(Operation::Claim, "short").is_err());
        assert!(IdempotencyKey::for_operation(Operation::Claim, "sys:starter-gems").is_err());
        assert!(IdempotencyKey::for_operation(Operation::Claim, &"k".repeat(65)).is_err());
        assert_eq!(IdempotencyKey::system("starter-gems").as_str(), "sys:starter-gems");
    }

    #[test]
    fn names_round_trip() {
        for c in [Currency::Points, Currency::Gems] {
            assert_eq!(Currency::parse(c.as_str()), Some(c));
        }
        for k in [TransactionKind::Claim, TransactionKind::StarterGift, TransactionKind::StakePack, TransactionKind::CountryUnlock] {
            assert_eq!(TransactionKind::parse(k.as_str()), k);
        }
        assert_eq!(TransactionKind::parse("boost_spend"), TransactionKind::Unknown);
    }
}
