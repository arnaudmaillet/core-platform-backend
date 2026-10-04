use std::fmt;

use serde::{Deserialize, Serialize};

use crate::error::AccountError;

/// Lifecycle state of an account.
///
/// Transitions are governed by [`AccountStatus::can_transition_to`]. Any
/// attempt to apply a domain method that would violate a guard is rejected at
/// the aggregate level before any state mutation occurs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccountStatus {
    /// Email verification is pending. The account exists but cannot perform
    /// most operations until the email is confirmed.
    PendingVerification,
    /// Fully operational account.
    Active,
    /// Temporarily restricted by an administrator. The account holder cannot
    /// log in or perform operations until the suspension is lifted.
    Suspended,
    /// Self-deactivated by the account holder. Can be reactivated.
    Deactivated,
    /// Permanently closed. Reached after GDPR anonymisation completes or via
    /// an admin hard-delete. Terminal state — no transitions out.
    Deleted,
}

impl AccountStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::PendingVerification => "pending_verification",
            Self::Active              => "active",
            Self::Suspended           => "suspended",
            Self::Deactivated         => "deactivated",
            Self::Deleted             => "deleted",
        }
    }

    /// Returns `true` if the transition `self → next` is permitted by the
    /// domain state machine.
    ///
    /// A suspended account cannot deactivate itself: signing in reactivates a
    /// self-deactivated account, so `Suspended → Deactivated` would let the
    /// holder lift an admin suspension by deactivating and logging back in.
    pub fn can_transition_to(&self, next: AccountStatus) -> bool {
        use AccountStatus::*;
        matches!(
            (self, next),
            (PendingVerification, Active)
                | (PendingVerification, Deleted)
                | (Active, Suspended)
                | (Active, Deactivated)
                | (Active, Deleted)
                | (Suspended, Active)
                | (Suspended, Deleted)
                | (Deactivated, Active)
                | (Deactivated, Deleted)
        )
    }
}

impl fmt::Display for AccountStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl TryFrom<&str> for AccountStatus {
    type Error = AccountError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s {
            "pending_verification" => Ok(Self::PendingVerification),
            "active"               => Ok(Self::Active),
            "suspended"            => Ok(Self::Suspended),
            "deactivated"          => Ok(Self::Deactivated),
            "deleted"              => Ok(Self::Deleted),
            other => Err(AccountError::InvalidAccountStatus(other.to_owned())),
        }
    }
}

impl TryFrom<String> for AccountStatus {
    type Error = AccountError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::try_from(s.as_str())
    }
}
