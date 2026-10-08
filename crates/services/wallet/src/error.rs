use error::{AppError, Severity};
use http::StatusCode;
use thiserror::Error;

/// The wallet service's errors.
///
/// | Code     | Variant               | HTTP | Retryable |
/// |----------|-----------------------|------|-----------|
/// | WAL-3001 | GemSpendingRestricted | 403  | No        |
/// | WAL-5001 | LedgerInconsistent    | 500  | No        |
/// | WAL-6001 | PeerUnavailable       | 503  | **Yes**   |
/// | WAL-6002 | EventPublishFailed    | 503  | **Yes**   |
/// | WAL-9001 | InvalidAccountId      | 422  | No        |
/// | WAL-9002 | InvalidIdempotencyKey | 422  | No        |
/// | WAL-9003 | InvalidPageToken      | 422  | No        |
/// | WAL-9004 | InvalidSpend          | 422  | No        |
/// | DB-*     | Storage (delegated)   | var  | var       |
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum WalletError {
    #[error(transparent)]
    Storage(#[from] postgres_storage::StorageError),

    /// A stored row the service cannot read (fail closed: never guess).
    #[error("the ledger holds an unreadable row: {reason}")]
    LedgerInconsistent { reason: String },

    #[error("invalid account id: '{value}'")]
    InvalidAccountId { value: String },

    #[error("an idempotency key is 8–64 characters of letters, digits, '-' or '_'")]
    InvalidIdempotencyKey,

    #[error("invalid page_token: '{value}'")]
    InvalidPageToken { value: String },

    /// Gems are not spent under 18 (or with an unknown age).
    #[error("gems cannot be spent by this account")]
    GemSpendingRestricted,

    #[error("invalid gem spend: {reason}")]
    InvalidSpend { reason: String },

    /// post or comment could not tell what a like lands on (fail closed).
    #[error("{service} unavailable: {reason}")]
    PeerUnavailable { service: &'static str, reason: String },

    /// A stake was recorded but not announced: the batch's retry (same key)
    /// announces it.
    #[error("event publish failed: {0}")]
    EventPublishFailed(String),
}

impl AppError for WalletError {
    fn error_code(&self) -> &'static str {
        match self {
            WalletError::Storage(e) => e.error_code(),
            WalletError::LedgerInconsistent { .. } => "WAL-5001",
            WalletError::InvalidAccountId { .. } => "WAL-9001",
            WalletError::InvalidIdempotencyKey => "WAL-9002",
            WalletError::InvalidPageToken { .. } => "WAL-9003",
            WalletError::GemSpendingRestricted => "WAL-3001",
            WalletError::InvalidSpend { .. } => "WAL-9004",
            WalletError::PeerUnavailable { .. } => "WAL-6001",
            WalletError::EventPublishFailed(_) => "WAL-6002",
        }
    }

    fn http_status(&self) -> StatusCode {
        match self {
            WalletError::Storage(e) => e.http_status(),
            WalletError::LedgerInconsistent { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            WalletError::GemSpendingRestricted => StatusCode::FORBIDDEN,
            WalletError::PeerUnavailable { .. } | WalletError::EventPublishFailed(_) => StatusCode::SERVICE_UNAVAILABLE,
            _ => StatusCode::UNPROCESSABLE_ENTITY,
        }
    }

    fn severity(&self) -> Severity {
        match self {
            WalletError::Storage(e) => e.severity(),
            WalletError::LedgerInconsistent { .. } => Severity::Critical,
            WalletError::PeerUnavailable { .. } | WalletError::EventPublishFailed(_) => Severity::Medium,
            _ => Severity::Low,
        }
    }

    fn is_retryable(&self) -> bool {
        match self {
            WalletError::Storage(e) => e.is_retryable(),
            WalletError::PeerUnavailable { .. } | WalletError::EventPublishFailed(_) => true,
            _ => false,
        }
    }

    fn category(&self) -> &'static str {
        match self {
            WalletError::Storage(e) => e.category(),
            _ => "WAL",
        }
    }

    fn user_facing_message(&self) -> &'static str {
        match self {
            WalletError::Storage(e) => e.user_facing_message(),
            WalletError::LedgerInconsistent { .. } => "Your wallet is unavailable right now; please try again later.",
            WalletError::InvalidAccountId { .. } => "This account id is not valid.",
            WalletError::InvalidIdempotencyKey => "This request key is not valid.",
            WalletError::InvalidPageToken { .. } => "This page is no longer valid; start again.",
            WalletError::GemSpendingRestricted => "Gems can't be spent before 18.",
            WalletError::InvalidSpend { .. } => "This purchase is not valid.",
            WalletError::PeerUnavailable { .. } | WalletError::EventPublishFailed(_) => {
                "Your likes couldn't be sent right now; they will be retried."
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_are_in_the_wal_namespace() {
        assert_eq!(WalletError::InvalidIdempotencyKey.error_code(), "WAL-9002");
        assert_eq!(WalletError::InvalidIdempotencyKey.category(), "WAL");
        assert_eq!(WalletError::LedgerInconsistent { reason: "x".into() }.http_status(), StatusCode::INTERNAL_SERVER_ERROR);
    }
}
