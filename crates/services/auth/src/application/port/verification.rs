use async_trait::async_trait;
use chrono::Duration;

use crate::error::AuthError;

/// Where a one-time code goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerificationChannel {
    Email,
    /// Not exposed yet (phone accounts need `account` to make the email optional).
    Sms,
}

impl VerificationChannel {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Email => "email",
            Self::Sms => "sms",
        }
    }
}

/// A code that was sent, as stored (the code itself only as a hash).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingChallenge {
    pub challenge_id: String,
    pub channel:      VerificationChannel,
    /// The normalized address.
    pub destination:  String,
    /// `SHA-256(challenge_id ":" code)`, hex.
    pub code_hash:    String,
}

/// An address someone just proved they control.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedDestination {
    pub channel:     VerificationChannel,
    pub destination: String,
}

/// Whether another code may be sent to an address now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendAdmission {
    Allowed,
    /// Too many codes for this address: retry after this many seconds.
    Refused { retry_after_secs: i64 },
}

/// Pending codes and the per-address send budget (Redis).
#[async_trait]
pub trait VerificationStore: Send + Sync + 'static {
    /// Counts one send to `destination_key` (a hash of the address): at most
    /// `per_hour` an hour, and not within `resend` of the previous one.
    async fn admit_send(
        &self,
        destination_key: &str,
        per_hour: u32,
        resend: Duration,
    ) -> Result<SendAdmission, AuthError>;

    async fn save(&self, challenge: &PendingChallenge, ttl: Duration, max_attempts: u32) -> Result<(), AuthError>;

    /// Checks `code_hash` against the challenge: on a match the challenge is
    /// consumed (single use) and its address returned; on a miss one attempt is
    /// spent, and the challenge is dropped once they are all spent. `None` for a
    /// wrong code, an unknown or an expired challenge — never told apart.
    async fn consume(&self, challenge_id: &str, code_hash: &str) -> Result<Option<VerifiedDestination>, AuthError>;

    async fn discard(&self, challenge_id: &str) -> Result<(), AuthError>;
}

/// Delivers a one-time code (email via SES SMTP; a log line locally).
#[async_trait]
pub trait CodeSender: Send + Sync + 'static {
    /// Errors: [`AuthError::VerificationChannelUnavailable`] (no transport for
    /// the channel) or [`AuthError::VerificationSendFailed`].
    async fn send(
        &self,
        channel: VerificationChannel,
        destination: &str,
        code: &str,
        locale: Option<&str>,
    ) -> Result<(), AuthError>;
}
