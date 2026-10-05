use async_trait::async_trait;
use chrono::Duration;

use crate::error::AuthError;

/// Where a one-time code goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerificationChannel {
    Email,
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
    /// The address's hash, keying its send budget and failure count.
    pub destination_key: String,
    /// `SHA-256(challenge_id ":" code)`, hex.
    pub code_hash:    String,
    /// The reader's language, for a notice about this address later.
    pub locale:       Option<String>,
}

/// An address someone just proved they control.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedDestination {
    pub channel:     VerificationChannel,
    pub destination: String,
}

/// What checking a code found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConsumeOutcome {
    /// The code matched: the challenge is consumed.
    Verified { destination: VerifiedDestination, destination_key: String },
    /// A wrong code for this challenge (an attempt was spent). Carries the
    /// address so its owner can be told when it gets locked.
    Miss { destination_key: String, destination: VerifiedDestination, locale: Option<String> },
    /// No such challenge (unknown, expired, used up).
    Unknown,
}

/// Per-address send limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SendLimits {
    pub per_hour: u32,
    pub per_day:  u32,
    pub resend:   Duration,
}

/// Whether another code may be sent to an address now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendAdmission {
    Allowed,
    /// Too many codes for this address: retry after this many seconds.
    Refused { retry_after_secs: i64 },
}

/// SMS per UTC day: for the whole service, and for one destination country.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SmsBudget {
    pub daily:         u32,
    pub country_daily: u32,
}

/// Whether one more SMS fits today's budgets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SmsReservation {
    Reserved,
    /// The destination country's budget is spent.
    CountryExhausted,
    /// The service's budget is spent.
    Exhausted,
}

/// Pending codes and the per-address send budget (Redis).
#[async_trait]
pub trait VerificationStore: Send + Sync + 'static {
    /// Counts one send to `destination_key` (a hash of the address): at most
    /// `per_hour` an hour, and not within `resend` of the previous one.
    async fn admit_send(&self, destination_key: &str, limits: SendLimits) -> Result<SendAdmission, AuthError>;

    /// Counts a wrong code for an address, across its challenges, in a window
    /// opened by the first failure; returns the count.
    async fn record_failure(&self, destination_key: &str, window: Duration) -> Result<u32, AuthError>;

    /// The address's failures in the current window, and the seconds left in it.
    async fn failures(&self, destination_key: &str) -> Result<(u32, i64), AuthError>;

    async fn save(&self, challenge: &PendingChallenge, ttl: Duration, max_attempts: u32) -> Result<(), AuthError>;

    /// Checks `code_hash` against the challenge: on a match the challenge is
    /// consumed (single use); on a miss one attempt is spent, and the challenge
    /// is dropped once they are all spent.
    async fn consume(&self, challenge_id: &str, code_hash: &str) -> Result<ConsumeOutcome, AuthError>;

    async fn discard(&self, challenge_id: &str) -> Result<(), AuthError>;

    /// Counts one SMS to `country` against today's (UTC) budgets: the
    /// country's first — a refusal there spends nothing of the service's —
    /// then the service's.
    async fn reserve_sms(&self, country: &str, budget: SmsBudget) -> Result<SmsReservation, AuthError>;

    /// Gives back a unit [`reserve_sms`](Self::reserve_sms) took for an SMS that
    /// was not sent.
    async fn refund_sms(&self, country: &str) -> Result<(), AuthError>;
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

    /// Tells the owner of `destination` that wrong codes locked it for
    /// everyone. Transports that cannot (or should not: SMS costs money and is
    /// a pumping vector) send nothing.
    async fn send_lockout_notice(
        &self,
        _channel: VerificationChannel,
        _destination: &str,
        _locale: Option<&str>,
    ) -> Result<(), AuthError> {
        Ok(())
    }

    /// Tells the holder at `email` that the account's `changed` address (its
    /// email, or its phone) was just changed, so a takeover is visible to the
    /// real owner (#651). Email only, like the lockout notice.
    async fn send_contact_changed_notice(
        &self,
        _changed: VerificationChannel,
        _email: &str,
        _locale: Option<&str>,
    ) -> Result<(), AuthError> {
        Ok(())
    }

    /// Tells the holder at `email` that their account was just signed in to from
    /// a device it never saw (#649), described by `device` (its user agent, client-
    /// written: never trusted as markup) and the `ip` it came from.
    /// Email only.
    async fn send_new_login_notice(
        &self,
        _email: &str,
        _device: Option<&str>,
        _ip: Option<&str>,
        _locale: Option<&str>,
    ) -> Result<(), AuthError> {
        Ok(())
    }

    /// Tells the holder at `email` that their two-step sign-in changed
    /// (#649): turned on, off, or its backup codes regenerated — so a
    /// takeover that disables it is visible to them. Email only.
    async fn send_mfa_changed_notice(
        &self,
        _email: &str,
        _change: super::MfaChange,
        _locale: Option<&str>,
    ) -> Result<(), AuthError> {
        Ok(())
    }

    /// Tells the holder at `email` that their GDPR data export is ready
    /// (#653): its download `link`, working until `expires_at`. Email only.
    async fn send_export_ready_notice(
        &self,
        _email: &str,
        _link: &str,
        _expires_at: chrono::DateTime<chrono::Utc>,
        _locale: Option<&str>,
    ) -> Result<(), AuthError> {
        Ok(())
    }
}
