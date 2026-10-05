//! Two-step sign-in ports (#649): sealing TOTP seeds, keyed-hashing backup
//! codes, and the short-lived state a second factor needs (a sign-in waiting
//! for its code, spent TOTP steps, wrong-code counts).

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::domain::value_object::{AccountId, DeviceFingerprint};
use crate::error::AuthError;

/// auth's key over the two-step material. account stores what this seals and
/// the hashes it makes, and never reads either.
pub trait MfaSeedCipher: Send + Sync + 'static {
    /// AES-256-GCM over a TOTP seed, stamped with the key id.
    fn seal(&self, seed: &[u8]) -> Result<Vec<u8>, AuthError>;

    /// The seed back. [`AuthError::MfaUnavailable`] without the key that
    /// sealed it.
    fn open(&self, sealed: &[u8]) -> Result<Vec<u8>, AuthError>;

    /// HMAC-SHA-256 of a normalized backup code under a key derived from the
    /// current seed key, stamped with its id: deterministic, so account
    /// matches it exactly, and useless without the key if the stored hashes
    /// leak.
    fn code_hash(&self, normalized_code: &str) -> Result<String, AuthError>;

    /// The code's hash under every key this cipher holds (current first), so
    /// codes issued before a key rotation still match.
    fn code_hashes(&self, normalized_code: &str) -> Result<Vec<String>, AuthError> {
        Ok(vec![self.code_hash(normalized_code)?])
    }
}

/// A sign-in whose credential was proven, waiting for its second factor. The
/// session is only issued — and a deactivated account only resumed, a first
/// link only made — once the code is.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingLogin {
    pub account_id:          AccountId,
    pub issuer:              String,
    pub subject:             String,
    /// The subject → account link is still to be made.
    pub needs_link:          bool,
    pub device:              DeviceFingerprint,
    pub guest_refresh_token: Option<String>,
    pub started_at:          DateTime<Utc>,
}

/// Short-lived two-step state, in Redis.
#[async_trait]
pub trait MfaStore: Send + Sync + 'static {
    /// Keeps `login` under `token_hash` (the challenge token's hash) for
    /// `ttl_secs`.
    async fn save_pending_login(&self, token_hash: &str, login: &PendingLogin, ttl_secs: u64) -> Result<(), AuthError>;

    /// The pending sign-in, still there.
    async fn pending_login(&self, token_hash: &str) -> Result<Option<PendingLogin>, AuthError>;

    /// Takes the pending sign-in away (single use): exactly one caller gets it.
    async fn take_pending_login(&self, token_hash: &str) -> Result<Option<PendingLogin>, AuthError>;

    /// Marks TOTP `step` used for `account`; `false` when it already was (a
    /// replayed code). Kept past the code's acceptance window.
    async fn claim_step(&self, account: &AccountId, step: i64, ttl_secs: u64) -> Result<bool, AuthError>;

    /// Reserves one code attempt for `account` **before** the code is
    /// checked, atomically (one increment, the window starting at the first):
    /// returns the attempts in the current `window_secs` window, this one
    /// included, and the seconds left. However many checks run in parallel,
    /// each gets its own count — the limit is never read-then-acted on.
    async fn reserve_attempt(&self, account: &AccountId, window_secs: u64) -> Result<(u32, i64), AuthError>;

    /// Forgets `account`'s attempts (a right code came).
    async fn clear_failures(&self, account: &AccountId) -> Result<(), AuthError>;
}

/// The holder's two-step material, as account keeps it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MfaSecret {
    pub enrolled:                 bool,
    /// What [`MfaSeedCipher::seal`] made (empty when not enrolled).
    pub sealed_seed:              Vec<u8>,
    pub recovery_codes_remaining: u32,
}
