use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::domain::value_object::{EncryptedBytes, RecoveryCodeHash};

/// Multi-factor authentication configuration for an account.
///
/// Held as an embedded entity inside the [`Account`] aggregate root.
/// All mutations go through the aggregate's domain methods, never directly
/// through this struct's fields.
///
/// [`Account`]: crate::domain::aggregate::account::Account
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MfaState {
    /// When `true`, the login flow requires a second factor to complete.
    pub enforced: bool,

    /// AES-256-GCM ciphertext of the TOTP seed.
    /// `None` means MFA has not been enrolled.
    pub totp_secret: Option<EncryptedBytes>,

    /// Timestamp at which TOTP was first successfully enrolled.
    pub totp_enrolled_at: Option<DateTime<Utc>>,

    /// Hashed one-time recovery codes.
    /// Each code is consumed exactly once and removed from this list.
    pub recovery_codes: Vec<RecoveryCodeHash>,

    /// Timestamp of the most recent recovery code consumption.
    pub backup_verified_at: Option<DateTime<Utc>>,
}

impl MfaState {
    /// Reconstructs MFA state from persistence (no events emitted).
    pub fn reconstitute(
        enforced: bool,
        totp_secret: Option<EncryptedBytes>,
        totp_enrolled_at: Option<DateTime<Utc>>,
        recovery_codes: Vec<RecoveryCodeHash>,
        backup_verified_at: Option<DateTime<Utc>>,
    ) -> Self {
        Self { enforced, totp_secret, totp_enrolled_at, recovery_codes, backup_verified_at }
    }

    pub fn enforced(&self) -> bool { self.enforced }
    pub fn totp_secret(&self) -> Option<&EncryptedBytes> { self.totp_secret.as_ref() }
    pub fn totp_enrolled_at(&self) -> Option<DateTime<Utc>> { self.totp_enrolled_at }
    pub fn recovery_codes(&self) -> &[RecoveryCodeHash] { &self.recovery_codes }
    pub fn backup_verified_at(&self) -> Option<DateTime<Utc>> { self.backup_verified_at }

    /// Returns `true` if TOTP is enrolled (a secret is present).
    pub fn is_enrolled(&self) -> bool {
        self.totp_secret.is_some()
    }

    /// Attempts to consume a recovery code.
    ///
    /// Searches for `code_hash` in the list, removes it on match, and
    /// updates `backup_verified_at`. Returns `true` if the code was found
    /// and consumed, `false` otherwise.
    pub fn consume_recovery_code(&mut self, code_hash: &str) -> bool {
        if let Some(pos) = self.recovery_codes.iter().position(|c| c.as_str() == code_hash) {
            self.recovery_codes.remove(pos);
            self.backup_verified_at = Some(Utc::now());
            true
        } else {
            false
        }
    }

    /// Enrolls TOTP: stores the encrypted secret and the initial recovery codes.
    pub fn enroll(&mut self, secret: EncryptedBytes, recovery_codes: Vec<RecoveryCodeHash>) {
        self.totp_secret = Some(secret);
        self.recovery_codes = recovery_codes;
        self.totp_enrolled_at = Some(Utc::now());
    }

    /// Replaces every backup code with `recovery_codes` (a regenerated set).
    pub fn replace_recovery_codes(&mut self, recovery_codes: Vec<RecoveryCodeHash>) {
        self.recovery_codes = recovery_codes;
    }

    /// Revokes all MFA state, returning the account to an unenrolled state.
    pub fn revoke(&mut self) {
        self.totp_secret = None;
        self.totp_enrolled_at = None;
        self.recovery_codes.clear();
        self.backup_verified_at = None;
        self.enforced = false;
    }
}
