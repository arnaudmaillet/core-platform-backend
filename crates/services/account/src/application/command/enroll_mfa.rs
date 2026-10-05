use std::sync::Arc;

use cqrs::{Command, CommandHandler, Envelope};
use validate_core::{FieldViolation, Validate};

use crate::application::command::helpers::load_account;
use crate::application::port::AccountRepository;
use crate::domain::value_object::{EncryptedBytes, RecoveryCodeHash};
use crate::error::AccountError;

/// The fewest backup codes an enrolment (or a regenerated set) may carry.
pub const MIN_RECOVERY_CODES: usize = 6;

/// `hashes` as backup codes: at least [`MIN_RECOVERY_CODES`], none empty or
/// repeated.
pub(crate) fn recovery_codes(hashes: &[String]) -> Result<Vec<RecoveryCodeHash>, AccountError> {
    let distinct: std::collections::HashSet<&str> = hashes.iter().map(String::as_str).collect();
    if hashes.len() < MIN_RECOVERY_CODES || distinct.len() != hashes.len() || distinct.contains("") {
        return Err(AccountError::DomainViolation {
            field: "recovery_code_hashes".into(),
            message: format!("at least {MIN_RECOVERY_CODES} distinct backup code hashes are required"),
        });
    }
    Ok(hashes.iter().map(|h| RecoveryCodeHash::from_hash(h.clone())).collect())
}

#[derive(Debug, Clone)]
pub struct EnrollMfaCommand {
    pub account_id: String,
    /// AES-256-GCM ciphertext of the TOTP seed, under auth's key.
    pub totp_secret_ciphertext: Vec<u8>,
    /// auth's hash of each one-time backup code; at least 6 required.
    pub recovery_code_hashes: Vec<String>,
}

impl Command for EnrollMfaCommand {}

impl Validate for EnrollMfaCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        let mut v = Vec::new();
        if self.totp_secret_ciphertext.is_empty() {
            v.push(FieldViolation::new(
                "totp_secret_ciphertext",
                "VAL-2020",
                "TOTP secret ciphertext must not be empty",
            ));
        }
        if self.recovery_code_hashes.len() < MIN_RECOVERY_CODES {
            v.push(FieldViolation::new(
                "recovery_code_hashes",
                "VAL-2021",
                "at least 6 recovery code hashes are required",
            ));
        }
        if v.is_empty() { Ok(()) } else { Err(v) }
    }
}

pub struct EnrollMfaHandler {
    repo: Arc<dyn AccountRepository>,
}

impl EnrollMfaHandler {
    pub fn new(repo: Arc<dyn AccountRepository>) -> Self {
        Self { repo }
    }
}

impl CommandHandler<EnrollMfaCommand> for EnrollMfaHandler {
    type Error = AccountError;

    async fn handle(&self, envelope: Envelope<EnrollMfaCommand>) -> Result<(), Self::Error> {
        let cmd = &envelope.payload;
        let mut account = load_account(&self.repo, &cmd.account_id).await?;

        // The bus does not run `Validate`: the handler holds the line.
        if cmd.totp_secret_ciphertext.is_empty() {
            return Err(AccountError::DomainViolation {
                field: "totp_secret".into(),
                message: "the TOTP secret is required".into(),
            });
        }
        let secret = EncryptedBytes::from_ciphertext(cmd.totp_secret_ciphertext.clone());
        let codes = recovery_codes(&cmd.recovery_code_hashes)?;

        account.enroll_mfa(secret, codes, envelope.correlation_id)?;
        self.repo.save(&account).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backup_codes_are_at_least_six_distinct_non_empty_hashes() {
        let set = |n: usize| (0..n).map(|i| format!("h{i}")).collect::<Vec<_>>();
        assert_eq!(recovery_codes(&set(10)).unwrap().len(), 10);
        assert!(recovery_codes(&set(5)).is_err(), "too few");
        let mut repeated = set(6);
        repeated[5] = "h0".into();
        assert!(recovery_codes(&repeated).is_err(), "repeated");
        let mut empty = set(6);
        empty[0] = String::new();
        assert!(recovery_codes(&empty).is_err(), "empty");
    }
}
