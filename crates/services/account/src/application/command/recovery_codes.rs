//! Backup codes after enrolment (#649): spending one at sign-in, and replacing
//! the whole set. auth hashes the codes; account only matches hashes.

use std::sync::Arc;

use cqrs::{Command, CommandHandler, Envelope};
use validate_core::Validate;

use crate::application::command::enroll_mfa::recovery_codes;
use crate::application::command::helpers::load_account;
use crate::application::port::AccountRepository;
use crate::error::AccountError;

/// Spends the backup code hashed `code_hash`: it works once.
#[derive(Debug, Clone)]
pub struct ConsumeRecoveryCodeCommand {
    pub account_id: String,
    pub code_hash:  String,
}

impl Command for ConsumeRecoveryCodeCommand {}
impl Validate for ConsumeRecoveryCodeCommand {}

pub struct ConsumeRecoveryCodeHandler {
    repo: Arc<dyn AccountRepository>,
}

impl ConsumeRecoveryCodeHandler {
    pub fn new(repo: Arc<dyn AccountRepository>) -> Self {
        Self { repo }
    }
}

impl CommandHandler<ConsumeRecoveryCodeCommand> for ConsumeRecoveryCodeHandler {
    type Error = AccountError;

    /// The save is version-checked: of two concurrent spends of one code, one
    /// fails with `ConcurrentModification` (and its retry finds it spent).
    async fn handle(&self, envelope: Envelope<ConsumeRecoveryCodeCommand>) -> Result<(), Self::Error> {
        let cmd = &envelope.payload;
        let mut account = load_account(&self.repo, &cmd.account_id).await?;
        account.consume_recovery_code(&cmd.code_hash)?;
        self.repo.save(&account).await
    }
}

/// Replaces every backup code with a regenerated set.
#[derive(Debug, Clone)]
pub struct ReplaceRecoveryCodesCommand {
    pub account_id:           String,
    pub recovery_code_hashes: Vec<String>,
}

impl Command for ReplaceRecoveryCodesCommand {}
impl Validate for ReplaceRecoveryCodesCommand {}

pub struct ReplaceRecoveryCodesHandler {
    repo: Arc<dyn AccountRepository>,
}

impl ReplaceRecoveryCodesHandler {
    pub fn new(repo: Arc<dyn AccountRepository>) -> Self {
        Self { repo }
    }
}

impl CommandHandler<ReplaceRecoveryCodesCommand> for ReplaceRecoveryCodesHandler {
    type Error = AccountError;

    async fn handle(&self, envelope: Envelope<ReplaceRecoveryCodesCommand>) -> Result<(), Self::Error> {
        let cmd = &envelope.payload;
        let codes = recovery_codes(&cmd.recovery_code_hashes)?;
        let mut account = load_account(&self.repo, &cmd.account_id).await?;
        account.replace_recovery_codes(codes)?;
        self.repo.save(&account).await
    }
}
