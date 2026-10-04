use std::sync::Arc;

use cqrs::{Command, CommandHandler, Envelope};
use validate_core::Validate;

use crate::application::command::helpers::load_account;
use crate::application::port::AccountRepository;
use crate::error::AccountError;

/// Returns a self-deactivated account to Active because its holder signed back
/// in. Sent by `auth` on login; never lifts a suspension.
#[derive(Debug, Clone)]
pub struct ResumeDeactivatedAccountCommand {
    pub account_id: String,
}

impl Command for ResumeDeactivatedAccountCommand {}
impl Validate for ResumeDeactivatedAccountCommand {}

pub struct ResumeDeactivatedAccountHandler {
    repo: Arc<dyn AccountRepository>,
}

impl ResumeDeactivatedAccountHandler {
    pub fn new(repo: Arc<dyn AccountRepository>) -> Self {
        Self { repo }
    }
}

impl CommandHandler<ResumeDeactivatedAccountCommand> for ResumeDeactivatedAccountHandler {
    type Error = AccountError;

    async fn handle(
        &self,
        envelope: Envelope<ResumeDeactivatedAccountCommand>,
    ) -> Result<(), Self::Error> {
        let mut account = load_account(&self.repo, &envelope.payload.account_id).await?;
        if account.resume_after_deactivation(envelope.correlation_id)? {
            self.repo.save(&account).await?;
        }
        Ok(())
    }
}
