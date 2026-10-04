use std::sync::Arc;

use cqrs::{Command, CommandHandler, Envelope};
use validate_core::Validate;

use crate::application::command::helpers::load_account;
use crate::application::port::AccountRepository;
use crate::error::AccountError;

/// Withdraws a pending erasure within its grace period. The account's status
/// is unchanged: a deactivated account is resumed by signing back in, which
/// cancels the deletion too.
#[derive(Debug, Clone)]
pub struct CancelGdprDeletionCommand {
    pub account_id: String,
}

impl Command for CancelGdprDeletionCommand {}
impl Validate for CancelGdprDeletionCommand {}

pub struct CancelGdprDeletionHandler {
    repo: Arc<dyn AccountRepository>,
}

impl CancelGdprDeletionHandler {
    pub fn new(repo: Arc<dyn AccountRepository>) -> Self {
        Self { repo }
    }
}

impl CommandHandler<CancelGdprDeletionCommand> for CancelGdprDeletionHandler {
    type Error = AccountError;

    async fn handle(&self, envelope: Envelope<CancelGdprDeletionCommand>) -> Result<(), Self::Error> {
        let mut account = load_account(&self.repo, &envelope.payload.account_id).await?;
        account.cancel_gdpr_deletion(envelope.correlation_id)?;
        self.repo.save(&account).await
    }
}
