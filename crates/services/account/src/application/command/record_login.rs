use std::sync::Arc;

use cqrs::{Command, CommandHandler, Envelope};
use validate_core::Validate;

use crate::application::command::helpers::load_account;
use crate::application::port::AccountRepository;
use crate::error::AccountError;

/// Records a successful sign-in: updates `last_login_at`.
#[derive(Debug, Clone)]
pub struct RecordLoginCommand {
    pub account_id: String,
}

impl Command for RecordLoginCommand {}
impl Validate for RecordLoginCommand {}

pub struct RecordLoginHandler {
    repo: Arc<dyn AccountRepository>,
}

impl RecordLoginHandler {
    pub fn new(repo: Arc<dyn AccountRepository>) -> Self {
        Self { repo }
    }
}

impl CommandHandler<RecordLoginCommand> for RecordLoginHandler {
    type Error = AccountError;

    async fn handle(&self, envelope: Envelope<RecordLoginCommand>) -> Result<(), Self::Error> {
        let mut account = load_account(&self.repo, &envelope.payload.account_id).await?;
        account.record_login();
        self.repo.save(&account).await
    }
}
