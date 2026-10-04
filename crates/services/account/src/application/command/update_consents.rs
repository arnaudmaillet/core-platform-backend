use std::sync::Arc;

use cqrs::{Command, CommandHandler, Envelope};
use validate_core::{FieldViolation, Validate};

use crate::application::command::helpers::load_account;
use crate::application::port::AccountRepository;
use crate::domain::value_object::ConsentPurpose;
use crate::error::AccountError;

/// The holder gives or withdraws consents. `None` leaves a purpose as it is.
#[derive(Debug, Clone)]
pub struct UpdateConsentsCommand {
    pub account_id: String,
    pub data_processing: Option<bool>,
    pub marketing: Option<bool>,
    pub analytics: Option<bool>,
    /// The privacy-policy version the holder saw, if the client sent one.
    pub policy_version: Option<String>,
}

impl UpdateConsentsCommand {
    fn requested(&self) -> Vec<(ConsentPurpose, bool)> {
        [
            (ConsentPurpose::DataProcessing, self.data_processing),
            (ConsentPurpose::Marketing, self.marketing),
            (ConsentPurpose::Analytics, self.analytics),
        ]
        .into_iter()
        .filter_map(|(purpose, granted)| granted.map(|g| (purpose, g)))
        .collect()
    }
}

impl Command for UpdateConsentsCommand {}

impl Validate for UpdateConsentsCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        if let Some(version) = &self.policy_version
            && version.chars().count() > 64
        {
            return Err(vec![FieldViolation::new(
                "policy_version",
                "VAL-2080",
                "policy_version must be at most 64 characters",
            )]);
        }
        Ok(())
    }
}

pub struct UpdateConsentsHandler {
    repo: Arc<dyn AccountRepository>,
}

impl UpdateConsentsHandler {
    pub fn new(repo: Arc<dyn AccountRepository>) -> Self {
        Self { repo }
    }
}

impl CommandHandler<UpdateConsentsCommand> for UpdateConsentsHandler {
    type Error = AccountError;

    async fn handle(&self, envelope: Envelope<UpdateConsentsCommand>) -> Result<(), Self::Error> {
        let cmd = envelope.payload;
        let mut account = load_account(&self.repo, &cmd.account_id).await?;
        let policy_version = cmd.policy_version.clone().filter(|v| !v.trim().is_empty());
        account.update_consents(&cmd.requested(), policy_version, envelope.correlation_id)?;
        // Nothing changed ⇒ nothing to write (the version did not move either).
        if account.events().is_empty() {
            return Ok(());
        }
        self.repo.save(&account).await
    }
}
