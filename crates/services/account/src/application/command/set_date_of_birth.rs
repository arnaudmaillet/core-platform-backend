use std::sync::Arc;

use chrono::{NaiveDate, Utc};
use cqrs::{Command, CommandHandler, Envelope};
use validate_core::{FieldViolation, Validate};

use crate::application::command::helpers::load_account;
use crate::application::port::AccountRepository;
use crate::error::AccountError;

/// The holder records a date of birth when none is on file (accounts created
/// before it was collected).
#[derive(Debug, Clone)]
pub struct SetDateOfBirthCommand {
    pub account_id: String,
    /// ISO 8601 (`YYYY-MM-DD`).
    pub date_of_birth: String,
}

impl Command for SetDateOfBirthCommand {}

impl Validate for SetDateOfBirthCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        if NaiveDate::parse_from_str(&self.date_of_birth, "%Y-%m-%d").is_err() {
            return Err(vec![FieldViolation::new(
                "date_of_birth",
                "VAL-2006",
                "date_of_birth must be an ISO 8601 date (YYYY-MM-DD)",
            )]);
        }
        Ok(())
    }
}

pub struct SetDateOfBirthHandler {
    repo: Arc<dyn AccountRepository>,
}

impl SetDateOfBirthHandler {
    pub fn new(repo: Arc<dyn AccountRepository>) -> Self {
        Self { repo }
    }
}

impl CommandHandler<SetDateOfBirthCommand> for SetDateOfBirthHandler {
    type Error = AccountError;

    async fn handle(&self, envelope: Envelope<SetDateOfBirthCommand>) -> Result<(), Self::Error> {
        let cmd = envelope.payload;
        let dob = NaiveDate::parse_from_str(&cmd.date_of_birth, "%Y-%m-%d").map_err(|_| {
            AccountError::DomainViolation {
                field: "date_of_birth".into(),
                message: "date_of_birth must be an ISO 8601 date (YYYY-MM-DD)".into(),
            }
        })?;
        let mut account = load_account(&self.repo, &cmd.account_id).await?;
        account.set_date_of_birth(dob, Utc::now().date_naive(), envelope.correlation_id)?;
        self.repo.save(&account).await
    }
}
