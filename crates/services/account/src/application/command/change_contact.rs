//! A holder changes their email or phone (#651). Mesh only: auth calls these
//! once the holder proved the new address (a one-time code sent to it), so the
//! address is set and verified at once. Another account's address is refused.

use std::sync::Arc;

use cqrs::{Command, CommandHandler, Envelope};
use validate_core::Validate;

use crate::application::command::helpers::load_account;
use crate::application::port::AccountRepository;
use crate::domain::value_object::{EmailAddress, PhoneNumber};
use crate::error::AccountError;

#[derive(Debug, Clone)]
pub struct ChangeEmailCommand {
    pub account_id: String,
    /// The new address, already proven by its holder.
    pub email:      String,
}

impl Command for ChangeEmailCommand {}
impl Validate for ChangeEmailCommand {}

pub struct ChangeEmailHandler {
    repo: Arc<dyn AccountRepository>,
}

impl ChangeEmailHandler {
    pub fn new(repo: Arc<dyn AccountRepository>) -> Self {
        Self { repo }
    }
}

impl CommandHandler<ChangeEmailCommand> for ChangeEmailHandler {
    type Error = AccountError;

    async fn handle(&self, envelope: Envelope<ChangeEmailCommand>) -> Result<(), Self::Error> {
        let cmd = &envelope.payload;
        let email = EmailAddress::new(cmd.email.trim())?;
        let mut account = load_account(&self.repo, &cmd.account_id).await?;
        if account.email() == Some(&email) && account.email_verified() {
            return Ok(());
        }
        if let Some(holder) = self.repo.find_by_email(&email).await?
            && holder.id() != account.id()
        {
            return Err(AccountError::EmailAlreadyRegistered { email: cmd.email.clone() });
        }
        account.replace_email_proven(email, envelope.correlation_id)?;
        self.repo.save(&account).await
    }
}

#[derive(Debug, Clone)]
pub struct ChangePhoneCommand {
    pub account_id: String,
    /// The new number (E.164), already proven by its holder.
    pub phone:      String,
}

impl Command for ChangePhoneCommand {}
impl Validate for ChangePhoneCommand {}

pub struct ChangePhoneHandler {
    repo: Arc<dyn AccountRepository>,
}

impl ChangePhoneHandler {
    pub fn new(repo: Arc<dyn AccountRepository>) -> Self {
        Self { repo }
    }
}

impl CommandHandler<ChangePhoneCommand> for ChangePhoneHandler {
    type Error = AccountError;

    async fn handle(&self, envelope: Envelope<ChangePhoneCommand>) -> Result<(), Self::Error> {
        let cmd = &envelope.payload;
        let phone = PhoneNumber::new(cmd.phone.trim())?;
        let mut account = load_account(&self.repo, &cmd.account_id).await?;
        if account.phone() == Some(&phone) && account.phone_verified() {
            return Ok(());
        }
        if let Some(holder) = self.repo.find_by_phone(&phone).await?
            && holder.id() != account.id()
        {
            return Err(AccountError::PhoneAlreadyRegistered);
        }
        account.replace_phone_proven(phone, envelope.correlation_id)?;
        self.repo.save(&account).await
    }
}
