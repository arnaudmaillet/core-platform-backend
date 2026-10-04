use std::sync::Arc;

use cqrs::{Envelope, Query, QueryHandler};

use crate::application::port::AccountRepository;
use crate::application::query::get_account_by_id::AccountView;
use crate::domain::value_object::EmailAddress;
use crate::error::AccountError;

/// The account holding an email address. Mesh only (auth's sign-up): never on
/// the client edge, where it would enumerate addresses.
#[derive(Debug, Clone)]
pub struct GetAccountByEmailQuery {
    pub email: String,
}

impl Query for GetAccountByEmailQuery {
    type Response = AccountView;
}

pub struct GetAccountByEmailHandler {
    repo: Arc<dyn AccountRepository>,
}

impl GetAccountByEmailHandler {
    pub fn new(repo: Arc<dyn AccountRepository>) -> Self {
        Self { repo }
    }
}

impl QueryHandler<GetAccountByEmailQuery> for GetAccountByEmailHandler {
    type Error = AccountError;

    async fn handle(&self, envelope: Envelope<GetAccountByEmailQuery>) -> Result<AccountView, Self::Error> {
        let email = EmailAddress::new(envelope.payload.email.clone())?;
        let account = self
            .repo
            .find_by_email(&email)
            .await?
            .ok_or_else(|| AccountError::AccountNotFound { id: "email".to_owned() })?;
        Ok(AccountView::from(&account))
    }
}
