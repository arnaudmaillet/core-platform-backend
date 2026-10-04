use std::sync::Arc;

use cqrs::{Envelope, Query, QueryHandler};

use crate::application::port::AccountRepository;
use crate::application::query::get_account_by_id::AccountView;
use crate::domain::value_object::PhoneNumber;
use crate::error::AccountError;

/// The account holding a phone number (E.164). Mesh only (auth's sign-up):
/// never on the client edge, where it would enumerate numbers.
#[derive(Debug, Clone)]
pub struct GetAccountByPhoneQuery {
    pub phone: String,
}

impl Query for GetAccountByPhoneQuery {
    type Response = AccountView;
}

pub struct GetAccountByPhoneHandler {
    repo: Arc<dyn AccountRepository>,
}

impl GetAccountByPhoneHandler {
    pub fn new(repo: Arc<dyn AccountRepository>) -> Self {
        Self { repo }
    }
}

impl QueryHandler<GetAccountByPhoneQuery> for GetAccountByPhoneHandler {
    type Error = AccountError;

    async fn handle(&self, envelope: Envelope<GetAccountByPhoneQuery>) -> Result<AccountView, Self::Error> {
        let phone = PhoneNumber::new(envelope.payload.phone.trim())?;
        let account = self
            .repo
            .find_by_phone(&phone)
            .await?
            .ok_or_else(|| AccountError::AccountNotFound { id: "phone".to_owned() })?;
        Ok(AccountView::from(&account))
    }
}
