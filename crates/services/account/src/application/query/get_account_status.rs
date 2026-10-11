use std::sync::Arc;

use cqrs::{Envelope, Query, QueryHandler};
use uuid::Uuid;

use crate::application::port::AccountRepository;
use crate::domain::value_object::{AccountId, AccountStatus};
use crate::error::AccountError;

#[derive(Debug, Clone)]
pub struct AccountStatusView {
    pub account_id: String,
    pub status: AccountStatus,
    pub suspension_reason: Option<String>,
}

#[derive(Debug, Clone)]
pub struct GetAccountStatusQuery {
    pub account_id: String,
}

impl Query for GetAccountStatusQuery {
    type Response = AccountStatusView;
}

pub struct GetAccountStatusHandler {
    repo: Arc<dyn AccountRepository>,
}

impl GetAccountStatusHandler {
    pub fn new(repo: Arc<dyn AccountRepository>) -> Self {
        Self { repo }
    }
}

impl QueryHandler<GetAccountStatusQuery> for GetAccountStatusHandler {
    type Error = AccountError;

    async fn handle(
        &self,
        envelope: Envelope<GetAccountStatusQuery>,
    ) -> Result<AccountStatusView, Self::Error> {
        let id_str = &envelope.payload.account_id;
        let uuid = id_str.parse::<Uuid>().map_err(|_| AccountError::DomainViolation {
            field: "account_id".into(),
            message: "invalid UUID format".into(),
        })?;
        let id = AccountId::from_uuid(uuid);
        let account = self
            .repo
            .find_by_id(&id)
            .await?
            .ok_or_else(|| AccountError::AccountNotFound { id: id_str.clone() })?;

        Ok(AccountStatusView {
            account_id: id_str.clone(),
            status: account.status(),
            suspension_reason: account.suspension_reason().map(str::to_owned),
        })
    }
}
pub type GetAccountStatusResponse = AccountStatusView;
