use account_api::account_service_client::AccountServiceClient;
use account_api::{
    AccountStatus, GetAccountByIdRequest, GetAccountByIdentityIdRequest,
    ResumeDeactivatedAccountRequest,
};
use async_trait::async_trait;
use tonic::transport::Channel;
use tonic::Code;
use tracing::instrument;

use crate::application::port::{AccountActivation, AccountDirectory, AccountSnapshot};
use crate::domain::value_object::{AccountId, IdpSubject, Permission};
use crate::error::AuthError;

/// gRPC implementation of [`AccountDirectory`], backed by the `account` service.
///
/// The tonic client is cheaply cloneable (the `Channel` is `Arc`-backed), so each
/// call clones it to satisfy the `&self` port signature.
#[derive(Clone)]
pub struct GrpcAccountDirectory {
    client: AccountServiceClient<Channel>,
}

impl GrpcAccountDirectory {
    pub fn new(channel: Channel) -> Self {
        Self { client: AccountServiceClient::new(channel) }
    }
}

/// Maps an `account.v1` identity to the IdP subject string the `account` service
/// stores as `identity_id`. The composite `issuer#subject` keeps subjects from
/// distinct issuers unambiguous after an IdP migration.
fn identity_id(subject: &IdpSubject) -> String {
    subject.to_string()
}

#[async_trait]
impl AccountDirectory for GrpcAccountDirectory {
    #[instrument(name = "auth.directory.resolve", skip(self), fields(subject = %subject))]
    async fn resolve_or_provision(&self, subject: &IdpSubject) -> Result<AccountId, AuthError> {
        let mut client = self.client.clone();
        let response = client
            .get_account_by_identity_id(GetAccountByIdentityIdRequest {
                identity_id: identity_id(subject),
            })
            .await;

        match response {
            Ok(view) => AccountId::try_from(view.into_inner().id.as_str()),
            // Auto-provisioning from IdP claims (which requires the email/profile,
            // i.e. extending NormalizedClaims) is a product decision deferred to a
            // follow-up; for now an unknown subject cannot establish a session.
            Err(status) if status.code() == Code::NotFound => {
                Err(AuthError::AccountNotActive { current: "account_not_provisioned".into() })
            }
            Err(_) => Err(AuthError::AccountDirectoryUnavailable),
        }
    }

    #[instrument(name = "auth.directory.lookup", skip(self), fields(account.id = %account_id))]
    async fn lookup(&self, account_id: &AccountId) -> Result<AccountSnapshot, AuthError> {
        let mut client = self.client.clone();
        let view = client
            .get_account_by_id(GetAccountByIdRequest { account_id: account_id.as_str() })
            .await
            .map_err(|status| match status.code() {
                Code::NotFound => AuthError::AccountNotActive { current: "not_found".into() },
                _ => AuthError::AccountDirectoryUnavailable,
            })?
            .into_inner();

        let activation = match AccountStatus::try_from(view.status).unwrap_or(AccountStatus::Unspecified) {
            AccountStatus::Active => AccountActivation::Active,
            AccountStatus::Deactivated => AccountActivation::Deactivated,
            other => AccountActivation::Inactive { reason: status_name(other) },
        };
        // Union of coarse role names (pre-existing behaviour — downstream gates
        // may match on them) and account's effective fine-grained grants (the
        // `permissions` field, e.g. `audit:read`; empty from servers predating
        // it, which degrades to exactly the old roles-only token).
        let mut grants = view.roles;
        grants.extend(view.permissions);
        grants.sort_unstable();
        grants.dedup();
        let permissions = grants.into_iter().map(Permission::new).collect();

        Ok(AccountSnapshot { activation, permissions })
    }

    #[instrument(name = "auth.directory.resume", skip(self), fields(account.id = %account_id))]
    async fn resume_deactivated(&self, account_id: &AccountId) -> Result<(), AuthError> {
        let mut client = self.client.clone();
        client
            .resume_deactivated_account(ResumeDeactivatedAccountRequest {
                account_id: account_id.as_str(),
            })
            .await
            .map(|_| ())
            .map_err(|status| match status.code() {
                Code::NotFound => AuthError::AccountNotActive { current: "not_found".into() },
                // No longer deactivated (suspended or deleted since the lookup).
                Code::FailedPrecondition => {
                    AuthError::AccountNotActive { current: "not_resumable".into() }
                }
                _ => AuthError::AccountDirectoryUnavailable,
            })
    }
}

fn status_name(status: AccountStatus) -> String {
    match status {
        AccountStatus::Unspecified => "unspecified",
        AccountStatus::PendingVerification => "pending_verification",
        AccountStatus::Active => "active",
        AccountStatus::Suspended => "suspended",
        AccountStatus::Deactivated => "deactivated",
        AccountStatus::Deleted => "deleted",
    }
    .to_owned()
}
