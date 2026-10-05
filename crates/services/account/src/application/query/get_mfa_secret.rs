use std::sync::Arc;

use cqrs::{Envelope, Query, QueryHandler};
use uuid::Uuid;

use crate::application::port::AccountRepository;
use crate::domain::value_object::AccountId;
use crate::error::AccountError;

/// The holder's MFA material, for auth to check a code (#649). Mesh only:
/// the seed is auth's ciphertext, never shown to anyone.
#[derive(Debug, Clone)]
pub struct MfaSecretView {
    pub account_id:               String,
    pub enrolled:                 bool,
    pub totp_secret:              Vec<u8>,
    pub recovery_codes_remaining: usize,
}

#[derive(Debug, Clone)]
pub struct GetMfaSecretQuery {
    pub account_id: String,
}

impl Query for GetMfaSecretQuery {
    type Response = MfaSecretView;
}

pub struct GetMfaSecretHandler {
    repo: Arc<dyn AccountRepository>,
}

impl GetMfaSecretHandler {
    pub fn new(repo: Arc<dyn AccountRepository>) -> Self {
        Self { repo }
    }
}

impl QueryHandler<GetMfaSecretQuery> for GetMfaSecretHandler {
    type Error = AccountError;

    async fn handle(&self, envelope: Envelope<GetMfaSecretQuery>) -> Result<MfaSecretView, Self::Error> {
        let id_str = &envelope.payload.account_id;
        let uuid = id_str.parse::<Uuid>().map_err(|_| AccountError::DomainViolation {
            field: "account_id".into(),
            message: "invalid UUID format".into(),
        })?;
        let account = self
            .repo
            .find_by_id(&AccountId::from_uuid(uuid))
            .await?
            .ok_or_else(|| AccountError::AccountNotFound { id: id_str.clone() })?;
        let mfa = account.mfa();
        Ok(MfaSecretView {
            account_id:               id_str.clone(),
            enrolled:                 mfa.is_enrolled(),
            totp_secret:              mfa.totp_secret().map(|s| s.as_bytes().to_vec()).unwrap_or_default(),
            recovery_codes_remaining: if mfa.is_enrolled() { mfa.recovery_codes().len() } else { 0 },
        })
    }
}
