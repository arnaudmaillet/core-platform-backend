use std::sync::Arc;

use chrono::{DateTime, Utc};
use cqrs::{Envelope, Query, QueryHandler};

use crate::application::port::AccountRepository;
use crate::domain::aggregate::Account;
use crate::domain::value_object::{AccountId, AgeBracket};
use crate::error::AccountError;
use uuid::Uuid;

/// Flat read-model of an Account, safe to send over the wire.
///
/// All value objects are serialised to their string/primitive representations
/// so the caller (gRPC mapper, REST controller) needs no domain imports.
#[derive(Debug, Clone)]
pub struct AccountView {
    pub id: String,
    pub identity_id: String,
    pub status: String,
    pub suspension_reason: Option<String>,
    pub deactivated_at: Option<DateTime<Utc>>,
    pub email: String,
    pub email_verified: bool,
    pub email_verified_at: Option<DateTime<Utc>>,
    pub phone: Option<String>,
    pub phone_verified: bool,
    pub kyc_status: String,
    pub roles: Vec<String>,
    pub permission_overrides: Vec<String>,
    /// Effective fine-grained grants (role expansion ∪ overrides) — what auth
    /// mints into edge tokens alongside the role names.
    pub permissions: Vec<String>,
    pub country_of_residence: Option<String>,
    pub last_login_at: Option<DateTime<Utc>>,
    pub is_locked: bool,
    pub mfa_enforced: bool,
    /// Two-step sign-in is on (#649).
    pub mfa_enrolled: bool,
    /// Unused backup codes left (0 when MFA is off).
    pub mfa_recovery_codes_remaining: usize,
    pub version: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub created_by: Option<String>,
    /// Private: the holder's own view, or the mesh.
    pub date_of_birth: Option<chrono::NaiveDate>,
    /// From the date of birth, today.
    pub age_bracket: Option<AgeBracket>,
}

impl From<&Account> for AccountView {
    fn from(a: &Account) -> Self {
        Self {
            id: a.id().as_uuid().to_string(),
            identity_id: a.identity_id().as_str().to_owned(),
            status: a.status().to_string(),
            suspension_reason: a.suspension_reason().map(str::to_owned),
            deactivated_at: a.deactivated_at(),
            // Empty for a phone-only account (no email on file).
            email: a.email().map(|e| e.as_str().to_owned()).unwrap_or_default(),
            email_verified: a.email_verified(),
            email_verified_at: a.email_verified_at(),
            phone: a.phone().map(|p| p.as_str().to_owned()),
            phone_verified: a.phone_verified(),
            kyc_status: a.kyc_status().to_string(),
            roles: a.roles().iter().map(|r| r.to_string()).collect(),
            permissions: a.effective_permissions(),
            permission_overrides: a
                .permission_overrides()
                .iter()
                .map(|p| p.as_str().to_owned())
                .collect(),
            country_of_residence: a.country_of_residence().map(|c| c.as_str().to_owned()),
            last_login_at: a.last_login_at(),
            is_locked: a.is_locked(),
            mfa_enforced: a.mfa().enforced(),
            mfa_enrolled: a.mfa().is_enrolled(),
            mfa_recovery_codes_remaining: if a.mfa().is_enrolled() { a.mfa().recovery_codes().len() } else { 0 },
            version: a.version(),
            created_at: a.created_at(),
            updated_at: a.updated_at(),
            created_by: a.created_by().map(|id| id.as_uuid().to_string()),
            date_of_birth: a.date_of_birth(),
            age_bracket: a.age_bracket(Utc::now().date_naive()),
        }
    }
}

// ── Query ─────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct GetAccountByIdQuery {
    pub account_id: String,
}

impl Query for GetAccountByIdQuery {
    type Response = AccountView;
}

// ── Handler ───────────────────────────────────────────────────────────────────

pub struct GetAccountByIdHandler {
    repo: Arc<dyn AccountRepository>,
}

impl GetAccountByIdHandler {
    pub fn new(repo: Arc<dyn AccountRepository>) -> Self {
        Self { repo }
    }
}

impl QueryHandler<GetAccountByIdQuery> for GetAccountByIdHandler {
    type Error = AccountError;

    async fn handle(
        &self,
        envelope: Envelope<GetAccountByIdQuery>,
    ) -> Result<AccountView, Self::Error> {
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
        Ok(AccountView::from(&account))
    }
}

pub type GetAccountByIdResponse = AccountView;
