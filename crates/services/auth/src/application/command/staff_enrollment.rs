//! Staff passkey enrollment (#880). A staff account's privileged grants ride
//! only in sessions opened with a **staff-enrolled** passkey, and a passkey is
//! staff-enrolled only when its registration presents a grant:
//!
//! - single use, [`STAFF_GRANT_TTL_SECS`] (24 h), bound to one account;
//! - issued by an `admin` / `super_admin` in a staff passkey session (those
//!   roles are only in such a session's token), never for themselves;
//! - or, for the very first one, by the bootstrap (`AUTH_STAFF_BOOTSTRAP=true`,
//!   the `auth-server bootstrap-staff` command), only while no staff passkey
//!   exists anywhere.
//!
//! Without the grant, someone who controls a staff member's mailbox signs in by
//! code, registers their own passkey — and still gets a member passkey. Each
//! grant and each enrollment is published (`auth.staff_enrollment_granted`,
//! `auth.staff_passkey_enrolled`) and recorded by audit as a privileged action.

use std::sync::Arc;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use chrono::{DateTime, Duration, Utc};
use rand::RngCore;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::application::port::{
    AccountActivation, AccountDirectory, EventPublisher, PasskeyRepository, StaffEnrollmentGrants, StaffGrant,
};
use crate::domain::event::{DomainEvent, StaffEnrollmentGranted};
use crate::domain::value_object::AccountId;
use crate::error::AuthError;

/// How long a grant may be used.
pub const STAFF_GRANT_TTL_SECS: i64 = 24 * 3600;

/// The roles that may issue a grant.
const ISSUER_ROLES: [&str; 2] = ["admin", "super_admin"];

/// The grant's stored form: only its SHA-256 is kept.
pub(crate) fn grant_hash(grant: &str) -> String {
    data_encoding::HEXLOWER.encode(&Sha256::digest(grant.as_bytes()))
}

/// The caller asking for a grant, as its verified token says.
#[derive(Debug, Clone)]
pub struct StaffIssuer {
    pub account_id:  AccountId,
    pub permissions: Vec<String>,
}

/// A grant just issued: the secret goes to the caller once.
#[derive(Clone, PartialEq, Eq)]
pub struct IssuedStaffGrant {
    pub grant:      String,
    pub grant_id:   Uuid,
    pub expires_at: DateTime<Utc>,
}

impl std::fmt::Debug for IssuedStaffGrant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IssuedStaffGrant").field("grant_id", &self.grant_id).finish_non_exhaustive()
    }
}

pub struct StaffEnrollment {
    grants:            Arc<dyn StaffEnrollmentGrants>,
    passkeys:          Arc<dyn PasskeyRepository>,
    directory:         Arc<dyn AccountDirectory>,
    publisher:         Arc<dyn EventPublisher>,
    bootstrap_enabled: bool,
}

impl StaffEnrollment {
    pub fn new(
        grants: Arc<dyn StaffEnrollmentGrants>,
        passkeys: Arc<dyn PasskeyRepository>,
        directory: Arc<dyn AccountDirectory>,
        publisher: Arc<dyn EventPublisher>,
    ) -> Self {
        Self { grants, passkeys, directory, publisher, bootstrap_enabled: false }
    }

    /// Opens the one-off bootstrap (`AUTH_STAFF_BOOTSTRAP=true`).
    pub fn with_bootstrap(mut self, enabled: bool) -> Self {
        self.bootstrap_enabled = enabled;
        self
    }

    pub fn grants(&self) -> &Arc<dyn StaffEnrollmentGrants> {
        &self.grants
    }

    /// A grant for `account_id`, issued by an admin in a staff passkey session
    /// (`AUT-5033` otherwise, and for oneself).
    pub async fn issue(
        &self,
        issuer: &StaffIssuer,
        account_id: &str,
        now: DateTime<Utc>,
        correlation_id: Uuid,
    ) -> Result<IssuedStaffGrant, AuthError> {
        let target = AccountId::try_from(account_id)?;
        let admin = issuer.permissions.iter().any(|p| ISSUER_ROLES.contains(&p.as_str()));
        if !admin || issuer.account_id == target {
            tracing::warn!(issuer = %issuer.account_id.as_str(), target = %target.as_str(), "staff enrollment grant refused");
            return Err(AuthError::StaffEnrollmentForbidden);
        }
        self.mint(target, Some(issuer.account_id), now, correlation_id).await
    }

    /// The first grant (#880): only with the bootstrap on and while no staff
    /// passkey exists anywhere (`AUT-5034` otherwise).
    pub async fn bootstrap(&self, account_id: &str, now: DateTime<Utc>, correlation_id: Uuid) -> Result<IssuedStaffGrant, AuthError> {
        let target = AccountId::try_from(account_id)?;
        if !self.bootstrap_enabled || self.passkeys.any_staff_enrolled().await? {
            return Err(AuthError::StaffBootstrapClosed);
        }
        self.mint(target, None, now, correlation_id).await
    }

    async fn mint(
        &self,
        account_id: AccountId,
        issued_by: Option<AccountId>,
        now: DateTime<Utc>,
        correlation_id: Uuid,
    ) -> Result<IssuedStaffGrant, AuthError> {
        // The account must be one that may sign in.
        if let AccountActivation::Inactive { reason } = self.directory.lookup(&account_id).await?.activation {
            return Err(AuthError::AccountNotActive { current: reason });
        }
        let mut secret = [0u8; 32];
        rand::rng().fill_bytes(&mut secret);
        let grant = URL_SAFE_NO_PAD.encode(secret);
        let record = StaffGrant {
            grant_id: Uuid::now_v7(),
            account_id,
            issued_by,
            issued_at: now,
            expires_at: now + Duration::seconds(STAFF_GRANT_TTL_SECS),
        };
        // On record first: a grant never exists that audit was not told of.
        self.publisher
            .publish(&DomainEvent::StaffEnrollmentGranted(StaffEnrollmentGranted {
                account_id,
                grant_id: record.grant_id,
                granted_by: issued_by,
                expires_at: record.expires_at,
                occurred_at: now,
                correlation_id,
            }))
            .await?;
        self.grants.issue(&record, &grant_hash(&grant)).await?;
        tracing::info!(
            account.id = %account_id.as_str(),
            grant.id = %record.grant_id,
            bootstrap = issued_by.is_none(),
            "staff enrollment grant issued"
        );
        Ok(IssuedStaffGrant { grant, grant_id: record.grant_id, expires_at: record.expires_at })
    }
}
