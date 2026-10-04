use chrono::{DateTime, Duration, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::domain::entity::{GdprRecord, MfaState};
use crate::domain::event::{
    AccountActivated, AccountCreated, AccountDeactivated, AccountDeleted, AccountSuspended,
    DomainEvent, EmailChanged, EmailVerified, GdprDataExportRequested, GdprDeletionRequested,
    KycStatusChanged, MfaEnrolled, MfaRevoked, PasswordChanged, PhoneChanged, RoleAssigned,
    RoleRevoked,
};
use crate::domain::value_object::{
    AccountId, AccountRole, AccountStatus, CountryCode, EmailAddress, EncryptedBytes, IdentityId,
    KycStatus, PasswordHash, PhoneNumber, RecoveryCodeHash,
};
use crate::error::AccountError;

/// Parameters required to create a new Account aggregate.
#[derive(Debug, Clone)]
pub struct AccountCreateParams {
    pub identity_id: IdentityId,
    pub email: EmailAddress,
    /// Pre-hashed Argon2id password; `None` for SSO-only accounts.
    pub password_hash: Option<PasswordHash>,
    pub phone: Option<PhoneNumber>,
    /// Primary role assigned at creation; defaults to `User` for self-registration.
    pub role: AccountRole,
    /// ISO 3166-1 alpha-2; optional at creation for progressive-profile flows.
    pub country_of_residence: Option<CountryCode>,
    /// UUID of the admin account that provisioned this account; `None` for self-registration.
    pub created_by: Option<AccountId>,
    pub correlation_id: Uuid,
}

/// The Account aggregate root.
///
/// Manages identity verification, credentials, MFA, KYC, GDPR compliance, and
/// role-based access control for a single physical person on the platform.
/// Financial state is owned by the dedicated `ledger` microservice.
///
/// All state mutations go through domain methods that enforce invariants and
/// emit [`DomainEvent`]s. The aggregate never interacts with I/O directly.
///
/// # Invariants
///
/// - Status transitions are gated by [`AccountStatus::can_transition_to`].
/// - `version` is incremented on every write.
/// - `password_hash` is `None` for SSO-only accounts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Account {
    id: AccountId,
    version: i64,
    status: AccountStatus,
    suspension_reason: Option<String>,
    deactivated_at: Option<DateTime<Utc>>,

    identity_id: IdentityId,

    email: EmailAddress,
    email_verified: bool,
    email_verified_at: Option<DateTime<Utc>>,

    phone: Option<PhoneNumber>,
    phone_verified: bool,
    phone_verified_at: Option<DateTime<Utc>>,

    password_hash: Option<PasswordHash>,
    password_changed_at: Option<DateTime<Utc>>,

    failed_login_attempts: i32,
    locked_until: Option<DateTime<Utc>>,
    last_login_at: Option<DateTime<Utc>>,

    mfa: MfaState,

    kyc_status: KycStatus,
    kyc_reviewed_at: Option<DateTime<Utc>>,
    kyc_reviewer_id: Option<AccountId>,
    date_of_birth: Option<NaiveDate>,
    country_of_residence: Option<CountryCode>,

    gdpr: GdprRecord,

    roles: Vec<AccountRole>,
    permission_overrides: Vec<String>,

    created_by: Option<AccountId>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,

    /// Pending domain events accumulated during this unit of work.
    #[serde(skip)]
    pending_events: Vec<DomainEvent>,
}

impl Account {
    // ─── Constructors ───────────────────────────────────────────────────────

    /// Creates a new Account in `PendingVerification` status.
    ///
    /// Emits [`AccountCreated`].
    pub fn create(params: AccountCreateParams) -> Self {
        let id = AccountId::new();
        let now = Utc::now();

        let event = DomainEvent::AccountCreated(AccountCreated {
            account_id: id,
            identity_id: params.identity_id.clone(),
            email: params.email.clone(),
            role: params.role,
            status: AccountStatus::PendingVerification,
            country_of_residence: params.country_of_residence.clone(),
            occurred_at: now,
            correlation_id: params.correlation_id,
        });

        let mut account = Self {
            id,
            version: 0,
            status: AccountStatus::PendingVerification,
            suspension_reason: None,
            deactivated_at: None,
            identity_id: params.identity_id,
            email: params.email,
            email_verified: false,
            email_verified_at: None,
            phone: params.phone,
            phone_verified: false,
            phone_verified_at: None,
            password_hash: params.password_hash,
            password_changed_at: None,
            failed_login_attempts: 0,
            locked_until: None,
            last_login_at: None,
            mfa: MfaState::default(),
            kyc_status: KycStatus::NotStarted,
            kyc_reviewed_at: None,
            kyc_reviewer_id: None,
            date_of_birth: None,
            country_of_residence: params.country_of_residence,
            gdpr: GdprRecord::default(),
            roles: vec![params.role],
            permission_overrides: Vec::new(),
            created_by: params.created_by,
            created_at: now,
            updated_at: now,
            pending_events: Vec::new(),
        };
        account.pending_events.push(event);
        account
    }

    /// Reconstructs an Account from a persistence row (no events emitted).
    #[allow(clippy::too_many_arguments)]
    pub fn reconstitute(
        id: AccountId,
        identity_id: IdentityId,
        status: AccountStatus,
        suspension_reason: Option<String>,
        deactivated_at: Option<DateTime<Utc>>,
        email: EmailAddress,
        email_verified: bool,
        email_verified_at: Option<DateTime<Utc>>,
        phone: Option<PhoneNumber>,
        phone_verified: bool,
        phone_verified_at: Option<DateTime<Utc>>,
        password_hash: Option<PasswordHash>,
        password_changed_at: Option<DateTime<Utc>>,
        failed_login_attempts: i32,
        locked_until: Option<DateTime<Utc>>,
        last_login_at: Option<DateTime<Utc>>,
        mfa: MfaState,
        kyc_status: KycStatus,
        kyc_reviewed_at: Option<DateTime<Utc>>,
        kyc_reviewer_id: Option<AccountId>,
        date_of_birth: Option<NaiveDate>,
        country_of_residence: Option<CountryCode>,
        gdpr: GdprRecord,
        roles: Vec<AccountRole>,
        permission_overrides: Vec<String>,
        version: i64,
        created_at: DateTime<Utc>,
        updated_at: DateTime<Utc>,
        created_by: Option<AccountId>,
    ) -> Self {
        Self {
            id,
            version,
            status,
            suspension_reason,
            deactivated_at,
            identity_id,
            email,
            email_verified,
            email_verified_at,
            phone,
            phone_verified,
            phone_verified_at,
            password_hash,
            password_changed_at,
            failed_login_attempts,
            locked_until,
            last_login_at,
            mfa,
            kyc_status,
            kyc_reviewed_at,
            kyc_reviewer_id,
            date_of_birth,
            country_of_residence,
            gdpr,
            roles,
            permission_overrides,
            created_by,
            created_at,
            updated_at,
            pending_events: Vec::new(),
        }
    }

    // ─── Domain Mutations ───────────────────────────────────────────────────

    /// Marks the primary email as verified and transitions status to `Active`.
    ///
    /// Emits [`EmailVerified`].
    pub fn verify_email(&mut self, correlation_id: Uuid) -> Result<(), AccountError> {
        if self.email_verified {
            return Err(AccountError::EmailAlreadyVerified);
        }
        self.transition_status(AccountStatus::Active)?;
        let now = Utc::now();
        self.email_verified = true;
        self.email_verified_at = Some(now);
        self.touch(now);
        self.pending_events.push(DomainEvent::EmailVerified(EmailVerified {
            account_id: self.id,
            email: self.email.clone(),
            verified_at: now,
            occurred_at: now,
            correlation_id,
        }));
        Ok(())
    }

    /// Marks the phone number as verified.
    pub fn verify_phone(&mut self, correlation_id: Uuid) -> Result<(), AccountError> {
        self.require_active()?;
        if self.phone.is_none() {
            return Err(AccountError::DomainViolation {
                field: "phone".into(),
                message: "no phone number is set on this account".into(),
            });
        }
        let now = self.touch_now();
        self.phone_verified = true;
        self.phone_verified_at = Some(now);
        self.pending_events.push(DomainEvent::PhoneChanged(PhoneChanged {
            account_id: self.id,
            new_phone: self.phone.clone(),
            occurred_at: now,
            correlation_id,
        }));
        Ok(())
    }

    /// Replaces the stored password hash.
    ///
    /// Requires `Active` status. Emits [`PasswordChanged`].
    pub fn change_password(
        &mut self,
        new_hash: PasswordHash,
        correlation_id: Uuid,
    ) -> Result<(), AccountError> {
        self.require_active()?;
        self.password_hash = Some(new_hash);
        let now = self.touch_now();
        self.password_changed_at = Some(now);
        self.pending_events.push(DomainEvent::PasswordChanged(PasswordChanged {
            account_id: self.id,
            occurred_at: now,
            correlation_id,
        }));
        Ok(())
    }

    /// Changes the primary email. The new address must be reverified.
    ///
    /// Requires `Active` status. Emits [`EmailChanged`].
    pub fn change_email(
        &mut self,
        new_email: EmailAddress,
        correlation_id: Uuid,
    ) -> Result<(), AccountError> {
        self.require_active()?;
        let old_email = self.email.clone();
        self.email = new_email.clone();
        self.email_verified = false;
        self.email_verified_at = None;
        let now = self.touch_now();
        self.pending_events.push(DomainEvent::EmailChanged(EmailChanged {
            account_id: self.id,
            old_email,
            new_email,
            occurred_at: now,
            correlation_id,
        }));
        Ok(())
    }

    /// Updates (or removes) the phone number.
    ///
    /// Requires `Active` status. Emits [`PhoneChanged`].
    pub fn change_phone(
        &mut self,
        new_phone: Option<PhoneNumber>,
        correlation_id: Uuid,
    ) -> Result<(), AccountError> {
        self.require_active()?;
        self.phone = new_phone.clone();
        self.phone_verified = false;
        self.phone_verified_at = None;
        let now = self.touch_now();
        self.pending_events.push(DomainEvent::PhoneChanged(PhoneChanged {
            account_id: self.id,
            new_phone,
            occurred_at: now,
            correlation_id,
        }));
        Ok(())
    }

    /// Enrolls TOTP MFA with a fresh encrypted secret and initial recovery codes.
    ///
    /// Requires `Active` status. Emits [`MfaEnrolled`].
    pub fn enroll_mfa(
        &mut self,
        totp_secret: EncryptedBytes,
        recovery_codes: Vec<RecoveryCodeHash>,
        correlation_id: Uuid,
    ) -> Result<(), AccountError> {
        self.require_active()?;
        if self.mfa.is_enrolled() {
            return Err(AccountError::MfaAlreadyEnrolled);
        }
        let codes_count = recovery_codes.len();
        self.mfa.enroll(totp_secret, recovery_codes);
        let now = self.touch_now();
        self.pending_events.push(DomainEvent::MfaEnrolled(MfaEnrolled {
            account_id: self.id,
            recovery_codes_count: codes_count,
            occurred_at: now,
            correlation_id,
        }));
        Ok(())
    }

    /// Revokes all MFA state.
    ///
    /// Requires `Active` status. Emits [`MfaRevoked`].
    pub fn revoke_mfa(&mut self, correlation_id: Uuid) -> Result<(), AccountError> {
        self.require_active()?;
        if !self.mfa.is_enrolled() {
            return Err(AccountError::MfaNotEnrolled);
        }
        self.mfa.revoke();
        let now = self.touch_now();
        self.pending_events.push(DomainEvent::MfaRevoked(MfaRevoked {
            account_id: self.id,
            occurred_at: now,
            correlation_id,
        }));
        Ok(())
    }

    /// Assigns a role to this account. Emits [`RoleAssigned`].
    pub fn assign_role(
        &mut self,
        role: AccountRole,
        correlation_id: Uuid,
    ) -> Result<(), AccountError> {
        if self.roles.contains(&role) {
            return Err(AccountError::RoleAlreadyAssigned(role.as_str().to_owned()));
        }
        self.roles.push(role);
        let now = self.touch_now();
        self.pending_events.push(DomainEvent::RoleAssigned(RoleAssigned {
            account_id: self.id,
            role,
            occurred_at: now,
            correlation_id,
        }));
        Ok(())
    }

    /// Revokes a role from this account.
    ///
    /// The last remaining role cannot be revoked. Emits [`RoleRevoked`].
    pub fn revoke_role(
        &mut self,
        role: AccountRole,
        correlation_id: Uuid,
    ) -> Result<(), AccountError> {
        let pos = self
            .roles
            .iter()
            .position(|r| *r == role)
            .ok_or_else(|| AccountError::RoleNotAssigned(role.as_str().to_owned()))?;
        if self.roles.len() == 1 {
            return Err(AccountError::DomainViolation {
                field: "roles".into(),
                message: "cannot revoke the last role from an account".into(),
            });
        }
        self.roles.remove(pos);
        let now = self.touch_now();
        self.pending_events.push(DomainEvent::RoleRevoked(RoleRevoked {
            account_id: self.id,
            role,
            occurred_at: now,
            correlation_id,
        }));
        Ok(())
    }

    /// Suspends the account. Emits [`AccountSuspended`].
    pub fn suspend(
        &mut self,
        reason: String,
        correlation_id: Uuid,
    ) -> Result<(), AccountError> {
        self.transition_status(AccountStatus::Suspended)?;
        self.suspension_reason = Some(reason.clone());
        let now = self.touch_now();
        self.pending_events.push(DomainEvent::AccountSuspended(AccountSuspended {
            account_id: self.id,
            reason,
            occurred_at: now,
            correlation_id,
        }));
        Ok(())
    }

    /// Re-activates a suspended (or deactivated) account — the admin path.
    /// Emits [`AccountActivated`].
    pub fn activate(&mut self, correlation_id: Uuid) -> Result<(), AccountError> {
        self.transition_status(AccountStatus::Active)?;
        self.suspension_reason = None;
        self.deactivated_at = None;
        let now = self.touch_now();
        self.pending_events.push(DomainEvent::AccountActivated(AccountActivated {
            account_id: self.id,
            occurred_at: now,
            correlation_id,
        }));
        Ok(())
    }

    /// Deactivates the account (self-service closure). Emits [`AccountDeactivated`].
    pub fn deactivate(&mut self, correlation_id: Uuid) -> Result<(), AccountError> {
        self.transition_status(AccountStatus::Deactivated)?;
        let now = self.touch_now();
        self.deactivated_at = Some(now);
        self.pending_events.push(DomainEvent::AccountDeactivated(AccountDeactivated {
            account_id: self.id,
            occurred_at: now,
            correlation_id,
        }));
        Ok(())
    }

    /// Returns a self-deactivated account to Active — the holder signed back in.
    /// Emits [`AccountActivated`] and returns `true`.
    ///
    /// Idempotent for a concurrent sign-in: an account that is already Active
    /// returns `false` with no event. Every other status is refused — a
    /// suspension is lifted only by an admin through [`Self::activate`].
    pub fn resume_after_deactivation(&mut self, correlation_id: Uuid) -> Result<bool, AccountError> {
        match self.status {
            AccountStatus::Active => Ok(false),
            AccountStatus::Deactivated => {
                self.activate(correlation_id)?;
                Ok(true)
            }
            other => Err(AccountError::InvalidStatusTransition {
                from: other.as_str().to_owned(),
                to: AccountStatus::Active.as_str().to_owned(),
            }),
        }
    }

    /// Hard-deletes the account (terminal state). Emits [`AccountDeleted`].
    pub fn delete(
        &mut self,
        deleted_by: Option<AccountId>,
        correlation_id: Uuid,
    ) -> Result<(), AccountError> {
        self.transition_status(AccountStatus::Deleted)?;
        let now = self.touch_now();
        self.pending_events.push(DomainEvent::AccountDeleted(AccountDeleted {
            account_id: self.id,
            deleted_by,
            occurred_at: now,
            correlation_id,
        }));
        Ok(())
    }

    /// Updates the KYC status and records the reviewer. Emits [`KycStatusChanged`].
    pub fn update_kyc_status(
        &mut self,
        new_status: KycStatus,
        reviewer_id: AccountId,
        correlation_id: Uuid,
    ) -> Result<(), AccountError> {
        if !self.kyc_status.can_transition_to(new_status) {
            return Err(AccountError::InvalidKycTransition {
                from: self.kyc_status.as_str().to_owned(),
                to: new_status.as_str().to_owned(),
            });
        }
        let old_status = self.kyc_status;
        self.kyc_status = new_status;
        let now = Utc::now();
        self.kyc_reviewed_at = Some(now);
        self.kyc_reviewer_id = Some(reviewer_id);
        self.touch(now);
        self.pending_events.push(DomainEvent::KycStatusChanged(KycStatusChanged {
            account_id: self.id,
            old_status,
            new_status,
            occurred_at: now,
            correlation_id,
        }));
        Ok(())
    }

    /// Records a GDPR Art. 17 erasure request and schedules anonymisation.
    ///
    /// Emits [`GdprDeletionRequested`].
    pub fn request_gdpr_deletion(
        &mut self,
        retention_days: u32,
        correlation_id: Uuid,
    ) -> Result<(), AccountError> {
        if self.gdpr.has_pending_deletion() {
            return Err(AccountError::GdprDeletionAlreadyRequested);
        }
        self.gdpr.request_deletion(retention_days);
        let scheduled = self.gdpr.deletion_scheduled_at.expect("just set");
        let now = self.touch_now();
        self.pending_events.push(DomainEvent::GdprDeletionRequested(GdprDeletionRequested {
            account_id: self.id,
            retention_days,
            scheduled_deletion_at: scheduled,
            occurred_at: now,
            correlation_id,
        }));
        Ok(())
    }

    /// Records a GDPR Art. 20 data portability export request.
    ///
    /// Emits [`GdprDataExportRequested`].
    pub fn request_gdpr_data_export(&mut self, correlation_id: Uuid) -> Result<(), AccountError> {
        let now = self.touch_now();
        self.gdpr.data_export_requested_at = Some(now);
        self.pending_events.push(DomainEvent::GdprDataExportRequested(
            GdprDataExportRequested {
                account_id: self.id,
                requested_at: now,
                occurred_at: now,
                correlation_id,
            },
        ));
        Ok(())
    }

    /// Anonymises the account: clears PII fields and marks as deleted.
    ///
    /// Called by the GDPR janitor worker once `deletion_scheduled_at` has elapsed.
    /// Emits [`AccountDeleted`].
    pub fn anonymize(&mut self, correlation_id: Uuid) -> Result<(), AccountError> {
        if self.gdpr.is_anonymized() {
            return Err(AccountError::AccountAlreadyAnonymized);
        }
        let now = Utc::now();
        self.gdpr.anonymized_at = Some(now);
        self.phone = None;
        self.phone_verified = false;
        self.phone_verified_at = None;
        self.password_hash = None;
        self.date_of_birth = None;
        self.mfa.revoke();
        let _ = self.transition_status(AccountStatus::Deleted);
        self.touch(now);
        self.pending_events.push(DomainEvent::AccountDeleted(AccountDeleted {
            account_id: self.id,
            deleted_by: None,
            occurred_at: now,
            correlation_id,
        }));
        Ok(())
    }

    /// Records a successful login: resets the failure counter and updates `last_login_at`.
    pub fn record_login(&mut self) {
        self.failed_login_attempts = 0;
        self.locked_until = None;
        let now = Utc::now();
        self.last_login_at = Some(now);
        self.touch(now);
    }

    /// Increments the failed-login counter; applies a timed lockout when
    /// `max_attempts` is exceeded.
    pub fn record_failed_login(&mut self, max_attempts: u16, lockout_duration_secs: u64) {
        self.failed_login_attempts += 1;
        if self.failed_login_attempts as u16 >= max_attempts {
            self.locked_until =
                Some(Utc::now() + Duration::seconds(lockout_duration_secs as i64));
        }
        let now = Utc::now();
        self.touch(now);
    }

    // ─── Event Drain ────────────────────────────────────────────────────────

    /// Drains and returns all pending domain events, clearing the buffer.
    pub fn drain_events(&mut self) -> Vec<DomainEvent> {
        std::mem::take(&mut self.pending_events)
    }

    /// The pending domain events without consuming them — used by the repository to
    /// publish after a successful durable write (the aggregate is dropped at the end
    /// of the command, so there is no double-publish risk).
    pub fn events(&self) -> &[DomainEvent] {
        &self.pending_events
    }

    // ─── Getters ────────────────────────────────────────────────────────────

    pub fn id(&self) -> AccountId { self.id }

    pub fn version(&self) -> i64 { self.version }

    pub fn status(&self) -> AccountStatus { self.status }

    pub fn is_active(&self) -> bool { self.status == AccountStatus::Active }

    pub fn suspension_reason(&self) -> Option<&str> { self.suspension_reason.as_deref() }

    pub fn deactivated_at(&self) -> Option<DateTime<Utc>> { self.deactivated_at }

    pub fn identity_id(&self) -> &IdentityId { &self.identity_id }

    pub fn email(&self) -> &EmailAddress { &self.email }

    pub fn email_verified(&self) -> bool { self.email_verified }

    pub fn email_verified_at(&self) -> Option<DateTime<Utc>> { self.email_verified_at }

    pub fn phone(&self) -> Option<&PhoneNumber> { self.phone.as_ref() }

    pub fn phone_verified(&self) -> bool { self.phone_verified }

    pub fn phone_verified_at(&self) -> Option<DateTime<Utc>> { self.phone_verified_at }

    pub fn password_hash(&self) -> Option<&PasswordHash> { self.password_hash.as_ref() }

    pub fn password_changed_at(&self) -> Option<DateTime<Utc>> { self.password_changed_at }

    pub fn failed_login_attempts(&self) -> i32 { self.failed_login_attempts }

    pub fn locked_until(&self) -> Option<DateTime<Utc>> { self.locked_until }

    pub fn is_locked(&self) -> bool {
        self.locked_until.is_some_and(|until| until > Utc::now())
    }

    pub fn last_login_at(&self) -> Option<DateTime<Utc>> { self.last_login_at }

    pub fn mfa(&self) -> &MfaState { &self.mfa }

    pub fn mfa_mut(&mut self) -> &mut MfaState { &mut self.mfa }

    pub fn kyc_status(&self) -> KycStatus { self.kyc_status }

    pub fn kyc_reviewed_at(&self) -> Option<DateTime<Utc>> { self.kyc_reviewed_at }

    pub fn kyc_reviewer_id(&self) -> Option<AccountId> { self.kyc_reviewer_id }

    pub fn date_of_birth(&self) -> Option<NaiveDate> { self.date_of_birth }

    pub fn country_of_residence(&self) -> Option<&CountryCode> { self.country_of_residence.as_ref() }

    pub fn gdpr(&self) -> &GdprRecord { &self.gdpr }

    pub fn gdpr_mut(&mut self) -> &mut GdprRecord { &mut self.gdpr }

    pub fn roles(&self) -> &[AccountRole] { &self.roles }

    pub fn has_role(&self, role: AccountRole) -> bool { self.roles.contains(&role) }

    pub fn permission_overrides(&self) -> &[String] { &self.permission_overrides }

    /// The effective fine-grained permission set: the union of every assigned
    /// role's grants and the per-account `permission_overrides`, deduplicated
    /// and sorted (deterministic output — these are minted into edge tokens by
    /// `auth` and compared in tests/audit trails).
    pub fn effective_permissions(&self) -> Vec<String> {
        let mut permissions: Vec<String> = self
            .roles
            .iter()
            .flat_map(|role| role.granted_permissions().iter().map(|p| (*p).to_owned()))
            .chain(self.permission_overrides.iter().cloned())
            .collect();
        permissions.sort_unstable();
        permissions.dedup();
        permissions
    }

    pub fn created_by(&self) -> Option<AccountId> { self.created_by }

    pub fn created_at(&self) -> DateTime<Utc> { self.created_at }

    pub fn updated_at(&self) -> DateTime<Utc> { self.updated_at }

    // ─── Private Helpers ────────────────────────────────────────────────────

    fn require_active(&self) -> Result<(), AccountError> {
        if self.status != AccountStatus::Active {
            return Err(AccountError::AccountNotActive {
                current: self.status.as_str().to_owned(),
            });
        }
        Ok(())
    }

    fn transition_status(&mut self, next: AccountStatus) -> Result<(), AccountError> {
        if !self.status.can_transition_to(next) {
            return Err(AccountError::InvalidStatusTransition {
                from: self.status.as_str().to_owned(),
                to: next.as_str().to_owned(),
            });
        }
        self.status = next;
        Ok(())
    }

    fn touch(&mut self, now: DateTime<Utc>) {
        self.version += 1;
        self.updated_at = now;
    }

    fn touch_now(&mut self) -> DateTime<Utc> {
        let now = Utc::now();
        self.touch(now);
        now
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn admin_account_with_overrides(overrides: Vec<String>) -> Account {
        Account::reconstitute(
            AccountId::new(),
            IdentityId::new("idp|test-subject").expect("identity id"),
            AccountStatus::Active,
            None,
            None,
            EmailAddress::new("ops@example.com").expect("email"),
            true,
            None,
            None,
            false,
            None,
            None,
            None,
            0,
            None,
            None,
            MfaState::default(),
            KycStatus::NotStarted,
            None,
            None,
            None,
            None,
            GdprRecord::default(),
            vec![AccountRole::Admin, AccountRole::SuperAdmin],
            overrides,
            1,
            Utc::now(),
            Utc::now(),
            None,
        )
    }

    /// Union semantics: overlapping role grants (Admin ⊂ SuperAdmin) collapse,
    /// overrides join the set, duplicates against role grants disappear, and
    /// the result is sorted — the exact string set auth mints into the token.
    #[test]
    fn effective_permissions_union_roles_and_overrides_deduped_sorted() {
        let account = admin_account_with_overrides(vec![
            "audit:read".to_owned(),      // duplicate of a role grant
            "compliance:hold".to_owned(), // pure override
        ]);

        assert_eq!(
            account.effective_permissions(),
            vec![
                "audit:export".to_owned(),
                "audit:read".to_owned(),
                "audit:record".to_owned(),
                "audit:verify".to_owned(),
                "compliance:hold".to_owned(),
            ]
        );
    }

    /// A plain user's token must carry no fine-grained grants at all.
    #[test]
    fn baseline_account_has_no_effective_permissions() {
        let account = Account::reconstitute(
            AccountId::new(),
            IdentityId::new("idp|plain-user").expect("identity id"),
            AccountStatus::Active,
            None,
            None,
            EmailAddress::new("user@example.com").expect("email"),
            true,
            None,
            None,
            false,
            None,
            None,
            None,
            0,
            None,
            None,
            MfaState::default(),
            KycStatus::NotStarted,
            None,
            None,
            None,
            None,
            GdprRecord::default(),
            vec![AccountRole::User],
            Vec::new(),
            1,
            Utc::now(),
            Utc::now(),
            None,
        );
        assert!(account.effective_permissions().is_empty());
    }

    fn account_in(status: AccountStatus, suspension_reason: Option<String>) -> Account {
        Account::reconstitute(
            AccountId::new(),
            IdentityId::new("idp|lifecycle").expect("identity id"),
            status,
            suspension_reason,
            (status == AccountStatus::Deactivated).then(Utc::now),
            EmailAddress::new("user@example.com").expect("email"),
            true,
            None,
            None,
            false,
            None,
            None,
            None,
            0,
            None,
            None,
            MfaState::default(),
            KycStatus::NotStarted,
            None,
            None,
            None,
            None,
            GdprRecord::default(),
            vec![AccountRole::User],
            Vec::new(),
            1,
            Utc::now(),
            Utc::now(),
            None,
        )
    }

    /// Signing back in returns a self-deactivated account to Active, clears
    /// the deactivation timestamp and tells the fleet (profiles un-hide on it).
    #[test]
    fn resume_returns_a_deactivated_account_to_active() {
        let mut account = account_in(AccountStatus::Deactivated, None);

        assert!(account.resume_after_deactivation(Uuid::now_v7()).unwrap());

        assert_eq!(account.status(), AccountStatus::Active);
        assert_eq!(account.deactivated_at(), None);
        let events = account.drain_events();
        assert!(matches!(events.as_slice(), [DomainEvent::AccountActivated(_)]));
    }

    /// A second sign-in racing the first finds the account already Active:
    /// nothing to do, nothing to publish.
    #[test]
    fn resume_is_a_no_op_on_an_active_account() {
        let mut account = account_in(AccountStatus::Active, None);

        assert!(!account.resume_after_deactivation(Uuid::now_v7()).unwrap());

        assert!(account.drain_events().is_empty());
    }

    /// Only an admin lifts a suspension: signing in must not.
    #[test]
    fn resume_refuses_every_status_but_deactivated() {
        for status in [
            AccountStatus::Suspended,
            AccountStatus::PendingVerification,
            AccountStatus::Deleted,
        ] {
            let mut account = account_in(status, Some("spam".into()));
            let err = account.resume_after_deactivation(Uuid::now_v7()).unwrap_err();
            assert!(matches!(err, AccountError::InvalidStatusTransition { .. }), "{status}");
            assert_eq!(account.status(), status);
            assert!(account.drain_events().is_empty());
        }
    }

    /// Deactivating while suspended would turn the next sign-in into a way
    /// around the suspension.
    #[test]
    fn a_suspended_account_cannot_deactivate() {
        let mut account = account_in(AccountStatus::Suspended, Some("spam".into()));

        let err = account.deactivate(Uuid::now_v7()).unwrap_err();

        assert!(matches!(err, AccountError::InvalidStatusTransition { .. }));
        assert_eq!(account.status(), AccountStatus::Suspended);
    }
}
