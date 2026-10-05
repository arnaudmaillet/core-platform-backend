use account_api::account_service_client::AccountServiceClient;
use account_api::{
    AccountStatus, AgeBracket as ProtoAgeBracket, CreateAccountRequest, GetAccountByEmailRequest,
    GetAccountByIdRequest, GetAccountByIdentityIdRequest, GetAccountByPhoneRequest,
    ChangeEmailRequest, ChangePhoneRequest, ConsumeRecoveryCodeRequest, EnrollMfaRequest, GetMfaSecretRequest,
    ReplaceRecoveryCodesRequest, ResumeDeactivatedAccountRequest, RevokeMfaRequest, UpdateConsentsRequest,
    VerifyEmailRequest, VerifyPhoneRequest,
};
use async_trait::async_trait;
use tonic::transport::Channel;
use tonic::Code;
use tracing::instrument;

use crate::application::port::{
    AccountActivation, AccountDirectory, AccountSnapshot, ContactDetails, EmailHolder, MfaSecret, NewAccount,
    VerificationChannel,
};
use crate::domain::value_object::{AccountId, AgeBracket, IdpSubject, Permission};
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

        // UNSPECIFIED (no date of birth, or a server predating the field) ⇒ none.
        let age_bracket = match ProtoAgeBracket::try_from(view.age_bracket) {
            Ok(ProtoAgeBracket::AgeBracket1315) => Some(AgeBracket::Teen13To15),
            Ok(ProtoAgeBracket::AgeBracket1617) => Some(AgeBracket::Teen16To17),
            Ok(ProtoAgeBracket::Adult) => Some(AgeBracket::Adult),
            _ => None,
        };

        Ok(AccountSnapshot { activation, permissions, age_bracket, mfa_enrolled: view.mfa_enrolled })
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

    /// `CreateAccount` (mesh) → its id → `VerifyEmail` when the IdP vouches for
    /// the address (the account becomes active) → `UpdateConsents`. Each step is
    /// idempotent, so a retried sign-up finishes what a failed one left: an
    /// account already created for this subject is picked up, an already
    /// verified email is fine, consents re-apply.
    #[instrument(name = "auth.directory.provision", skip(self, account), fields(subject = %account.subject))]
    async fn provision(&self, account: &NewAccount) -> Result<AccountId, AuthError> {
        let identity = identity_id(&account.subject);
        let mut client = self.client.clone();
        let created = client
            .create_account(CreateAccountRequest {
                identity_id: identity.clone(),
                email: account.email.clone().unwrap_or_default(),
                phone: account.phone.clone().unwrap_or_default(),
                country_of_residence: account.country.clone().unwrap_or_default(),
                date_of_birth: account.date_of_birth.clone(),
                ..Default::default()
            })
            .await;
        if let Err(status) = created {
            match error_code(&status) {
                Some("ACC-1002") => {} // this subject's account exists: finish it
                Some("ACC-1003") => return Err(AuthError::EmailAlreadyRegistered),
                Some("ACC-1004") => return Err(AuthError::PhoneAlreadyRegistered),
                Some("ACC-2004") => return Err(AuthError::AgeBelowMinimum),
                _ if status.code() == Code::FailedPrecondition || status.code() == Code::InvalidArgument => {
                    return Err(AuthError::DomainViolation {
                        field: "account".into(),
                        message: status.message().to_owned(),
                    });
                }
                _ => return Err(AuthError::AccountDirectoryUnavailable),
            }
        }
        // CreateAccount's response does not carry the generated id: read it back.
        let id = self
            .account_by_identity(&identity)
            .await?
            .ok_or(AuthError::AccountDirectoryUnavailable)?;
        let account_id = AccountId::try_from(id.as_str())?;

        if account.email_verified && account.email.is_some() {
            match client.verify_email(VerifyEmailRequest { account_id: id.clone() }).await {
                Ok(_) => {}
                Err(status) if error_code(&status) == Some("ACC-2003") => {} // already verified
                Err(_) => return Err(AuthError::AccountDirectoryUnavailable),
            }
        }
        // A phone-only account: verifying the number activates it (verifying
        // it again on a retry is harmless).
        if account.phone_verified && account.phone.is_some() {
            client
                .verify_phone(VerifyPhoneRequest { account_id: id.clone() })
                .await
                .map_err(|_| AuthError::AccountDirectoryUnavailable)?;
        }

        let consent = &account.consent;
        client
            .update_consents(UpdateConsentsRequest {
                account_id: id,
                data_processing: Some(consent.data_processing),
                marketing: Some(consent.marketing),
                analytics: Some(consent.analytics),
                policy_version: consent.policy_version.clone(),
            })
            .await
            .map_err(|_| AuthError::AccountDirectoryUnavailable)?;

        Ok(account_id)
    }

    #[instrument(name = "auth.directory.find_by_phone", skip(self, phone))]
    async fn find_by_phone(&self, phone: &str) -> Result<Option<EmailHolder>, AuthError> {
        match self
            .client
            .clone()
            .get_account_by_phone(GetAccountByPhoneRequest { phone: phone.to_owned() })
            .await
        {
            Ok(view) => {
                let view = view.into_inner();
                Ok(Some(EmailHolder {
                    account_id: AccountId::try_from(view.id.as_str())?,
                    identity_id: view.identity_id,
                }))
            }
            Err(status) if status.code() == Code::NotFound => Ok(None),
            Err(status) if status.code() == Code::FailedPrecondition => Ok(None),
            Err(_) => Err(AuthError::AccountDirectoryUnavailable),
        }
    }

    #[instrument(name = "auth.directory.contact", skip(self))]
    async fn contact(&self, account_id: &AccountId) -> Result<ContactDetails, AuthError> {
        let view = self
            .client
            .clone()
            .get_account_by_id(GetAccountByIdRequest { account_id: account_id.as_str() })
            .await
            .map_err(|_| AuthError::AccountDirectoryUnavailable)?
            .into_inner();
        Ok(ContactDetails {
            email: Some(view.email).filter(|e| !e.is_empty()),
            phone: Some(view.phone).filter(|p| !p.is_empty()),
        })
    }

    #[instrument(name = "auth.directory.change_contact", skip(self, destination))]
    async fn change_contact(
        &self,
        account_id: &AccountId,
        channel: VerificationChannel,
        destination: &str,
    ) -> Result<(), AuthError> {
        let mut client = self.client.clone();
        let changed = match channel {
            VerificationChannel::Email => client
                .change_email(ChangeEmailRequest { account_id: account_id.as_str(), email: destination.to_owned() })
                .await
                .map(|_| ()),
            VerificationChannel::Sms => client
                .change_phone(ChangePhoneRequest { account_id: account_id.as_str(), phone: destination.to_owned() })
                .await
                .map(|_| ()),
        };
        changed.map_err(|status| match error_code(&status) {
            Some("ACC-1003") => AuthError::EmailAlreadyRegistered,
            Some("ACC-1004") => AuthError::PhoneAlreadyRegistered,
            _ if status.code() == Code::FailedPrecondition => {
                AuthError::AccountNotActive { current: status.message().to_owned() }
            }
            _ => AuthError::AccountDirectoryUnavailable,
        })
    }

    #[instrument(name = "auth.directory.mfa_secret", skip(self), fields(account.id = %account_id))]
    async fn mfa_secret(&self, account_id: &AccountId) -> Result<MfaSecret, AuthError> {
        let view = self
            .client
            .clone()
            .get_mfa_secret(GetMfaSecretRequest { account_id: account_id.as_str() })
            .await
            .map_err(|status| match status.code() {
                Code::NotFound => AuthError::AccountNotActive { current: "not_found".into() },
                _ => AuthError::AccountDirectoryUnavailable,
            })?
            .into_inner();
        Ok(MfaSecret {
            enrolled: view.enrolled,
            sealed_seed: view.totp_secret,
            recovery_codes_remaining: u32::try_from(view.recovery_codes_remaining).unwrap_or(0),
        })
    }

    #[instrument(name = "auth.directory.consume_recovery_code", skip(self, code_hash), fields(account.id = %account_id))]
    async fn consume_recovery_code(&self, account_id: &AccountId, code_hash: &str) -> Result<bool, AuthError> {
        let spent = self
            .client
            .clone()
            .consume_recovery_code(ConsumeRecoveryCodeRequest {
                account_id: account_id.as_str(),
                code_hash: code_hash.to_owned(),
            })
            .await;
        match spent {
            Ok(_) => Ok(true),
            // No unused code matches, or MFA is off: the code proves nothing.
            Err(status) if matches!(error_code(&status), Some("ACC-5003" | "ACC-5002")) => Ok(false),
            // Two sign-ins spent codes at once: the code may still be unused.
            Err(status) if status.code() == Code::Aborted => Err(AuthError::ConcurrentModification),
            Err(_) => Err(AuthError::AccountDirectoryUnavailable),
        }
    }

    #[instrument(name = "auth.directory.enroll_mfa", skip_all, fields(account.id = %account_id))]
    async fn enroll_mfa(&self, account_id: &AccountId, sealed_seed: &[u8], code_hashes: &[String]) -> Result<(), AuthError> {
        self.client
            .clone()
            .enroll_mfa(EnrollMfaRequest {
                account_id: account_id.as_str(),
                totp_secret: sealed_seed.to_vec(),
                recovery_code_hashes: code_hashes.to_vec(),
            })
            .await
            .map(|_| ())
            .map_err(mfa_write_error)
    }

    #[instrument(name = "auth.directory.revoke_mfa", skip(self), fields(account.id = %account_id))]
    async fn revoke_mfa(&self, account_id: &AccountId) -> Result<(), AuthError> {
        self.client
            .clone()
            .revoke_mfa(RevokeMfaRequest { account_id: account_id.as_str() })
            .await
            .map(|_| ())
            .map_err(mfa_write_error)
    }

    #[instrument(name = "auth.directory.replace_recovery_codes", skip_all, fields(account.id = %account_id))]
    async fn replace_recovery_codes(&self, account_id: &AccountId, code_hashes: &[String]) -> Result<(), AuthError> {
        self.client
            .clone()
            .replace_recovery_codes(ReplaceRecoveryCodesRequest {
                account_id: account_id.as_str(),
                recovery_code_hashes: code_hashes.to_vec(),
            })
            .await
            .map(|_| ())
            .map_err(mfa_write_error)
    }

    #[instrument(name = "auth.directory.find_by_email", skip(self, email))]
    async fn find_by_email(&self, email: &str) -> Result<Option<EmailHolder>, AuthError> {
        match self
            .client
            .clone()
            .get_account_by_email(GetAccountByEmailRequest { email: email.to_owned() })
            .await
        {
            Ok(view) => {
                let view = view.into_inner();
                Ok(Some(EmailHolder {
                    account_id: AccountId::try_from(view.id.as_str())?,
                    identity_id: view.identity_id,
                }))
            }
            Err(status) if status.code() == Code::NotFound => Ok(None),
            // A malformed address holds no account.
            Err(status) if status.code() == Code::FailedPrecondition => Ok(None),
            Err(_) => Err(AuthError::AccountDirectoryUnavailable),
        }
    }
}

/// The `account` error code a status carries (`x-error-code`), if any.
fn error_code(status: &tonic::Status) -> Option<&str> {
    status.metadata().get("x-error-code").and_then(|v| v.to_str().ok())
}

impl GrpcAccountDirectory {
    async fn account_by_identity(&self, identity_id: &str) -> Result<Option<String>, AuthError> {
        match self
            .client
            .clone()
            .get_account_by_identity_id(GetAccountByIdentityIdRequest { identity_id: identity_id.to_owned() })
            .await
        {
            Ok(view) => Ok(Some(view.into_inner().id)),
            Err(status) if status.code() == Code::NotFound => Ok(None),
            Err(_) => Err(AuthError::AccountDirectoryUnavailable),
        }
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

/// account's answer to an MFA write (#649).
fn mfa_write_error(status: tonic::Status) -> AuthError {
    match error_code(&status) {
        Some("ACC-5001") => AuthError::MfaAlreadyEnabled,
        Some("ACC-5002") => AuthError::MfaNotEnabled,
        _ if status.code() == Code::Aborted => AuthError::ConcurrentModification,
        _ if status.code() == Code::FailedPrecondition => AuthError::AccountNotActive { current: status.message().to_owned() },
        _ => AuthError::AccountDirectoryUnavailable,
    }
}
