use crate::domain::aggregate::account::Account;
use crate::domain::entity::{gdpr_record::GdprRecord, mfa_state::MfaState};
use crate::domain::value_object::{
    account_id::AccountId,
    account_role::AccountRole,
    account_status::AccountStatus,
    country_code::CountryCode,
    email_address::EmailAddress,
    encrypted_bytes::EncryptedBytes,
    identity_id::IdentityId,
    kyc_status::KycStatus,
    password_hash::PasswordHash,
    phone_number::PhoneNumber,
    recovery_code_hash::RecoveryCodeHash,
};
use crate::error::AccountError;
use chrono::{DateTime, NaiveDate, Utc};
use uuid::Uuid;

/// Flat database projection for the `accounts` table.
///
/// Contains only primitive types that `sqlx` can decode directly from
/// PostgreSQL wire format. Domain type construction — including validation,
/// enum parsing, and value-object invariant enforcement — happens in the
/// `TryFrom<AccountRow> for Account` conversion below, keeping the
/// persistence layer free from domain logic.
#[derive(Debug, sqlx::FromRow)]
pub struct AccountRow {
    pub id: Uuid,
    pub identity_id: String,
    pub status: String,
    pub suspension_reason: Option<String>,
    pub deactivated_at: Option<DateTime<Utc>>,

    /// `NULL` for a phone-only account.
    pub email: Option<String>,
    pub email_verified: bool,
    pub email_verified_at: Option<DateTime<Utc>>,
    pub phone: Option<String>,
    pub phone_verified: bool,
    pub phone_verified_at: Option<DateTime<Utc>>,

    pub password_hash: Option<String>,
    pub password_changed_at: Option<DateTime<Utc>>,
    pub failed_login_attempts: i32,
    pub locked_until: Option<DateTime<Utc>>,
    pub last_login_at: Option<DateTime<Utc>>,

    pub mfa_enforced: bool,
    pub mfa_totp_secret: Option<Vec<u8>>,
    pub mfa_totp_enrolled_at: Option<DateTime<Utc>>,
    pub mfa_recovery_codes: Vec<String>,
    pub mfa_backup_verified_at: Option<DateTime<Utc>>,

    pub kyc_status: String,
    pub kyc_reviewed_at: Option<DateTime<Utc>>,
    pub kyc_reviewer_id: Option<Uuid>,
    pub date_of_birth: Option<NaiveDate>,
    pub country_of_residence: Option<String>,

    pub gdpr_data_processing_consented_at: Option<DateTime<Utc>>,
    pub gdpr_marketing_consented_at: Option<DateTime<Utc>>,
    pub gdpr_consent_ip: Option<String>,
    pub gdpr_last_consent_version: Option<String>,
    pub gdpr_deletion_requested_at: Option<DateTime<Utc>>,
    pub gdpr_deletion_scheduled_at: Option<DateTime<Utc>>,
    pub gdpr_anonymized_at: Option<DateTime<Utc>>,
    pub gdpr_data_export_requested_at: Option<DateTime<Utc>>,
    pub gdpr_data_export_completed_at: Option<DateTime<Utc>>,
    pub gdpr_analytics_consented_at: Option<DateTime<Utc>>,
    pub gdpr_data_export_key: Option<String>,
    pub gdpr_data_export_expires_at: Option<DateTime<Utc>>,

    pub roles: Vec<String>,
    pub permission_overrides: Vec<String>,

    pub version: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub created_by: Option<Uuid>,
}

impl TryFrom<AccountRow> for Account {
    type Error = AccountError;

    fn try_from(row: AccountRow) -> Result<Self, Self::Error> {
        let id = AccountId::from_uuid(row.id);

        let identity_id = IdentityId::new(row.identity_id)?;

        let status = AccountStatus::try_from(row.status.as_str())
            .map_err(|_| AccountError::InvalidAccountStatus(row.status.clone()))?;

        let email = row.email.map(EmailAddress::new).transpose()?;

        let phone = row.phone.map(PhoneNumber::new).transpose()?;

        let password_hash = row.password_hash.map(PasswordHash::from_hash);

        let mfa_recovery_codes: Vec<RecoveryCodeHash> = row
            .mfa_recovery_codes
            .into_iter()
            .map(RecoveryCodeHash::from_hash)
            .collect();
        let mfa = MfaState::reconstitute(
            row.mfa_enforced,
            row.mfa_totp_secret.map(EncryptedBytes::from_ciphertext),
            row.mfa_totp_enrolled_at,
            mfa_recovery_codes,
            row.mfa_backup_verified_at,
        );

        let kyc_status = KycStatus::try_from(row.kyc_status.as_str())
            .map_err(|_| AccountError::InvalidKycStatus(row.kyc_status.clone()))?;
        let kyc_reviewer_id = row.kyc_reviewer_id.map(AccountId::from_uuid);

        let country_of_residence = row
            .country_of_residence
            .map(CountryCode::new)
            .transpose()?;

        let gdpr = GdprRecord::reconstitute(
            row.gdpr_data_processing_consented_at,
            row.gdpr_marketing_consented_at,
            row.gdpr_consent_ip,
            row.gdpr_last_consent_version,
            row.gdpr_deletion_requested_at,
            row.gdpr_deletion_scheduled_at,
            row.gdpr_anonymized_at,
            row.gdpr_data_export_requested_at,
            row.gdpr_data_export_completed_at,
        )
        .with_analytics_consented_at(row.gdpr_analytics_consented_at)
        .with_data_export_delivery(row.gdpr_data_export_key, row.gdpr_data_export_expires_at);

        let roles: Vec<AccountRole> = row
            .roles
            .iter()
            .map(|s| {
                AccountRole::try_from(s.as_str())
                    .map_err(|_| AccountError::InvalidAccountRole(s.clone()))
            })
            .collect::<Result<_, _>>()?;

        Ok(Account::reconstitute(
            id,
            identity_id,
            status,
            row.suspension_reason,
            row.deactivated_at,
            email,
            row.email_verified,
            row.email_verified_at,
            phone,
            row.phone_verified,
            row.phone_verified_at,
            password_hash,
            row.password_changed_at,
            row.failed_login_attempts,
            row.locked_until,
            row.last_login_at,
            mfa,
            kyc_status,
            row.kyc_reviewed_at,
            kyc_reviewer_id,
            row.date_of_birth,
            country_of_residence,
            gdpr,
            roles,
            row.permission_overrides,
            row.version,
            row.created_at,
            row.updated_at,
            row.created_by.map(AccountId::from_uuid),
        ))
    }
}
