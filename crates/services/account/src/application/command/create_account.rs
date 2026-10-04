use std::sync::Arc;

use cqrs::{Command, CommandHandler, Envelope};
use validate_core::{FieldViolation, Validate};

use crate::application::port::AccountRepository;
use crate::domain::aggregate::{Account, AccountCreateParams};
use crate::domain::value_object::{
    check_date_of_birth, AccountId, AccountRole, CountryCode, EmailAddress, IdentityId,
    PasswordHash, PhoneNumber,
};
use crate::error::AccountError;

/// Command: register a new account from a completed sign-up or admin provisioning flow.
#[derive(Debug, Clone)]
pub struct CreateAccountCommand {
    /// Raw IdP subject claim — validated for non-emptiness; format is IdP-specific.
    pub identity_id: String,
    pub email: String,
    pub phone: Option<String>,
    /// Pre-hashed password (Argon2id); `None` for SSO-only accounts.
    pub password_hash: Option<String>,
    /// ISO 3166-1 alpha-2 country code; optional at creation.
    pub country_of_residence: Option<String>,
    /// Role variant name (e.g. `"user"`); defaults to `User` if absent.
    pub role: Option<String>,
    /// UUID string of the admin account that provisioned this account; `None` for
    /// self-registration.
    pub created_by: Option<String>,
    /// ISO 8601 (`YYYY-MM-DD`); refused under the minimum age (13; 16 where the
    /// country of residence requires it).
    pub date_of_birth: Option<String>,
}

impl Command for CreateAccountCommand {}

impl Validate for CreateAccountCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        let mut violations = Vec::new();

        if self.identity_id.trim().is_empty() {
            violations.push(FieldViolation::new(
                "identity_id",
                "VAL-2001",
                "identity_id must not be empty",
            ));
        }

        let email = self.email.trim();
        if email.is_empty() {
            violations.push(FieldViolation::new("email", "VAL-2002", "email must not be empty"));
        } else if !email.contains('@') || email.len() > 254 {
            violations.push(FieldViolation::new("email", "VAL-2003", "email format is invalid"));
        }

        if let Some(phone) = &self.phone
            && (!phone.starts_with('+') || phone.len() < 7) {
                violations.push(FieldViolation::new(
                    "phone",
                    "VAL-2004",
                    "phone must be in E.164 format (e.g. +12025551234)",
                ));
            }

        if let Some(country) = &self.country_of_residence
            && (country.len() != 2 || !country.chars().all(|c| c.is_ascii_alphabetic())) {
                violations.push(FieldViolation::new(
                    "country_of_residence",
                    "VAL-2005",
                    "country_of_residence must be an ISO 3166-1 alpha-2 code",
                ));
            }

        if let Some(dob) = &self.date_of_birth
            && chrono::NaiveDate::parse_from_str(dob, "%Y-%m-%d").is_err()
        {
            violations.push(FieldViolation::new(
                "date_of_birth",
                "VAL-2006",
                "date_of_birth must be an ISO 8601 date (YYYY-MM-DD)",
            ));
        }

        if violations.is_empty() { Ok(()) } else { Err(violations) }
    }
}

/// Registers a new account, enforcing email and identity-ID uniqueness.
pub struct CreateAccountHandler {
    repo: Arc<dyn AccountRepository>,
}

impl CreateAccountHandler {
    pub fn new(repo: Arc<dyn AccountRepository>) -> Self {
        Self { repo }
    }
}

impl CommandHandler<CreateAccountCommand> for CreateAccountHandler {
    type Error = AccountError;

    async fn handle(
        &self,
        envelope: Envelope<CreateAccountCommand>,
    ) -> Result<(), Self::Error> {
        let cmd = &envelope.payload;

        let identity_id = IdentityId::new(cmd.identity_id.trim().to_owned())?;
        let email = EmailAddress::new(cmd.email.trim())?;

        if self.repo.exists_by_identity_id(&identity_id).await? {
            return Err(AccountError::IdentityAlreadyRegistered {
                identity_id: cmd.identity_id.clone(),
            });
        }
        if self.repo.exists_by_email(&email).await? {
            return Err(AccountError::EmailAlreadyRegistered {
                email: cmd.email.clone(),
            });
        }

        let phone = cmd.phone.as_deref().map(PhoneNumber::new).transpose()?;
        let password_hash = cmd.password_hash.as_deref().map(PasswordHash::from_hash);

        let country_of_residence = cmd
            .country_of_residence
            .as_deref()
            .map(CountryCode::new)
            .transpose()?;

        let date_of_birth = cmd
            .date_of_birth
            .as_deref()
            .and_then(|s| chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").ok());
        if let Some(dob) = date_of_birth {
            check_date_of_birth(dob, country_of_residence.as_ref(), chrono::Utc::now().date_naive())?;
        }

        let role = cmd
            .role
            .as_deref()
            .filter(|s| !s.trim().is_empty())
            .map(|s| {
                AccountRole::try_from(s).map_err(|_| AccountError::InvalidAccountRole(s.to_owned()))
            })
            .transpose()?
            .unwrap_or(AccountRole::User);

        let created_by = cmd
            .created_by
            .as_deref()
            .map(|s| {
                s.parse::<uuid::Uuid>()
                    .map(AccountId::from_uuid)
                    .map_err(|_| AccountError::DomainViolation {
                        field: "created_by".into(),
                        message: "invalid UUID format".into(),
                    })
            })
            .transpose()?;

        let params = AccountCreateParams {
            identity_id,
            email,
            phone,
            password_hash,
            role,
            country_of_residence,
            created_by,
            date_of_birth,
            correlation_id: envelope.correlation_id,
        };

        let account = Account::create(params);
        self.repo.save(&account).await
    }
}
