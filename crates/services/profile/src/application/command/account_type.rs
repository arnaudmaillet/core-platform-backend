//! Account type and verification requests (#668).

use std::sync::Arc;

use chrono::Utc;
use cqrs::{Command, CommandHandler, Envelope};
use validate_core::{FieldViolation, Validate};

use crate::application::port::{EventPublisher, ProfileCache, ProfileRepository, VerificationStore};
use crate::domain::entity::{VerificationRequest, VerificationStatus};
use crate::domain::value_object::{BusinessInfo, ProfileId, ProfileKind, VerificationKind};
use crate::error::ProfileError;

fn profile_id_present(id: &str) -> Result<(), Vec<FieldViolation>> {
    if id.trim().is_empty() {
        return Err(vec![FieldViolation::new("profile_id", "VAL-3061", "profile_id must not be empty")]);
    }
    Ok(())
}

/// The owner switches between personal, professional (creator) and brand
/// (business); a brand may carry a public business card.
#[derive(Debug, Clone)]
pub struct SetAccountTypeCommand {
    pub profile_id: String,
    pub kind:       ProfileKind,
    pub business:   Option<BusinessInfo>,
}

impl Command for SetAccountTypeCommand {}

impl Validate for SetAccountTypeCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        profile_id_present(&self.profile_id)
    }
}

pub struct SetAccountTypeHandler {
    pub repo:      Arc<dyn ProfileRepository>,
    pub cache:     Arc<dyn ProfileCache>,
    pub publisher: Arc<dyn EventPublisher>,
}

impl CommandHandler<SetAccountTypeCommand> for SetAccountTypeHandler {
    type Error = ProfileError;

    async fn handle(&self, envelope: Envelope<SetAccountTypeCommand>) -> Result<(), ProfileError> {
        let cmd = &envelope.payload;
        let id = ProfileId::try_from(cmd.profile_id.as_str())?;
        let mut profile = self
            .repo
            .find_by_id(&id)
            .await?
            .ok_or_else(|| ProfileError::ProfileNotFound { id: cmd.profile_id.clone() })?;
        if !profile.set_account_type(cmd.kind, cmd.business.clone(), envelope.correlation_id)? {
            return Ok(());
        }
        self.repo.save(&profile).await?;
        // The account listing carries the kind too.
        self.repo.save_account_index(&profile).await?;
        for event in profile.drain_events() {
            self.publisher.publish(&event).await?;
        }
        let _ = self.cache.invalidate_by_id(&id).await;
        let _ = self.cache.invalidate_account_profiles(&profile.account_id()).await;
        Ok(())
    }
}

/// The owner asks for a verification badge, with supporting documents.
#[derive(Debug, Clone)]
pub struct RequestVerificationCommand {
    pub profile_id: String,
    pub category:   VerificationKind,
    pub documents:  Vec<String>,
}

impl Command for RequestVerificationCommand {}

impl Validate for RequestVerificationCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        profile_id_present(&self.profile_id)
    }
}

pub struct RequestVerificationHandler {
    pub repo:          Arc<dyn ProfileRepository>,
    pub verifications: Arc<dyn VerificationStore>,
}

impl CommandHandler<RequestVerificationCommand> for RequestVerificationHandler {
    type Error = ProfileError;

    async fn handle(&self, envelope: Envelope<RequestVerificationCommand>) -> Result<(), ProfileError> {
        let cmd = &envelope.payload;
        let id = ProfileId::try_from(cmd.profile_id.as_str())?;
        let profile = self
            .repo
            .find_by_id(&id)
            .await?
            .ok_or_else(|| ProfileError::ProfileNotFound { id: cmd.profile_id.clone() })?;
        if profile.verified() {
            return Err(ProfileError::ProfileAlreadyVerified);
        }
        if self.verifications.get(&id).await?.is_some_and(|r| r.status == VerificationStatus::Pending) {
            return Err(ProfileError::VerificationPending);
        }
        let request = VerificationRequest::submit(cmd.category, cmd.documents.clone(), Utc::now())?;
        self.verifications.put(&id, &request).await
    }
}

/// Staff decide a pending request: approval verifies the profile (the admin
/// `VerifyProfile` path); a rejection carries the reason the owner sees.
#[derive(Debug, Clone)]
pub struct DecideVerificationCommand {
    pub profile_id: String,
    pub approve:    bool,
    pub reason:     Option<String>,
}

impl Command for DecideVerificationCommand {}

impl Validate for DecideVerificationCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        profile_id_present(&self.profile_id)
    }
}

pub struct DecideVerificationHandler {
    pub repo:          Arc<dyn ProfileRepository>,
    pub cache:         Arc<dyn ProfileCache>,
    pub publisher:     Arc<dyn EventPublisher>,
    pub verifications: Arc<dyn VerificationStore>,
}

impl CommandHandler<DecideVerificationCommand> for DecideVerificationHandler {
    type Error = ProfileError;

    async fn handle(&self, envelope: Envelope<DecideVerificationCommand>) -> Result<(), ProfileError> {
        let cmd = &envelope.payload;
        let id = ProfileId::try_from(cmd.profile_id.as_str())?;
        let mut request = self.verifications.get(&id).await?.ok_or(ProfileError::NoPendingVerification)?;
        request.decide(cmd.approve, cmd.reason.clone(), Utc::now())?;

        if cmd.approve {
            let mut profile = self
                .repo
                .find_by_id(&id)
                .await?
                .ok_or_else(|| ProfileError::ProfileNotFound { id: cmd.profile_id.clone() })?;
            match profile.verify(request.category, envelope.correlation_id) {
                // Verified meanwhile (the admin path): the request is approved all the same.
                Ok(()) | Err(ProfileError::ProfileAlreadyVerified) => {}
                Err(e) => return Err(e),
            }
            if profile.has_pending_events() {
                self.repo.save(&profile).await?;
                for event in profile.drain_events() {
                    self.publisher.publish(&event).await?;
                }
                let _ = self.cache.invalidate_by_id(&id).await;
            }
        }
        self.verifications.put(&id, &request).await
    }
}
