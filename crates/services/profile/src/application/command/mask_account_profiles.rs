//! Account lifecycle → every profile of the account. A suspension or deletion
//! hides each profile the account owns; a reactivation restores the ones the
//! suspension hid. Dispatched by the `account.v1.events` consumer.

use std::sync::Arc;

use cqrs::{Command, CommandHandler, Envelope};
use uuid::Uuid;
use validate_core::{FieldViolation, Validate};

use crate::application::port::{EventPublisher, ProfileCache, ProfileRepository};
use crate::domain::aggregate::Profile;
use crate::domain::value_object::{AccountId, MaskingReason, ProfileStatus};
use crate::error::ProfileError;

/// Page size when walking an account's profiles.
const PAGE: i32 = 100;

/// Hides every active profile of `account_id`.
#[derive(Debug, Clone)]
pub struct HideAccountProfilesCommand {
    pub account_id:        String,
    /// `account_suspended` or `account_deleted`.
    pub masking_reason:    String,
    pub suspension_reason: Option<String>,
}

impl Command for HideAccountProfilesCommand {}

impl Validate for HideAccountProfilesCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        let mut violations = Vec::new();
        if self.account_id.trim().is_empty() {
            violations.push(FieldViolation::new("account_id", "VAL-3080", "account_id must not be empty"));
        }
        if self.masking_reason.trim().is_empty() {
            violations.push(FieldViolation::new("masking_reason", "VAL-3071", "masking_reason must not be empty"));
        }
        if violations.is_empty() { Ok(()) } else { Err(violations) }
    }
}

/// Restores the profiles of `account_id` that an account suspension hid.
/// Profiles hidden for another reason (a content-policy violation, a deleted
/// account) stay hidden.
#[derive(Debug, Clone)]
pub struct RestoreAccountProfilesCommand {
    pub account_id: String,
}

impl Command for RestoreAccountProfilesCommand {}

impl Validate for RestoreAccountProfilesCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        if self.account_id.trim().is_empty() {
            return Err(vec![FieldViolation::new("account_id", "VAL-3080", "account_id must not be empty")]);
        }
        Ok(())
    }
}

/// Handles both commands: walks the account's profiles and applies the
/// transition to each one it concerns. Idempotent under redelivery: a profile
/// already in the target state is skipped, so a retry after a partial walk
/// finishes the rest.
pub struct MaskAccountProfilesHandler {
    repo:      Arc<dyn ProfileRepository>,
    cache:     Arc<dyn ProfileCache>,
    publisher: Arc<dyn EventPublisher>,
}

impl MaskAccountProfilesHandler {
    pub fn new(
        repo: Arc<dyn ProfileRepository>,
        cache: Arc<dyn ProfileCache>,
        publisher: Arc<dyn EventPublisher>,
    ) -> Self {
        Self { repo, cache, publisher }
    }

    /// Applies `transition` to every profile of `account_id`; it returns whether
    /// it changed the profile.
    async fn for_each_profile(
        &self,
        account_id: &AccountId,
        mut transition: impl FnMut(&mut Profile) -> Result<bool, ProfileError>,
    ) -> Result<(), ProfileError> {
        let mut token: Option<String> = None;
        loop {
            let (page, next) = self.repo.list_by_account(account_id, PAGE, token.as_deref()).await?;
            for summary in page {
                let Some(mut profile) = self.repo.find_by_id(&summary.profile_id).await? else {
                    continue;
                };
                if !transition(&mut profile)? {
                    continue;
                }
                self.repo.save(&profile).await?;
                for event in profile.drain_events() {
                    self.publisher.publish(&event).await?;
                }
                let _ = self.cache.invalidate_by_id(&profile.id()).await;
            }
            match next {
                Some(t) if !t.is_empty() => token = Some(t),
                _ => break,
            }
        }
        let _ = self.cache.invalidate_account_profiles(account_id).await;
        Ok(())
    }
}

impl CommandHandler<HideAccountProfilesCommand> for MaskAccountProfilesHandler {
    type Error = ProfileError;

    async fn handle(&self, envelope: Envelope<HideAccountProfilesCommand>) -> Result<(), Self::Error> {
        let cmd = &envelope.payload;
        let account_id = AccountId::try_from(cmd.account_id.as_str())?;
        let reason = MaskingReason::try_from(cmd.masking_reason.as_str())?;
        let correlation_id: Uuid = envelope.correlation_id;

        self.for_each_profile(&account_id, |profile| {
            if profile.status() != ProfileStatus::Active {
                return Ok(false); // already hidden or deleted
            }
            profile.hide(reason, cmd.suspension_reason.clone(), correlation_id)?;
            Ok(true)
        })
        .await
    }
}

impl CommandHandler<RestoreAccountProfilesCommand> for MaskAccountProfilesHandler {
    type Error = ProfileError;

    async fn handle(&self, envelope: Envelope<RestoreAccountProfilesCommand>) -> Result<(), Self::Error> {
        let account_id = AccountId::try_from(envelope.payload.account_id.as_str())?;
        let correlation_id = envelope.correlation_id;

        self.for_each_profile(&account_id, |profile| {
            let hidden_by_suspension = profile.status() == ProfileStatus::Hidden
                && profile.masking_reason() == Some(MaskingReason::AccountSuspended);
            if !hidden_by_suspension {
                return Ok(false);
            }
            profile.restore(correlation_id)?;
            Ok(true)
        })
        .await
    }
}
