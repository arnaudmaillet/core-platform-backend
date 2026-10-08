//! Family supervision floors (#670 part 2), from account's
//! `supervision_limits_set` / `supervision_limits_cleared`: the floors are
//! kept per account, and every profile of the account is tightened to them at
//! once (private, message / comment audiences, hidden from search). Lifted,
//! the settings keep their values, unlocked. Idempotent (a redelivery changes
//! nothing more).

use std::sync::Arc;

use cqrs::{Command, CommandHandler, Envelope};
use validate_core::{FieldViolation, Validate};

use crate::application::port::{EventPublisher, ProfileCache, ProfileRepository, SupervisionFloors};
use crate::domain::value_object::{AccountId, ProfileStatus, SupervisionFloor};
use crate::error::ProfileError;

const PAGE: i32 = 100;

#[derive(Debug, Clone)]
pub struct ApplySupervisionFloorCommand {
    pub account_id: String,
    /// `None`: lifted.
    pub floor:      Option<SupervisionFloor>,
}

impl Command for ApplySupervisionFloorCommand {}

impl Validate for ApplySupervisionFloorCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        if self.account_id.trim().is_empty() {
            return Err(vec![FieldViolation::new("account_id", "VAL-3080", "account_id must not be empty")]);
        }
        Ok(())
    }
}

pub struct ApplySupervisionFloorHandler {
    pub repo:      Arc<dyn ProfileRepository>,
    pub cache:     Arc<dyn ProfileCache>,
    pub publisher: Arc<dyn EventPublisher>,
    pub floors:    Arc<dyn SupervisionFloors>,
}

impl ApplySupervisionFloorHandler {
    async fn save(&self, profile: &mut crate::domain::aggregate::Profile) -> Result<(), ProfileError> {
        self.repo.save(profile).await?;
        for event in profile.drain_events() {
            self.publisher.publish(&event).await?;
        }
        Ok(())
    }
}

impl CommandHandler<ApplySupervisionFloorCommand> for ApplySupervisionFloorHandler {
    type Error = ProfileError;

    async fn handle(&self, envelope: Envelope<ApplySupervisionFloorCommand>) -> Result<(), ProfileError> {
        let account = AccountId::try_from(envelope.payload.account_id.as_str())?;
        let Some(floor) = envelope.payload.floor else {
            return self.floors.clear(&account).await;
        };
        // Kept first: a setting changed meanwhile is already checked against it.
        self.floors.put(&account, &floor).await?;
        let mut token: Option<String> = None;
        loop {
            let (page, next) = self.repo.list_by_account(&account, PAGE, token.as_deref()).await?;
            for summary in page {
                let Some(mut profile) = self.repo.find_by_id(&summary.profile_id).await? else { continue };
                if profile.status() == ProfileStatus::Deleted {
                    continue;
                }
                // One change per save (the repository's version check).
                let mut changed = false;
                let visibility = floor.tighten_visibility(profile.visibility());
                if visibility != profile.visibility() {
                    profile.set_visibility(visibility, envelope.correlation_id)?;
                    self.save(&mut profile).await?;
                    changed = true;
                }
                let current = profile.interaction();
                if profile.set_interaction_settings(floor.tighten_interaction(current), envelope.correlation_id)? {
                    self.save(&mut profile).await?;
                    changed = true;
                }
                let current = profile.discovery();
                if profile.set_discovery_settings(floor.tighten_discovery(current), envelope.correlation_id)? {
                    self.save(&mut profile).await?;
                    changed = true;
                }
                if !changed {
                    continue;
                }
                let _ = self.cache.invalidate_by_id(&profile.id()).await;
            }
            match next {
                Some(t) if !t.is_empty() => token = Some(t),
                _ => break,
            }
        }
        let _ = self.cache.invalidate_account_profiles(&account).await;
        Ok(())
    }
}
