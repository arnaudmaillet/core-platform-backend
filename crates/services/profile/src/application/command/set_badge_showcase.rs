//! Picks the badges a profile shows (#912): each one its account earned (the
//! wallet says so, with when), in order, at most three.

use std::sync::Arc;

use cqrs::{Command, CommandHandler, Envelope};
use validate_core::{FieldViolation, Validate};

use crate::application::port::{EventPublisher, GemWallet, ProfileCache, ProfileRepository};
use crate::domain::aggregate::ShowcasedBadge;
use crate::domain::value_object::ProfileId;
use crate::error::ProfileError;

#[derive(Debug, Clone, Default)]
pub struct SetBadgeShowcaseCommand {
    pub profile_id: String,
    /// In order; empty: show none.
    pub badge_ids:  Vec<String>,
}

impl Command for SetBadgeShowcaseCommand {}

impl Validate for SetBadgeShowcaseCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        if self.profile_id.trim().is_empty() {
            return Err(vec![FieldViolation::new("profile_id", "VAL-3020", "profile_id must not be empty")]);
        }
        Ok(())
    }
}

pub struct SetBadgeShowcaseHandler {
    pub repo:      Arc<dyn ProfileRepository>,
    pub cache:     Arc<dyn ProfileCache>,
    pub publisher: Arc<dyn EventPublisher>,
    /// Says which badges the account earned; `None`: only an empty showcase
    /// can be set (`WalletUnavailable` otherwise).
    pub wallet:    Option<Arc<dyn GemWallet>>,
}

impl CommandHandler<SetBadgeShowcaseCommand> for SetBadgeShowcaseHandler {
    type Error = ProfileError;

    async fn handle(&self, envelope: Envelope<SetBadgeShowcaseCommand>) -> Result<(), Self::Error> {
        let cmd = &envelope.payload;
        let id = ProfileId::try_from(cmd.profile_id.as_str())?;
        let mut profile = self
            .repo
            .find_by_id(&id)
            .await?
            .ok_or_else(|| ProfileError::ProfileNotFound { id: cmd.profile_id.clone() })?;
        let showcase = if cmd.badge_ids.is_empty() {
            Vec::new()
        } else {
            let wallet = self.wallet.as_ref().ok_or_else(|| ProfileError::WalletUnavailable { reason: "not configured".into() })?;
            let earned = wallet.badges(&profile.account_id()).await?;
            cmd.badge_ids
                .iter()
                .map(|badge| {
                    earned
                        .iter()
                        .find(|(earned, _)| earned == badge)
                        .map(|(badge_id, awarded_at)| ShowcasedBadge { badge_id: badge_id.clone(), awarded_at: *awarded_at })
                        .ok_or_else(|| ProfileError::ItemNotOwned { item: badge.clone() })
                })
                .collect::<Result<Vec<_>, _>>()?
        };
        profile.set_badge_showcase(showcase, envelope.correlation_id)?;
        self.repo.save(&profile).await?;
        for event in profile.drain_events() {
            self.publisher.publish(&event).await?;
        }
        let _ = self.cache.invalidate_by_id(&id).await;
        let _ = self.cache.invalidate_handle(profile.handle().as_str()).await;
        Ok(())
    }
}
