//! Wears an avatar frame, or none (#913): a catalogue item the profile's
//! account owns (the wallet says so), equipped per profile.

use std::sync::Arc;

use cqrs::{Command, CommandHandler, Envelope};
use validate_core::{FieldViolation, Validate};

use crate::application::port::{EventPublisher, GemWallet, ProfileCache, ProfileRepository};
use crate::domain::value_object::ProfileId;
use crate::error::ProfileError;

#[derive(Debug, Clone, Default)]
pub struct EquipAvatarFrameCommand {
    pub profile_id: String,
    /// `None`: wear none.
    pub frame_id:   Option<String>,
}

impl Command for EquipAvatarFrameCommand {}

impl Validate for EquipAvatarFrameCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        if self.profile_id.trim().is_empty() {
            return Err(vec![FieldViolation::new("profile_id", "VAL-3020", "profile_id must not be empty")]);
        }
        Ok(())
    }
}

pub struct EquipAvatarFrameHandler {
    pub repo:      Arc<dyn ProfileRepository>,
    pub cache:     Arc<dyn ProfileCache>,
    pub publisher: Arc<dyn EventPublisher>,
    /// Says what the account owns; `None`: a frame cannot be worn
    /// (`WalletUnavailable`), only taken off.
    pub wallet:    Option<Arc<dyn GemWallet>>,
}

impl CommandHandler<EquipAvatarFrameCommand> for EquipAvatarFrameHandler {
    type Error = ProfileError;

    async fn handle(&self, envelope: Envelope<EquipAvatarFrameCommand>) -> Result<(), Self::Error> {
        let cmd = &envelope.payload;
        let id = ProfileId::try_from(cmd.profile_id.as_str())?;
        let mut profile = self
            .repo
            .find_by_id(&id)
            .await?
            .ok_or_else(|| ProfileError::ProfileNotFound { id: cmd.profile_id.clone() })?;
        let frame = cmd.frame_id.clone().filter(|f| !f.trim().is_empty());
        if profile.avatar_frame_id() == frame.as_deref() {
            return Ok(());
        }
        if let Some(frame) = &frame {
            let wallet = self.wallet.as_ref().ok_or_else(|| ProfileError::WalletUnavailable { reason: "not configured".into() })?;
            if !crate::domain::aggregate::is_frame_id(frame) {
                return Err(ProfileError::DomainViolation { field: "frame_id".into(), message: format!("'{frame}' is not an avatar frame") });
            }
            if !wallet.owns(&profile.account_id(), frame).await? {
                return Err(ProfileError::ItemNotOwned { item: frame.clone() });
            }
        }
        profile.equip_avatar_frame(frame, envelope.correlation_id)?;
        self.repo.save(&profile).await?;
        // The account's profile list carries the frame too.
        self.repo.save_account_index(&profile).await?;

        for event in profile.drain_events() {
            self.publisher.publish(&event).await?;
        }
        let _ = self.cache.invalidate_by_id(&id).await;
        let _ = self.cache.invalidate_handle(profile.handle().as_str()).await;
        let _ = self.cache.invalidate_account_profiles(&profile.account_id()).await;
        Ok(())
    }
}
