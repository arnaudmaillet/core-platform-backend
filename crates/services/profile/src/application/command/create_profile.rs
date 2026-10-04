use std::sync::Arc;

use cqrs::{Command, CommandHandler, Envelope};
use validate_core::{FieldViolation, Validate};

use crate::application::port::{EventPublisher, ProfileCache, ProfileRepository};
use crate::domain::aggregate::{Profile, ProfileCreateParams};
use crate::domain::value_object::{
    AccountId, AvatarUrl, BannerUrl, Bio, DisplayName, Handle, InteractionSettings, Locale, ProfileKind,
    ProfileVisibility,
};
use crate::error::ProfileError;

#[derive(Debug, Clone)]
pub struct CreateProfileCommand {
    pub account_id: String,
    pub handle: String,
    pub display_name: String,
    pub bio: Option<String>,
    pub avatar_url: Option<String>,
    pub banner_url: Option<String>,
    pub profile_kind: String,
    pub locale: String,
    /// The creator is 13–17 (the token's `age` claim): the profile starts
    /// private, with the teen interaction defaults.
    pub minor: bool,
}

impl Command for CreateProfileCommand {}

impl Validate for CreateProfileCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        let mut violations = Vec::new();

        if self.account_id.trim().is_empty() {
            violations.push(FieldViolation::new("account_id", "VAL-3001", "account_id must not be empty"));
        }
        if self.handle.trim().is_empty() {
            violations.push(FieldViolation::new("handle", "VAL-3002", "handle must not be empty"));
        }
        if self.display_name.trim().is_empty() {
            violations.push(FieldViolation::new("display_name", "VAL-3003", "display_name must not be empty"));
        }
        if self.locale.trim().is_empty() {
            violations.push(FieldViolation::new("locale", "VAL-3004", "locale must not be empty"));
        }

        if violations.is_empty() { Ok(()) } else { Err(violations) }
    }
}

pub struct CreateProfileHandler {
    repo: Arc<dyn ProfileRepository>,
    cache: Arc<dyn ProfileCache>,
    publisher: Arc<dyn EventPublisher>,
}

impl CreateProfileHandler {
    pub fn new(repo: Arc<dyn ProfileRepository>, cache: Arc<dyn ProfileCache>, publisher: Arc<dyn EventPublisher>) -> Self {
        Self { repo, cache, publisher }
    }
}

impl CommandHandler<CreateProfileCommand> for CreateProfileHandler {
    type Error = ProfileError;

    async fn handle(&self, envelope: Envelope<CreateProfileCommand>) -> Result<(), Self::Error> {
        let cmd = &envelope.payload;

        let account_id = AccountId::try_from(cmd.account_id.as_str())?;
        let handle = Handle::new(&cmd.handle)?;
        let display_name = DisplayName::new(&cmd.display_name)?;
        let bio = cmd.bio.as_deref().map(Bio::new).transpose()?;
        let avatar_url = cmd.avatar_url.as_deref().map(AvatarUrl::new).transpose()?;
        let banner_url = cmd.banner_url.as_deref().map(BannerUrl::new).transpose()?;
        let profile_kind = ProfileKind::try_from(cmd.profile_kind.as_str())?;
        let locale = Locale::new(&cmd.locale)?;

        if !self.repo.handle_is_available(&handle).await? {
            return Err(ProfileError::HandleAlreadyTaken { handle: handle.as_str().to_owned() });
        }

        let mut profile = Profile::create(ProfileCreateParams {
            account_id,
            handle: handle.clone(),
            display_name,
            bio,
            avatar_url,
            banner_url,
            profile_kind,
            locale,
            visibility: if cmd.minor { ProfileVisibility::Private } else { ProfileVisibility::Public },
            interaction: if cmd.minor { InteractionSettings::teen() } else { InteractionSettings::default() },
            correlation_id: envelope.correlation_id,
        });

        self.repo.save(&profile).await?;
        self.repo.save_account_index(&profile).await?;

        // LWT claim — if this returns false another concurrent create raced us.
        // The profile row is already written; we must surface the conflict.
        let claimed = self.repo.claim_handle(&handle, profile.id(), account_id).await?;
        if !claimed {
            return Err(ProfileError::HandleAlreadyTaken { handle: handle.as_str().to_owned() });
        }

        // Publish only after the claim succeeds — a lost race must not emit a
        // phantom ProfileCreated.
        for event in profile.drain_events() {
            self.publisher.publish(&event).await?;
        }

        let _ = self.cache.invalidate_account_profiles(&account_id).await;

        Ok(())
    }
}
