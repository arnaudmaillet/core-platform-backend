use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::domain::entity::ProfileLink;
use crate::domain::event::{
    DomainEvent, HandleChanged, ProfileCreated, ProfileDeleted, ProfileHidden, ProfileRestored,
    CommentFiltersChanged, DiscoverySettingsChanged, InteractionSettingsChanged, TabSettingsChanged, LocationSettingsChanged, ProfileUpdated, ProfileVerified, TierChanged, VisibilityChanged,
};
use crate::domain::value_object::{
    AccountId, AvatarUrl, BannerUrl, Bio, CommentFilters, DisplayName, DiscoverySettings, Handle, InteractionSettings,
    Locale, LocationSettings, TabSettings,
    MaskingReason, ProfileId, ProfileKind, ProfileStatus, ProfileVisibility, VerificationKind,
    WebsiteUrl,
};
use crate::error::ProfileError;

pub struct ProfileCreateParams {
    pub account_id: AccountId,
    pub handle: Handle,
    pub display_name: DisplayName,
    pub bio: Option<Bio>,
    pub avatar_url: Option<AvatarUrl>,
    pub banner_url: Option<BannerUrl>,
    pub profile_kind: ProfileKind,
    pub locale: Locale,
    /// `Private` for a 13–17 holder (the teen default); `Public` otherwise.
    pub visibility: ProfileVisibility,
    /// [`InteractionSettings::teen`] for a 13–17 holder; the defaults otherwise.
    pub interaction: InteractionSettings,
    /// [`LocationSettings::teen`] (ghost) for a 13–17 holder; the defaults otherwise.
    pub location: LocationSettings,
    /// [`DiscoverySettings::teen`] for a 13–17 holder; the defaults otherwise.
    pub discovery: DiscoverySettings,
    pub correlation_id: Uuid,
}

/// The Profile aggregate root.
///
/// Owns all public-facing identity metadata for a single public identity.
/// One AccountId may own multiple Profile instances (1-to-N relationship).
/// Social graph state (followers, friends) is strictly out of scope.
///
/// # Invariants
///
/// - `profile_kind` is immutable after creation.
/// - Status transitions are gated by [`ProfileStatus::can_transition_to`].
/// - `version` is incremented on every write cycle.
/// - `verified = false` implies `verification_kind = None`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Profile {
    id: ProfileId,
    account_id: AccountId,
    version: i64,
    handle: Handle,
    display_name: DisplayName,
    bio: Option<Bio>,
    avatar_url: Option<AvatarUrl>,
    banner_url: Option<BannerUrl>,
    website_url: Option<WebsiteUrl>,
    custom_links: Vec<ProfileLink>,
    profile_kind: ProfileKind,
    visibility: ProfileVisibility,
    /// Who may comment / mention / message, downloads, like counts.
    #[serde(default)]
    interaction: InteractionSettings,
    /// Ghost mode and location precision (geo-discovery applies them).
    #[serde(default)]
    location: LocationSettings,
    /// Activity status, read receipts and how people can find the profile.
    #[serde(default)]
    discovery: DiscoverySettings,
    /// Hidden words and the offensive-comment filter (comment applies them).
    #[serde(default)]
    comment_filters: CommentFilters,
    /// Post history window and tab visibility (post applies the window).
    #[serde(default)]
    tab_settings: TabSettings,
    verified: bool,
    verification_kind: Option<VerificationKind>,
    /// Author tier (0=Standard, 1=Premium, 2=Vip), denormalized from
    /// `social-graph.author_tier_changed`. Profile is the tier owner: it persists
    /// it and re-emits it on `profile.v1.events` for `post` to stamp onto posts.
    tier: u8,
    locale: Locale,
    timezone: Option<String>,
    status: ProfileStatus,
    suspension_reason: Option<String>,
    masked_at: Option<DateTime<Utc>>,
    masking_reason: Option<MaskingReason>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    deleted_at: Option<DateTime<Utc>>,
    #[serde(skip)]
    pending_events: Vec<DomainEvent>,
}

impl Profile {
    // ─── Constructors ───────────────────────────────────────────────────────

    pub fn create(params: ProfileCreateParams) -> Self {
        let id = ProfileId::new();
        let now = Utc::now();

        let event = DomainEvent::ProfileCreated(ProfileCreated {
            profile_id: id,
            account_id: params.account_id,
            handle: params.handle.clone(),
            profile_kind: params.profile_kind,
            occurred_at: now,
            correlation_id: params.correlation_id,
        });

        let mut profile = Self {
            id,
            account_id: params.account_id,
            version: 0,
            handle: params.handle,
            display_name: params.display_name,
            bio: params.bio,
            avatar_url: params.avatar_url,
            banner_url: params.banner_url,
            website_url: None,
            custom_links: Vec::new(),
            profile_kind: params.profile_kind,
            visibility: params.visibility,
            interaction: params.interaction,
            location: params.location,
            discovery: params.discovery,
            comment_filters: CommentFilters::default(),
            tab_settings: TabSettings::default(),
            verified: false,
            verification_kind: None,
            tier: 0,
            locale: params.locale,
            timezone: None,
            status: ProfileStatus::Active,
            suspension_reason: None,
            masked_at: None,
            masking_reason: None,
            created_at: now,
            updated_at: now,
            deleted_at: None,
            pending_events: Vec::new(),
        };
        profile.pending_events.push(event);
        // ProfileCreated carries no visibility; projections (social-graph's
        // audience, search) assume public until told otherwise, so a profile
        // born private says so right away.
        if params.interaction != InteractionSettings::default() {
            profile.pending_events.push(DomainEvent::InteractionSettingsChanged(
                InteractionSettingsChanged {
                    profile_id: id,
                    settings: params.interaction,
                    occurred_at: now,
                    correlation_id: params.correlation_id,
                },
            ));
        }
        if params.location != LocationSettings::default() {
            profile.pending_events.push(DomainEvent::LocationSettingsChanged(LocationSettingsChanged {
                profile_id: id,
                settings: params.location,
                occurred_at: now,
                correlation_id: params.correlation_id,
            }));
        }
        if params.discovery != DiscoverySettings::default() {
            profile.pending_events.push(DomainEvent::DiscoverySettingsChanged(DiscoverySettingsChanged {
                profile_id: id,
                settings: params.discovery,
                occurred_at: now,
                correlation_id: params.correlation_id,
            }));
        }
        if params.visibility == ProfileVisibility::Private {
            profile.pending_events.push(DomainEvent::VisibilityChanged(VisibilityChanged {
                profile_id: id,
                visibility: ProfileVisibility::Private,
                occurred_at: now,
                correlation_id: params.correlation_id,
            }));
        }
        profile
    }

    #[allow(clippy::too_many_arguments)]
    pub fn reconstitute(
        id: ProfileId,
        account_id: AccountId,
        version: i64,
        handle: Handle,
        display_name: DisplayName,
        bio: Option<Bio>,
        avatar_url: Option<AvatarUrl>,
        banner_url: Option<BannerUrl>,
        website_url: Option<WebsiteUrl>,
        custom_links: Vec<ProfileLink>,
        profile_kind: ProfileKind,
        visibility: ProfileVisibility,
        verified: bool,
        verification_kind: Option<VerificationKind>,
        tier: u8,
        locale: Locale,
        timezone: Option<String>,
        status: ProfileStatus,
        suspension_reason: Option<String>,
        masked_at: Option<DateTime<Utc>>,
        masking_reason: Option<MaskingReason>,
        created_at: DateTime<Utc>,
        updated_at: DateTime<Utc>,
        deleted_at: Option<DateTime<Utc>>,
    ) -> Self {
        Self {
            id,
            account_id,
            version,
            handle,
            display_name,
            bio,
            avatar_url,
            banner_url,
            website_url,
            custom_links,
            profile_kind,
            visibility,
            interaction: InteractionSettings::default(),
            location: LocationSettings::default(),
            discovery: DiscoverySettings::default(),
            comment_filters: CommentFilters::default(),
            tab_settings: TabSettings::default(),
            verified,
            verification_kind,
            tier,
            locale,
            timezone,
            status,
            suspension_reason,
            masked_at,
            masking_reason,
            created_at,
            updated_at,
            deleted_at,
            pending_events: Vec::new(),
        }
    }

    // ─── Domain Mutations ───────────────────────────────────────────────────

    pub fn update(
        &mut self,
        display_name: Option<DisplayName>,
        bio: Option<Bio>,
        website_url: Option<Option<WebsiteUrl>>,
        locale: Option<Locale>,
        custom_links: Vec<ProfileLink>,
        correlation_id: Uuid,
    ) -> Result<(), ProfileError> {
        self.require_active()?;
        if custom_links.len() > 5 {
            return Err(ProfileError::TooManyCustomLinks { count: custom_links.len() });
        }
        if let Some(dn) = display_name {
            self.display_name = dn;
        }
        if let Some(b) = bio {
            self.bio = if b.is_empty() { None } else { Some(b) };
        }
        if let Some(wu) = website_url {
            self.website_url = wu;
        }
        if let Some(l) = locale {
            self.locale = l;
        }
        self.custom_links = custom_links;
        let now = self.touch_now();
        self.pending_events.push(DomainEvent::ProfileUpdated(ProfileUpdated {
            profile_id: self.id,
            occurred_at: now,
            correlation_id,
        }));
        Ok(())
    }

    /// Returns the old handle so the command handler can tombstone the index entry.
    pub fn change_handle(
        &mut self,
        new_handle: Handle,
        correlation_id: Uuid,
    ) -> Result<Handle, ProfileError> {
        self.require_active()?;
        let old_handle = self.handle.clone();
        self.handle = new_handle.clone();
        let now = self.touch_now();
        self.pending_events.push(DomainEvent::HandleChanged(HandleChanged {
            profile_id: self.id,
            old_handle: old_handle.clone(),
            new_handle,
            occurred_at: now,
            correlation_id,
        }));
        Ok(old_handle)
    }

    pub fn update_avatar(
        &mut self,
        url: Option<AvatarUrl>,
        correlation_id: Uuid,
    ) -> Result<(), ProfileError> {
        self.require_active()?;
        self.avatar_url = url;
        let now = self.touch_now();
        self.pending_events.push(DomainEvent::ProfileUpdated(ProfileUpdated {
            profile_id: self.id,
            occurred_at: now,
            correlation_id,
        }));
        Ok(())
    }

    pub fn update_banner(
        &mut self,
        url: Option<BannerUrl>,
        correlation_id: Uuid,
    ) -> Result<(), ProfileError> {
        self.require_active()?;
        self.banner_url = url;
        let now = self.touch_now();
        self.pending_events.push(DomainEvent::ProfileUpdated(ProfileUpdated {
            profile_id: self.id,
            occurred_at: now,
            correlation_id,
        }));
        Ok(())
    }

    /// Changes who may comment / mention / message, downloads and like counts.
    /// An unchanged value is a no-op (no write, no event).
    pub fn set_interaction_settings(
        &mut self,
        settings: InteractionSettings,
        correlation_id: Uuid,
    ) -> Result<bool, ProfileError> {
        if self.status == ProfileStatus::Deleted {
            return Err(ProfileError::ProfileNotActive {
                current: self.status.as_str().to_owned(),
            });
        }
        if settings == self.interaction {
            return Ok(false);
        }
        self.interaction = settings;
        let now = self.touch_now();
        self.pending_events.push(DomainEvent::InteractionSettingsChanged(InteractionSettingsChanged {
            profile_id: self.id,
            settings,
            occurred_at: now,
            correlation_id,
        }));
        Ok(true)
    }

    /// Changes ghost mode / location precision. Unchanged ⇒ no-op.
    pub fn set_location_settings(
        &mut self,
        settings: LocationSettings,
        correlation_id: Uuid,
    ) -> Result<bool, ProfileError> {
        if self.status == ProfileStatus::Deleted {
            return Err(ProfileError::ProfileNotActive {
                current: self.status.as_str().to_owned(),
            });
        }
        if settings == self.location {
            return Ok(false);
        }
        self.location = settings;
        let now = self.touch_now();
        self.pending_events.push(DomainEvent::LocationSettingsChanged(LocationSettingsChanged {
            profile_id: self.id,
            settings,
            occurred_at: now,
            correlation_id,
        }));
        Ok(true)
    }

    /// Restores the stored location settings (a column added after
    /// [`Self::reconstitute`]'s set).
    pub fn with_location(mut self, settings: LocationSettings) -> Self {
        self.location = settings;
        self
    }

    pub fn location(&self) -> LocationSettings {
        self.location
    }

    /// Changes presence / discoverability. Unchanged ⇒ no-op.
    pub fn set_discovery_settings(
        &mut self,
        settings: DiscoverySettings,
        correlation_id: Uuid,
    ) -> Result<bool, ProfileError> {
        if self.status == ProfileStatus::Deleted {
            return Err(ProfileError::ProfileNotActive {
                current: self.status.as_str().to_owned(),
            });
        }
        if settings == self.discovery {
            return Ok(false);
        }
        self.discovery = settings;
        let now = self.touch_now();
        self.pending_events.push(DomainEvent::DiscoverySettingsChanged(DiscoverySettingsChanged {
            profile_id: self.id,
            settings,
            occurred_at: now,
            correlation_id,
        }));
        Ok(true)
    }

    /// Restores the stored discovery settings (a column added after
    /// [`Self::reconstitute`]'s set).
    pub fn with_discovery(mut self, settings: DiscoverySettings) -> Self {
        self.discovery = settings;
        self
    }

    pub fn discovery(&self) -> DiscoverySettings {
        self.discovery
    }

    /// Replaces the hidden words / offensive filter. Unchanged ⇒ no-op.
    pub fn set_comment_filters(
        &mut self,
        filters: CommentFilters,
        correlation_id: Uuid,
    ) -> Result<bool, ProfileError> {
        if self.status == ProfileStatus::Deleted {
            return Err(ProfileError::ProfileNotActive {
                current: self.status.as_str().to_owned(),
            });
        }
        if filters == self.comment_filters {
            return Ok(false);
        }
        self.comment_filters = filters.clone();
        let now = self.touch_now();
        self.pending_events.push(DomainEvent::CommentFiltersChanged(CommentFiltersChanged {
            profile_id: self.id,
            filters,
            occurred_at: now,
            correlation_id,
        }));
        Ok(true)
    }

    /// Restores the stored comment filters (a column added after
    /// [`Self::reconstitute`]'s set).
    pub fn with_comment_filters(mut self, filters: CommentFilters) -> Self {
        self.comment_filters = filters;
        self
    }

    pub fn comment_filters(&self) -> &CommentFilters {
        &self.comment_filters
    }

    /// Changes the post window / tab visibility. Unchanged ⇒ no-op.
    pub fn set_tab_settings(&mut self, settings: TabSettings, correlation_id: Uuid) -> Result<bool, ProfileError> {
        if self.status == ProfileStatus::Deleted {
            return Err(ProfileError::ProfileNotActive {
                current: self.status.as_str().to_owned(),
            });
        }
        if settings == self.tab_settings {
            return Ok(false);
        }
        self.tab_settings = settings;
        let now = self.touch_now();
        self.pending_events.push(DomainEvent::TabSettingsChanged(TabSettingsChanged {
            profile_id: self.id,
            settings,
            occurred_at: now,
            correlation_id,
        }));
        Ok(true)
    }

    /// Restores the stored tab settings (a column added after
    /// [`Self::reconstitute`]'s set).
    pub fn with_tab_settings(mut self, settings: TabSettings) -> Self {
        self.tab_settings = settings;
        self
    }

    pub fn tab_settings(&self) -> TabSettings {
        self.tab_settings
    }

    /// Restores the stored interaction settings (a column added after
    /// [`Self::reconstitute`]'s set).
    pub fn with_interaction(mut self, settings: InteractionSettings) -> Self {
        self.interaction = settings;
        self
    }

    pub fn interaction(&self) -> InteractionSettings {
        self.interaction
    }

    pub fn set_visibility(
        &mut self,
        v: ProfileVisibility,
        correlation_id: Uuid,
    ) -> Result<(), ProfileError> {
        if self.status == ProfileStatus::Deleted {
            return Err(ProfileError::ProfileNotActive {
                current: self.status.as_str().to_owned(),
            });
        }
        self.visibility = v;
        let now = self.touch_now();
        self.pending_events.push(DomainEvent::VisibilityChanged(VisibilityChanged {
            profile_id: self.id,
            visibility: v,
            occurred_at: now,
            correlation_id,
        }));
        Ok(())
    }

    pub fn verify(
        &mut self,
        kind: VerificationKind,
        correlation_id: Uuid,
    ) -> Result<(), ProfileError> {
        if self.verified {
            return Err(ProfileError::ProfileAlreadyVerified);
        }
        self.verified = true;
        self.verification_kind = Some(kind);
        let now = self.touch_now();
        self.pending_events.push(DomainEvent::ProfileVerified(ProfileVerified {
            profile_id: self.id,
            verification_kind: kind,
            occurred_at: now,
            correlation_id,
        }));
        Ok(())
    }

    pub fn hide(
        &mut self,
        reason: MaskingReason,
        suspension_reason: Option<String>,
        correlation_id: Uuid,
    ) -> Result<(), ProfileError> {
        self.transition_status(ProfileStatus::Hidden)?;
        let now = Utc::now();
        self.masked_at = Some(now);
        self.masking_reason = Some(reason);
        self.suspension_reason = suspension_reason;
        self.touch(now);
        self.pending_events.push(DomainEvent::ProfileHidden(ProfileHidden {
            profile_id: self.id,
            masking_reason: reason,
            occurred_at: now,
            correlation_id,
        }));
        Ok(())
    }

    pub fn restore(&mut self, correlation_id: Uuid) -> Result<(), ProfileError> {
        self.transition_status(ProfileStatus::Active)?;
        self.masked_at = None;
        self.masking_reason = None;
        self.suspension_reason = None;
        let now = self.touch_now();
        self.pending_events.push(DomainEvent::ProfileRestored(ProfileRestored {
            profile_id: self.id,
            occurred_at: now,
            correlation_id,
        }));
        Ok(())
    }

    pub fn delete(&mut self, correlation_id: Uuid) -> Result<(), ProfileError> {
        self.transition_status(ProfileStatus::Deleted)?;
        let now = Utc::now();
        self.deleted_at = Some(now);
        self.touch(now);
        self.pending_events.push(DomainEvent::ProfileDeleted(ProfileDeleted {
            profile_id: self.id,
            handle: self.handle.clone(),
            occurred_at: now,
            correlation_id,
        }));
        Ok(())
    }

    /// Set the author tier (denormalized from `social-graph.author_tier_changed`).
    /// Idempotent: an unchanged tier is a no-op that emits nothing. A new tier is
    /// persisted and re-emitted on `profile.v1.events` (`TierChanged`). Values
    /// above the known taxonomy (`> 2`) are a contract fault.
    pub fn set_tier(&mut self, new_tier: u8, correlation_id: Uuid) -> Result<(), ProfileError> {
        if new_tier > 2 {
            return Err(ProfileError::DomainViolation {
                field: "tier".to_owned(),
                message: format!("unknown author tier {new_tier}"),
            });
        }
        if new_tier == self.tier {
            return Ok(());
        }
        self.tier = new_tier;
        let now = self.touch_now();
        self.pending_events.push(DomainEvent::TierChanged(TierChanged {
            profile_id: self.id,
            tier: new_tier,
            occurred_at: now,
            correlation_id,
        }));
        Ok(())
    }

    // ─── Event Drain ────────────────────────────────────────────────────────

    pub fn drain_events(&mut self) -> Vec<DomainEvent> {
        std::mem::take(&mut self.pending_events)
    }

    // ─── Getters ────────────────────────────────────────────────────────────

    pub fn id(&self) -> ProfileId { self.id }
    pub fn account_id(&self) -> AccountId { self.account_id }
    pub fn version(&self) -> i64 { self.version }
    pub fn tier(&self) -> u8 { self.tier }
    pub fn handle(&self) -> &Handle { &self.handle }
    pub fn display_name(&self) -> &DisplayName { &self.display_name }
    pub fn bio(&self) -> Option<&Bio> { self.bio.as_ref() }
    pub fn avatar_url(&self) -> Option<&AvatarUrl> { self.avatar_url.as_ref() }
    pub fn banner_url(&self) -> Option<&BannerUrl> { self.banner_url.as_ref() }
    pub fn website_url(&self) -> Option<&WebsiteUrl> { self.website_url.as_ref() }
    pub fn custom_links(&self) -> &[ProfileLink] { &self.custom_links }
    pub fn profile_kind(&self) -> ProfileKind { self.profile_kind }
    pub fn visibility(&self) -> ProfileVisibility { self.visibility }
    pub fn verified(&self) -> bool { self.verified }
    pub fn verification_kind(&self) -> Option<VerificationKind> { self.verification_kind }
    pub fn locale(&self) -> &Locale { &self.locale }
    pub fn timezone(&self) -> Option<&str> { self.timezone.as_deref() }
    pub fn status(&self) -> ProfileStatus { self.status }
    pub fn suspension_reason(&self) -> Option<&str> { self.suspension_reason.as_deref() }
    pub fn masked_at(&self) -> Option<DateTime<Utc>> { self.masked_at }
    pub fn masking_reason(&self) -> Option<MaskingReason> { self.masking_reason }
    pub fn created_at(&self) -> DateTime<Utc> { self.created_at }
    pub fn updated_at(&self) -> DateTime<Utc> { self.updated_at }
    pub fn deleted_at(&self) -> Option<DateTime<Utc>> { self.deleted_at }

    // ─── Private Helpers ────────────────────────────────────────────────────

    fn require_active(&self) -> Result<(), ProfileError> {
        if self.status != ProfileStatus::Active {
            return Err(ProfileError::ProfileNotActive {
                current: self.status.as_str().to_owned(),
            });
        }
        Ok(())
    }

    fn transition_status(&mut self, next: ProfileStatus) -> Result<(), ProfileError> {
        if !self.status.can_transition_to(next) {
            return Err(ProfileError::InvalidStatusTransition {
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
    use crate::domain::event::DomainEvent;
    use crate::domain::value_object::{AccountId, Handle, Locale, ProfileKind};

    fn sample_profile() -> Profile {
        let mut p = Profile::create(ProfileCreateParams {
            account_id: AccountId::try_from(uuid::Uuid::now_v7().to_string().as_str()).unwrap(),
            handle: Handle::new("alicehandle").unwrap(),
            display_name: DisplayName::new("Alice").unwrap(),
            bio: None,
            avatar_url: None,
            banner_url: None,
            profile_kind: ProfileKind::try_from("personal").unwrap(),
            locale: Locale::new("en-US").unwrap(),
            visibility: ProfileVisibility::Public,
            interaction: InteractionSettings::default(),
            location: LocationSettings::default(),
            discovery: DiscoverySettings::default(),
            correlation_id: Uuid::now_v7(),
        });
        p.drain_events(); // discard the ProfileCreated event
        p
    }

    /// A 13–17 holder's profile is born private, and says so to projections.
    #[test]
    fn a_profile_created_private_announces_its_visibility() {
        let mut p = Profile::create(ProfileCreateParams {
            account_id: AccountId::try_from(uuid::Uuid::now_v7().to_string().as_str()).unwrap(),
            handle: Handle::new("teenhandle").unwrap(),
            display_name: DisplayName::new("Teen").unwrap(),
            bio: None,
            avatar_url: None,
            banner_url: None,
            profile_kind: ProfileKind::try_from("personal").unwrap(),
            locale: Locale::new("en-US").unwrap(),
            visibility: ProfileVisibility::Private,
            interaction: InteractionSettings::teen(),
            location: LocationSettings::teen(),
            discovery: DiscoverySettings::teen(),
            correlation_id: Uuid::now_v7(),
        });
        assert_eq!(p.visibility(), ProfileVisibility::Private);
        assert_eq!(p.interaction(), InteractionSettings::teen());
        let events = p.drain_events();
        assert!(matches!(events.as_slice(), [
            DomainEvent::ProfileCreated(_),
            DomainEvent::InteractionSettingsChanged(_),
            DomainEvent::LocationSettingsChanged(_),
            DomainEvent::DiscoverySettingsChanged(_),
            DomainEvent::VisibilityChanged(VisibilityChanged { visibility: ProfileVisibility::Private, .. })
        ]));
    }

    #[test]
    fn interaction_settings_change_once_and_emit_only_on_change() {
        use crate::domain::value_object::InteractionAudience;
        let mut p = sample_profile();
        let version = p.version();
        let quieter = InteractionSettings { comments: InteractionAudience::Mutuals, ..InteractionSettings::default() };
        assert!(p.set_interaction_settings(quieter, Uuid::now_v7()).unwrap());
        assert_eq!(p.version(), version + 1);
        assert!(matches!(p.drain_events().as_slice(), [DomainEvent::InteractionSettingsChanged(_)]));
        assert!(!p.set_interaction_settings(quieter, Uuid::now_v7()).unwrap(), "unchanged ⇒ no-op");
        assert!(p.drain_events().is_empty());
    }

    #[test]
    fn set_tier_emits_on_change_and_updates_state() {
        let mut p = sample_profile();
        assert_eq!(p.tier(), 0);

        p.set_tier(2, Uuid::now_v7()).unwrap();
        assert_eq!(p.tier(), 2);
        let events = p.drain_events();
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], DomainEvent::TierChanged(_)));
    }

    #[test]
    fn set_visibility_emits_the_new_visibility() {
        let mut p = sample_profile();
        p.set_visibility(ProfileVisibility::Private, Uuid::now_v7()).unwrap();
        let events = p.drain_events();
        assert_eq!(events.len(), 1);
        match &events[0] {
            DomainEvent::VisibilityChanged(e) => assert_eq!(e.visibility, ProfileVisibility::Private),
            other => panic!("expected VisibilityChanged, got {other:?}"),
        }
        // On the wire, as social-graph reads it.
        let wire = crate::infrastructure::publisher::wire::ProfileEventWire::from(&events[0]);
        let json = serde_json::to_value(&wire).unwrap();
        assert_eq!(json["type"], "ProfileVisibilityChanged");
        assert_eq!(json["visibility"], "private");
    }

    #[test]
    fn set_tier_is_idempotent_when_unchanged() {
        let mut p = sample_profile();
        p.set_tier(1, Uuid::now_v7()).unwrap();
        p.drain_events();

        // Same tier again → no event, no version bump.
        let version_before = p.version();
        p.set_tier(1, Uuid::now_v7()).unwrap();
        assert!(p.drain_events().is_empty());
        assert_eq!(p.version(), version_before);
    }

    #[test]
    fn set_tier_rejects_unknown_tier() {
        let mut p = sample_profile();
        assert!(p.set_tier(5, Uuid::now_v7()).is_err());
    }
}
