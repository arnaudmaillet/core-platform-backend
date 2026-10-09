use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::domain::value_object::{
    BusinessInfo, CommentFilters, DiscoverySettings, FeedSettings, InteractionSettings, LocationSettings, TabSettings,
    VisibleTabs,
};
use crate::domain::aggregate::Profile;
use crate::domain::entity::ProfileLink;
use crate::domain::value_object::{AccountId, ProfileId, ProfileStatus, Viewer};
use crate::error::ProfileError;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfileLinkView {
    pub label: String,
    pub url: String,
}

impl From<&ProfileLink> for ProfileLinkView {
    fn from(l: &ProfileLink) -> Self {
        Self {
            label: l.label.clone(),
            url: l.url.as_str().to_owned(),
        }
    }
}

/// Full serialized profile view cached in Redis at key `profile:v1:{id}`.
///
/// All value objects are flattened to primitives so the cache layer has zero
/// dependency on the domain module — any service can deserialize this view
/// from Redis without importing the profile crate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfileView {
    pub id: String,
    pub account_id: String,
    pub handle: String,
    pub display_name: String,
    pub bio: Option<String>,
    pub avatar_url: Option<String>,
    pub banner_url: Option<String>,
    pub website_url: Option<String>,
    pub custom_links: Vec<ProfileLinkView>,
    pub profile_kind: String,
    pub visibility: String,
    pub verified: bool,
    pub verification_kind: Option<String>,
    pub locale: String,
    pub timezone: Option<String>,
    pub status: String,
    pub masked_at: Option<DateTime<Utc>>,
    pub masking_reason: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub version: i64,
    /// Entries cached before the field existed read as the defaults.
    #[serde(default)]
    pub interaction: InteractionSettings,
    /// Owner-only (cleared for others): whether they ghost the map.
    #[serde(default)]
    pub location: Option<LocationSettings>,
    /// Owner-only (cleared for others): presence and discoverability.
    #[serde(default)]
    pub discovery: Option<DiscoverySettings>,
    /// Owner-only (cleared for others): hidden words, offensive filter.
    #[serde(default)]
    pub comment_filters: Option<CommentFilters>,
    /// Owner-only (cleared for others): post window, tab visibility.
    #[serde(default)]
    pub tab_settings: Option<TabSettings>,
    /// Owner-only (cleared for others): feed controls.
    #[serde(default)]
    pub feed_settings: Option<FeedSettings>,
    /// A brand's public contact card (#668), shown to everyone.
    #[serde(default)]
    pub business_info: Option<BusinessInfo>,
    /// Everyone: which tabs the owner shows (#829); derived from
    /// `tab_settings` for each reader, never cached on its own.
    #[serde(skip)]
    pub visible_tabs: Option<VisibleTabs>,
}

impl From<&Profile> for ProfileView {
    fn from(p: &Profile) -> Self {
        Self {
            id: p.id().as_str(),
            account_id: p.account_id().as_str(),
            handle: p.handle().as_str().to_owned(),
            display_name: p.display_name().as_str().to_owned(),
            bio: p.bio().map(|b| b.as_str().to_owned()),
            avatar_url: p.avatar_url().map(|u| u.as_str().to_owned()),
            banner_url: p.banner_url().map(|u| u.as_str().to_owned()),
            website_url: p.website_url().map(|u| u.as_str().to_owned()),
            custom_links: p.custom_links().iter().map(ProfileLinkView::from).collect(),
            profile_kind: p.profile_kind().as_str().to_owned(),
            visibility: p.visibility().as_str().to_owned(),
            verified: p.verified(),
            verification_kind: p.verification_kind().map(|v| v.as_str().to_owned()),
            locale: p.locale().as_str().to_owned(),
            timezone: p.timezone().map(str::to_owned),
            status: p.status().as_str().to_owned(),
            masked_at: p.masked_at(),
            masking_reason: p.masking_reason().map(|r| r.as_str().to_owned()),
            created_at: p.created_at(),
            updated_at: p.updated_at(),
            version: p.version(),
            interaction: p.interaction(),
            location: Some(p.location()),
            discovery: Some(p.discovery()),
            comment_filters: Some(p.comment_filters().clone()),
            tab_settings: Some(p.tab_settings()),
            feed_settings: Some(p.feed_settings()),
            business_info: p.business_info().cloned(),
            visible_tabs: Some(p.tab_settings().visible()),
        }
    }
}

impl ProfileView {
    /// The view as `viewer` may see it, or `None` when the profile is not
    /// visible to them.
    ///
    /// The owner (and a trusted internal caller) gets everything. Anyone else
    /// gets an **active** profile only (hidden, suspended and deleted ones are
    /// not found), with the owner-only fields cleared: the account id (which
    /// would link an account's profiles together), locale, timezone and masking
    /// details. A private profile still shows its header; its posts and lists
    /// are what privacy withholds.
    pub fn for_viewer(mut self, viewer: &Viewer) -> Option<Self> {
        // Told to every reader before the owner-only settings are cleared.
        self.visible_tabs = Some(self.tab_settings.unwrap_or_default().visible());
        if viewer.sees_everything_of(&self.account_id) {
            return Some(self);
        }
        if self.status != ProfileStatus::Active.as_str() {
            return None;
        }
        self.account_id = String::new();
        self.locale = String::new();
        self.timezone = None;
        self.masked_at = None;
        self.masking_reason = None;
        self.location = None;
        self.discovery = None;
        self.comment_filters = None;
        self.tab_settings = None;
        self.feed_settings = None;
        Some(self)
    }
}

/// Cache port for the profile read path.
///
/// Three independent Redis key namespaces; TTLs are externalized to the `[cache]`
/// section of `infrastructure.toml` and hot-reload (no redeploy):
/// - `profile:v1:{id}` — full ProfileView, TTL from the `profile-view` binding.
/// - `handle:v1:{handle}` — profile_id string, TTL from the `handle-lookup` binding.
/// - `account:profiles:v1:{account_id}` — evicted on writes; no SET, only DEL.
#[async_trait]
pub trait ProfileCache: Send + Sync + 'static {
    async fn get_by_id(&self, id: &ProfileId) -> Result<Option<ProfileView>, ProfileError>;
    async fn set_by_id(&self, view: &ProfileView) -> Result<(), ProfileError>;
    async fn invalidate_by_id(&self, id: &ProfileId) -> Result<(), ProfileError>;

    async fn get_profile_id_by_handle(
        &self,
        handle: &str,
    ) -> Result<Option<ProfileId>, ProfileError>;
    async fn set_handle_mapping(
        &self,
        handle: &str,
        id: ProfileId,
    ) -> Result<(), ProfileError>;
    async fn invalidate_handle(&self, handle: &str) -> Result<(), ProfileError>;

    async fn invalidate_account_profiles(
        &self,
        account_id: &AccountId,
    ) -> Result<(), ProfileError>;
}

#[cfg(test)]
mod viewer_tests {
    use super::*;

    fn view(status: ProfileStatus) -> ProfileView {
        ProfileView {
            id: "p-1".into(),
            account_id: "acct-1".into(),
            handle: "alice".into(),
            display_name: "Alice".into(),
            bio: Some("hi".into()),
            avatar_url: None,
            banner_url: None,
            website_url: Some("https://alice.example".into()),
            custom_links: Vec::new(),
            profile_kind: "personal".into(),
            visibility: "private".into(),
            verified: false,
            verification_kind: None,
            locale: "fr-FR".into(),
            timezone: Some("Europe/Paris".into()),
            status: status.as_str().into(),
            masked_at: None,
            masking_reason: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            version: 3,
            interaction: InteractionSettings::default(),
            location: Some(LocationSettings::teen()),
            discovery: Some(DiscoverySettings::teen()),
            comment_filters: None,
            tab_settings: None,
            feed_settings: None,
            business_info: None,
            visible_tabs: None,
        }
    }

    /// #829: every reader is told which tabs the owner shows; the settings
    /// themselves stay the owner's.
    #[test]
    fn every_reader_learns_the_visible_tabs() {
        let mut owned = view(ProfileStatus::Active);
        owned.tab_settings = Some(TabSettings { show_likes: false, show_places: false, ..TabSettings::default() });
        let seen = owned.for_viewer(&Viewer::Account("someone-else".into())).expect("visible");
        assert_eq!(seen.visible_tabs, Some(VisibleTabs { likes: false, saved: false, reposts: true, places: false }));
        assert!(seen.tab_settings.is_none(), "the settings stay the owner's");
        let defaults = view(ProfileStatus::Active).for_viewer(&Viewer::Account("someone-else".into())).unwrap();
        assert_eq!(defaults.visible_tabs, Some(TabSettings::default().visible()), "no settings: the defaults");
    }

    #[test]
    fn the_owner_and_the_mesh_get_everything() {
        for viewer in [Viewer::Internal, Viewer::Account("acct-1".into())] {
            let seen = view(ProfileStatus::Hidden).for_viewer(&viewer).expect("visible");
            assert_eq!(seen.account_id, "acct-1");
            assert_eq!(seen.locale, "fr-FR");
        }
    }

    #[test]
    fn others_get_active_profiles_without_owner_only_fields() {
        for viewer in [Viewer::Anonymous, Viewer::Account("acct-2".into())] {
            let seen = view(ProfileStatus::Active).for_viewer(&viewer).expect("visible");
            assert!(seen.account_id.is_empty(), "the account id would link profiles");
            assert!(seen.locale.is_empty());
            assert_eq!(seen.timezone, None);
            // The public header survives, private profile or not.
            assert_eq!(seen.handle, "alice");
            assert_eq!(seen.bio.as_deref(), Some("hi"));
            assert_eq!(seen.website_url.as_deref(), Some("https://alice.example"));
            assert_eq!(seen.visibility, "private");
        }
    }

    #[test]
    fn hidden_suspended_and_deleted_profiles_are_not_found_for_others() {
        for status in [ProfileStatus::Hidden, ProfileStatus::Suspended, ProfileStatus::Deleted] {
            assert!(view(status).for_viewer(&Viewer::Anonymous).is_none(), "{status:?}");
            assert!(view(status).for_viewer(&Viewer::Account("acct-2".into())).is_none());
        }
    }
}
