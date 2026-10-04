//! Serde wire schema for `profile.v1.events`.
//!
//! The wire contract is decoupled from the domain value objects: a thin,
//! string-typed enum (ids + timestamps, no display content) so a consumer
//! deserializes a stable shape and a VO refactor never breaks the wire.
//! Internally tagged on `type` (the moderation-service convention), keyed by
//! `profile_id`. Events are intentionally **thin** — a consumer that needs the
//! full profile (e.g. search) hydrates it from `GetProfileById`.

use serde::{Deserialize, Serialize};

use crate::domain::event::DomainEvent;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ProfileEventWire {
    ProfileCreated {
        profile_id: String,
        account_id: String,
        handle: String,
        profile_kind: String,
        occurred_at_ms: i64,
    },
    ProfileUpdated {
        profile_id: String,
        occurred_at_ms: i64,
    },
    HandleChanged {
        profile_id: String,
        new_handle: String,
        occurred_at_ms: i64,
    },
    ProfileVerified {
        profile_id: String,
        occurred_at_ms: i64,
    },
    ProfileHidden {
        profile_id: String,
        masking_reason: String,
        occurred_at_ms: i64,
    },
    ProfileRestored {
        profile_id: String,
        occurred_at_ms: i64,
    },
    ProfileDeleted {
        profile_id: String,
        occurred_at_ms: i64,
    },
    /// The author tier changed (denormalized from `social-graph`). `post`
    /// consumes this to stamp the current tier onto new posts. `tier` is the
    /// shared `u8` taxonomy (0=Standard, 1=Premium, 2=Vip).
    ProfileTierChanged {
        profile_id: String,
        tier: u8,
        occurred_at_ms: i64,
    },
    /// The owner switched the profile between `public` and `private`.
    /// `social-graph` projects it for its access check (private profiles show
    /// their content to followers only).
    ProfileVisibilityChanged {
        profile_id: String,
        visibility: String,
        occurred_at_ms: i64,
    },
    /// The owner changed who may comment / mention / message (`everyone`,
    /// `followers`, `mutuals`, `no_one`), downloads and like counts.
    /// `social-graph` projects it for `CheckInteraction`.
    ProfileInteractionSettingsChanged {
        profile_id: String,
        comments: String,
        mentions: String,
        messages: String,
        allow_downloads: bool,
        show_like_counts: bool,
        /// Remix / original-sound reuse defaults (#669); `post` applies them.
        allow_remix: bool,
        allow_sound_reuse: bool,
        occurred_at_ms: i64,
    },
    /// Ghost mode and location precision (`precise` | `city`).
    /// `geo-discovery` projects it onto every map surface, `post` onto `GetPost`.
    ProfileLocationSettingsChanged {
        profile_id: String,
        ghost: bool,
        precision: String,
        occurred_at_ms: i64,
    },
    /// Activity status, read receipts and how people can find the profile.
    /// `search` projects `by_handle_search`; chat, the presence flags.
    /// Hidden words and the offensive-comment filter (#660); `comment`
    /// applies them to the comments on the profile's posts.
    ProfileCommentFiltersChanged {
        profile_id: String,
        hidden_words: Vec<String>,
        filter_offensive: bool,
        occurred_at_ms: i64,
    },
    /// Post history window (`all` | `six_months` | `one_month` | `three_days`)
    /// and tab visibility (#664); `post` applies the window.
    ProfileTabSettingsChanged {
        profile_id: String,
        post_window: String,
        show_likes: bool,
        show_saved: bool,
        show_reposts: bool,
        show_places: bool,
        occurred_at_ms: i64,
    },
    ProfileDiscoverySettingsChanged {
        profile_id: String,
        activity_status: bool,
        read_receipts: bool,
        by_phone: bool,
        by_email: bool,
        by_handle_search: bool,
        by_qr: bool,
        in_suggestions: bool,
        occurred_at_ms: i64,
    },
}

impl ProfileEventWire {
    /// The partition key — guarantees per-profile ordering on the topic.
    pub fn profile_id(&self) -> &str {
        match self {
            ProfileEventWire::ProfileCreated { profile_id, .. }
            | ProfileEventWire::ProfileUpdated { profile_id, .. }
            | ProfileEventWire::HandleChanged { profile_id, .. }
            | ProfileEventWire::ProfileVerified { profile_id, .. }
            | ProfileEventWire::ProfileHidden { profile_id, .. }
            | ProfileEventWire::ProfileRestored { profile_id, .. }
            | ProfileEventWire::ProfileDeleted { profile_id, .. }
            | ProfileEventWire::ProfileTierChanged { profile_id, .. }
            | ProfileEventWire::ProfileVisibilityChanged { profile_id, .. }
            | ProfileEventWire::ProfileInteractionSettingsChanged { profile_id, .. }
            | ProfileEventWire::ProfileLocationSettingsChanged { profile_id, .. }
            | ProfileEventWire::ProfileDiscoverySettingsChanged { profile_id, .. }
            | ProfileEventWire::ProfileCommentFiltersChanged { profile_id, .. }
            | ProfileEventWire::ProfileTabSettingsChanged { profile_id, .. } => profile_id,
        }
    }

    /// The `type` tag, also set as the `event_type` Kafka header for routing.
    pub fn event_type(&self) -> &'static str {
        match self {
            ProfileEventWire::ProfileCreated { .. } => "ProfileCreated",
            ProfileEventWire::ProfileUpdated { .. } => "ProfileUpdated",
            ProfileEventWire::HandleChanged { .. } => "HandleChanged",
            ProfileEventWire::ProfileVerified { .. } => "ProfileVerified",
            ProfileEventWire::ProfileHidden { .. } => "ProfileHidden",
            ProfileEventWire::ProfileRestored { .. } => "ProfileRestored",
            ProfileEventWire::ProfileDeleted { .. } => "ProfileDeleted",
            ProfileEventWire::ProfileTierChanged { .. } => "ProfileTierChanged",
            ProfileEventWire::ProfileVisibilityChanged { .. } => "ProfileVisibilityChanged",
            ProfileEventWire::ProfileInteractionSettingsChanged { .. } => {
                "ProfileInteractionSettingsChanged"
            }
            ProfileEventWire::ProfileLocationSettingsChanged { .. } => "ProfileLocationSettingsChanged",
            ProfileEventWire::ProfileDiscoverySettingsChanged { .. } => "ProfileDiscoverySettingsChanged",
            ProfileEventWire::ProfileCommentFiltersChanged { .. } => "ProfileCommentFiltersChanged",
            ProfileEventWire::ProfileTabSettingsChanged { .. } => "ProfileTabSettingsChanged",
        }
    }
}

impl From<&DomainEvent> for ProfileEventWire {
    fn from(event: &DomainEvent) -> Self {
        match event {
            DomainEvent::ProfileCreated(e) => ProfileEventWire::ProfileCreated {
                profile_id: e.profile_id.to_string(),
                account_id: e.account_id.to_string(),
                handle: e.handle.to_string(),
                profile_kind: e.profile_kind.to_string(),
                occurred_at_ms: e.occurred_at.timestamp_millis(),
            },
            DomainEvent::ProfileUpdated(e) => ProfileEventWire::ProfileUpdated {
                profile_id: e.profile_id.to_string(),
                occurred_at_ms: e.occurred_at.timestamp_millis(),
            },
            DomainEvent::HandleChanged(e) => ProfileEventWire::HandleChanged {
                profile_id: e.profile_id.to_string(),
                new_handle: e.new_handle.to_string(),
                occurred_at_ms: e.occurred_at.timestamp_millis(),
            },
            DomainEvent::ProfileVerified(e) => ProfileEventWire::ProfileVerified {
                profile_id: e.profile_id.to_string(),
                occurred_at_ms: e.occurred_at.timestamp_millis(),
            },
            DomainEvent::ProfileHidden(e) => ProfileEventWire::ProfileHidden {
                profile_id: e.profile_id.to_string(),
                masking_reason: e.masking_reason.to_string(),
                occurred_at_ms: e.occurred_at.timestamp_millis(),
            },
            DomainEvent::ProfileRestored(e) => ProfileEventWire::ProfileRestored {
                profile_id: e.profile_id.to_string(),
                occurred_at_ms: e.occurred_at.timestamp_millis(),
            },
            DomainEvent::ProfileDeleted(e) => ProfileEventWire::ProfileDeleted {
                profile_id: e.profile_id.to_string(),
                occurred_at_ms: e.occurred_at.timestamp_millis(),
            },
            DomainEvent::TierChanged(e) => ProfileEventWire::ProfileTierChanged {
                profile_id: e.profile_id.to_string(),
                tier: e.tier,
                occurred_at_ms: e.occurred_at.timestamp_millis(),
            },
            DomainEvent::VisibilityChanged(e) => ProfileEventWire::ProfileVisibilityChanged {
                profile_id: e.profile_id.to_string(),
                visibility: e.visibility.as_str().to_owned(),
                occurred_at_ms: e.occurred_at.timestamp_millis(),
            },
            DomainEvent::InteractionSettingsChanged(e) => {
                ProfileEventWire::ProfileInteractionSettingsChanged {
                    profile_id: e.profile_id.to_string(),
                    comments: e.settings.comments.as_str().to_owned(),
                    mentions: e.settings.mentions.as_str().to_owned(),
                    messages: e.settings.messages.as_str().to_owned(),
                    allow_downloads: e.settings.allow_downloads,
                    show_like_counts: e.settings.show_like_counts,
                    allow_remix: e.settings.allow_remix,
                    allow_sound_reuse: e.settings.allow_sound_reuse,
                    occurred_at_ms: e.occurred_at.timestamp_millis(),
                }
            }
            DomainEvent::TabSettingsChanged(e) => ProfileEventWire::ProfileTabSettingsChanged {
                profile_id: e.profile_id.to_string(),
                post_window: e.settings.post_window.as_str().to_owned(),
                show_likes: e.settings.show_likes,
                show_saved: e.settings.show_saved,
                show_reposts: e.settings.show_reposts,
                show_places: e.settings.show_places,
                occurred_at_ms: e.occurred_at.timestamp_millis(),
            },
            DomainEvent::CommentFiltersChanged(e) => ProfileEventWire::ProfileCommentFiltersChanged {
                profile_id: e.profile_id.to_string(),
                hidden_words: e.filters.hidden_words.clone(),
                filter_offensive: e.filters.filter_offensive,
                occurred_at_ms: e.occurred_at.timestamp_millis(),
            },
            DomainEvent::DiscoverySettingsChanged(e) => ProfileEventWire::ProfileDiscoverySettingsChanged {
                profile_id: e.profile_id.to_string(),
                activity_status: e.settings.activity_status,
                read_receipts: e.settings.read_receipts,
                by_phone: e.settings.by_phone,
                by_email: e.settings.by_email,
                by_handle_search: e.settings.by_handle_search,
                by_qr: e.settings.by_qr,
                in_suggestions: e.settings.in_suggestions,
                occurred_at_ms: e.occurred_at.timestamp_millis(),
            },
            DomainEvent::LocationSettingsChanged(e) => ProfileEventWire::ProfileLocationSettingsChanged {
                profile_id: e.profile_id.to_string(),
                ghost: e.settings.ghost,
                precision: e.settings.precision.as_str().to_owned(),
                occurred_at_ms: e.occurred_at.timestamp_millis(),
            },
        }
    }
}
