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
        occurred_at_ms: i64,
    },
    /// Ghost mode and location precision (`precise` | `city`).
    /// `geo-discovery` projects it onto every map surface.
    ProfileLocationSettingsChanged {
        profile_id: String,
        ghost: bool,
        precision: String,
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
            | ProfileEventWire::ProfileLocationSettingsChanged { profile_id, .. } => profile_id,
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
                    occurred_at_ms: e.occurred_at.timestamp_millis(),
                }
            }
            DomainEvent::LocationSettingsChanged(e) => ProfileEventWire::ProfileLocationSettingsChanged {
                profile_id: e.profile_id.to_string(),
                ghost: e.settings.ghost,
                precision: e.settings.precision.as_str().to_owned(),
                occurred_at_ms: e.occurred_at.timestamp_millis(),
            },
        }
    }
}
