use serde::{Deserialize, Serialize};

use crate::error::ProfileError;

/// Who may interact with a profile in a given way. Enforced server-side by the
/// services that own each interaction, through social-graph's
/// `CheckInteraction` (which knows follows and blocks); a block always wins.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InteractionAudience {
    #[default]
    Everyone,
    /// Profiles that follow this one.
    Followers,
    /// Profiles this one follows back (mutual follows).
    Mutuals,
    NoOne,
}

impl InteractionAudience {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Everyone => "everyone",
            Self::Followers => "followers",
            Self::Mutuals => "mutuals",
            Self::NoOne => "no_one",
        }
    }
}

impl TryFrom<&str> for InteractionAudience {
    type Error = ProfileError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s {
            "everyone" => Ok(Self::Everyone),
            "followers" => Ok(Self::Followers),
            "mutuals" => Ok(Self::Mutuals),
            "no_one" => Ok(Self::NoOne),
            other => Err(ProfileError::DomainViolation {
                field: "interaction_audience".into(),
                message: format!("unknown interaction audience: '{other}'"),
            }),
        }
    }
}

/// Whom a temporary interaction limit holds back (#669).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LimitAudience {
    /// Profiles that do not follow this one.
    NonFollowers,
    /// Non-followers and profiles that followed in the last week.
    RecentFollowers,
}

impl LimitAudience {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::NonFollowers => "non_followers",
            Self::RecentFollowers => "recent_followers",
        }
    }
}

/// The longest temporary limit (Instagram's "Limits": up to four weeks).
pub const MAX_LIMIT: chrono::Duration = chrono::Duration::weeks(4);

/// A temporary interaction limit (#669): until `until`, comments and messages
/// from `audience` are held for the holder's review instead of shown.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct InteractionLimit {
    pub audience: LimitAudience,
    pub until:    chrono::DateTime<chrono::Utc>,
}

impl InteractionLimit {
    /// `until` must be in the future, at most [`MAX_LIMIT`] ahead (a minute of
    /// slack for the client's clock).
    pub fn new(audience: LimitAudience, until: chrono::DateTime<chrono::Utc>, now: chrono::DateTime<chrono::Utc>) -> Result<Self, ProfileError> {
        if until <= now || until > now + MAX_LIMIT + chrono::Duration::minutes(1) {
            return Err(ProfileError::DomainViolation {
                field:   "limit.until".into(),
                message: "a limit ends in the future, at most four weeks ahead".into(),
            });
        }
        Ok(Self { audience, until })
    }
}

/// A profile's interaction settings. Stored as one JSON column; a profile
/// without one has the defaults (everyone, downloads on, like counts shown).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct InteractionSettings {
    /// Who may comment on this profile's posts.
    pub comments: InteractionAudience,
    /// Who may mention or tag this profile.
    pub mentions: InteractionAudience,
    /// Who may message this profile directly.
    pub messages: InteractionAudience,
    /// Whether others may download this profile's posts.
    pub allow_downloads: bool,
    /// Whether others see like counts on this profile's posts.
    pub show_like_counts: bool,
    /// Whether others may remix this profile's posts (#669; a post may
    /// override it).
    pub allow_remix: bool,
    /// Whether others may reuse the original sound of this profile's posts
    /// (#669; a post may override it). post enforces it.
    pub allow_sound_reuse: bool,
    /// A temporary interaction limit, if one is on (#669).
    pub limit: Option<InteractionLimit>,
}

impl Default for InteractionSettings {
    fn default() -> Self {
        Self {
            comments: InteractionAudience::Everyone,
            mentions: InteractionAudience::Everyone,
            messages: InteractionAudience::Everyone,
            allow_downloads: true,
            show_like_counts: true,
            allow_remix: true,
            allow_sound_reuse: true,
            limit: None,
        }
    }
}

impl InteractionSettings {
    /// The teen defaults (13–17): only followers comment, mention or message
    /// (a teen's profile is private, so followers are the approved ones), and
    /// nobody downloads, remixes or reuses the sound of their posts. The holder
    /// may relax them later.
    pub fn teen() -> Self {
        Self {
            comments: InteractionAudience::Followers,
            mentions: InteractionAudience::Followers,
            messages: InteractionAudience::Followers,
            allow_downloads: false,
            show_like_counts: true,
            allow_remix: false,
            allow_sound_reuse: false,
            limit: None,
        }
    }

    /// The stored form (one text column).
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }

    /// From the stored form; `None` or unreadable ⇒ the defaults.
    pub fn from_json(json: Option<&str>) -> Self {
        json.and_then(|j| serde_json::from_str(j).ok()).unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stored_form_round_trips_and_absent_reads_as_defaults() {
        let teen = InteractionSettings::teen();
        assert_eq!(InteractionSettings::from_json(Some(&teen.to_json())), teen);
        assert_eq!(InteractionSettings::from_json(None), InteractionSettings::default());
        // A row written before a field existed keeps that field's default.
        assert_eq!(
            InteractionSettings::from_json(Some(r#"{"comments":"no_one"}"#)),
            InteractionSettings { comments: InteractionAudience::NoOne, ..InteractionSettings::default() }
        );
    }
}
