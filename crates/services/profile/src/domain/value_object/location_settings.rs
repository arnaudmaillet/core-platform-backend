use serde::{Deserialize, Serialize};

/// How precisely others see where a profile's posts were made.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LocationPrecision {
    /// The post's own point.
    #[default]
    Precise,
    /// City level: only the coarse map band, at a coarse point.
    City,
}

impl LocationPrecision {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Precise => "precise",
            Self::City => "city",
        }
    }
}

/// Who sees where a profile's posts were made (#657): on the map and on the
/// post itself. Outside it, a reader sees the posts without their place.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LocationAudience {
    #[default]
    Everyone,
    /// Profiles that follow the owner.
    Followers,
    /// Profiles the owner follows back.
    Mutuals,
}

impl LocationAudience {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Everyone => "everyone",
            Self::Followers => "followers",
            Self::Mutuals => "mutuals",
        }
    }
}

/// A profile's location sharing (#657). Location is the most sensitive data
/// on a map-first app: geo-discovery applies it to every map surface, post to
/// a post's place. Stored as one JSON column; a profile without one shares
/// precisely, with everyone, not ghosted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct LocationSettings {
    /// Ghost mode: the profile's posts leave everyone else's map.
    pub ghost: bool,
    pub precision: LocationPrecision,
    /// Who sees the location, when not a ghost.
    pub audience: LocationAudience,
    /// The app adds the location to a new post by default (a stored
    /// preference: the server never changes a post for it).
    pub on_new_posts: bool,
}

impl Default for LocationSettings {
    fn default() -> Self {
        Self {
            ghost: false,
            precision: LocationPrecision::Precise,
            audience: LocationAudience::Everyone,
            on_new_posts: true,
        }
    }
}

impl LocationSettings {
    /// The teen default (13–17): location sharing off (ghost) until the holder
    /// turns it on — then for mutuals only, at city level — and no location on
    /// new posts unless they add it.
    pub fn teen() -> Self {
        Self { ghost: true, precision: LocationPrecision::City, audience: LocationAudience::Mutuals, on_new_posts: false }
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }

    pub fn from_json(json: Option<&str>) -> Self {
        json.and_then(|j| serde_json::from_str(j).ok()).unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stored_form_round_trips_and_absent_shares_precisely() {
        let teen = LocationSettings::teen();
        assert_eq!(LocationSettings::from_json(Some(&teen.to_json())), teen);
        assert_eq!(LocationSettings::from_json(None), LocationSettings::default());
        assert!(!LocationSettings::default().ghost);
    }

    #[test]
    fn a_stored_row_from_before_the_audience_shares_with_everyone_and_teens_with_mutuals() {
        let old = LocationSettings::from_json(Some(r#"{"ghost":false,"precision":"city"}"#));
        assert_eq!(old.audience, LocationAudience::Everyone);
        assert!(old.on_new_posts);
        assert_eq!(old.precision, LocationPrecision::City);
        let teen = LocationSettings::teen();
        assert_eq!((teen.audience, teen.on_new_posts), (LocationAudience::Mutuals, false));
    }
}
