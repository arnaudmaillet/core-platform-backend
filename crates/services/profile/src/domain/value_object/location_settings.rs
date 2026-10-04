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

/// A profile's location sharing (#657). Location is the most sensitive data
/// on a map-first app: geo-discovery applies it to every map surface. Stored
/// as one JSON column; a profile without one shares precisely, not ghosted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct LocationSettings {
    /// Ghost mode: the profile's posts leave everyone else's map.
    pub ghost: bool,
    pub precision: LocationPrecision,
}

impl LocationSettings {
    /// The teen default (13–17): location sharing off (ghost) until the holder
    /// turns it on.
    pub fn teen() -> Self {
        Self { ghost: true, precision: LocationPrecision::City }
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
}
