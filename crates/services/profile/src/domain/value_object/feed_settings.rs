use serde::{Deserialize, Serialize};

/// How much sensitive content the holder's discovery feeds show (#662).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SensitiveContent {
    /// Age-gated posts left out (timeline's RESTRICTED level).
    #[default]
    Less,
    /// Age-gated posts shown (timeline's STANDARD level). Never applied to a
    /// guest or a 13–17 holder: timeline clamps them to `Less`.
    Standard,
}

/// The holder's feed controls, synced across their devices; the client sends
/// them with each discovery request. Stored as one JSON column; a profile
/// without one has the default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct FeedSettings {
    pub sensitive_content: SensitiveContent,
    /// For You not ranked by the profile's interest tags (timeline, #662).
    pub non_personalized:  bool,
}

impl FeedSettings {
    /// The teen defaults (13–17, #662): For You not personalised (no interest
    /// profiling unless the holder turns it on — UK Children's Code).
    pub fn teen() -> Self {
        Self { non_personalized: true, ..Self::default() }
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
    fn the_default_is_less_and_the_stored_form_round_trips() {
        assert_eq!(FeedSettings::from_json(None).sensitive_content, SensitiveContent::Less);
        assert!(!FeedSettings::from_json(None).non_personalized);
        let s = FeedSettings { sensitive_content: SensitiveContent::Standard, non_personalized: true };
        assert_eq!(FeedSettings::from_json(Some(&s.to_json())), s);
        // Stored before the flag existed: personalised.
        assert!(!FeedSettings::from_json(Some(r#"{"sensitive_content":"standard"}"#)).non_personalized);
        assert!(FeedSettings::teen().non_personalized);
        assert_eq!(FeedSettings::teen().sensitive_content, SensitiveContent::Less);
    }
}
