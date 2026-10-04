use serde::{Deserialize, Serialize};

/// A profile's presence and discoverability (#661). Each flag is "on" by
/// default; a teen (13–17) starts unfindable by phone, email and suggestions.
/// Stored as one JSON column; a profile without one has the defaults.
///
/// Who honours what: search drops a profile from handle search
/// (`by_handle_search`); chat honours `activity_status` and `read_receipts`.
/// Phone / email lookup, QR and suggestions have no server surface yet: the
/// flags are kept for the client and for those surfaces when they come.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct DiscoverySettings {
    /// Others see when the profile is active.
    pub activity_status:  bool,
    /// Others see when the profile has read their messages.
    pub read_receipts:    bool,
    pub by_phone:         bool,
    pub by_email:         bool,
    pub by_handle_search: bool,
    /// Reachable through a QR code or a shared profile link.
    pub by_qr:            bool,
    pub in_suggestions:   bool,
}

impl Default for DiscoverySettings {
    fn default() -> Self {
        Self {
            activity_status:  true,
            read_receipts:    true,
            by_phone:         true,
            by_email:         true,
            by_handle_search: true,
            by_qr:            true,
            in_suggestions:   true,
        }
    }
}

impl DiscoverySettings {
    /// The teen default (13–17): not findable by phone or email, not suggested.
    pub fn teen() -> Self {
        Self { by_phone: false, by_email: false, in_suggestions: false, ..Self::default() }
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
    fn absent_means_everything_on_and_the_stored_form_round_trips() {
        assert_eq!(DiscoverySettings::from_json(None), DiscoverySettings::default());
        assert_eq!(DiscoverySettings::from_json(Some("{}")), DiscoverySettings::default());
        let teen = DiscoverySettings::teen();
        assert_eq!(DiscoverySettings::from_json(Some(&teen.to_json())), teen);
        assert!(!teen.by_phone && !teen.by_email && !teen.in_suggestions);
        assert!(teen.by_handle_search && teen.activity_status);
    }
}
