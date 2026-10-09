use serde::{Deserialize, Serialize};

/// How far back others can see the profile's posts (#664). Older posts are
/// not deleted: they are only hidden from everyone but the owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PostWindow {
    #[default]
    All,
    SixMonths,
    OneMonth,
    ThreeDays,
}

impl PostWindow {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::All => "all",
            Self::SixMonths => "six_months",
            Self::OneMonth => "one_month",
            Self::ThreeDays => "three_days",
        }
    }

    /// The window in days; `None` for all posts.
    pub fn days(&self) -> Option<u32> {
        match self {
            Self::All => None,
            Self::SixMonths => Some(183),
            Self::OneMonth => Some(30),
            Self::ThreeDays => Some(3),
        }
    }
}

/// The profile's post history window and which profile tabs others see
/// (#664). Stored as one JSON column; a profile without one has the defaults.
/// post enforces the window and serves the Reposts and Places tabs,
/// engagement the Likes tab (#829); Saved has no server list yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct TabSettings {
    pub post_window:  PostWindow,
    pub show_likes:   bool,
    pub show_saved:   bool,
    pub show_reposts: bool,
    pub show_places:  bool,
}

impl Default for TabSettings {
    fn default() -> Self {
        Self { post_window: PostWindow::All, show_likes: true, show_saved: false, show_reposts: true, show_places: true }
    }
}

/// Which of a profile's tabs its owner shows to others (#829): told to every
/// reader, so a visitor's app leaves a hidden tab out instead of showing it
/// empty.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct VisibleTabs {
    pub likes:   bool,
    pub saved:   bool,
    pub reposts: bool,
    pub places:  bool,
}

impl TabSettings {
    /// The tabs others see.
    pub fn visible(&self) -> VisibleTabs {
        VisibleTabs { likes: self.show_likes, saved: self.show_saved, reposts: self.show_reposts, places: self.show_places }
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
    fn defaults_round_trip_and_windows_have_their_length() {
        assert_eq!(TabSettings::from_json(None), TabSettings::default());
        assert!(!TabSettings::default().show_saved, "saved is private by default");
        let s = TabSettings { post_window: PostWindow::OneMonth, show_likes: false, ..TabSettings::default() };
        assert_eq!(TabSettings::from_json(Some(&s.to_json())), s);
        assert_eq!(PostWindow::All.days(), None);
        assert_eq!(PostWindow::ThreeDays.days(), Some(3));
    }
}
