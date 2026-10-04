//! Notification preferences (#654): push and email per category, a pause, and
//! quiet hours in the holder's time zone. The push sender asks
//! [`NotificationPreferences::push_allowed`] before sending anything.
//!
//! Marketing email is not here: it is the account's `marketing` consent
//! (`account.v1.UpdateConsents`), one source of truth.

use std::collections::BTreeSet;

use chrono::{DateTime, Duration, Timelike, Utc};
use chrono_tz::Tz;
use serde::{Deserialize, Serialize};

/// What a push or an email is about.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PushCategory {
    Likes,
    Comments,
    Mentions,
    NewFollowers,
    FollowRequests,
    Messages,
    /// New posts from accounts the holder follows.
    FollowedPosts,
    PlacesNearby,
    Wallet,
}

impl PushCategory {
    pub const ALL: [Self; 9] = [
        Self::Likes,
        Self::Comments,
        Self::Mentions,
        Self::NewFollowers,
        Self::FollowRequests,
        Self::Messages,
        Self::FollowedPosts,
        Self::PlacesNearby,
        Self::Wallet,
    ];
}

/// A daily window, in minutes after local midnight; it may wrap midnight
/// (22:00–07:00). Equal ends make an empty window.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuietHours {
    pub start_minute: u16,
    pub end_minute:   u16,
}

impl QuietHours {
    /// 22:00–07:00, on by default for 13–17.
    pub const TEEN: Self = Self { start_minute: 22 * 60, end_minute: 7 * 60 };

    pub fn new(start_minute: u16, end_minute: u16) -> Option<Self> {
        (start_minute < 24 * 60 && end_minute < 24 * 60).then_some(Self { start_minute, end_minute })
    }

    pub fn contains(&self, minute: u16) -> bool {
        let (s, e) = (self.start_minute, self.end_minute);
        if s < e {
            s <= minute && minute < e
        } else if s > e {
            minute >= s || minute < e
        } else {
            false
        }
    }
}

/// The longest pause the holder may ask for (the app offers 15 min – 8 h).
pub const MAX_PAUSE: Duration = Duration::hours(8);

/// A holder's preferences. The defaults: every push on, every email off, no
/// pause, no quiet hours (teens: [`Self::teen`]).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct NotificationPreferences {
    /// Categories whose push is off.
    pub push_off:     BTreeSet<PushCategory>,
    /// Categories whose email is on.
    pub email_on:     BTreeSet<PushCategory>,
    pub paused_until: Option<DateTime<Utc>>,
    pub quiet_hours:  Option<QuietHours>,
    /// IANA zone the quiet hours are read in (UTC when unknown).
    pub timezone:     Option<String>,
}

impl NotificationPreferences {
    /// The defaults for a 13–17 holder: quiet hours 22:00–07:00.
    pub fn teen() -> Self {
        Self { quiet_hours: Some(QuietHours::TEEN), ..Self::default() }
    }

    /// The defaults for a holder with nothing stored.
    pub fn defaults(minor: bool) -> Self {
        if minor { Self::teen() } else { Self::default() }
    }

    pub fn push_on(&self, category: PushCategory) -> bool {
        !self.push_off.contains(&category)
    }

    pub fn email_on(&self, category: PushCategory) -> bool {
        self.email_on.contains(&category)
    }

    pub fn set_push(&mut self, category: PushCategory, on: bool) {
        if on { self.push_off.remove(&category); } else { self.push_off.insert(category); }
    }

    pub fn set_email(&mut self, category: PushCategory, on: bool) {
        if on { self.email_on.insert(category); } else { self.email_on.remove(&category); }
    }

    /// May a push about `category` go out at `now`? Its category is on, no
    /// pause is running, and `now` is outside the quiet hours (local time).
    pub fn push_allowed(&self, category: PushCategory, now: DateTime<Utc>) -> bool {
        if !self.push_on(category) {
            return false;
        }
        if self.paused_until.is_some_and(|until| now < until) {
            return false;
        }
        match self.quiet_hours {
            Some(quiet) => !quiet.contains(self.local_minute(now)),
            None => true,
        }
    }

    fn local_minute(&self, now: DateTime<Utc>) -> u16 {
        let tz: Tz = self.timezone.as_deref().and_then(|z| z.parse().ok()).unwrap_or(Tz::UTC);
        let local = now.with_timezone(&tz);
        (local.hour() * 60 + local.minute()) as u16
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }

    pub fn from_json(json: &str) -> Option<Self> {
        serde_json::from_str(json).ok()
    }
}

/// `zone` is a known IANA time zone.
pub fn is_time_zone(zone: &str) -> bool {
    zone.parse::<Tz>().is_ok()
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    fn at(h: u32, m: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 1, 15, h, m, 0).unwrap()
    }

    #[test]
    fn a_category_turned_off_stops_its_push_only() {
        let mut p = NotificationPreferences::default();
        assert!(PushCategory::ALL.iter().all(|c| p.push_allowed(*c, at(12, 0))));
        p.set_push(PushCategory::Likes, false);
        assert!(!p.push_allowed(PushCategory::Likes, at(12, 0)));
        assert!(p.push_allowed(PushCategory::Comments, at(12, 0)));
        p.set_push(PushCategory::Likes, true);
        assert!(p.push_allowed(PushCategory::Likes, at(12, 0)));
    }

    #[test]
    fn a_pause_holds_every_push_until_it_ends() {
        let p = NotificationPreferences { paused_until: Some(at(13, 0)), ..Default::default() };
        assert!(!p.push_allowed(PushCategory::Messages, at(12, 59)));
        assert!(p.push_allowed(PushCategory::Messages, at(13, 0)));
    }

    #[test]
    fn quiet_hours_wrap_midnight_in_the_holders_zone() {
        let mut p = NotificationPreferences::teen();
        assert!(!p.push_allowed(PushCategory::Likes, at(23, 0)), "22:00–07:00 UTC");
        assert!(!p.push_allowed(PushCategory::Likes, at(6, 59)));
        assert!(p.push_allowed(PushCategory::Likes, at(7, 0)));
        // Paris is UTC+1 in January: 21:30 UTC is 22:30 there.
        p.timezone = Some("Europe/Paris".into());
        assert!(!p.push_allowed(PushCategory::Likes, at(21, 30)));
        assert!(p.push_allowed(PushCategory::Likes, at(6, 30)), "07:30 in Paris");
    }

    #[test]
    fn teens_get_quiet_hours_by_default_and_emails_start_off() {
        assert_eq!(NotificationPreferences::defaults(true).quiet_hours, Some(QuietHours::TEEN));
        assert_eq!(NotificationPreferences::defaults(false).quiet_hours, None);
        assert!(PushCategory::ALL.iter().all(|c| !NotificationPreferences::default().email_on(*c)));
    }

    #[test]
    fn the_stored_form_round_trips() {
        let mut p = NotificationPreferences::teen();
        p.set_push(PushCategory::Wallet, false);
        p.set_email(PushCategory::Mentions, true);
        p.timezone = Some("America/New_York".into());
        assert_eq!(NotificationPreferences::from_json(&p.to_json()), Some(p));
        assert!(is_time_zone("Europe/Paris"));
        assert!(!is_time_zone("Mars/Olympus"));
    }
}
