//! Who may see a profile's follower and following lists (#659): the owner's
//! per-list audience, stored by social-graph, enforced by `ListFollowers` /
//! `ListFollowing` on top of the access rule (private profile, blocks).

use serde::{Deserialize, Serialize};

use crate::domain::interaction::InteractionAudience;

/// One of a profile's relationship lists.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FollowList {
    /// Who follows the profile.
    Followers,
    /// Whom the profile follows.
    Following,
}

/// A profile's list audiences (the same audiences as interactions:
/// everyone | followers | mutuals | no_one, where no one leaves the owner
/// alone). Absent ⇒ everyone for both.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ListPrivacy {
    pub followers: InteractionAudience,
    pub following: InteractionAudience,
}

impl ListPrivacy {
    pub fn audience(&self, list: FollowList) -> InteractionAudience {
        match list {
            FollowList::Followers => self.followers,
            FollowList::Following => self.following,
        }
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
    fn the_stored_form_round_trips_and_absent_is_everyone() {
        let p = ListPrivacy { following: InteractionAudience::NoOne, ..ListPrivacy::default() };
        assert_eq!(ListPrivacy::from_json(Some(&p.to_json())), p);
        assert_eq!(p.audience(FollowList::Followers), InteractionAudience::Everyone);
        assert_eq!(p.audience(FollowList::Following), InteractionAudience::NoOne);
        assert_eq!(ListPrivacy::from_json(None), ListPrivacy::default());
        assert_eq!(ListPrivacy::from_json(Some("{}")), ListPrivacy::default());
    }
}
