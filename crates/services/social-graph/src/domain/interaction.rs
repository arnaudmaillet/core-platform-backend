//! Who may interact with a profile (#656): the owner's per-kind audience,
//! projected from profile, decided against the follow graph and blocks.

use serde::{Deserialize, Serialize};

use crate::domain::aggregate::Relation;

/// An interaction a profile's owner controls.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InteractionKind {
    /// Commenting on the target's posts.
    Comment,
    /// Mentioning or tagging the target.
    Mention,
    /// Messaging the target directly.
    Message,
}

/// Who may do it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InteractionAudience {
    #[default]
    Everyone,
    /// Profiles that follow the target.
    Followers,
    /// Profiles the target follows back.
    Mutuals,
    NoOne,
}

impl InteractionAudience {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "everyone" => Some(Self::Everyone),
            "followers" => Some(Self::Followers),
            "mutuals" => Some(Self::Mutuals),
            "no_one" => Some(Self::NoOne),
            _ => None,
        }
    }

    /// Does this audience take a profile that follows the owner (`follows`)
    /// and that the owner follows back (`followed_back`)?
    pub fn admits(self, follows: bool, followed_back: bool) -> bool {
        match self {
            Self::Everyone => true,
            Self::Followers => follows,
            Self::Mutuals => follows && followed_back,
            Self::NoOne => false,
        }
    }
}

/// A profile's interaction policy; absent ⇒ everyone for everything.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct InteractionPolicy {
    pub comments: InteractionAudience,
    pub mentions: InteractionAudience,
    pub messages: InteractionAudience,
}

impl InteractionPolicy {
    pub fn audience(&self, kind: InteractionKind) -> InteractionAudience {
        match kind {
            InteractionKind::Comment => self.comments,
            InteractionKind::Mention => self.mentions,
            InteractionKind::Message => self.messages,
        }
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }

    pub fn from_json(json: Option<&str>) -> Self {
        json.and_then(|j| serde_json::from_str(j).ok()).unwrap_or_default()
    }
}

/// May the relation's actor do `kind` to its target, whose policy is `policy`?
/// A block either way always refuses; then the target's audience decides.
pub fn may_interact(relation: &Relation, policy: &InteractionPolicy, kind: InteractionKind) -> bool {
    if relation.actor_blocks_target() || relation.target_blocks_actor() {
        return false;
    }
    policy.audience(kind).admits(
        relation.actor_follows_target_since().is_some(),
        relation.target_follows_actor_since().is_some(),
    )
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use uuid::Uuid;

    use super::*;
    use crate::domain::aggregate::RelationContext;
    use crate::domain::value_object::ProfileId;

    fn relation(follows: bool, followed_back: bool, blocked: bool) -> Relation {
        Relation::from_context(
            ProfileId::from_uuid(Uuid::now_v7()),
            ProfileId::from_uuid(Uuid::now_v7()),
            RelationContext {
                actor_follows_target_since: follows.then(Utc::now),
                target_follows_actor_since: followed_back.then(Utc::now),
                actor_blocks_target: false,
                target_blocks_actor: blocked,
                actor_requested_target_at: None,
                target_requested_actor_at: None,
            },
        )
    }

    fn only(audience: InteractionAudience) -> InteractionPolicy {
        InteractionPolicy { comments: audience, ..InteractionPolicy::default() }
    }

    #[test]
    fn each_audience_lets_in_exactly_who_it_names() {
        let c = InteractionKind::Comment;
        let (stranger, follower, mutual) =
            (relation(false, false, false), relation(true, false, false), relation(true, true, false));
        let everyone = only(InteractionAudience::Everyone);
        assert!(may_interact(&stranger, &everyone, c));
        let followers = only(InteractionAudience::Followers);
        assert!(!may_interact(&stranger, &followers, c));
        assert!(may_interact(&follower, &followers, c));
        let mutuals = only(InteractionAudience::Mutuals);
        assert!(!may_interact(&follower, &mutuals, c));
        assert!(may_interact(&mutual, &mutuals, c));
        assert!(!may_interact(&mutual, &only(InteractionAudience::NoOne), c));
        // Other kinds keep their own (default) audience.
        assert!(may_interact(&stranger, &followers, InteractionKind::Message));
    }

    #[test]
    fn a_block_always_refuses() {
        assert!(!may_interact(&relation(true, true, true), &InteractionPolicy::default(), InteractionKind::Comment));
    }

    #[test]
    fn the_stored_form_round_trips_and_absent_is_everyone() {
        let p = InteractionPolicy { messages: InteractionAudience::Mutuals, ..InteractionPolicy::default() };
        assert_eq!(InteractionPolicy::from_json(Some(&p.to_json())), p);
        assert_eq!(InteractionPolicy::from_json(None), InteractionPolicy::default());
    }
}
