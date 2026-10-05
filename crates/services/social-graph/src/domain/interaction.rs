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

/// Whom a temporary interaction limit holds back (#669).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LimitAudience {
    /// Profiles that do not follow the target.
    NonFollowers,
    /// Non-followers and profiles that followed less than [`RECENT_FOLLOW`] ago.
    RecentFollowers,
}

impl LimitAudience {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "non_followers" => Some(Self::NonFollowers),
            "recent_followers" => Some(Self::RecentFollowers),
            _ => None,
        }
    }
}

/// How recent a follow is still "recent" for [`LimitAudience::RecentFollowers`].
pub const RECENT_FOLLOW: chrono::Duration = chrono::Duration::days(7);

/// A temporary interaction limit (#669): until `until_ms`, comments and
/// messages from `audience` are held for the target's review instead of shown.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct InteractionLimit {
    pub audience: LimitAudience,
    pub until_ms: i64,
}

/// A profile's interaction policy; absent ⇒ everyone for everything, no limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct InteractionPolicy {
    pub comments: InteractionAudience,
    pub mentions: InteractionAudience,
    pub messages: InteractionAudience,
    pub limit:    Option<InteractionLimit>,
}

/// The answer to "may the actor do this?".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InteractionVerdict {
    Allowed,
    /// Accepted, but held for the target's review (a limit is on, #669).
    Held,
    Refused(Refusal),
}

impl InteractionVerdict {
    pub fn is_refused(self) -> bool {
        matches!(self, Self::Refused(_))
    }
}

/// Why an interaction is refused. The owning service decides what the actor
/// learns: chat turns an [`Audience`](Self::Audience) refusal into a message
/// request (#656), and must never tell a blocked actor they are blocked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refusal {
    /// A block, either way.
    Blocked,
    /// The target's audience for this kind is no one.
    NoOne,
    /// The target's audience (followers / mutuals) excludes the actor.
    Audience,
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

/// [`may_interact`], then the target's temporary limit: while it is on (at
/// `now`), a comment or message from its audience is held, not refused.
pub fn interaction_verdict(
    relation: &Relation,
    policy: &InteractionPolicy,
    kind: InteractionKind,
    now: chrono::DateTime<chrono::Utc>,
) -> InteractionVerdict {
    if let Some(refusal) = refusal(relation, policy, kind) {
        return InteractionVerdict::Refused(refusal);
    }
    let Some(limit) = policy.limit.filter(|l| now.timestamp_millis() < l.until_ms) else {
        return InteractionVerdict::Allowed;
    };
    if !matches!(kind, InteractionKind::Comment | InteractionKind::Message) {
        return InteractionVerdict::Allowed;
    }
    let held = match (limit.audience, relation.actor_follows_target_since()) {
        (_, None) => true,
        (LimitAudience::NonFollowers, Some(_)) => false,
        (LimitAudience::RecentFollowers, Some(since)) => now - since < RECENT_FOLLOW,
    };
    if held { InteractionVerdict::Held } else { InteractionVerdict::Allowed }
}

/// May the relation's actor do `kind` to its target, whose policy is `policy`?
/// A block either way always refuses; then the target's audience decides.
pub fn may_interact(relation: &Relation, policy: &InteractionPolicy, kind: InteractionKind) -> bool {
    refusal(relation, policy, kind).is_none()
}

/// Why the relation's actor may not do `kind` to its target, if it may not.
pub fn refusal(relation: &Relation, policy: &InteractionPolicy, kind: InteractionKind) -> Option<Refusal> {
    if relation.actor_blocks_target() || relation.target_blocks_actor() {
        return Some(Refusal::Blocked);
    }
    let audience = policy.audience(kind);
    if audience == InteractionAudience::NoOne {
        return Some(Refusal::NoOne);
    }
    let admitted =
        audience.admits(relation.actor_follows_target_since().is_some(), relation.target_follows_actor_since().is_some());
    (!admitted).then_some(Refusal::Audience)
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
    fn a_limit_holds_its_audiences_comments_and_messages_until_it_ends() {
        let now = Utc::now();
        let follower_since = |days: i64| {
            Relation::from_context(
                ProfileId::from_uuid(Uuid::now_v7()),
                ProfileId::from_uuid(Uuid::now_v7()),
                RelationContext {
                    actor_follows_target_since: Some(now - chrono::Duration::days(days)),
                    target_follows_actor_since: None,
                    actor_blocks_target: false,
                    target_blocks_actor: false,
                    actor_requested_target_at: None,
                    target_requested_actor_at: None,
                },
            )
        };
        let stranger = relation(false, false, false);
        let limited = |audience| InteractionPolicy {
            limit: Some(InteractionLimit { audience, until_ms: (now + chrono::Duration::days(1)).timestamp_millis() }),
            ..InteractionPolicy::default()
        };
        let c = InteractionKind::Comment;

        let non_followers = limited(LimitAudience::NonFollowers);
        assert_eq!(interaction_verdict(&stranger, &non_followers, c, now), InteractionVerdict::Held);
        assert_eq!(interaction_verdict(&follower_since(1), &non_followers, c, now), InteractionVerdict::Allowed);
        assert_eq!(interaction_verdict(&stranger, &non_followers, InteractionKind::Message, now), InteractionVerdict::Held);
        assert_eq!(interaction_verdict(&stranger, &non_followers, InteractionKind::Mention, now), InteractionVerdict::Allowed);

        let recent = limited(LimitAudience::RecentFollowers);
        assert_eq!(interaction_verdict(&follower_since(1), &recent, c, now), InteractionVerdict::Held);
        assert_eq!(interaction_verdict(&follower_since(30), &recent, c, now), InteractionVerdict::Allowed);

        // Expired: allowed again. A block still refuses.
        let later = now + chrono::Duration::days(2);
        assert_eq!(interaction_verdict(&stranger, &non_followers, c, later), InteractionVerdict::Allowed);
        assert_eq!(
            interaction_verdict(&relation(false, false, true), &non_followers, c, now),
            InteractionVerdict::Refused(Refusal::Blocked)
        );
    }

    #[test]
    fn a_refusal_says_why_a_block_first() {
        let m = InteractionKind::Message;
        let policy = |messages| InteractionPolicy { messages, ..InteractionPolicy::default() };
        let (stranger, follower) = (relation(false, false, false), relation(true, false, false));
        assert_eq!(refusal(&stranger, &policy(InteractionAudience::Followers), m), Some(Refusal::Audience));
        assert_eq!(refusal(&follower, &policy(InteractionAudience::Mutuals), m), Some(Refusal::Audience));
        assert_eq!(refusal(&follower, &policy(InteractionAudience::Followers), m), None);
        assert_eq!(refusal(&relation(true, true, false), &policy(InteractionAudience::NoOne), m), Some(Refusal::NoOne));
        // A block outranks every audience, "no one" included.
        assert_eq!(refusal(&relation(true, true, true), &policy(InteractionAudience::NoOne), m), Some(Refusal::Blocked));
        assert_eq!(refusal(&relation(true, true, true), &policy(InteractionAudience::Everyone), m), Some(Refusal::Blocked));
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
