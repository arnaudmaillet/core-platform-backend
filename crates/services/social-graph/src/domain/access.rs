//! Who may see whose content: the audience rule every viewer-aware read in the
//! fleet applies (post, comment, social-graph's own lists), decided here because
//! social-graph owns the follows and blocks it depends on.

use std::collections::HashSet;

use crate::domain::value_object::ProfileId;

/// Who is reading, taken from how the request arrived
/// (`transport::grpc::edge::viewer`), never from a request field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Viewer {
    /// A trusted in-cluster caller (the mesh): sees everything.
    Internal,
    /// A client and the profiles its account owns (empty when anonymous).
    Profiles(Vec<ProfileId>),
}

impl Viewer {
    /// `true` when the viewer sees everything of `target`: the mesh, or the
    /// target's owner.
    pub fn sees_everything_of(&self, target: &ProfileId) -> bool {
        match self {
            Self::Internal => true,
            Self::Profiles(ids) => ids.contains(target),
        }
    }
}

/// What a viewer may see of a target profile's content.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentAccess {
    /// Posts, comments and lists are visible.
    Visible,
    /// A private profile the viewer does not follow: its header only, no
    /// content and no lists.
    HeaderOnly,
    /// Nothing: a block in either direction, or a profile hidden by
    /// moderation or an account suspension.
    Hidden,
}

/// The facts the rule needs, for one set of viewer profiles against a set of
/// targets. Loaded in bulk by the repository.
#[derive(Debug, Default, Clone)]
pub struct AccessFacts {
    /// `(viewer, target)` pairs where the viewer follows the target.
    pub follows: HashSet<(ProfileId, ProfileId)>,
    /// `(target, viewer)` pairs where the target follows the viewer back.
    pub followed_back: HashSet<(ProfileId, ProfileId)>,
    /// `(blocker, blockee)` pairs between a viewer and a target, either way.
    pub blocks:  HashSet<(ProfileId, ProfileId)>,
    /// Targets whose owner made them private.
    pub private: HashSet<ProfileId>,
    /// Targets hidden (moderation, account suspension or deletion).
    pub hidden:  HashSet<ProfileId>,
}

impl AccessFacts {
    /// The access `viewers` (the profiles a caller's account owns; empty for an
    /// anonymous caller) have to `target`'s content.
    pub fn access(&self, viewers: &[ProfileId], target: &ProfileId) -> ContentAccess {
        if viewers.contains(target) {
            return ContentAccess::Visible; // one's own profile
        }
        if self.hidden.contains(target) {
            return ContentAccess::Hidden;
        }
        let blocked = viewers.iter().any(|v| {
            self.blocks.contains(&(*v, *target)) || self.blocks.contains(&(*target, *v))
        });
        if blocked {
            return ContentAccess::Hidden;
        }
        if self.private.contains(target)
            && !viewers.iter().any(|v| self.follows.contains(&(*v, *target)))
        {
            return ContentAccess::HeaderOnly;
        }
        ContentAccess::Visible
    }

    /// How `viewers` relate to `target` (#657: an author's location audience).
    /// One's own profile counts as both.
    pub fn relation(&self, viewers: &[ProfileId], target: &ProfileId) -> Relationship {
        if viewers.contains(target) {
            return Relationship { follows: true, mutual: true };
        }
        let follows = |v: &ProfileId| self.follows.contains(&(*v, *target));
        Relationship {
            follows: viewers.iter().any(follows),
            mutual:  viewers.iter().any(|v| follows(v) && self.followed_back.contains(&(*target, *v))),
        }
    }
}

/// Whether some viewer profile follows the target, and whether some viewer
/// profile and the target follow each other.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Relationship {
    pub follows: bool,
    pub mutual:  bool,
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;

    fn id() -> ProfileId {
        ProfileId::from_uuid(Uuid::now_v7())
    }

    #[test]
    fn a_public_profile_is_visible_to_anyone() {
        let target = id();
        let facts = AccessFacts::default();
        assert_eq!(facts.access(&[], &target), ContentAccess::Visible);
        assert_eq!(facts.access(&[id()], &target), ContentAccess::Visible);
    }

    #[test]
    fn a_private_profile_shows_its_content_to_followers_only() {
        let (me, alt, target) = (id(), id(), id());
        let mut facts = AccessFacts::default();
        facts.private.insert(target);
        assert_eq!(facts.access(&[], &target), ContentAccess::HeaderOnly, "anonymous");
        assert_eq!(facts.access(&[me, alt], &target), ContentAccess::HeaderOnly);
        facts.follows.insert((alt, target));
        assert_eq!(facts.access(&[me, alt], &target), ContentAccess::Visible, "any own profile follows");
        assert_eq!(facts.access(&[target], &target), ContentAccess::Visible, "the owner");
    }

    #[test]
    fn a_block_either_way_hides_everything_even_from_a_follower() {
        let (me, target) = (id(), id());
        for block in [(me, target), (target, me)] {
            let mut facts = AccessFacts::default();
            facts.follows.insert((me, target));
            facts.blocks.insert(block);
            assert_eq!(facts.access(&[me], &target), ContentAccess::Hidden, "{block:?}");
        }
    }

    #[test]
    fn a_hidden_profile_is_hidden_from_everyone_but_its_owner() {
        let target = id();
        let mut facts = AccessFacts::default();
        facts.hidden.insert(target);
        assert_eq!(facts.access(&[], &target), ContentAccess::Hidden);
        assert_eq!(facts.access(&[id()], &target), ContentAccess::Hidden);
        assert_eq!(facts.access(&[target], &target), ContentAccess::Visible);
    }

    #[test]
    fn the_relation_needs_one_profile_following_and_followed_back() {
        let (a, b, target) = (id(), id(), id());
        let mut facts = AccessFacts::default();
        facts.follows.insert((a, target));
        facts.followed_back.insert((target, b));
        let r = facts.relation(&[a, b], &target);
        assert_eq!((r.follows, r.mutual), (true, false), "a follows, b is followed back: no single mutual");
        facts.followed_back.insert((target, a));
        assert!(facts.relation(&[a], &target).mutual);
        assert!(facts.relation(&[target], &target).mutual, "oneself");
        assert_eq!(facts.relation(&[], &target), Relationship::default());
    }
}
