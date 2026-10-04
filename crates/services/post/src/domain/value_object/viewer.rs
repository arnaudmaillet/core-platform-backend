use super::{ModerationRestriction, PostStatus, ProfileId};

/// Who is reading a post. Decides what a reader who is not the author may see;
/// taken from how the request arrived (`transport::grpc::edge::viewer`), never
/// from a request field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Viewer {
    /// A trusted in-cluster caller (the mesh): sees every post as stored.
    Internal,
    /// A client with no identity: sees only published posts.
    Anonymous,
    /// A client acting for the profiles its account owns: sees everything those
    /// profiles authored, and only published posts of anyone else.
    Profiles(Vec<ProfileId>),
}

impl Viewer {
    /// `true` when `author` is one of the viewer's own profiles.
    pub fn is_author(&self, author: &ProfileId) -> bool {
        match self {
            Self::Profiles(ids) => ids.contains(author),
            Self::Internal | Self::Anonymous => false,
        }
    }

    /// `true` when the viewer sees `author`'s posts whatever their status: the
    /// author itself, or a trusted internal caller.
    pub fn sees_every_post_of(&self, author: &ProfileId) -> bool {
        matches!(self, Self::Internal) || self.is_author(author)
    }

    /// Whether a post by `author` in `status`, under `restriction`, is visible to
    /// this viewer. Drafts, deleted posts and posts moderation removed belong to
    /// their author alone.
    pub fn may_see(
        &self,
        author: &ProfileId,
        status: PostStatus,
        restriction: ModerationRestriction,
    ) -> bool {
        self.sees_every_post_of(author)
            || (status == PostStatus::Published && restriction != ModerationRestriction::Removed)
    }

    /// [`Self::may_see`], and an age-gated post only for a reader cleared for
    /// mature content (`mature`: not a guest, not 13–17) — or its author.
    pub fn may_see_rated(
        &self,
        author: &ProfileId,
        status: PostStatus,
        restriction: ModerationRestriction,
        mature: bool,
    ) -> bool {
        self.may_see(author, status, restriction)
            && (mature || restriction != ModerationRestriction::AgeGated || self.sees_every_post_of(author))
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;

    fn profile() -> ProfileId {
        ProfileId::from_uuid(Uuid::now_v7())
    }

    const NONE: ModerationRestriction = ModerationRestriction::None;

    #[test]
    fn published_posts_are_visible_to_everyone() {
        let author = profile();
        for viewer in [Viewer::Internal, Viewer::Anonymous, Viewer::Profiles(vec![profile()])] {
            assert!(viewer.may_see(&author, PostStatus::Published, NONE), "{viewer:?}");
            // Limited / age-gated posts stay readable; discovery applies those.
            for r in [ModerationRestriction::Limited, ModerationRestriction::AgeGated] {
                assert!(viewer.may_see(&author, PostStatus::Published, r), "{viewer:?} {r:?}");
            }
        }
    }

    #[test]
    fn an_age_gated_post_is_kept_from_readers_not_cleared_for_mature_content() {
        let author = profile();
        let gated = ModerationRestriction::AgeGated;
        let reader = Viewer::Profiles(vec![profile()]);
        assert!(reader.may_see_rated(&author, PostStatus::Published, gated, true), "an adult");
        assert!(!reader.may_see_rated(&author, PostStatus::Published, gated, false), "13–17 / guest");
        assert!(!Viewer::Anonymous.may_see_rated(&author, PostStatus::Published, gated, false));
        assert!(Viewer::Profiles(vec![author.clone()]).may_see_rated(&author, PostStatus::Published, gated, false), "the author");
        assert!(reader.may_see_rated(&author, PostStatus::Published, NONE, false), "other posts unaffected");
    }

    #[test]
    fn a_removed_post_belongs_to_its_author() {
        let author = profile();
        let removed = ModerationRestriction::Removed;
        assert!(!Viewer::Anonymous.may_see(&author, PostStatus::Published, removed));
        assert!(!Viewer::Profiles(vec![profile()]).may_see(&author, PostStatus::Published, removed));
        assert!(Viewer::Profiles(vec![author.clone()]).may_see(&author, PostStatus::Published, removed));
        assert!(Viewer::Internal.may_see(&author, PostStatus::Published, removed));
    }

    #[test]
    fn drafts_and_deleted_posts_belong_to_their_author() {
        let author = profile();
        let other = profile();
        for status in [PostStatus::Draft, PostStatus::Deleted] {
            assert!(!Viewer::Anonymous.may_see(&author, status, NONE));
            assert!(!Viewer::Profiles(vec![other.clone()]).may_see(&author, status, NONE));
            assert!(Viewer::Profiles(vec![other.clone(), author.clone()]).may_see(&author, status, NONE));
            assert!(Viewer::Internal.may_see(&author, status, NONE), "the mesh is trusted");
        }
    }
}
