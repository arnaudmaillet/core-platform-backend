use async_trait::async_trait;

use crate::domain::value_object::{ContentAccess, ProfileId, Viewer};
use crate::error::PostError;

/// The audience check, owned by social-graph (`CheckAccess`): what a reader's
/// profiles may see of an author's content.
#[async_trait]
pub trait AudienceGate: Send + Sync + 'static {
    /// `viewers` are the reader's profiles (empty when anonymous). Errors are
    /// `AccessCheckUnavailable`; callers fail closed.
    async fn access(&self, viewers: &[ProfileId], author: &ProfileId) -> Result<ContentAccess, PostError>;

    /// May `author` mention `mentioned`? Their "who can mention me" setting
    /// (#656, social-graph `CheckInteraction`, a block either way refuses).
    /// Errors are `AccessCheckUnavailable`; the write fails closed.
    async fn may_mention(&self, author: &ProfileId, mentioned: &ProfileId) -> Result<bool, PostError>;
}

/// Refuses a caption mentioning more than [`MAX_MENTIONS`] profiles, or one
/// that does not take mentions from `author` (#656). Mentioning oneself is
/// always fine.
pub async fn check_mentions(
    gate: &dyn AudienceGate,
    author: &ProfileId,
    caption: &crate::domain::value_object::Caption,
) -> Result<(), PostError> {
    let mentioned: Vec<ProfileId> = caption.mentions().into_iter().filter(|m| m != author).collect();
    if mentioned.len() > crate::domain::value_object::MAX_MENTIONS {
        return Err(PostError::DomainViolation {
            field:   "caption".into(),
            message: format!("a caption mentions at most {} profiles", crate::domain::value_object::MAX_MENTIONS),
        });
    }
    for profile in &mentioned {
        if !gate.may_mention(author, profile).await? {
            return Err(PostError::MentionNotAllowed { profile_id: profile.as_str() });
        }
    }
    Ok(())
}

/// Whether `viewer` may see `author`'s posts at all. The author itself and a
/// trusted internal caller skip the check (no network hop); anyone else is
/// checked, anonymous readers included (private and hidden authors).
pub async fn author_visible_to(
    gate: &dyn AudienceGate,
    viewer: &Viewer,
    author: &ProfileId,
) -> Result<bool, PostError> {
    if viewer.sees_every_post_of(author) {
        return Ok(true);
    }
    let viewers: &[ProfileId] = match viewer {
        Viewer::Profiles(ids) => ids,
        Viewer::Anonymous | Viewer::Internal => &[],
    };
    Ok(gate.access(viewers, author).await? == ContentAccess::Visible)
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use uuid::Uuid;

    use super::*;

    /// Answers `answer`, or fails; records the viewers it was asked about.
    struct Gate {
        answer: Option<ContentAccess>,
        asked:  Mutex<Vec<Vec<ProfileId>>>,
    }

    #[async_trait]
    impl AudienceGate for Gate {
        async fn access(&self, viewers: &[ProfileId], _: &ProfileId) -> Result<ContentAccess, PostError> {
            self.asked.lock().unwrap().push(viewers.to_vec());
            self.answer.ok_or(PostError::AccessCheckUnavailable { reason: "down".into() })
        }

        async fn may_mention(&self, _: &ProfileId, _: &ProfileId) -> Result<bool, PostError> {
            self.answer.map(|_| true).ok_or(PostError::AccessCheckUnavailable { reason: "down".into() })
        }
    }

    fn gate(answer: Option<ContentAccess>) -> Gate {
        Gate { answer, asked: Mutex::new(Vec::new()) }
    }

    fn id() -> ProfileId {
        ProfileId::from_uuid(Uuid::now_v7())
    }

    #[tokio::test]
    async fn the_author_and_the_mesh_never_call_the_check() {
        let (author, down) = (id(), gate(None));
        assert!(author_visible_to(&down, &Viewer::Internal, &author).await.unwrap());
        assert!(author_visible_to(&down, &Viewer::Profiles(vec![author.clone()]), &author).await.unwrap());
        assert!(down.asked.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn others_are_checked_and_only_visible_opens() {
        let (author, reader) = (id(), id());
        for (answer, open) in [
            (ContentAccess::Visible, true),
            (ContentAccess::HeaderOnly, false),
            (ContentAccess::Hidden, false),
        ] {
            let g = gate(Some(answer));
            let viewer = Viewer::Profiles(vec![reader.clone()]);
            assert_eq!(author_visible_to(&g, &viewer, &author).await.unwrap(), open, "{answer:?}");
            assert_eq!(g.asked.lock().unwrap()[0], vec![reader.clone()]);
        }
        // Anonymous readers are checked too, with no profiles.
        let g = gate(Some(ContentAccess::HeaderOnly));
        assert!(!author_visible_to(&g, &Viewer::Anonymous, &author).await.unwrap());
        assert!(g.asked.lock().unwrap()[0].is_empty());
    }

    #[tokio::test]
    async fn an_unavailable_check_is_an_error_not_an_open_door() {
        let err = author_visible_to(&gate(None), &Viewer::Anonymous, &id()).await.unwrap_err();
        assert!(matches!(err, PostError::AccessCheckUnavailable { .. }));
    }
}
