use std::sync::Arc;

use cqrs::{Command, CommandHandler, Envelope};
use validate_core::{FieldViolation, Validate};

use crate::{
    application::port::PostRepository,
    domain::value_object::{ModerationRestriction, PostId},
    error::PostError,
};

/// Records a moderation outcome on a post: an enforcement applied
/// (`restriction` ≠ None) or reversed (`restriction` = None). Issued by the
/// `moderation.v1.events` consumer, never by a client.
#[derive(Debug, PartialEq, Eq)]
pub struct ApplyModerationCommand {
    pub post_id:     String,
    pub restriction: ModerationRestriction,
    /// Moderation's per-subject `EnforcementVersion`.
    pub version:     i64,
}

impl Command for ApplyModerationCommand {}

impl Validate for ApplyModerationCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        if self.post_id.trim().is_empty() {
            return Err(vec![FieldViolation::new("post_id", "PST-VAL-001", "post_id must not be empty")]);
        }
        Ok(())
    }
}

pub struct ApplyModerationHandler<R> {
    pub repository: Arc<R>,
}

impl<R: PostRepository> CommandHandler<ApplyModerationCommand> for ApplyModerationHandler<R> {
    type Error = PostError;

    /// Idempotent: a post that does not exist (an id moderation knows but post
    /// never stored) or an event older than the state held is a no-op.
    ///
    /// The read-then-write guard is safe without an LWT because moderation keys
    /// its events by `actor_id`: every enforcement on one post (one author) lands
    /// on one partition, and the consumer processes a partition sequentially.
    async fn handle(&self, envelope: Envelope<ApplyModerationCommand>) -> Result<(), PostError> {
        let cmd     = &envelope.payload;
        let post_id = PostId::try_from(cmd.post_id.as_str())?;

        let Some(mut post) = self.repository.find_by_id(&post_id).await? else {
            tracing::info!(post_id = %cmd.post_id, "moderation outcome for an unknown post; skipped");
            return Ok(());
        };
        let changed = post.apply_moderation(cmd.restriction, cmd.version);
        // The same event redelivered: rewrite both tables anyway. The two UPDATEs
        // are not atomic, so a retry after "posts written, posts_by_profile failed"
        // finds posts already at this version; skipping it would leave the list
        // table on the old restriction for good (a removed post still listed).
        let redelivered = !changed && post.moderation().version == cmd.version;
        if changed || redelivered {
            self.repository.update_moderation(&post).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;
    use uuid::Uuid;

    use super::*;
    use crate::application::port::PostSummary;
    use crate::domain::aggregate::Post;
    use crate::domain::value_object::{Caption, PostKind, ProfileId};

    /// One stored post; counts `update_moderation` calls.
    struct OnePost {
        post:    Mutex<Option<Post>>,
        updates: Mutex<u32>,
    }

    #[async_trait]
    impl PostRepository for OnePost {
        async fn insert(&self, _: &Post) -> Result<(), PostError> { unreachable!() }
        async fn update_content(&self, _: &Post) -> Result<(), PostError> { unreachable!() }
        async fn update_lifecycle(&self, _: &Post) -> Result<(), PostError> { unreachable!() }
        async fn update_moderation(&self, post: &Post) -> Result<(), PostError> {
            *self.updates.lock().unwrap() += 1;
            let mut stored = self.post.lock().unwrap();
            stored.as_mut().unwrap().apply_moderation(post.moderation().restriction, post.moderation().version);
            Ok(())
        }
        async fn find_by_id(&self, _: &PostId) -> Result<Option<Post>, PostError> {
            let stored = self.post.lock().unwrap();
            Ok(stored.as_ref().map(|p| {
                let mut copy = Post::create(
                    p.id().clone(), p.profile_id().clone(), p.kind(), p.caption().clone(),
                    vec![], None, None, None, None,
                ).unwrap();
                copy.apply_moderation(p.moderation().restriction, p.moderation().version);
                copy
            }))
        }
        async fn list_by_profile(
            &self, _: &ProfileId, _: i32, _: Option<&str>,
        ) -> Result<(Vec<PostSummary>, Option<String>), PostError> { unreachable!() }
    }

    fn repo_with_post() -> (Arc<OnePost>, String) {
        let id = PostId::from_uuid(Uuid::now_v7());
        let post = Post::create(
            id.clone(), ProfileId::from_uuid(Uuid::now_v7()), PostKind::TextOnly,
            Caption::new("hello").unwrap(), vec![], None, None, None, None,
        ).unwrap();
        (Arc::new(OnePost { post: Mutex::new(Some(post)), updates: Mutex::new(0) }), id.as_str())
    }

    async fn apply(repo: &Arc<OnePost>, post_id: &str, restriction: ModerationRestriction, version: i64) {
        let handler = ApplyModerationHandler { repository: Arc::clone(repo) };
        let cmd = ApplyModerationCommand { post_id: post_id.into(), restriction, version };
        handler.handle(Envelope::new(Uuid::now_v7(), cmd)).await.unwrap();
    }

    #[tokio::test]
    async fn a_redelivered_event_rewrites_both_tables_and_a_stale_one_does_not() {
        let (repo, post_id) = repo_with_post();

        apply(&repo, &post_id, ModerationRestriction::Removed, 1).await;
        assert_eq!(*repo.updates.lock().unwrap(), 1);

        // Redelivery after a partial write: posts already holds v1, yet the
        // handler rewrites so posts_by_profile catches up.
        apply(&repo, &post_id, ModerationRestriction::Removed, 1).await;
        assert_eq!(*repo.updates.lock().unwrap(), 2);

        // The reversal (v2), then the stale v1 again: nothing to write.
        apply(&repo, &post_id, ModerationRestriction::None, 2).await;
        apply(&repo, &post_id, ModerationRestriction::Removed, 1).await;
        assert_eq!(*repo.updates.lock().unwrap(), 3);
    }

    #[tokio::test]
    async fn an_unknown_post_is_a_no_op() {
        let repo = Arc::new(OnePost { post: Mutex::new(None), updates: Mutex::new(0) });
        apply(&repo, &Uuid::now_v7().to_string(), ModerationRestriction::Removed, 1).await;
        assert_eq!(*repo.updates.lock().unwrap(), 0);
    }
}
