use std::sync::Arc;

use cqrs::{Envelope, Query, QueryHandler};

use crate::application::port::{LikeVisibility, PostEngagementSnapshot, ScoreStore};
use crate::domain::value_object::PostId;
use crate::error::EngagementError;

/// Who reads a post's engagement: the mesh, or a client with its profiles
/// (none for a guest or an anonymous caller).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EngagementReader {
    Internal,
    Profiles(Vec<String>),
}

pub struct GetPostEngagementQuery {
    pub post_id: String,
    pub reader:  EngagementReader,
}

impl Query for GetPostEngagementQuery {
    type Response = PostEngagementSnapshot;
}

pub struct GetPostEngagementHandler<S> {
    pub score_store: Arc<S>,
    /// Likes withheld from anyone but the author when the author hides them
    /// (#809). `None`: not wired (no post endpoint) — nothing is withheld.
    pub likes:       Option<Arc<dyn LikeVisibility>>,
}

impl<S: ScoreStore> QueryHandler<GetPostEngagementQuery> for GetPostEngagementHandler<S> {
    type Error = EngagementError;

    async fn handle(
        &self,
        envelope: Envelope<GetPostEngagementQuery>,
    ) -> Result<PostEngagementSnapshot, EngagementError> {
        let query = &envelope.payload;
        let post_id = PostId::try_from(query.post_id.as_str())?;
        let mut snapshot = self.score_store.get_snapshot(&post_id).await?;

        if let (Some(likes), EngagementReader::Profiles(own)) = (&self.likes, &query.reader) {
            let withheld = match likes.of(&query.post_id).await {
                Ok(Some(post)) => post.hidden && !own.contains(&post.author_id),
                Ok(None) => false,
                Err(error) => {
                    tracing::warn!(%error, "like visibility unknown; likes withheld");
                    true
                }
            };
            if withheld {
                snapshot.reaction_scores.clear();
            }
        }
        Ok(snapshot)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use uuid::Uuid;

    use super::*;
    use crate::application::port::PostLikeVisibility;
    use crate::domain::value_object::{ProfileId, ReactionKind};

    struct Snapshots;

    #[async_trait]
    impl ScoreStore for Snapshots {
        async fn atomic_upsert_reaction(
            &self,
            _: &PostId,
            _: &ProfileId,
            _: ReactionKind,
            _: i64,
        ) -> Result<Option<(ReactionKind, i64)>, EngagementError> {
            unimplemented!()
        }
        async fn atomic_remove_reaction(&self, _: &PostId, _: &ProfileId) -> Result<Option<(ReactionKind, i64)>, EngagementError> {
            unimplemented!()
        }
        async fn incr_view(&self, _: &PostId) -> Result<(), EngagementError> { unimplemented!() }
        async fn incr_share(&self, _: &PostId) -> Result<(), EngagementError> { unimplemented!() }
        async fn incr_comment(&self, _: &PostId) -> Result<(), EngagementError> { unimplemented!() }
        async fn decr_comment(&self, _: &PostId) -> Result<(), EngagementError> { unimplemented!() }
        async fn get_snapshot(&self, _: &PostId) -> Result<PostEngagementSnapshot, EngagementError> {
            Ok(PostEngagementSnapshot {
                reaction_scores: HashMap::from([("heart".to_owned(), 7)]),
                view_count:      40,
                share_count:     2,
                comment_count:   3,
            })
        }
    }

    /// One post, by `author`, hiding likes or not; `down` fails every call.
    struct Post {
        author: String,
        hidden: bool,
        down:   bool,
        calls:  Mutex<usize>,
    }

    #[async_trait]
    impl LikeVisibility for Post {
        async fn of(&self, _: &str) -> Result<Option<PostLikeVisibility>, EngagementError> {
            *self.calls.lock().unwrap() += 1;
            if self.down {
                return Err(EngagementError::PostUnavailable { message: "down".into() });
            }
            Ok(Some(PostLikeVisibility { author_id: self.author.clone(), hidden: self.hidden }))
        }
    }

    async fn read(likes: Option<Arc<Post>>, reader: EngagementReader) -> PostEngagementSnapshot {
        let handler = GetPostEngagementHandler {
            score_store: Arc::new(Snapshots),
            likes:       likes.map(|l| l as Arc<dyn LikeVisibility>),
        };
        let query = GetPostEngagementQuery { post_id: Uuid::now_v7().to_string(), reader };
        handler.handle(Envelope::new(Uuid::now_v7(), query)).await.unwrap()
    }

    fn post(hidden: bool, down: bool) -> Arc<Post> {
        Arc::new(Post { author: "author".into(), hidden, down, calls: Mutex::new(0) })
    }

    #[tokio::test]
    async fn hidden_likes_reach_only_the_author_and_the_mesh() {
        let stranger = EngagementReader::Profiles(vec!["someone".into()]);
        let guest = EngagementReader::Profiles(vec![]);
        let author = EngagementReader::Profiles(vec!["other".into(), "author".into()]);

        let shown = read(Some(post(false, false)), stranger.clone()).await;
        assert_eq!(shown.total_weighted_score(), 7);

        for reader in [stranger.clone(), guest] {
            let withheld = read(Some(post(true, false)), reader).await;
            assert!(withheld.reaction_scores.is_empty());
            assert_eq!((withheld.view_count, withheld.share_count, withheld.comment_count), (40, 2, 3), "only likes");
        }
        assert_eq!(read(Some(post(true, false)), author).await.total_weighted_score(), 7);

        // The mesh is not asked about, and reads everything.
        let mesh = post(true, false);
        assert_eq!(read(Some(Arc::clone(&mesh)), EngagementReader::Internal).await.total_weighted_score(), 7);
        assert_eq!(*mesh.calls.lock().unwrap(), 0);

        // post unreachable: withheld (fail closed); not wired: shown.
        assert!(read(Some(post(false, true)), stranger.clone()).await.reaction_scores.is_empty());
        assert_eq!(read(None, stranger).await.total_weighted_score(), 7);
    }
}
