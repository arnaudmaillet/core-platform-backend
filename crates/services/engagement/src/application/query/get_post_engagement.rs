use std::sync::Arc;

use cqrs::{Envelope, Query, QueryHandler};

use crate::application::port::{LikeLedger, LikeStore, PostEngagementSnapshot, ScoreStore};
use crate::domain::value_object::{LikeTarget, PostId};
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
    /// The reader's account (a member on the edge): its own likes.
    pub account: Option<String>,
}

/// A post's or comment's likes, as a reader sees them (#665). Like counts
/// are never withheld (#973: an author can no longer hide them).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LikeSummary {
    /// Points staked on it by everyone.
    pub count: i64,
    /// The reader's own.
    pub mine:  i64,
}

/// A post's engagement: the counters and its likes.
#[derive(Debug)]
pub struct PostEngagement {
    pub snapshot: PostEngagementSnapshot,
    pub likes:    LikeSummary,
}

impl Query for GetPostEngagementQuery {
    type Response = PostEngagement;
}

pub struct GetPostEngagementHandler<S> {
    pub score_store: Arc<S>,
    /// The likes themselves (#665).
    pub like_store:  Arc<dyn LikeStore>,
    /// Their durable copy, for a reader whose likes expired from Redis.
    pub like_ledger: Option<Arc<dyn LikeLedger>>,
}

/// The likes of `targets` (`account`: its own likes).
pub(crate) async fn read_likes(
    store: &Arc<dyn LikeStore>,
    ledger: Option<&Arc<dyn LikeLedger>>,
    account: Option<&str>,
    targets: &[LikeTarget],
) -> Result<Vec<LikeSummary>, EngagementError> {
    let counts = store.counts(targets).await?;
    let mine = match account {
        Some(account) => crate::application::likes::mine(store, ledger, account, targets).await?,
        None => vec![0; targets.len()],
    };
    Ok(counts.into_iter().zip(mine).map(|(count, mine)| LikeSummary { count, mine }).collect())
}

impl<S: ScoreStore> QueryHandler<GetPostEngagementQuery> for GetPostEngagementHandler<S> {
    type Error = EngagementError;

    async fn handle(
        &self,
        envelope: Envelope<GetPostEngagementQuery>,
    ) -> Result<PostEngagement, EngagementError> {
        let query = &envelope.payload;
        let post_id = PostId::try_from(query.post_id.as_str())?;
        let snapshot = self.score_store.get_snapshot(&post_id).await?;
        let target = [LikeTarget::Post(query.post_id.clone())];
        let likes = read_likes(&self.like_store, self.like_ledger.as_ref(), query.account.as_deref(), &target)
            .await?
            .pop()
            .unwrap_or_default();
        Ok(PostEngagement { snapshot, likes })
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use uuid::Uuid;

    use super::*;
    use crate::application::port::{Applied, Position};

    struct Snapshots;

    #[async_trait]
    impl ScoreStore for Snapshots {
        async fn incr_view(&self, _: &PostId) -> Result<(), EngagementError> { unimplemented!() }
        async fn incr_share(&self, _: &PostId) -> Result<(), EngagementError> { unimplemented!() }
        async fn incr_comment(&self, _: &PostId) -> Result<(), EngagementError> { unimplemented!() }
        async fn decr_comment(&self, _: &PostId) -> Result<(), EngagementError> { unimplemented!() }
        async fn get_snapshot(&self, _: &PostId) -> Result<PostEngagementSnapshot, EngagementError> {
            Ok(PostEngagementSnapshot {
                view_count:      40,
                share_count:     2,
                comment_count:   3,
            })
        }
    }

    /// 9 likes on every target; "me" put 2 of them.
    struct Likes;

    #[async_trait]
    impl LikeStore for Likes {
        async fn apply_total(&self, _: &LikeTarget, _: &str, _: i64) -> Result<Option<Applied>, EngagementError> {
            unimplemented!()
        }
        async fn counts(&self, targets: &[LikeTarget]) -> Result<Vec<i64>, EngagementError> {
            Ok(vec![9; targets.len()])
        }
        async fn positions(&self, account: &str, targets: &[LikeTarget]) -> Result<Vec<Option<Position>>, EngagementError> {
            let total = if account == "me" { 2 } else { 0 };
            Ok(vec![Some(Position { total, arrival: None }); targets.len()])
        }
        async fn rehydrate(&self, _: &LikeTarget, _: &[(String, Position)], _: bool) -> Result<(), EngagementError> {
            unimplemented!()
        }
        async fn claim_rehydration(&self, _: &LikeTarget) -> Result<bool, EngagementError> {
            unimplemented!()
        }
        async fn forget(&self, _: &str, _: &[LikeTarget]) -> Result<(), EngagementError> {
            unimplemented!()
        }
    }

    async fn read(reader: EngagementReader) -> PostEngagement {
        let handler = GetPostEngagementHandler {
            score_store: Arc::new(Snapshots),
            like_store:  Arc::new(Likes),
            like_ledger: None,
        };
        let query = GetPostEngagementQuery { post_id: Uuid::now_v7().to_string(), reader, account: Some("me".into()) };
        handler.handle(Envelope::new(Uuid::now_v7(), query)).await.unwrap()
    }

    /// #973: like counts reach every reader, guests included.
    #[tokio::test]
    async fn like_counts_reach_every_reader() {
        for reader in [
            EngagementReader::Profiles(vec!["someone".into()]),
            EngagementReader::Profiles(vec![]),
            EngagementReader::Internal,
        ] {
            let read = read(reader).await;
            assert_eq!(read.likes, LikeSummary { count: 9, mine: 2 });
            let s = &read.snapshot;
            assert_eq!((s.view_count, s.share_count, s.comment_count), (40, 2, 3));
        }
    }
}
