use std::sync::Arc;

use cqrs::{Envelope, Query, QueryHandler};

use crate::application::port::{LikeStore, LikeVisibility, PostEngagementSnapshot, ScoreStore};
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

/// A post's or comment's likes, as a reader sees them (#665).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LikeSummary {
    /// Points staked on it by everyone (0 when withheld).
    pub count:  i64,
    /// The reader's own (always shown to them).
    pub mine:   i64,
    /// The author hides like counts (#809) and the reader is not the author.
    pub hidden: bool,
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
    /// Likes withheld from anyone but the author when the author hides them
    /// (#809). `None`: not wired (no post endpoint) — nothing is withheld.
    pub likes:       Option<Arc<dyn LikeVisibility>>,
    /// The likes themselves (#665).
    pub like_store:  Arc<dyn LikeStore>,
}

/// Whether a post's likes are withheld from `reader` (#809): only the author
/// and the mesh see hidden likes; an unknown answer withholds (fail closed).
pub(crate) async fn withheld(likes: Option<&Arc<dyn LikeVisibility>>, reader: &EngagementReader, post_id: &str) -> bool {
    let (Some(likes), EngagementReader::Profiles(own)) = (likes, reader) else { return false };
    match likes.of(post_id).await {
        Ok(Some(post)) => post.hidden && !own.contains(&post.author_id),
        Ok(None) => false,
        Err(error) => {
            tracing::warn!(%error, "like visibility unknown; likes withheld");
            true
        }
    }
}

/// The likes of `targets` for `reader` (`account`: its own likes).
pub(crate) async fn read_likes(
    store: &dyn LikeStore,
    visibility: Option<&Arc<dyn LikeVisibility>>,
    reader: &EngagementReader,
    account: Option<&str>,
    targets: &[LikeTarget],
) -> Result<Vec<LikeSummary>, EngagementError> {
    let counts = store.counts(targets).await?;
    let mine = match account {
        Some(account) => store.mine(account, targets).await?,
        None => vec![0; targets.len()],
    };
    let mut out = Vec::with_capacity(targets.len());
    for ((target, count), mine) in targets.iter().zip(counts).zip(mine) {
        // Only posts hide their likes (#809).
        let hidden = match target {
            LikeTarget::Post(id) => withheld(visibility, reader, id).await,
            LikeTarget::Comment(_) => false,
        };
        out.push(LikeSummary { count: if hidden { 0 } else { count }, mine, hidden });
    }
    Ok(out)
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
        let likes = read_likes(self.like_store.as_ref(), self.likes.as_ref(), &query.reader, query.account.as_deref(), &target)
            .await?
            .pop()
            .unwrap_or_default();
        Ok(PostEngagement { snapshot, likes })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;
    use uuid::Uuid;

    use super::*;
    use crate::application::port::PostLikeVisibility;

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

    /// 9 likes on every target; "me" put 2 of them.
    struct Likes;

    #[async_trait]
    impl LikeStore for Likes {
        async fn apply_total(&self, _: &LikeTarget, _: &str, _: i64) -> Result<i64, EngagementError> {
            unimplemented!()
        }
        async fn counts(&self, targets: &[LikeTarget]) -> Result<Vec<i64>, EngagementError> {
            Ok(vec![9; targets.len()])
        }
        async fn mine(&self, account: &str, targets: &[LikeTarget]) -> Result<Vec<i64>, EngagementError> {
            Ok(vec![if account == "me" { 2 } else { 0 }; targets.len()])
        }
    }

    async fn read(likes: Option<Arc<Post>>, reader: EngagementReader) -> PostEngagement {
        let handler = GetPostEngagementHandler {
            score_store: Arc::new(Snapshots),
            likes:       likes.map(|l| l as Arc<dyn LikeVisibility>),
            like_store:  Arc::new(Likes),
        };
        let query = GetPostEngagementQuery { post_id: Uuid::now_v7().to_string(), reader, account: Some("me".into()) };
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
        assert_eq!(shown.likes, LikeSummary { count: 9, mine: 2, hidden: false });

        for reader in [stranger.clone(), guest] {
            let withheld = read(Some(post(true, false)), reader).await;
            assert_eq!(withheld.likes, LikeSummary { count: 0, mine: 2, hidden: true }, "one's own likes stay shown");
            let s = &withheld.snapshot;
            assert_eq!((s.view_count, s.share_count, s.comment_count), (40, 2, 3), "only likes");
        }
        assert_eq!(read(Some(post(true, false)), author).await.likes.count, 9);

        // The mesh is not asked about, and reads everything.
        let mesh = post(true, false);
        assert_eq!(read(Some(Arc::clone(&mesh)), EngagementReader::Internal).await.likes.count, 9);
        assert_eq!(*mesh.calls.lock().unwrap(), 0);

        // post unreachable: withheld (fail closed); not wired: shown.
        assert!(read(Some(post(false, true)), stranger.clone()).await.likes.hidden);
        assert_eq!(read(None, stranger).await.likes.count, 9);
    }
}
