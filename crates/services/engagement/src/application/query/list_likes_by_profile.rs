//! A profile's Likes tab (#829): the posts it liked, newest posts first. The
//! owner (and the mesh) always see it; anyone else only when the owner shows
//! the tab and may see the profile at all (a private profile they don't
//! follow, a block either way) — otherwise the list is empty. The client
//! hydrates the posts through post, which withholds what the reader can't see.

use std::sync::Arc;

use cqrs::{Envelope, Query, QueryHandler};

use crate::application::port::{LikeLedger, ProfileAccess, ProfileTabs};
use crate::application::query::get_post_engagement::EngagementReader;
use crate::error::EngagementError;

pub const DEFAULT_LIMIT: i32 = 30;
pub const MAX_LIMIT: i32 = 100;

pub struct ListLikesByProfileQuery {
    pub profile_id: String,
    /// `0` ⇒ [`DEFAULT_LIMIT`]; capped at [`MAX_LIMIT`].
    pub limit:      i32,
    /// The last post of the previous page.
    pub after:      Option<String>,
    pub reader:     EngagementReader,
}

/// A page of liked posts; `next` is the last one when more may follow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LikedPosts {
    pub post_ids: Vec<String>,
    pub next:     Option<String>,
}

impl Query for ListLikesByProfileQuery {
    type Response = LikedPosts;
}

pub struct ListLikesByProfileHandler {
    /// `None`: this instance runs without the durable copy.
    pub ledger: Option<Arc<dyn LikeLedger>>,
    pub tabs:   Option<Arc<dyn ProfileTabs>>,
    /// `None` (not wired): nobody but the owner and the mesh sees the tab.
    pub access: Option<Arc<dyn ProfileAccess>>,
}

impl ListLikesByProfileHandler {
    /// Whether `reader` may see `profile_id`'s Likes tab.
    async fn may_read(&self, reader: &EngagementReader, profile_id: &str) -> Result<bool, EngagementError> {
        let viewers = match reader {
            EngagementReader::Internal => return Ok(true),
            EngagementReader::Profiles(own) if own.iter().any(|p| p == profile_id) => return Ok(true),
            EngagementReader::Profiles(own) => own,
        };
        let (Some(tabs), Some(access)) = (&self.tabs, &self.access) else { return Ok(false) };
        Ok(tabs.shows_likes(profile_id).await? && access.visible(viewers, profile_id).await?)
    }
}

impl QueryHandler<ListLikesByProfileQuery> for ListLikesByProfileHandler {
    type Error = EngagementError;

    async fn handle(&self, envelope: Envelope<ListLikesByProfileQuery>) -> Result<LikedPosts, EngagementError> {
        let query = &envelope.payload;
        if query.profile_id.is_empty() || query.profile_id.len() > 64 {
            return Err(EngagementError::DomainViolation { field: "profile_id".into(), message: query.profile_id.clone() });
        }
        let ledger = self.ledger.as_ref().ok_or(EngagementError::LedgerUnavailable)?;
        if !self.may_read(&query.reader, &query.profile_id).await? {
            return Ok(LikedPosts { post_ids: Vec::new(), next: None });
        }
        let limit = match query.limit {
            l if l <= 0 => DEFAULT_LIMIT,
            l => l.min(MAX_LIMIT),
        };
        let post_ids = ledger.liked_posts_by_profile(&query.profile_id, limit, query.after.as_deref()).await?;
        let next = (post_ids.len() == limit as usize).then(|| post_ids.last().cloned()).flatten();
        Ok(LikedPosts { post_ids, next })
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;

    use super::*;
    use crate::application::fakes::Likes;
    use crate::application::port::Position;
    use crate::domain::value_object::LikeTarget;

    /// `shown`: the owner shows the tab; `visible`: the reader may see the
    /// profile (social-graph).
    struct Tabs {
        shown:   bool,
        visible: bool,
    }

    #[async_trait]
    impl ProfileTabs for Tabs {
        async fn shows_likes(&self, _: &str) -> Result<bool, EngagementError> {
            Ok(self.shown)
        }
        async fn set_shows_likes(&self, _: &str, _: bool) -> Result<(), EngagementError> {
            unimplemented!()
        }
    }

    #[async_trait]
    impl ProfileAccess for Tabs {
        async fn visible(&self, _: &[String], _: &str) -> Result<bool, EngagementError> {
            Ok(self.visible)
        }
    }

    async fn handler(shown: bool, visible: bool, wired: bool) -> ListLikesByProfileHandler {
        let likes = Arc::new(Likes::default());
        for post in ["0199-a", "0199-b", "0199-c"] {
            let target = LikeTarget::Post(post.into());
            likes.record(&target, "acct", "liker", Position { total: 1, arrival: None }, 1).await.unwrap();
        }
        // Another profile of the same account: not in this profile's tab.
        likes.record(&LikeTarget::Post("0199-z".into()), "acct", "other-profile", Position { total: 1, arrival: None }, 1).await.unwrap();
        let tabs = Arc::new(Tabs { shown, visible });
        ListLikesByProfileHandler {
            ledger: Some(likes),
            tabs:   wired.then(|| tabs.clone() as Arc<dyn ProfileTabs>),
            access: wired.then_some(tabs as Arc<dyn ProfileAccess>),
        }
    }

    async fn read(h: &ListLikesByProfileHandler, reader: EngagementReader, limit: i32, after: Option<&str>) -> LikedPosts {
        let query = ListLikesByProfileQuery { profile_id: "liker".into(), limit, after: after.map(Into::into), reader };
        h.handle(Envelope::new(uuid::Uuid::now_v7(), query)).await.unwrap()
    }

    fn visitor() -> EngagementReader {
        EngagementReader::Profiles(vec!["someone".into()])
    }

    #[tokio::test]
    async fn a_shown_tab_lists_the_profiles_likes_newest_first_page_by_page() {
        let h = handler(true, true, true).await;
        let first = read(&h, visitor(), 2, None).await;
        assert_eq!(first, LikedPosts { post_ids: vec!["0199-c".into(), "0199-b".into()], next: Some("0199-b".into()) });
        let rest = read(&h, visitor(), 2, first.next.as_deref()).await;
        assert_eq!(rest, LikedPosts { post_ids: vec!["0199-a".into()], next: None });
        assert_eq!(read(&h, EngagementReader::Profiles(vec![]), 10, None).await.post_ids.len(), 3, "a guest too");
    }

    #[tokio::test]
    async fn a_hidden_tab_or_an_unseen_profile_is_empty_but_for_the_owner_and_the_mesh() {
        for (shown, visible, wired) in [(false, true, true), (true, false, true), (true, true, false)] {
            let h = handler(shown, visible, wired).await;
            assert!(read(&h, visitor(), 10, None).await.post_ids.is_empty(), "{shown} {visible} {wired}");
            let owner = EngagementReader::Profiles(vec!["another-of-mine".into(), "liker".into()]);
            assert_eq!(read(&h, owner, 10, None).await.post_ids.len(), 3, "the owner sees it all");
            assert_eq!(read(&h, EngagementReader::Internal, 10, None).await.post_ids.len(), 3);
        }
    }
}
