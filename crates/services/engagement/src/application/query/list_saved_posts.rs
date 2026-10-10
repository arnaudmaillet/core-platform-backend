//! A profile's Saved tab (#872): the posts it saved, most recently saved
//! first, as [`crate::application::query::profile_tab`] lets the reader see
//! it (the owner shows the tab: hidden by default). And an account's saves,
//! every profile's, for the GDPR export (mesh only).

use std::sync::Arc;

use cqrs::{Envelope, Query, QueryHandler};

use crate::application::port::{SavedCursor, SavedPost, SavedPosts, Tab};
use crate::application::query::get_post_engagement::EngagementReader;
use crate::application::query::profile_tab::{page_limit, Sight, TabReaders};
use crate::error::EngagementError;

pub struct ListSavedPostsQuery {
    pub profile_id: String,
    /// `0` ⇒ the default; capped (see [`page_limit`]).
    pub limit:      i32,
    /// Where the previous page ended.
    pub after:      Option<SavedCursor>,
    pub reader:     EngagementReader,
}

/// A page of saves; `next` is where the following one starts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SavedPage {
    pub posts: Vec<SavedPost>,
    pub next:  Option<SavedCursor>,
}

impl Query for ListSavedPostsQuery {
    type Response = SavedPage;
}

pub struct ListSavedPostsHandler {
    /// `None`: this instance runs without the durable store.
    pub saves:   Option<Arc<dyn SavedPosts>>,
    pub readers: TabReaders,
}

impl QueryHandler<ListSavedPostsQuery> for ListSavedPostsHandler {
    type Error = EngagementError;

    async fn handle(&self, envelope: Envelope<ListSavedPostsQuery>) -> Result<SavedPage, EngagementError> {
        let query = &envelope.payload;
        if query.profile_id.is_empty() || query.profile_id.len() > 64 {
            return Err(EngagementError::DomainViolation { field: "profile_id".into(), message: query.profile_id.clone() });
        }
        let saves = self.saves.as_ref().ok_or(EngagementError::LedgerUnavailable)?;
        let sight = self.readers.sight(&query.reader, &query.profile_id, Tab::Saved).await?;
        if let Sight::Nothing = sight {
            return Ok(SavedPage { posts: Vec::new(), next: None });
        }
        let (posts, next) = saves.list(&query.profile_id, page_limit(query.limit), query.after.as_ref()).await?;
        let posts = sight.filter(posts, |save| save.post_id.as_str()).await?;
        Ok(SavedPage { posts, next })
    }
}

pub const ACCOUNT_DEFAULT_LIMIT: i32 = 100;
pub const ACCOUNT_MAX_LIMIT: i32 = 500;

pub struct ListSavedPostsByAccountQuery {
    pub account_id: String,
    /// `0` ⇒ [`ACCOUNT_DEFAULT_LIMIT`]; capped at [`ACCOUNT_MAX_LIMIT`].
    pub limit:      i32,
    /// The last `(profile_id, post_id)` of the previous page.
    pub after:      Option<(String, String)>,
}

impl Query for ListSavedPostsByAccountQuery {
    type Response = Vec<SavedPost>;
}

pub struct ListSavedPostsByAccountHandler {
    pub saves: Option<Arc<dyn SavedPosts>>,
}

impl QueryHandler<ListSavedPostsByAccountQuery> for ListSavedPostsByAccountHandler {
    type Error = EngagementError;

    async fn handle(&self, envelope: Envelope<ListSavedPostsByAccountQuery>) -> Result<Vec<SavedPost>, EngagementError> {
        let query = &envelope.payload;
        let saves = self.saves.as_ref().ok_or(EngagementError::LedgerUnavailable)?;
        let limit = match query.limit {
            l if l <= 0 => ACCOUNT_DEFAULT_LIMIT,
            l => l.min(ACCOUNT_MAX_LIMIT),
        };
        let after = query.after.as_ref().map(|(profile, post)| (profile.as_str(), post.as_str()));
        saves.list_by_account(&query.account_id, limit, after).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;

    use super::*;
    use crate::application::port::{LikeVisibility, PostLikeVisibility, ProfileAccess, ProfileTabs, TabFlags};

    /// `saved` saves (newest first) for profile `saver`; posts by
    /// `author-<post>`, `unseen` authors the reader may not see.
    #[derive(Default)]
    struct World {
        saved:  Mutex<Vec<String>>,
        shown:  Option<bool>,
        unseen: Vec<&'static str>,
    }

    #[async_trait]
    impl SavedPosts for World {
        async fn save(&self, _: &str, _: &str, post: &str, _: i64) -> Result<(), EngagementError> {
            self.saved.lock().unwrap().insert(0, post.to_owned());
            Ok(())
        }
        async fn unsave(&self, _: &str, _: &str, _: &str, _: i64) -> Result<(), EngagementError> {
            unimplemented!()
        }
        async fn list(
            &self,
            profile_id: &str,
            limit: i32,
            after: Option<&SavedCursor>,
        ) -> Result<(Vec<SavedPost>, Option<SavedCursor>), EngagementError> {
            let all: Vec<SavedPost> = self
                .saved
                .lock()
                .unwrap()
                .iter()
                .enumerate()
                .map(|(i, post)| SavedPost { profile_id: profile_id.into(), post_id: post.clone(), saved_at_ms: 1_000 - i as i64 })
                .collect();
            let page: Vec<SavedPost> = all
                .into_iter()
                .filter(|s| after.is_none_or(|a| s.saved_at_ms < a.saved_at_ms))
                .take(limit as usize)
                .collect();
            let next = (page.len() == limit as usize)
                .then(|| page.last().map(|s| SavedCursor { saved_at_ms: s.saved_at_ms, post_id: s.post_id.clone() }))
                .flatten();
            Ok((page, next))
        }
        async fn list_by_account(&self, _: &str, _: i32, _: Option<(&str, &str)>) -> Result<Vec<SavedPost>, EngagementError> {
            unimplemented!()
        }
        async fn forget_profile(&self, _: &str, _: i64) -> Result<(), EngagementError> {
            unimplemented!()
        }
        async fn forget_account(&self, _: &str, _: i64) -> Result<(), EngagementError> {
            unimplemented!()
        }
    }

    #[async_trait]
    impl ProfileTabs for World {
        async fn shows(&self, _: &str, tab: Tab) -> Result<bool, EngagementError> {
            assert_eq!(tab, Tab::Saved);
            Ok(self.shown.unwrap_or(TabFlags::default().saved))
        }
        async fn set_tabs(&self, _: &str, _: TabFlags) -> Result<(), EngagementError> {
            unimplemented!()
        }
        async fn forget(&self, _: &str, _: i64) -> Result<(), EngagementError> {
            unimplemented!()
        }
    }

    #[async_trait]
    impl ProfileAccess for World {
        async fn visible(&self, _: &[String], _: &str) -> Result<bool, EngagementError> {
            Ok(true)
        }
        async fn visible_among(&self, _: &[String], profile_ids: &[String]) -> Result<Vec<String>, EngagementError> {
            Ok(profile_ids.iter().filter(|p| !self.unseen.contains(&p.as_str())).cloned().collect())
        }
    }

    #[async_trait]
    impl LikeVisibility for World {
        async fn of(&self, post_id: &str) -> Result<Option<PostLikeVisibility>, EngagementError> {
            Ok(Some(PostLikeVisibility { author_id: format!("author-{post_id}"), hidden: false }))
        }
    }

    async fn handler(shown: Option<bool>, unseen: Vec<&'static str>) -> ListSavedPostsHandler {
        let world = Arc::new(World { shown, unseen, ..World::default() });
        for post in ["a", "b", "c"] {
            world.save("acct", "saver", post, 0).await.unwrap();
        }
        ListSavedPostsHandler {
            saves:   Some(world.clone()),
            readers: TabReaders { tabs: Some(world.clone()), access: Some(world.clone()), posts: Some(world) },
        }
    }

    async fn read(h: &ListSavedPostsHandler, reader: EngagementReader, limit: i32, after: Option<SavedCursor>) -> SavedPage {
        let query = ListSavedPostsQuery { profile_id: "saver".into(), limit, after, reader };
        h.handle(Envelope::new(uuid::Uuid::now_v7(), query)).await.unwrap()
    }

    fn posts(page: &SavedPage) -> Vec<&str> {
        page.posts.iter().map(|s| s.post_id.as_str()).collect()
    }

    fn visitor() -> EngagementReader {
        EngagementReader::Profiles(vec!["someone".into()])
    }

    #[tokio::test]
    async fn the_saved_tab_is_its_owners_unless_shown() {
        let h = handler(None, vec![]).await;
        assert!(read(&h, visitor(), 10, None).await.posts.is_empty(), "hidden by default");
        assert!(read(&h, EngagementReader::Profiles(vec![]), 10, None).await.posts.is_empty(), "from guests too");
        let owner = read(&h, EngagementReader::Profiles(vec!["saver".into()]), 10, None).await;
        assert_eq!(posts(&owner), vec!["c", "b", "a"], "newest saves first");
        assert_eq!(posts(&read(&h, EngagementReader::Internal, 10, None).await), vec!["c", "b", "a"]);
    }

    #[tokio::test]
    async fn a_shown_tab_keeps_what_the_visitor_may_see_and_pages_on() {
        let h = handler(Some(true), vec!["author-b"]).await;
        let first = read(&h, visitor(), 2, None).await;
        assert_eq!(posts(&first), vec!["c"], "b's author is unseen");
        let rest = read(&h, visitor(), 2, first.next).await;
        assert_eq!(posts(&rest), vec!["a"]);
    }
}
