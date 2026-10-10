//! A profile's Likes tab (#829): the posts it liked, newest posts first. The
//! owner (and the mesh) always see it; anyone else only when the owner shows
//! the tab and may see the profile at all (a private profile they don't
//! follow, a block either way) — otherwise the list is empty. For them, each
//! page keeps only the posts whose authors they may see too (#873): not even
//! the id of a post they couldn't open.

use std::collections::HashSet;
use std::sync::Arc;

use cqrs::{Envelope, Query, QueryHandler};

use crate::application::port::{LikeLedger, LikeVisibility, ProfileAccess, ProfileTabs};
use crate::application::query::get_post_engagement::EngagementReader;
use crate::error::EngagementError;

pub const DEFAULT_LIMIT: i32 = 30;
pub const MAX_LIMIT: i32 = 100;
// A page's authors are checked in one social-graph call.
const _: () = assert!(MAX_LIMIT as usize <= crate::application::port::profile_tabs::MAX_ACCESS_TARGETS);

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
    /// Whose each post is (post); `None` (not wired): likewise.
    pub posts:  Option<Arc<dyn LikeVisibility>>,
}

/// What `reader` sees of the tab.
enum Sight<'a> {
    /// Every like (the owner, the mesh).
    All,
    /// The likes of posts whose authors these profiles may see.
    Visitor(&'a [String], &'a dyn ProfileAccess, &'a dyn LikeVisibility),
    Nothing,
}

impl ListLikesByProfileHandler {
    async fn sight<'a>(&'a self, reader: &'a EngagementReader, profile_id: &str) -> Result<Sight<'a>, EngagementError> {
        let viewers = match reader {
            EngagementReader::Internal => return Ok(Sight::All),
            EngagementReader::Profiles(own) if own.iter().any(|p| p == profile_id) => return Ok(Sight::All),
            EngagementReader::Profiles(own) => own,
        };
        let (Some(tabs), Some(access), Some(posts)) = (&self.tabs, &self.access, &self.posts) else {
            return Ok(Sight::Nothing);
        };
        Ok(match tabs.shows_likes(profile_id).await? && access.visible(viewers, profile_id).await? {
            true => Sight::Visitor(viewers, access.as_ref(), posts.as_ref()),
            false => Sight::Nothing,
        })
    }
}

/// The posts of `post_ids` whose authors `viewers` may see; a post post
/// doesn't know goes too.
async fn visible_posts(
    post_ids: Vec<String>,
    viewers: &[String],
    access: &dyn ProfileAccess,
    posts: &dyn LikeVisibility,
) -> Result<Vec<String>, EngagementError> {
    let authors: Vec<Option<String>> = posts.of_many(&post_ids).await?.into_iter().map(|v| v.map(|v| v.author_id)).collect();
    let distinct: Vec<String> = authors.iter().flatten().cloned().collect::<HashSet<_>>().into_iter().collect();
    let visible: HashSet<String> = access.visible_among(viewers, &distinct).await?.into_iter().collect();
    Ok(post_ids
        .into_iter()
        .zip(authors)
        .filter_map(|(post, author)| author.filter(|a| visible.contains(a)).map(|_| post))
        .collect())
}

impl QueryHandler<ListLikesByProfileQuery> for ListLikesByProfileHandler {
    type Error = EngagementError;

    async fn handle(&self, envelope: Envelope<ListLikesByProfileQuery>) -> Result<LikedPosts, EngagementError> {
        let query = &envelope.payload;
        if query.profile_id.is_empty() || query.profile_id.len() > 64 {
            return Err(EngagementError::DomainViolation { field: "profile_id".into(), message: query.profile_id.clone() });
        }
        let ledger = self.ledger.as_ref().ok_or(EngagementError::LedgerUnavailable)?;
        let sight = self.sight(&query.reader, &query.profile_id).await?;
        if let Sight::Nothing = sight {
            return Ok(LikedPosts { post_ids: Vec::new(), next: None });
        }
        let limit = match query.limit {
            l if l <= 0 => DEFAULT_LIMIT,
            l => l.min(MAX_LIMIT),
        };
        let post_ids = ledger.liked_posts_by_profile(&query.profile_id, limit, query.after.as_deref()).await?;
        // From the page read, filtered or not: the next page starts after it.
        let next = (post_ids.len() == limit as usize).then(|| post_ids.last().cloned()).flatten();
        let post_ids = match sight {
            Sight::Visitor(viewers, access, posts) => visible_posts(post_ids, viewers, access, posts).await?,
            _ => post_ids,
        };
        Ok(LikedPosts { post_ids, next })
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;

    use super::*;
    use crate::application::fakes::Likes;
    use crate::application::port::{Position, PostLikeVisibility};
    use crate::domain::value_object::LikeTarget;

    /// `shown`: the owner shows the tab; `visible`: the reader may see the
    /// profile (social-graph); `unseen`: the authors the reader may not see.
    struct Tabs {
        shown:   bool,
        visible: bool,
        unseen:  Vec<&'static str>,
    }

    #[async_trait]
    impl ProfileTabs for Tabs {
        async fn shows_likes(&self, _: &str) -> Result<bool, EngagementError> {
            Ok(self.shown)
        }
        async fn set_shows_likes(&self, _: &str, _: bool) -> Result<(), EngagementError> {
            unimplemented!()
        }
        async fn forget(&self, _: &str, _: i64) -> Result<(), EngagementError> {
            unimplemented!()
        }
    }

    #[async_trait]
    impl ProfileAccess for Tabs {
        async fn visible(&self, _: &[String], _: &str) -> Result<bool, EngagementError> {
            Ok(self.visible)
        }
        async fn visible_among(&self, _: &[String], profile_ids: &[String]) -> Result<Vec<String>, EngagementError> {
            Ok(profile_ids.iter().filter(|p| !self.unseen.contains(&p.as_str())).cloned().collect())
        }
    }

    /// Post `0199-x` is by `author-x`; post does not know `0199-c`.
    struct Authors;

    #[async_trait]
    impl LikeVisibility for Authors {
        async fn of(&self, post_id: &str) -> Result<Option<PostLikeVisibility>, EngagementError> {
            let author = post_id.strip_prefix("0199-").filter(|p| *p != "c");
            Ok(author.map(|a| PostLikeVisibility { author_id: format!("author-{a}"), hidden: false }))
        }
    }

    async fn handler(tabs: Tabs, wired: bool) -> ListLikesByProfileHandler {
        let likes = Arc::new(Likes::default());
        for post in ["0199-a", "0199-b", "0199-c", "0199-d"] {
            let target = LikeTarget::Post(post.into());
            likes.record(&target, "acct", "liker", Position { total: 1, arrival: None }, 1).await.unwrap();
        }
        // Another profile of the same account: not in this profile's tab.
        likes.record(&LikeTarget::Post("0199-z".into()), "acct", "other-profile", Position { total: 1, arrival: None }, 1).await.unwrap();
        let tabs = Arc::new(tabs);
        ListLikesByProfileHandler {
            ledger: Some(likes),
            tabs:   wired.then(|| tabs.clone() as Arc<dyn ProfileTabs>),
            access: wired.then_some(tabs as Arc<dyn ProfileAccess>),
            posts:  wired.then_some(Arc::new(Authors) as Arc<dyn LikeVisibility>),
        }
    }

    fn tabs(shown: bool, visible: bool) -> Tabs {
        Tabs { shown, visible, unseen: Vec::new() }
    }

    async fn read(h: &ListLikesByProfileHandler, reader: EngagementReader, limit: i32, after: Option<&str>) -> LikedPosts {
        let query = ListLikesByProfileQuery { profile_id: "liker".into(), limit, after: after.map(Into::into), reader };
        h.handle(Envelope::new(uuid::Uuid::now_v7(), query)).await.unwrap()
    }

    fn visitor() -> EngagementReader {
        EngagementReader::Profiles(vec!["someone".into()])
    }

    fn ids(posts: &[&str]) -> Vec<String> {
        posts.iter().map(|p| p.to_string()).collect()
    }

    #[tokio::test]
    async fn a_shown_tab_lists_the_profiles_likes_newest_first_page_by_page() {
        let h = handler(tabs(true, true), true).await;
        let first = read(&h, visitor(), 2, None).await;
        assert_eq!(first, LikedPosts { post_ids: ids(&["0199-d"]), next: Some("0199-c".into()) }, "post doesn't know c");
        let rest = read(&h, visitor(), 2, first.next.as_deref()).await;
        assert_eq!(rest, LikedPosts { post_ids: ids(&["0199-b", "0199-a"]), next: Some("0199-a".into()) });
        assert_eq!(read(&h, visitor(), 2, rest.next.as_deref()).await, LikedPosts { post_ids: vec![], next: None });
        assert_eq!(read(&h, EngagementReader::Profiles(vec![]), 10, None).await.post_ids.len(), 3, "a guest too");
    }

    #[tokio::test]
    async fn a_visitor_does_not_see_the_posts_of_authors_it_may_not_see() {
        let h = handler(Tabs { shown: true, visible: true, unseen: vec!["author-b"] }, true).await;
        assert_eq!(read(&h, visitor(), 10, None).await.post_ids, ids(&["0199-d", "0199-a"]));
        let owner = EngagementReader::Profiles(vec!["liker".into()]);
        assert_eq!(read(&h, owner, 10, None).await.post_ids, ids(&["0199-d", "0199-c", "0199-b", "0199-a"]));
    }

    #[tokio::test]
    async fn a_hidden_tab_or_an_unseen_profile_is_empty_but_for_the_owner_and_the_mesh() {
        for (shown, visible, wired) in [(false, true, true), (true, false, true), (true, true, false)] {
            let h = handler(tabs(shown, visible), wired).await;
            assert!(read(&h, visitor(), 10, None).await.post_ids.is_empty(), "{shown} {visible} {wired}");
            let owner = EngagementReader::Profiles(vec!["another-of-mine".into(), "liker".into()]);
            assert_eq!(read(&h, owner, 10, None).await.post_ids.len(), 4, "the owner sees it all");
            assert_eq!(read(&h, EngagementReader::Internal, 10, None).await.post_ids.len(), 4);
        }
    }
}
