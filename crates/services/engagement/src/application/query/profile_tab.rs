//! Who reads a profile's tab of posts (#829, #872) — its Likes or its Saved
//! tab. The owner (one of the reader's profiles) and the mesh read it all;
//! anyone else only when the owner shows the tab and may see the profile at
//! all (a private profile they don't follow, a block either way), and then
//! only the posts whose authors they may see too (#873): not even the id of a
//! post they couldn't open. Anything unwired: the owner's only.

use std::collections::HashSet;
use std::sync::Arc;

use crate::application::port::{LikeVisibility, ProfileAccess, ProfileTabs, Tab};
use crate::application::query::get_post_engagement::EngagementReader;
use crate::error::EngagementError;

/// The pages served per call; a page's authors are checked in one
/// social-graph call.
pub const DEFAULT_LIMIT: i32 = 30;
pub const MAX_LIMIT: i32 = 100;
const _: () = assert!(MAX_LIMIT as usize <= crate::application::port::profile_tabs::MAX_ACCESS_TARGETS);

/// `0` ⇒ [`DEFAULT_LIMIT`]; capped at [`MAX_LIMIT`].
pub fn page_limit(limit: i32) -> i32 {
    match limit {
        l if l <= 0 => DEFAULT_LIMIT,
        l => l.min(MAX_LIMIT),
    }
}

/// What a tab's visitors are checked against; `None` (not wired): nobody but
/// the owner and the mesh reads the tab.
#[derive(Clone, Default)]
pub struct TabReaders {
    pub tabs:   Option<Arc<dyn ProfileTabs>>,
    /// Who may see a profile (social-graph).
    pub access: Option<Arc<dyn ProfileAccess>>,
    /// Whose each post is (post).
    pub posts:  Option<Arc<dyn LikeVisibility>>,
}

/// What a reader sees of a tab.
pub enum Sight<'a> {
    /// Every post (the owner, the mesh).
    All,
    /// The posts whose authors these profiles may see.
    Visitor(&'a [String], &'a dyn ProfileAccess, &'a dyn LikeVisibility),
    Nothing,
}

impl TabReaders {
    pub async fn sight<'a>(
        &'a self,
        reader: &'a EngagementReader,
        profile_id: &str,
        tab: Tab,
    ) -> Result<Sight<'a>, EngagementError> {
        let viewers = match reader {
            EngagementReader::Internal => return Ok(Sight::All),
            EngagementReader::Profiles(own) if own.iter().any(|p| p == profile_id) => return Ok(Sight::All),
            EngagementReader::Profiles(own) => own,
        };
        let (Some(tabs), Some(access), Some(posts)) = (&self.tabs, &self.access, &self.posts) else {
            return Ok(Sight::Nothing);
        };
        Ok(match tabs.shows(profile_id, tab).await? && access.visible(viewers, profile_id).await? {
            true => Sight::Visitor(viewers, access.as_ref(), posts.as_ref()),
            false => Sight::Nothing,
        })
    }
}

impl Sight<'_> {
    /// `items` as this reader sees them: a visitor keeps those whose post's
    /// author it may see (a post post doesn't know goes too).
    pub async fn filter<T>(&self, items: Vec<T>, post_of: impl Fn(&T) -> &str) -> Result<Vec<T>, EngagementError> {
        let (viewers, access, posts) = match self {
            Sight::All => return Ok(items),
            Sight::Nothing => return Ok(Vec::new()),
            Sight::Visitor(viewers, access, posts) => (viewers, access, posts),
        };
        let post_ids: Vec<String> = items.iter().map(|item| post_of(item).to_owned()).collect();
        let authors: Vec<Option<String>> =
            posts.of_many(&post_ids).await?.into_iter().map(|v| v.map(|v| v.author_id)).collect();
        let distinct: Vec<String> = authors.iter().flatten().cloned().collect::<HashSet<_>>().into_iter().collect();
        let visible: HashSet<String> = access.visible_among(viewers, &distinct).await?.into_iter().collect();
        Ok(items
            .into_iter()
            .zip(authors)
            .filter_map(|(item, author)| author.filter(|a| visible.contains(a)).map(|_| item))
            .collect())
    }
}
