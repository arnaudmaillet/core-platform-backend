use std::sync::Arc;

use cqrs::{Envelope, Query, QueryHandler};

use crate::{
    application::port::{
        window_start, AudienceGate, AuthorLocationStore, AuthorTabs, AuthorWindowStore, PostRepository, PostSummary,
    },
    domain::value_object::{ContentAccess, ProfileId, Viewer},
    error::PostError,
};

/// Which of a profile's post tabs a list is (#829).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ProfileTab {
    /// Every post.
    #[default]
    All,
    /// Posts with a parent.
    Reposts,
    /// Posts with a place.
    Places,
}

impl ProfileTab {
    fn holds(self, post: &PostSummary) -> bool {
        match self {
            Self::All => true,
            Self::Reposts => post.is_repost,
            Self::Places => post.has_place,
        }
    }

    /// Whether the owner shows this tab to others.
    fn shown(self, tabs: AuthorTabs) -> bool {
        match self {
            Self::All => true,
            Self::Reposts => tabs.show_reposts,
            Self::Places => tabs.show_places,
        }
    }
}

pub struct ListPostsByProfileQuery {
    pub profile_id: String,
    pub limit:      i32,
    pub page_token: Option<String>,
    /// Who is reading. Anyone but the author (or a trusted internal caller) gets
    /// published posts that moderation has not removed.
    pub viewer:     Viewer,
    /// The reader is cleared for mature content (not a guest, not 13–17):
    /// age-gated posts are left out otherwise.
    pub mature:     bool,
    /// Which tab (#829).
    pub tab:        ProfileTab,
}

impl Query for ListPostsByProfileQuery {
    type Response = (Vec<PostSummary>, Option<String>);
}

pub struct ListPostsByProfileHandler<R> {
    pub repository: Arc<R>,
    pub audience:   Arc<dyn AudienceGate>,
    pub windows:    Arc<dyn AuthorWindowStore>,
    /// The author's location sharing (#657): the Places tab honours it.
    pub locations:  Arc<dyn AuthorLocationStore>,
}

impl<R: PostRepository> QueryHandler<ListPostsByProfileQuery> for ListPostsByProfileHandler<R> {
    type Error = PostError;

    /// The filter runs on the page the store returned, so a page can come back
    /// shorter than `limit` (even empty) while `next_token` still points past it.
    /// Callers page until the token is empty, as they already must.
    async fn handle(
        &self,
        envelope: Envelope<ListPostsByProfileQuery>,
    ) -> Result<(Vec<PostSummary>, Option<String>), PostError> {
        let query      = &envelope.payload;
        let profile_id = ProfileId::try_from(query.profile_id.as_str())?;
        // What anyone but the owner (and the mesh) may see: a private author
        // the reader does not follow, a block either way or a hidden author
        // gives no posts at all (fail closed on a check error); a tab the
        // owner hides (#829) is empty; and the Places tab, like the location
        // itself, only reaches a reader the author shares its location with
        // (#657: not in ghost mode, the reader in its audience).
        let owner = query.viewer.sees_every_post_of(&profile_id);
        let mut hide_reposts = false;
        let mut places_shared = true;
        if !owner {
            let viewers: &[ProfileId] = match &query.viewer {
                Viewer::Profiles(ids) => ids,
                Viewer::Anonymous | Viewer::Internal => &[],
            };
            let access = self.audience.access_with_relation(viewers, &profile_id).await?;
            if access.content != ContentAccess::Visible {
                return Ok((Vec::new(), None));
            }
            let tabs = self.windows.tabs(&profile_id).await?;
            let sharing = self.locations.get(&profile_id).await?;
            places_shared = !sharing.ghost && sharing.audience.admits(access.follows, access.mutual);
            let shown = match query.tab {
                ProfileTab::Places => tabs.show_places && places_shared,
                tab => tab.shown(tabs),
            };
            if !shown {
                return Ok((Vec::new(), None));
            }
            // A hidden Reposts tab is not rebuilt from the full list either.
            hide_reposts = !tabs.show_reposts;
        }
        let (mut posts, next) = self
            .repository
            .list_by_profile(&profile_id, query.limit, query.page_token.as_deref())
            .await?;
        posts.retain(|post| {
            query.tab.holds(post)
                && !(hide_reposts && post.is_repost)
                && query.viewer.may_see_rated(&profile_id, post.status, post.moderation, query.mature)
        });
        // Whether a post carries a place is the author's to share, like the
        // place itself.
        if !places_shared {
            posts.iter_mut().for_each(|post| post.has_place = false);
        }

        // The author's post window (#664): clients other than the author see
        // posts created within it only. The list is newest first, so the first
        // older post ends it.
        if !owner {
            let days = self.windows.get(&profile_id).await?;
            if let Some(start) = window_start(days, chrono::Utc::now())
                && posts.iter().any(|p| p.created_at < start)
            {
                posts.retain(|p| p.created_at >= start);
                return Ok((posts, None));
            }
        }
        Ok((posts, next))
    }
}
