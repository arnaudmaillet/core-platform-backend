use std::sync::Arc;

use cqrs::{Envelope, Query, QueryHandler};

use crate::{
    application::port::{
        window_start, AudienceGate, AuthorLocationStore, AuthorWindowStore, PostRepository, ReuseRegistry,
    },
    domain::{aggregate::Post, value_object::{ContentAccess, LocationAudience, PostId, ProfileId, Viewer}},
    error::PostError,
};

pub struct GetPostQuery {
    pub post_id: String,
    /// Who is reading. A post the viewer may not see is reported as not found,
    /// so a draft's existence does not leak.
    pub viewer:  Viewer,
    /// The reader is cleared for mature content (not a guest, not 13–17): an
    /// age-gated post is not found otherwise.
    pub mature:  bool,
    /// The mesh reads on the author's behalf (the GDPR export): their own
    /// location, as they see it. Honoured for [`Viewer::Internal`] only, and
    /// only when it names the post's author.
    pub as_author: Option<String>,
}

impl Query for GetPostQuery {
    type Response = Post;
}

pub struct GetPostHandler<R> {
    pub repository: Arc<R>,
    pub audience:   Arc<dyn AudienceGate>,
    pub locations:  Arc<dyn AuthorLocationStore>,
    pub windows:    Arc<dyn AuthorWindowStore>,
    /// The authors' downloads / like-count settings (#809).
    pub authors:    Arc<dyn ReuseRegistry>,
}

impl<R: PostRepository> QueryHandler<GetPostQuery> for GetPostHandler<R> {
    type Error = PostError;

    async fn handle(&self, envelope: Envelope<GetPostQuery>) -> Result<Post, PostError> {
        let query     = &envelope.payload;
        let post_id   = PostId::try_from(query.post_id.as_str())?;
        let not_found = || PostError::PostNotFound { post_id: post_id.as_str() };

        // The post's own state first (no network hop), then its author's
        // audience: private, blocked or hidden authors (fail closed).
        let mut post = self.repository.find_by_id(&post_id).await?
            .filter(|post| {
                query.viewer.may_see_rated(post.profile_id(), post.status(), post.moderation().restriction, query.mature)
            })
            .ok_or_else(not_found)?;
        // One check gives both the content access and how the reader
        // relates to the author (its location audience, #657). The author
        // and the mesh skip it.
        let access = if query.viewer.sees_every_post_of(post.profile_id()) {
            None
        } else {
            let viewers: &[ProfileId] = match &query.viewer {
                Viewer::Profiles(ids) => ids,
                Viewer::Anonymous | Viewer::Internal => &[],
            };
            Some(self.audience.access_with_relation(viewers, post.profile_id()).await?)
        };
        if access.is_some_and(|a| a.content != ContentAccess::Visible) {
            return Err(not_found());
        }
        // The author's post window hides older posts from clients other than
        // the author (#664). The mesh still reads them, marked, so a service
        // serving clients (comment, search) withholds them too.
        if !query.viewer.is_author(post.profile_id()) {
            let days = self.windows.get(post.profile_id()).await?;
            if window_start(days, chrono::Utc::now()).is_some_and(|start| post.created_at() < start) {
                if query.viewer != Viewer::Internal {
                    return Err(not_found());
                }
                post.mark_outside_window();
            }
            // The mesh also learns when it leaves the window (search keeps it).
            if let (Some(days), Viewer::Internal) = (days, &query.viewer) {
                post.show_visible_until(post.created_at() + chrono::Duration::days(i64::from(days)));
            }
        }
        // The author's location sharing applies to everyone else, the mesh
        // included (fail closed: a store error fails the read). Its audience
        // (#657) takes only a reader who follows / is mutual with the author:
        // never the mesh, which reads for no one in particular.
        let on_authors_behalf = query.viewer == Viewer::Internal
            && query.as_author.as_deref().is_some_and(|a| a == post.profile_id().as_str());
        let as_author = query.viewer.is_author(post.profile_id()) || on_authors_behalf;
        if let Some(point) = post.location().filter(|_| !as_author) {
            let sharing = self.locations.get(post.profile_id()).await?;
            let in_audience = match (sharing.audience, access) {
                (LocationAudience::Everyone, _) => true,
                (audience, Some(a)) => audience.admits(a.follows, a.mutual),
                (_, None) => false,
            };
            post.show_location(sharing.shown(point).filter(|_| in_audience));
        }
        // Downloads and like counts (#809): the author's settings, for anyone
        // else (the mesh included, so a service serving clients withholds too).
        if !as_author {
            let defaults = self.authors.defaults(post.profile_id()).await?;
            post.apply_author_display(defaults.allow_downloads, defaults.show_like_counts);
        }
        Ok(post)
    }
}
