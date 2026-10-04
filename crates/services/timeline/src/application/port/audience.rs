use std::collections::HashSet;

use crate::application::port::SocialGraphClient;
use crate::domain::value_object::{AuthorId, ContentAccess, Viewer};
use crate::error::TimelineError;

/// The authors whose posts `viewer` may see in a discovery feed, or `None` when
/// everything is (the mesh). A feed shows content, so only `Visible` counts: a
/// private author the reader does not follow, a block or a hidden author keeps
/// the post out. One's own posts are always visible. Fails closed: a check
/// error is an error, never an unfiltered page.
pub async fn visible_authors<SG: SocialGraphClient + ?Sized>(
    social_graph: &SG,
    viewer:       &Viewer,
    authors:      impl IntoIterator<Item = AuthorId>,
) -> Result<Option<HashSet<AuthorId>>, TimelineError> {
    let Viewer::Profiles(own) = viewer else {
        return Ok(None);
    };
    let mut authors: Vec<AuthorId> = authors.into_iter().collect();
    authors.sort_by_key(AuthorId::as_uuid);
    authors.dedup();
    let (mine, others): (Vec<AuthorId>, Vec<AuthorId>) =
        authors.into_iter().partition(|a| own.iter().any(|o| *o == a.to_string()));
    let mut visible: HashSet<AuthorId> = mine.into_iter().collect();
    if !others.is_empty() {
        let answers = social_graph.check_access(own, &others).await?;
        visible.extend(others.into_iter().filter(|a| answers.get(a) == Some(&ContentAccess::Visible)));
    }
    Ok(Some(visible))
}

/// The authors `viewers` muted (scope posts), whose posts the feeds leave out.
/// Fails open: when social-graph cannot answer, nothing is muted for this
/// request (a mute is a preference, not a safety rule).
pub async fn muted_authors<SG: SocialGraphClient + ?Sized>(
    social_graph: &SG,
    viewers:      &[String],
) -> HashSet<AuthorId> {
    if viewers.is_empty() {
        return HashSet::new();
    }
    match social_graph.muted_authors(viewers).await {
        Ok(muted) => muted,
        Err(error) => {
            tracing::warn!(%error, "mute lookup failed; serving the feed unfiltered by mutes");
            HashSet::new()
        }
    }
}
