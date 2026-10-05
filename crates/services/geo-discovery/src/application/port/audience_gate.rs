use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use uuid::Uuid;

use crate::domain::value_object::{AuthorAccess, ContentAccess, LocationAudience, LocationSharing, Viewer};
use crate::error::GeoDiscoveryError;

/// social-graph's access check (`CheckAccess`): what `viewers` (a reader's
/// profile ids; empty when anonymous) may see of each author's content.
#[async_trait]
pub trait AudienceGate: Send + Sync + 'static {
    /// One answer per author; a missing one counts as hidden. Errors are
    /// `AccessCheckUnavailable`.
    async fn access(
        &self,
        viewers: &[String],
        authors: &[Uuid],
    ) -> Result<HashMap<Uuid, AuthorAccess>, GeoDiscoveryError>;
}

/// The authors whose map posts `viewer` may see, or `None` when everything is.
/// A post on the map is content, so only `Visible` counts: a private author the
/// reader does not follow, a block or a hidden author keeps it off. Then the
/// author's location audience (#657, from `sharing`): followers / mutuals only
/// take a reader who follows / is mutual with them — never the mesh (it reads
/// for no one in particular, e.g. NEARBY candidates) nor an anonymous reader.
/// One's own posts are always visible. Fails closed on a check error.
pub async fn visible_authors(
    gate: &dyn AudienceGate,
    viewer: &Viewer,
    authors: impl IntoIterator<Item = Uuid>,
    sharing: &HashMap<Uuid, LocationSharing>,
) -> Result<Option<HashSet<Uuid>>, GeoDiscoveryError> {
    let audience = |a: &Uuid| sharing.get(a).map_or(LocationAudience::Everyone, |s| s.audience);
    let mut authors: Vec<Uuid> = authors.into_iter().collect();
    authors.sort();
    authors.dedup();
    let Viewer::Profiles(own) = viewer else {
        // The mesh: everything, but an author whose audience is restricted.
        if authors.iter().all(|a| audience(a) == LocationAudience::Everyone) {
            return Ok(None);
        }
        return Ok(Some(authors.into_iter().filter(|a| audience(a) == LocationAudience::Everyone).collect()));
    };
    let (mine, others): (Vec<Uuid>, Vec<Uuid>) =
        authors.into_iter().partition(|a| own.iter().any(|o| *o == a.to_string()));
    let mut visible: HashSet<Uuid> = mine.into_iter().collect();
    if !others.is_empty() {
        let answers = gate.access(own, &others).await?;
        visible.extend(others.into_iter().filter(|a| {
            answers.get(a).is_some_and(|access| {
                access.content == ContentAccess::Visible && audience(a).admits(access.follows, access.mutual)
            })
        }));
    }
    Ok(Some(visible))
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    struct Gate {
        answers: Option<HashMap<Uuid, AuthorAccess>>,
        asked:   Mutex<Vec<Uuid>>,
    }

    #[async_trait]
    impl AudienceGate for Gate {
        async fn access(&self, _: &[String], authors: &[Uuid]) -> Result<HashMap<Uuid, AuthorAccess>, GeoDiscoveryError> {
            self.asked.lock().unwrap().extend_from_slice(authors);
            self.answers.clone().ok_or(GeoDiscoveryError::AccessCheckUnavailable { reason: "down".into() })
        }
    }

    fn content(content: ContentAccess) -> AuthorAccess {
        AuthorAccess { content, follows: false, mutual: false }
    }

    #[tokio::test]
    async fn only_visible_and_own_authors_pass_and_the_mesh_is_unfiltered() {
        let (me, open, private, blocked, unknown) =
            (Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7());
        let gate = Gate {
            answers: Some(HashMap::from([
                (open, content(ContentAccess::Visible)),
                (private, content(ContentAccess::HeaderOnly)),
                (blocked, content(ContentAccess::Hidden)),
            ])),
            asked: Mutex::new(Vec::new()),
        };
        let viewer = Viewer::Profiles(vec![me.to_string()]);
        let none = HashMap::new();
        let visible = visible_authors(&gate, &viewer, [me, open, private, blocked, unknown, open], &none)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(visible, HashSet::from([me, open]));
        assert!(!gate.asked.lock().unwrap().contains(&me), "one's own posts need no check");

        assert_eq!(visible_authors(&gate, &Viewer::Internal, [blocked], &none).await.unwrap(), None);
    }

    /// #657: followers / mutuals only take a reader who follows / is mutual;
    /// the mesh and an anonymous reader never — and a block still wins.
    #[tokio::test]
    async fn the_location_audience_takes_only_who_it_names() {
        let (me, everyone, followers, mutuals, blocked) =
            (Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7());
        let audience = |audience| LocationSharing { audience, ..LocationSharing::default() };
        let sharing = HashMap::from([
            (followers, audience(LocationAudience::Followers)),
            (mutuals, audience(LocationAudience::Mutuals)),
            (blocked, audience(LocationAudience::Followers)),
        ]);
        let all = [everyone, followers, mutuals, blocked];
        let gate = |follows: bool, mutual: bool| Gate {
            answers: Some(HashMap::from([
                (everyone, AuthorAccess::visible(follows, mutual)),
                (followers, AuthorAccess::visible(follows, mutual)),
                (mutuals, AuthorAccess::visible(follows, mutual)),
                (blocked, AuthorAccess { content: ContentAccess::Hidden, follows: true, mutual: true }),
            ])),
            asked: Mutex::new(Vec::new()),
        };
        let seen = |follows, mutual| {
            let sharing = &sharing;
            async move { visible_authors(&gate(follows, mutual), &viewer_of(me), all, sharing).await.unwrap().unwrap() }
        };
        fn viewer_of(me: Uuid) -> Viewer {
            Viewer::Profiles(vec![me.to_string()])
        }
        assert_eq!(seen(false, false).await, HashSet::from([everyone]), "a stranger");
        assert_eq!(seen(true, false).await, HashSet::from([everyone, followers]), "a follower");
        assert_eq!(seen(true, true).await, HashSet::from([everyone, followers, mutuals]), "a mutual; a block still hides");

        let mesh = visible_authors(&gate(true, true), &Viewer::Internal, all, &sharing).await.unwrap();
        assert_eq!(mesh, Some(HashSet::from([everyone])), "the mesh reads for no one: restricted authors leave");
        let anonymous = visible_authors(&gate(false, false), &Viewer::Profiles(vec![]), all, &sharing).await.unwrap();
        assert_eq!(anonymous, Some(HashSet::from([everyone])));
    }

    #[tokio::test]
    async fn an_unavailable_check_is_an_error() {
        let gate = Gate { answers: None, asked: Mutex::new(Vec::new()) };
        let err = visible_authors(&gate, &Viewer::Profiles(vec![]), [Uuid::now_v7()], &HashMap::new()).await.unwrap_err();
        assert!(matches!(err, GeoDiscoveryError::AccessCheckUnavailable { .. }));
    }
}
