use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use uuid::Uuid;

use crate::domain::value_object::{ContentAccess, Viewer};
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
    ) -> Result<HashMap<Uuid, ContentAccess>, GeoDiscoveryError>;
}

/// The authors whose map posts `viewer` may see, or `None` when everything is
/// (the mesh). A post on the map is content, so only `Visible` counts: a private
/// author the reader does not follow, a block or a hidden author keeps it off.
/// One's own posts are always visible. Fails closed on a check error.
pub async fn visible_authors(
    gate: &dyn AudienceGate,
    viewer: &Viewer,
    authors: impl IntoIterator<Item = Uuid>,
) -> Result<Option<HashSet<Uuid>>, GeoDiscoveryError> {
    let Viewer::Profiles(own) = viewer else {
        return Ok(None);
    };
    let mut authors: Vec<Uuid> = authors.into_iter().collect();
    authors.sort();
    authors.dedup();
    let (mine, others): (Vec<Uuid>, Vec<Uuid>) =
        authors.into_iter().partition(|a| own.iter().any(|o| *o == a.to_string()));
    let mut visible: HashSet<Uuid> = mine.into_iter().collect();
    if !others.is_empty() {
        let answers = gate.access(own, &others).await?;
        visible.extend(
            others.into_iter().filter(|a| answers.get(a) == Some(&ContentAccess::Visible)),
        );
    }
    Ok(Some(visible))
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    struct Gate {
        answers: Option<HashMap<Uuid, ContentAccess>>,
        asked:   Mutex<Vec<Uuid>>,
    }

    #[async_trait]
    impl AudienceGate for Gate {
        async fn access(&self, _: &[String], authors: &[Uuid]) -> Result<HashMap<Uuid, ContentAccess>, GeoDiscoveryError> {
            self.asked.lock().unwrap().extend_from_slice(authors);
            self.answers.clone().ok_or(GeoDiscoveryError::AccessCheckUnavailable { reason: "down".into() })
        }
    }

    #[tokio::test]
    async fn only_visible_and_own_authors_pass_and_the_mesh_is_unfiltered() {
        let (me, open, private, blocked, unknown) =
            (Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7());
        let gate = Gate {
            answers: Some(HashMap::from([
                (open, ContentAccess::Visible),
                (private, ContentAccess::HeaderOnly),
                (blocked, ContentAccess::Hidden),
            ])),
            asked: Mutex::new(Vec::new()),
        };
        let viewer = Viewer::Profiles(vec![me.to_string()]);
        let visible = visible_authors(&gate, &viewer, [me, open, private, blocked, unknown, open])
            .await
            .unwrap()
            .unwrap();
        assert_eq!(visible, HashSet::from([me, open]));
        assert!(!gate.asked.lock().unwrap().contains(&me), "one's own posts need no check");

        assert_eq!(visible_authors(&gate, &Viewer::Internal, [blocked]).await.unwrap(), None);
    }

    #[tokio::test]
    async fn an_unavailable_check_is_an_error() {
        let gate = Gate { answers: None, asked: Mutex::new(Vec::new()) };
        let err = visible_authors(&gate, &Viewer::Profiles(vec![]), [Uuid::now_v7()]).await.unwrap_err();
        assert!(matches!(err, GeoDiscoveryError::AccessCheckUnavailable { .. }));
    }
}
