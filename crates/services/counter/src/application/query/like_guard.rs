//! Hidden like counts (#809): the `LIKE` metric of a post whose author hides
//! like counts is withheld from anyone but the author — in a batch read (the
//! value is dropped) and in a `LIKE` trending list (the post is dropped, the
//! ranks close up, so its place does not tell either). The mesh reads
//! everything. Whose post it is comes from post; when post cannot tell, every
//! post's likes are withheld and the read is marked degraded.

use std::collections::HashSet;
use std::sync::Arc;

use crate::application::port::LikeVisibility;
use crate::domain::{BatchReadout, EntityKind, Metric, TrendingItem};

/// Who reads counts: the mesh, or a client with its profiles (none for a
/// guest or an anonymous caller).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CountReader {
    Internal,
    Profiles(Vec<String>),
}

/// `None`: not wired (no post endpoint) — nothing is withheld.
#[derive(Clone, Default)]
pub struct LikeGuard {
    likes: Option<Arc<dyn LikeVisibility>>,
}

impl LikeGuard {
    pub fn new(likes: Arc<dyn LikeVisibility>) -> Self {
        Self { likes: Some(likes) }
    }

    /// Of `posts`, those whose likes `reader` may not see, and whether post
    /// could not tell (then all of them).
    async fn withheld(&self, reader: &CountReader, posts: Vec<String>) -> (HashSet<String>, bool) {
        let (Some(likes), CountReader::Profiles(own)) = (&self.likes, reader) else {
            return (HashSet::new(), false);
        };
        if posts.is_empty() {
            return (HashSet::new(), false);
        }
        match likes.of(&posts).await {
            Ok(known) => (
                posts
                    .into_iter()
                    .filter(|p| known.get(p).is_some_and(|v| v.hidden && !own.contains(&v.author_id)))
                    .collect(),
                false,
            ),
            Err(error) => {
                tracing::warn!(%error, "like visibility unknown; likes withheld");
                (posts.into_iter().collect(), true)
            }
        }
    }

    /// Drops the `LIKE` values the reader may not see.
    pub async fn apply_batch(&self, reader: &CountReader, readout: &mut BatchReadout) {
        let posts: Vec<String> = readout
            .snapshots
            .iter()
            .filter(|s| s.entity.kind == EntityKind::Post && s.values.iter().any(|v| v.metric == Metric::Like))
            .map(|s| s.entity.id.as_str().to_owned())
            .collect();
        let (withheld, unknown) = self.withheld(reader, posts).await;
        if withheld.is_empty() {
            return;
        }
        for snapshot in &mut readout.snapshots {
            if snapshot.entity.kind == EntityKind::Post && withheld.contains(snapshot.entity.id.as_str()) {
                snapshot.values.retain(|v| v.metric != Metric::Like);
            }
        }
        readout.degraded |= unknown;
    }

    /// Drops from a `LIKE` ranking the posts the reader may not see; the
    /// ranks close up.
    pub async fn apply_trending(&self, reader: &CountReader, metric: Metric, items: Vec<TrendingItem>) -> Vec<TrendingItem> {
        if metric != Metric::Like {
            return items;
        }
        let posts = items.iter().filter(|i| i.entity.kind == EntityKind::Post).map(|i| i.entity.id.as_str().to_owned()).collect();
        let (withheld, _) = self.withheld(reader, posts).await;
        if withheld.is_empty() {
            return items;
        }
        items
            .into_iter()
            .filter(|i| !(i.entity.kind == EntityKind::Post && withheld.contains(i.entity.id.as_str())))
            .enumerate()
            .map(|(n, item)| TrendingItem { rank: n as u32 + 1, ..item })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use async_trait::async_trait;

    use super::*;
    use crate::application::port::PostLikeVisibility;
    use crate::domain::{CountSnapshot, CounterValue, EntityId, EntityRef, MetricKind};
    use crate::error::CounterError;

    /// `hidden` posts by "author"; every other known post is open; `down` fails.
    struct Post {
        hidden: Vec<&'static str>,
        down:   bool,
    }

    #[async_trait]
    impl LikeVisibility for Post {
        async fn of(&self, posts: &[String]) -> Result<HashMap<String, PostLikeVisibility>, CounterError> {
            if self.down {
                return Err(CounterError::PostUnavailable { reason: "down".into() });
            }
            Ok(posts
                .iter()
                .map(|p| {
                    (p.clone(), PostLikeVisibility { author_id: "author".into(), hidden: self.hidden.contains(&p.as_str()) })
                })
                .collect())
        }
    }

    fn post(id: &str) -> EntityRef {
        EntityRef::new(EntityKind::Post, EntityId::new(id.to_owned()).unwrap())
    }

    fn readout() -> BatchReadout {
        let values = || {
            vec![
                CounterValue { metric: Metric::Like, value: 9, kind: MetricKind::Exact },
                CounterValue { metric: Metric::View, value: 90, kind: MetricKind::Approximate },
            ]
        };
        BatchReadout {
            snapshots: vec![CountSnapshot::new(post("hidden"), values()), CountSnapshot::new(post("open"), values())],
            degraded:  false,
        }
    }

    fn metrics(readout: &BatchReadout, i: usize) -> Vec<Metric> {
        readout.snapshots[i].values.iter().map(|v| v.metric).collect()
    }

    #[tokio::test]
    async fn a_batch_loses_only_the_hidden_likes_and_only_for_others() {
        let guard = LikeGuard::new(Arc::new(Post { hidden: vec!["hidden"], down: false }));
        let stranger = CountReader::Profiles(vec!["someone".into()]);

        let mut read = readout();
        guard.apply_batch(&stranger, &mut read).await;
        assert_eq!(metrics(&read, 0), vec![Metric::View]);
        assert_eq!(metrics(&read, 1), vec![Metric::Like, Metric::View]);
        assert!(!read.degraded);

        for reader in [CountReader::Profiles(vec!["author".into()]), CountReader::Internal] {
            let mut read = readout();
            guard.apply_batch(&reader, &mut read).await;
            assert_eq!(metrics(&read, 0), vec![Metric::Like, Metric::View], "{reader:?}");
        }

        // post down: every like withheld, marked degraded; not wired: all shown.
        let down = LikeGuard::new(Arc::new(Post { hidden: vec![], down: true }));
        let mut read = readout();
        down.apply_batch(&CountReader::Profiles(vec![]), &mut read).await;
        assert_eq!((metrics(&read, 0), metrics(&read, 1)), (vec![Metric::View], vec![Metric::View]));
        assert!(read.degraded);
        let mut read = readout();
        LikeGuard::default().apply_batch(&stranger, &mut read).await;
        assert_eq!(metrics(&read, 0), vec![Metric::Like, Metric::View]);
    }

    #[tokio::test]
    async fn a_like_ranking_drops_hidden_posts_and_closes_up() {
        let guard = LikeGuard::new(Arc::new(Post { hidden: vec!["b"], down: false }));
        let items = || {
            ["a", "b", "c"]
                .iter()
                .enumerate()
                .map(|(n, id)| TrendingItem { entity: post(id), score: 10 - n as i64, rank: n as u32 + 1 })
                .collect::<Vec<_>>()
        };
        let stranger = CountReader::Profiles(vec![]);
        let ranked = guard.apply_trending(&stranger, Metric::Like, items()).await;
        assert_eq!(
            ranked.iter().map(|i| (i.entity.id.as_str().to_owned(), i.rank)).collect::<Vec<_>>(),
            vec![("a".to_owned(), 1), ("c".to_owned(), 2)]
        );
        assert_eq!(guard.apply_trending(&stranger, Metric::View, items()).await.len(), 3, "likes only");
        assert_eq!(guard.apply_trending(&CountReader::Internal, Metric::Like, items()).await.len(), 3);
    }
}
