//! The hashtags' own documents (#831), derived from the post documents: no
//! event carries a tag's count, so a periodic refresh aggregates the
//! searchable posts' tags and keeps one hashtag document per used tag —
//! its post count, a volume `popularity` (Search and Suggest rank by it) and a
//! velocity `trending` score (the trending hashtags). Deleting, hiding or
//! windowing a post lowers its tags' counts at the next refresh; a tag no
//! longer used loses its document.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};

use crate::application::port::{HashtagStats, SearchIndex, TagUsage};
use crate::domain::{DocVersion, EntityKind, HashtagDoc, IndexDocument, PopularityScore, Searchable};
use crate::error::SearchError;

/// Tags kept per refresh: the most used.
pub const MAX_TAGS: usize = 5_000;

/// Volume: all-time use, then this week's.
pub fn popularity(usage: &TagUsage) -> f64 {
    (1.0 + usage.total as f64).ln() + (1.0 + usage.last_week as f64).ln()
}

/// Velocity: this week's use, the last day's three times over. A tag nobody
/// used this week is not trending, however large.
pub fn trending(usage: &TagUsage) -> f64 {
    3.0 * usage.last_day as f64 + (usage.last_week - usage.last_day) as f64
}

/// What one refresh did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RefreshReport {
    pub written: usize,
    pub removed: u64,
}

pub struct HashtagRefresh {
    index: Arc<dyn SearchIndex>,
    stats: Arc<dyn HashtagStats>,
    /// The use each tag was last written with (only changes are written again);
    /// empty until the first refresh, which writes every tag and purges the
    /// documents an earlier process left.
    last:  Mutex<Option<HashMap<String, TagUsage>>>,
    /// The last revision written: strictly increasing.
    revision: Mutex<u64>,
}

impl HashtagRefresh {
    pub fn new(index: Arc<dyn SearchIndex>, stats: Arc<dyn HashtagStats>) -> Self {
        Self { index, stats, last: Mutex::new(None), revision: Mutex::new(0) }
    }

    pub async fn run_once(&self, now: DateTime<Utc>) -> Result<RefreshReport, SearchError> {
        let usage = self.stats.tag_usage(MAX_TAGS).await?;
        let revision = {
            let mut last = self.revision.lock().unwrap();
            *last = (*last + 1).max(now.timestamp_millis().max(0) as u64);
            *last
        };
        let previous = self.last.lock().unwrap().clone();
        let mut report = RefreshReport::default();
        let mut current = HashMap::with_capacity(usage.len());
        for tag in usage {
            let changed = previous.as_ref().is_none_or(|p| p.get(&tag.tag) != Some(&tag));
            if changed {
                let doc = HashtagDoc {
                    tag:        tag.tag.clone(),
                    post_count: tag.total,
                    searchable: Searchable::VISIBLE,
                    popularity: PopularityScore::new(popularity(&tag)),
                    trending:   trending(&tag),
                    indexed_at: now,
                    version:    DocVersion::new(revision),
                };
                self.index.upsert(&IndexDocument::Hashtag(doc)).await?;
                report.written += 1;
            }
            current.insert(tag.tag.clone(), tag);
        }
        match &previous {
            // First refresh of this process: whatever it did not write is stale.
            None => report.removed = self.stats.purge_hashtags_before(revision).await?,
            Some(previous) => {
                for gone in previous.keys().filter(|t| !current.contains_key(*t)) {
                    self.index.delete(EntityKind::Hashtag, gone).await?;
                    report.removed += 1;
                }
            }
        }
        *self.last.lock().unwrap() = Some(current);
        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::fakes::InMemorySearchIndex;
    use async_trait::async_trait;

    fn usage(tag: &str, total: i64, last_day: i64, last_week: i64) -> TagUsage {
        TagUsage { tag: tag.to_owned(), total, last_day, last_week }
    }

    #[derive(Default)]
    struct ScriptedStats {
        usage:  Mutex<Vec<TagUsage>>,
        purges: Mutex<Vec<u64>>,
    }

    #[async_trait]
    impl HashtagStats for ScriptedStats {
        async fn tag_usage(&self, _: usize) -> Result<Vec<TagUsage>, SearchError> {
            Ok(self.usage.lock().unwrap().clone())
        }
        async fn purge_hashtags_before(&self, revision: u64) -> Result<u64, SearchError> {
            self.purges.lock().unwrap().push(revision);
            Ok(0)
        }
        async fn trending(&self, _: usize) -> Result<Vec<crate::application::port::TrendingTag>, SearchError> {
            Ok(Vec::new())
        }
    }

    #[test]
    fn velocity_beats_size_for_trending_and_volume_for_popularity() {
        let old_giant = usage("giant", 900, 0, 0);
        let rising = usage("rising", 6, 4, 6);
        assert_eq!(trending(&old_giant), 0.0);
        assert!(trending(&rising) > trending(&old_giant));
        assert!(popularity(&old_giant) > popularity(&usage("oneoff", 1, 1, 1)));
    }

    #[tokio::test]
    async fn refresh_writes_changes_and_removes_tags_no_longer_used() {
        let index = Arc::new(InMemorySearchIndex::new());
        let stats = Arc::new(ScriptedStats::default());
        let refresh = HashtagRefresh::new(index.clone(), stats.clone());
        let t0 = Utc::now();

        *stats.usage.lock().unwrap() = vec![usage("rust", 3, 1, 2), usage("go", 1, 0, 0)];
        let first = refresh.run_once(t0).await.unwrap();
        assert_eq!(first.written, 2);
        assert_eq!(stats.purges.lock().unwrap().len(), 1, "the first refresh purges what it did not write");
        assert_eq!(index.hashtag("rust").map(|d| d.post_count), Some(3));

        // Unchanged: nothing written. A tag gone: its document removed.
        *stats.usage.lock().unwrap() = vec![usage("rust", 3, 1, 2)];
        let second = refresh.run_once(t0).await.unwrap();
        assert_eq!(second, RefreshReport { written: 0, removed: 1 });
        assert!(!index.contains(EntityKind::Hashtag, "go"));

        // A post deleted: the count follows, at a newer revision.
        *stats.usage.lock().unwrap() = vec![usage("rust", 2, 0, 1)];
        assert_eq!(refresh.run_once(t0).await.unwrap().written, 1);
        assert_eq!(index.hashtag("rust").map(|d| d.post_count), Some(2));
    }
}
