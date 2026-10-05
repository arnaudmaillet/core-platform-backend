//! Blue-green reindex orchestration — the zero-downtime path for a mapping /
//! analyzer change, and the rebuild-from-truth path when the index is lost.
//!
//! The sequence is deliberate: build a fresh physical index, point the **write**
//! alias at it (so live writes *and* the backfill both land on the new index),
//! backfill from the source-of-record, then cut over the **read** alias last. A
//! backfilled document can never clobber a newer live write, because the engine's
//! external-version guard rejects the lower-versioned backfill write — so the two
//! streams converge safely without coordination.

use std::sync::Arc;

use crate::application::port::{BackfillSource, IndexAdmin, SearchIndex};
use crate::domain::EntityKind;
use crate::error::SearchError;

/// Outcome of a reindex run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReindexReport {
    pub kind: EntityKind,
    pub new_index: String,
    pub documents_backfilled: u64,
}

pub struct Reindexer {
    admin: Arc<dyn IndexAdmin>,
    index: Arc<dyn SearchIndex>,
}

impl Reindexer {
    pub fn new(admin: Arc<dyn IndexAdmin>, index: Arc<dyn SearchIndex>) -> Self {
        Self { admin, index }
    }

    /// Reindex one `kind` into a fresh physical index suffixed `new_suffix`.
    pub async fn reindex(
        &self,
        kind: EntityKind,
        new_suffix: &str,
        source: &dyn BackfillSource,
    ) -> Result<ReindexReport, SearchError> {
        // 1. Fresh physical index with the current mapping.
        let new_index = self.admin.create_index_version(kind, new_suffix).await?;

        // 2. Live writes follow the write alias onto the new index from here on.
        self.admin.swap_write_alias(kind, &new_index).await?;

        // 3. Backfill from the source-of-record (writes route to the new index via
        //    the write alias; external versioning protects newer live writes).
        let documents = source.scan(kind).await?;
        let mut backfilled = 0u64;
        for document in &documents {
            self.index.upsert(document).await?;
            backfilled += 1;
        }

        // 4. Cut readers over last — zero-downtime.
        self.admin.swap_read_alias(kind, &new_index).await?;

        Ok(ReindexReport {
            kind,
            new_index,
            documents_backfilled: backfilled,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;

    use super::*;
    use crate::application::fakes::InMemorySearchIndex;
    use crate::domain::{
        AuthorId, DocVersion, IndexDocument, PopularityScore, PostDoc, Searchable,
    };

    /// Records the alias operations in order so the test can assert the blue-green
    /// sequence (write-swap before backfill, read-swap last).
    #[derive(Default)]
    struct RecordingAdmin {
        log: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl IndexAdmin for RecordingAdmin {
        async fn ensure_indices(&self) -> Result<(), SearchError> {
            Ok(())
        }
        async fn create_index_version(
            &self,
            _kind: EntityKind,
            suffix: &str,
        ) -> Result<String, SearchError> {
            let name = format!("search-posts-{suffix}");
            self.log.lock().unwrap().push(format!("create:{name}"));
            Ok(name)
        }
        async fn swap_write_alias(
            &self,
            _kind: EntityKind,
            physical_index: &str,
        ) -> Result<(), SearchError> {
            self.log.lock().unwrap().push(format!("write:{physical_index}"));
            Ok(())
        }
        async fn swap_read_alias(
            &self,
            _kind: EntityKind,
            physical_index: &str,
        ) -> Result<(), SearchError> {
            self.log.lock().unwrap().push(format!("read:{physical_index}"));
            Ok(())
        }
    }

    struct StubSource(Vec<IndexDocument>);

    #[async_trait]
    impl BackfillSource for StubSource {
        async fn scan(&self, _kind: EntityKind) -> Result<Vec<IndexDocument>, SearchError> {
            Ok(self.0.clone())
        }
    }

    fn post(id: &str) -> IndexDocument {
        IndexDocument::Post(PostDoc {
            post_id: id.to_owned(),
            author_id: AuthorId::new("acct-1").unwrap(),
            author_handle: "alice".to_owned(),
            caption: "hello".to_owned(),
            hashtags: vec![],
            thumbnail_key: String::new(),
            searchable: Searchable::VISIBLE,
            popularity: PopularityScore::ZERO,
            created_at: chrono::Utc::now(),
            indexed_at: chrono::Utc::now(),
            version: DocVersion::new(1),
            visible_until: None,
        })
    }

    #[tokio::test]
    async fn reindex_backfills_then_cuts_over_in_order() {
        let admin = Arc::new(RecordingAdmin::default());
        let index = Arc::new(InMemorySearchIndex::new());
        let reindexer = Reindexer::new(admin.clone(), index.clone());
        let source = StubSource(vec![post("p1"), post("p2")]);

        let report = reindexer
            .reindex(EntityKind::Post, "v2", &source)
            .await
            .unwrap();

        assert_eq!(report.documents_backfilled, 2);
        assert_eq!(report.new_index, "search-posts-v2");
        // The backfilled documents landed in the (new) index.
        assert!(index.contains(EntityKind::Post, "p1"));
        assert!(index.contains(EntityKind::Post, "p2"));
        // Order: create → write-swap (before backfill) → read-swap (last).
        let log = admin.log.lock().unwrap().clone();
        assert_eq!(
            log,
            vec![
                "create:search-posts-v2".to_owned(),
                "write:search-posts-v2".to_owned(),
                "read:search-posts-v2".to_owned(),
            ]
        );
    }
}
