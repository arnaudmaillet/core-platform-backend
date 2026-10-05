//! A lifted takedown against the live engine, through the real moderation
//! consumer path (#663): the flag comes back and, for a post, search re-reads it
//! from `post` — a document it no longer holds is indexed again, one it still
//! holds is left as stored — and while `post` has not applied the reversal yet
//! the event is retried, not lost.

use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use error::AppError;

use search::domain::{EntityKind, PostEvent, PostSnapshot, SourceEvent};
use search::error::SearchError;
use search::infrastructure::decode::ContentRef;
use search::infrastructure::decode::ModerationWireEvent;
use search::infrastructure::decode::wire::{EnforcementWire, SubjectWire};
use search::infrastructure::hydrate::SourceHydrator;

use crate::search_it::harness::Harness;

/// `post`'s `GetPost`, as search's hydrator turns it into a source event: the
/// post reinstated (content version = its `updated_at`), or still restricted.
struct Post {
    caption: &'static str,
    updated_at_ms: u64,
    caught_up: AtomicBool,
}

#[async_trait]
impl SourceHydrator for Post {
    async fn hydrate(&self, _: ContentRef, _: DateTime<Utc>) -> Result<SourceEvent, SearchError> {
        unreachable!("moderation events need no content")
    }
    async fn reinstated_post(&self, post_id: &str) -> Result<SourceEvent, SearchError> {
        if !self.caught_up.load(Ordering::SeqCst) {
            return Err(SearchError::SourceNotConverged { id: post_id.to_owned() });
        }
        Ok(SourceEvent::Post(PostEvent::Published(PostSnapshot {
            post_id: post_id.to_owned(),
            author_id: "alice".to_owned(),
            author_handle: "alice".to_owned(),
            caption: self.caption.to_owned(),
            hashtags: vec![],
            thumbnail_key: String::new(),
            created_at: Utc.timestamp_opt(1_699_000_000, 0).unwrap(),
            visible_until: None,
            revision: self.updated_at_ms,
        })))
    }
}

fn reversal(post_id: &str) -> ModerationWireEvent {
    ModerationWireEvent::EnforcementReversed(EnforcementWire {
        subject: SubjectWire { entity_type: "post".to_owned(), entity_id: post_id.to_owned() },
        action: None,
        occurred_at: Utc.timestamp_millis_opt(300).unwrap(),
    })
}

#[tokio::test]
async fn a_post_dropped_during_its_takedown_comes_back_once_post_caught_up() {
    let h = Harness::start().await;
    h.index_post("p1", "alice", "lighthouse at dusk", 1).await;
    h.hide(EntityKind::Post, "p1", 100).await;
    // Re-hydrated (or deleted and restored) during the takedown: gone.
    h.delete_post("p1").await;
    h.refresh().await;
    assert!(h.search_ids("lighthouse").await.is_empty());

    let post = Post { caption: "lighthouse at dusk", updated_at_ms: 5, caught_up: AtomicBool::new(false) };

    // `post` has not applied the reversal yet: retried (SCH-8004), not lost.
    let err = h.moderation(&reversal("p1"), &post).await.unwrap_err();
    assert_eq!(err.error_code(), "SCH-8004");
    assert!(err.is_retryable());
    h.refresh().await;
    assert!(h.search_ids("lighthouse").await.is_empty());

    // The retry, once `post` caught up: indexed again and searchable.
    post.caught_up.store(true, Ordering::SeqCst);
    h.moderation(&reversal("p1"), &post).await.expect("reversal");
    h.refresh().await;
    assert_eq!(h.search_ids("lighthouse").await, vec!["p1".to_owned()]);
}

#[tokio::test]
async fn a_post_still_indexed_keeps_its_stored_content_on_a_reversal() {
    let h = Harness::start().await;
    h.index_post("p2", "alice", "harbour at noon", 50).await;
    h.hide(EntityKind::Post, "p2", 100).await;
    h.refresh().await;
    assert!(h.search_ids("harbour").await.is_empty());

    // The re-read is not newer than what is stored: the content stays as is,
    // only the moderation flag lifts.
    let post = Post { caption: "something else", updated_at_ms: 50, caught_up: AtomicBool::new(true) };
    h.moderation(&reversal("p2"), &post).await.expect("reversal");
    h.refresh().await;
    assert_eq!(h.search_ids("harbour").await, vec!["p2".to_owned()]);
    assert!(h.search_ids("something").await.is_empty(), "stored content kept");
}
