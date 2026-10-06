use std::collections::HashMap;

use async_trait::async_trait;

use crate::domain::value_object::{
    AuthorId, DiscoveryMeta, DiscoveryStream, PostId, Restriction, StreamPosition,
};
use crate::error::TimelineError;

/// One entry read from a pool stream, at its stream position.
#[derive(Debug, Clone, PartialEq)]
pub struct PoolEntry {
    pub post_id:   PostId,
    pub author_id: AuthorId,
    pub position:  StreamPosition,
}

/// The discovery pool: posts published in the last window, indexed newest-first
/// (`Recent`), by hot score once they have popularity (`Hot`) and newest-first
/// while they have none (`Fresh`), plus what is known of each post
/// ([`DiscoveryMeta`]).
///
/// The indices are candidates, not the truth: a read keeps a post only if its
/// meta says it may be shown ([`DiscoveryMeta::shown_at`]), so a write that
/// lands out of order (a takedown racing the publication) never surfaces a post.
/// Every write is idempotent.
#[async_trait]
pub trait DiscoveryPool: Send + Sync + 'static {
    /// A post was published (with its caption's hashtags). Indexed unless a
    /// recorded deletion or a hiding restriction says otherwise; ignored once
    /// older than the window.
    async fn record_published(
        &self,
        post_id:         &PostId,
        author_id:       &AuthorId,
        published_at_ms: i64,
        tags:            &[String],
    ) -> Result<(), TimelineError>;

    /// A post's (all-time) popularity changed: its hot score follows, and a
    /// fresh post with some popularity moves to `Hot`. Unknown posts are ignored.
    async fn record_popularity(&self, post_id: &PostId, popularity: f64) -> Result<(), TimelineError>;

    /// Moderation put `restriction` in force at `version`. An older version than
    /// the recorded one is ignored; an equal one re-applies (heals a partial
    /// write). Recorded even before the publication is seen.
    async fn record_restriction(
        &self,
        post_id:     &PostId,
        restriction: Restriction,
        version:     i64,
    ) -> Result<(), TimelineError>;

    /// The post was deleted: out of the pool for good.
    async fn record_deleted(&self, post_id: &PostId) -> Result<(), TimelineError>;

    /// Up to `count` entries of `stream` strictly after `after` (from the top
    /// when `None`), in stream order.
    async fn range(
        &self,
        stream: DiscoveryStream,
        after:  Option<&StreamPosition>,
        count:  usize,
    ) -> Result<Vec<PoolEntry>, TimelineError>;

    /// What is known of each post; posts the pool never heard of are absent.
    async fn meta(&self, posts: &[PostId]) -> Result<HashMap<PostId, DiscoveryMeta>, TimelineError>;
}
