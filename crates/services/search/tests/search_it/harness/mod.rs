//! Integration harness: boots an ephemeral single-node OpenSearch, creates a
//! freshly-namespaced index set per scenario (so the suite runs in parallel
//! against the shared container), and wires a real search graph against it through
//! the production composition root ([`search::app::App::compose`]). Ingestion is
//! driven through the real [`ProjectionHandler`] + [`OpenSearchIndex`] adapter with
//! fully-formed `SourceEvent`s; queries go through the gRPC handler.
#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, TimeZone, Utc};
use cqrs::Envelope;
use tonic::Request;
use uuid::Uuid;

use search::app::App;
use search::application::command::ProjectionHandler;
use search::application::port::{IndexAdmin, SearchIndex};
use search::domain::{
    ComplianceEvent, EntityKind, HashtagEvent, HashtagSnapshot, ModerationEvent, PostEvent,
    PostSnapshot, ProfileEvent, ProfileSnapshot, SourceEvent, VisibilityChange,
};
use search::error::SearchError;
use search::infrastructure::consumer::moderation_consumer;
use search::infrastructure::decode::ModerationWireEvent;
use search::infrastructure::grpc::{SearchServiceHandler, proto};
use search::infrastructure::hydrate::SourceHydrator;
use search::infrastructure::index::{OpenSearchConfig, OpenSearchIndex};

/// Proto `SearchEntityType` discriminants, for entity-type filtering in scenarios.
pub const PROFILE: i32 = 1;
pub const POST: i32 = 2;
pub const HASHTAG: i32 = 3;

pub struct Harness {
    pub handler: SearchServiceHandler,
    projection: ProjectionHandler,
    index: Arc<OpenSearchIndex>,
}

impl Harness {
    pub async fn start() -> Self {
        let base_url = test_support::containers::opensearch_ready().await;
        // Fresh per-scenario namespace so parallel scenarios never collide.
        let prefix = format!("search-it-{}", Uuid::now_v7().simple());
        let index = Arc::new(OpenSearchIndex::from_config(OpenSearchConfig::new(base_url, prefix)));

        // The container can report "started" before the REST layer accepts writes.
        {
            let index = Arc::clone(&index);
            test_support::await_until("opensearch REST ready", Duration::from_secs(90), move || {
                let index = Arc::clone(&index);
                async move { index.ping().await.is_ok() }
            })
            .await;
        }
        // Retried, not expected: all 13 scenarios boot in parallel and each
        // creates its own namespaced index set — the creation stampede against
        // one single-node OpenSearch times out sporadically under load
        // (EngineTimeout — the suite's only observed flake, on the CI matrix
        // and locally at 4-way parallelism). Creation is idempotent, so
        // retrying to a deadline is exactly right.
        {
            let index = Arc::clone(&index);
            test_support::await_until("opensearch indices created", Duration::from_secs(90), move || {
                let index = Arc::clone(&index);
                async move { index.ensure_indices().await.is_ok() }
            })
            .await;
        }

        let port: Arc<dyn SearchIndex> = Arc::clone(&index) as Arc<dyn SearchIndex>;
        let projection = ProjectionHandler::new(Arc::clone(&port));
        // The harness calls the handler without an edge principal (the mesh),
        // so the audience filter never runs; any gate will do.
        let handler = App::compose(port, Arc::new(NoAudienceCheck));
        Self {
            handler,
            projection,
            index,
        }
    }

    // ── Ingestion (drives the real ProjectionHandler → OpenSearch adapter) ─────

    async fn apply(&self, event: SourceEvent) {
        self.projection
            .apply(Envelope::new(Uuid::now_v7(), event), now())
            .await
            .expect("it: apply source event");
    }

    pub async fn index_post(&self, post_id: &str, author: &str, caption: &str, revision: u64) {
        self.index_post_tags(post_id, author, caption, vec![], revision).await
    }

    pub async fn index_post_tags(
        &self,
        post_id: &str,
        author: &str,
        caption: &str,
        hashtags: Vec<&str>,
        revision: u64,
    ) {
        self.apply(SourceEvent::Post(PostEvent::Published(PostSnapshot {
            post_id: post_id.to_owned(),
            author_id: author.to_owned(),
            author_handle: author.to_owned(),
            caption: caption.to_owned(),
            hashtags: hashtags.into_iter().map(str::to_owned).collect(),
            thumbnail_key: String::new(),
            created_at: created(),
            visible_until: None,
            revision,
        })))
        .await;
    }

    pub async fn index_profile(
        &self,
        profile_id: &str,
        handle: &str,
        display_name: &str,
        bio: &str,
        revision: u64,
    ) {
        self.apply(SourceEvent::Profile(ProfileEvent::Upserted(ProfileSnapshot {
            profile_id: profile_id.to_owned(),
            handle: handle.to_owned(),
            display_name: display_name.to_owned(),
            bio: bio.to_owned(),
            avatar_key: String::new(),
            verified: false,
            created_at: created(),
            revision,
        })))
        .await;
    }

    pub async fn index_hashtag(&self, tag: &str, post_count: i64, revision: u64) {
        self.apply(SourceEvent::Hashtag(HashtagEvent::Observed(HashtagSnapshot {
            tag: tag.to_owned(),
            post_count,
            revision,
        })))
        .await;
    }

    pub async fn hide(&self, kind: EntityKind, id: &str, occurred_ms: i64) {
        self.apply(SourceEvent::Moderation(ModerationEvent::VisibilityRevoked(
            VisibilityChange {
                kind: Some(kind),
                id: id.to_owned(),
                occurred_at: ms(occurred_ms),
            },
        )))
        .await;
    }

    /// A hard delete of a post document (as a re-hydration during a takedown,
    /// or a delete, leaves it).
    pub async fn delete_post(&self, post_id: &str) {
        self.apply(SourceEvent::Post(PostEvent::Deleted(search::domain::EntityDeletion { id: post_id.to_owned() })))
            .await;
    }

    /// Runs one `moderation.v1.events` event through the real consumer path,
    /// with `source` standing in for `post`'s `GetPost`.
    pub async fn moderation(&self, event: &ModerationWireEvent, source: &dyn SourceHydrator) -> Result<(), SearchError> {
        moderation_consumer::process(&self.projection, source, event).await
    }

    pub async fn restore(&self, kind: EntityKind, id: &str, occurred_ms: i64) {
        self.apply(SourceEvent::Moderation(ModerationEvent::VisibilityRestored(
            VisibilityChange {
                kind: Some(kind),
                id: id.to_owned(),
                occurred_at: ms(occurred_ms),
            },
        )))
        .await;
    }

    /// Owner-authority masking (a profile masking itself) — distinct from moderation.
    pub async fn owner_hide(&self, profile_id: &str, occurred_ms: i64) {
        self.apply(SourceEvent::Profile(ProfileEvent::OwnerHidden {
            profile_id: profile_id.to_owned(),
            occurred_at: ms(occurred_ms),
        }))
        .await;
    }

    pub async fn owner_restore(&self, profile_id: &str, occurred_ms: i64) {
        self.apply(SourceEvent::Profile(ProfileEvent::OwnerRestored {
            profile_id: profile_id.to_owned(),
            occurred_at: ms(occurred_ms),
        }))
        .await;
    }

    /// The profile's own "findable by search" setting (#661).
    /// Indexes a post created at `created_at`, leaving search at `visible_until`
    /// (its author's post window, as the hydrated view carries it).
    pub async fn index_post_at(
        &self,
        post_id: &str,
        author: &str,
        caption: &str,
        created_at: DateTime<Utc>,
        visible_until: Option<DateTime<Utc>>,
    ) {
        self.apply(SourceEvent::Post(PostEvent::Published(PostSnapshot {
            post_id: post_id.to_owned(),
            author_id: author.to_owned(),
            author_handle: author.to_owned(),
            caption: caption.to_owned(),
            hashtags: vec![],
            thumbnail_key: String::new(),
            created_at,
            visible_until,
            revision: 1,
        })))
        .await;
    }

    /// The author's post window changes (#664).
    pub async fn post_window(&self, author: &str, window_days: Option<u32>, occurred_ms: i64) {
        self.apply(SourceEvent::Profile(ProfileEvent::PostWindowChanged {
            profile_id: author.to_owned(),
            window_days,
            occurred_at: ms(occurred_ms),
        }))
        .await;
    }

    pub async fn discoverable(&self, profile_id: &str, searchable: bool, occurred_ms: i64) {
        self.apply(SourceEvent::Profile(ProfileEvent::DiscoverabilityChanged {
            profile_id: profile_id.to_owned(),
            searchable,
            occurred_at: ms(occurred_ms),
        }))
        .await;
    }

    pub async fn purge(&self, author: &str) {
        self.apply(SourceEvent::Compliance(ComplianceEvent::ActorPurged {
            author_id: author.to_owned(),
        }))
        .await;
    }

    /// Make prior writes searchable (OpenSearch is near-real-time).
    pub async fn refresh(&self) {
        self.index.refresh().await.expect("it: refresh");
    }

    // ── Query (through the gRPC handler) ──────────────────────────────────────

    pub async fn search(&self, query: &str) -> proto::SearchResponse {
        self.search_opts(query, vec![], vec![]).await
    }

    pub async fn search_opts(
        &self,
        query: &str,
        entity_types: Vec<i32>,
        exclude: Vec<&str>,
    ) -> proto::SearchResponse {
        let request = Request::new(proto::SearchRequest {
            query: query.to_owned(),
            entity_types,
            sort: 0,
            page_size: 20,
            page_token: String::new(),
            exclude_author_ids: exclude.into_iter().map(str::to_owned).collect(),
        });
        self.handler
            .search(request)
            .await
            .expect("it: search rpc")
            .into_inner()
    }

    /// Convenience: the sorted hit ids of a query (after a refresh).
    pub async fn search_ids(&self, query: &str) -> Vec<String> {
        self.refresh().await;
        ids(&self.search(query).await)
    }
}

/// Sorted hit ids, for order-independent assertions.
pub fn ids(resp: &proto::SearchResponse) -> Vec<String> {
    let mut ids: Vec<String> = resp.hits.iter().map(|h| h.id.clone()).collect();
    ids.sort();
    ids
}

fn now() -> DateTime<Utc> {
    Utc.timestamp_opt(1_700_000_500, 0).unwrap()
}

fn created() -> DateTime<Utc> {
    Utc.timestamp_opt(1_699_000_000, 0).unwrap()
}

fn ms(millis: i64) -> DateTime<Utc> {
    Utc.timestamp_millis_opt(millis).single().unwrap()
}

/// An audience gate for a harness that only reads as the mesh (never filtered).
struct NoAudienceCheck;

#[async_trait::async_trait]
impl search::application::port::AudienceGate for NoAudienceCheck {
    async fn access(
        &self,
        _viewers: &[String],
        _targets: &[String],
    ) -> Result<std::collections::HashMap<String, search::domain::ContentAccess>, search::error::SearchError> {
        unreachable!("mesh reads are not filtered")
    }
}
