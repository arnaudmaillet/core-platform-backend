//! gRPC request handler for `search.v1`. Each method translates an inbound
//! Protobuf request into a validated domain query, runs it through the query-bus
//! handler with a fresh correlation id, and maps the [`SearchResults`] /
//! [`Suggestions`] (or [`SearchError`]) back to Protobuf / [`Status`].

use std::sync::Arc;

use cqrs::{Envelope, QueryHandler};
use error::AppError;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::application::port::AudienceGate;
use crate::application::query::{
    filter_results, filter_suggestions, RunSearch, RunSuggest, SearchHandler, SuggestHandler,
};
use crate::domain::{
    AuthorId, EntityKind, HitDisplay, SearchHit, SearchQuery, SearchResults, SortStrategy,
    SuggestQuery, Suggestions, Viewer,
};
use transport::grpc::edge;
use crate::error::SearchError;

pub use search_api as proto;

/// gRPC handler for the `search.v1.SearchService`. Holds the two query handlers;
/// `MultiSearch` fans out over the search handler.
#[derive(Clone)]
pub struct SearchServiceHandler {
    search: Arc<SearchHandler>,
    suggest: Arc<SuggestHandler>,
    /// social-graph's access check: hits a client may not see are dropped.
    audience: Arc<dyn AudienceGate>,
}

impl SearchServiceHandler {
    pub fn new(
        search: Arc<SearchHandler>,
        suggest: Arc<SuggestHandler>,
        audience: Arc<dyn AudienceGate>,
    ) -> Self {
        Self { search, suggest, audience }
    }

    pub async fn search(
        &self,
        request: Request<proto::SearchRequest>,
    ) -> Result<Response<proto::SearchResponse>, Status> {
        let viewer = viewer_of(&request);
        let results = self.run_search(&viewer, request.into_inner()).await?;
        Ok(Response::new(results))
    }

    pub async fn suggest(
        &self,
        request: Request<proto::SuggestRequest>,
    ) -> Result<Response<proto::SuggestResponse>, Status> {
        let viewer = viewer_of(&request);
        let req = request.into_inner();
        let query = SuggestQuery::new(req.prefix, entity_kinds(&req.entity_types), req.limit.max(0) as u32)
            .map_err(to_status)?;
        let suggestions = self
            .suggest
            .handle(Envelope::new(Uuid::now_v7(), RunSuggest { query }))
            .await
            .map_err(to_status)?;
        let suggestions = filter_suggestions(self.audience.as_ref(), &viewer, suggestions).await;
        Ok(Response::new(suggestions_to_proto(suggestions)))
    }

    pub async fn multi_search(
        &self,
        request: Request<proto::MultiSearchRequest>,
    ) -> Result<Response<proto::MultiSearchResponse>, Status> {
        let viewer = viewer_of(&request);
        let mut responses = Vec::with_capacity(request.get_ref().searches.len());
        for search in request.into_inner().searches {
            responses.push(self.run_search(&viewer, search).await?);
        }
        Ok(Response::new(proto::MultiSearchResponse { responses }))
    }

    async fn run_search(
        &self,
        viewer: &Viewer,
        req: proto::SearchRequest,
    ) -> Result<proto::SearchResponse, Status> {
        let query = SearchQuery::new(
            req.query,
            entity_kinds(&req.entity_types),
            sort_from(req.sort),
            req.page_size.max(0) as u32,
            optional(req.page_token),
            req.exclude_author_ids
                .into_iter()
                .filter_map(|id| AuthorId::new(id).ok())
                .collect(),
        )
        .map_err(to_status)?;
        let results = self
            .search
            .handle(Envelope::new(Uuid::now_v7(), RunSearch { query }))
            .await
            .map_err(to_status)?;
        let results = filter_results(self.audience.as_ref(), viewer, results).await;
        Ok(results_to_proto(results))
    }
}

/// The reader, from how the request arrived: the mesh is unfiltered; an
/// anonymous client has no profiles.
fn viewer_of<T>(request: &Request<T>) -> Viewer {
    match edge::viewer(request) {
        edge::Viewer::Internal => Viewer::Internal,
        edge::Viewer::Anonymous => Viewer::Profiles(Vec::new()),
        edge::Viewer::Member { profile_ids, .. } => Viewer::Profiles(profile_ids),
    }
}

// ── proto → domain ────────────────────────────────────────────────────────────

fn entity_kinds(raw: &[i32]) -> Vec<EntityKind> {
    raw.iter().filter_map(|v| kind_from(*v)).collect()
}

fn kind_from(value: i32) -> Option<EntityKind> {
    match value {
        1 => Some(EntityKind::Profile),
        2 => Some(EntityKind::Post),
        3 => Some(EntityKind::Hashtag),
        _ => None,
    }
}

fn sort_from(value: i32) -> SortStrategy {
    match value {
        2 => SortStrategy::Recency,
        3 => SortStrategy::Popularity,
        _ => SortStrategy::Relevance,
    }
}

fn optional(token: String) -> Option<String> {
    if token.is_empty() { None } else { Some(token) }
}

// ── domain → proto ────────────────────────────────────────────────────────────

fn results_to_proto(results: SearchResults) -> proto::SearchResponse {
    proto::SearchResponse {
        hits: results.hits.into_iter().map(hit_to_proto).collect(),
        next_page_token: results.next_page_token.unwrap_or_default(),
        estimated_total: results.estimated_total as i64,
        degraded: results.degraded,
    }
}

fn hit_to_proto(hit: SearchHit) -> proto::SearchHit {
    proto::SearchHit {
        entity_type: kind_to_proto(hit.kind),
        id: hit.id,
        score: hit.score,
        snippet: hit.snippet,
        display: Some(display_to_proto(hit.display)),
    }
}

fn display_to_proto(display: HitDisplay) -> proto::search_hit::Display {
    use proto::search_hit::Display;
    match display {
        HitDisplay::Profile {
            handle,
            display_name,
            avatar_key,
            verified,
        } => Display::Profile(proto::ProfileHit {
            handle,
            display_name,
            avatar_key,
            verified,
        }),
        HitDisplay::Post {
            author_id,
            author_handle,
            thumbnail_key,
            created_at,
        } => Display::Post(proto::PostHit {
            author_id,
            author_handle,
            thumbnail_key,
            created_at: Some(to_timestamp(created_at)),
        }),
        HitDisplay::Hashtag { tag, post_count } => {
            Display::Hashtag(proto::HashtagHit { tag, post_count })
        }
    }
}

fn suggestions_to_proto(suggestions: Suggestions) -> proto::SuggestResponse {
    proto::SuggestResponse {
        suggestions: suggestions
            .suggestions
            .into_iter()
            .map(|s| proto::Suggestion {
                entity_type: kind_to_proto(s.kind),
                text: s.text,
                id: s.id.unwrap_or_default(),
                score: s.score,
            })
            .collect(),
    }
}

fn kind_to_proto(kind: EntityKind) -> i32 {
    match kind {
        EntityKind::Profile => 1,
        EntityKind::Post => 2,
        EntityKind::Hashtag => 3,
    }
}

fn to_timestamp(dt: chrono::DateTime<chrono::Utc>) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: dt.timestamp(),
        nanos: dt.timestamp_subsec_nanos() as i32,
    }
}

fn to_status(err: SearchError) -> Status {
    let message = err.to_string();
    match err.http_status().as_u16() {
        400 | 422 => Status::invalid_argument(message),
        404 => Status::not_found(message),
        503 => Status::unavailable(message),
        504 => Status::deadline_exceeded(message),
        _ => Status::internal(message),
    }
}
