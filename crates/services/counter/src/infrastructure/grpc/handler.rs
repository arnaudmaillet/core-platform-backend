//! gRPC request handler for `counter.v1`. Each method translates an inbound
//! Protobuf request into a validated domain query, runs it through the query-bus
//! handler with a fresh correlation id, and maps the domain result (or
//! [`CounterError`]) back to Protobuf / [`Status`].

use std::sync::Arc;

use chrono::{DateTime, Utc};
use cqrs::{Envelope, QueryHandler};
use error::AppError;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::application::query::{
    BatchGetHandler, CountReader, LikeGuard, RunBatchGet, RunTimeSeries, RunTrending, TimeSeriesHandler,
    TrendingHandler,
};
use transport::grpc::edge;
use crate::domain::{
    BatchGetQuery, BatchReadout, CountSnapshot, EntityId, EntityKind, EntityRef, Metric,
    MetricKind, TimeGranularity, TimeSeriesBucket, TimeSeriesQuery, TrendingItem, TrendingQuery,
    TrendingScope,
};
use crate::error::CounterError;

pub use counter_api as proto;

/// gRPC handler for `counter.v1.CounterService`. Holds the three read handlers.
#[derive(Clone)]
pub struct CounterServiceHandler {
    batch: Arc<BatchGetHandler>,
    trending: Arc<TrendingHandler>,
    timeseries: Arc<TimeSeriesHandler>,
    /// Hidden like counts (#809); off by default.
    likes: LikeGuard,
}

/// The reader from how the request arrived: hidden likes (#809) reach only
/// the author (one of the caller's profiles) and the mesh.
fn reader_of<T>(request: &Request<T>) -> CountReader {
    match edge::viewer(request) {
        edge::Viewer::Internal => CountReader::Internal,
        edge::Viewer::Anonymous => CountReader::Profiles(Vec::new()),
        edge::Viewer::Member { profile_ids, .. } => CountReader::Profiles(profile_ids),
    }
}

impl CounterServiceHandler {
    pub fn new(
        batch: Arc<BatchGetHandler>,
        trending: Arc<TrendingHandler>,
        timeseries: Arc<TimeSeriesHandler>,
    ) -> Self {
        Self {
            batch,
            trending,
            timeseries,
            likes: LikeGuard::default(),
        }
    }

    /// Withholds hidden like counts (#809).
    pub fn with_like_guard(mut self, likes: LikeGuard) -> Self {
        self.likes = likes;
        self
    }

    pub async fn batch_get_counters(
        &self,
        request: Request<proto::BatchGetCountersRequest>,
    ) -> Result<Response<proto::BatchGetCountersResponse>, Status> {
        let reader = reader_of(&request);
        let req = request.into_inner();
        let entities = req
            .entities
            .into_iter()
            .map(entity_from_proto)
            .collect::<Result<Vec<_>, _>>()
            .map_err(to_status)?;
        let metrics = req.metrics.into_iter().filter_map(metric_from_proto).collect();

        let query = BatchGetQuery::new(entities, metrics).map_err(to_status)?;
        let mut readout = self
            .batch
            .handle(Envelope::new(Uuid::now_v7(), RunBatchGet { query }))
            .await
            .map_err(to_status)?;
        self.likes.apply_batch(&reader, &mut readout).await;
        Ok(Response::new(batch_to_proto(readout)))
    }

    pub async fn get_trending(
        &self,
        request: Request<proto::GetTrendingRequest>,
    ) -> Result<Response<proto::GetTrendingResponse>, Status> {
        let reader = reader_of(&request);
        let req = request.into_inner();
        let scope = scope_from_proto(req.scope);
        let metric = metric_from_proto(req.metric).ok_or_else(|| {
            Status::invalid_argument("unsupported or unspecified trending metric")
        })?;
        let scope_key = optional(req.scope_key);
        let query = TrendingQuery::new(scope, scope_key, metric, req.limit.max(0) as usize)
            .map_err(to_status)?;
        let items = self
            .trending
            .handle(Envelope::new(Uuid::now_v7(), RunTrending { query }))
            .await
            .map_err(to_status)?;
        let items = self.likes.apply_trending(&reader, metric, items).await;
        Ok(Response::new(proto::GetTrendingResponse {
            entries: items.into_iter().map(trending_to_proto).collect(),
            degraded: false,
        }))
    }

    pub async fn get_time_series(
        &self,
        request: Request<proto::GetTimeSeriesRequest>,
    ) -> Result<Response<proto::GetTimeSeriesResponse>, Status> {
        let req = request.into_inner();
        let entity = entity_from_proto(
            req.entity
                .ok_or_else(|| Status::invalid_argument("entity is required"))?,
        )
        .map_err(to_status)?;
        let metric = metric_from_proto(req.metric)
            .ok_or_else(|| Status::invalid_argument("unsupported or unspecified metric"))?;
        let granularity = granularity_from_proto(req.granularity);
        let start = ts_to_dt(req.start).ok_or_else(|| Status::invalid_argument("invalid start"))?;
        let end = ts_to_dt(req.end).ok_or_else(|| Status::invalid_argument("invalid end"))?;

        let query =
            TimeSeriesQuery::new(entity, metric, granularity, start, end).map_err(to_status)?;
        let buckets = self
            .timeseries
            .handle(Envelope::new(Uuid::now_v7(), RunTimeSeries { query }))
            .await
            .map_err(to_status)?;
        Ok(Response::new(proto::GetTimeSeriesResponse {
            buckets: buckets.into_iter().map(bucket_to_proto).collect(),
            kind: value_kind_to_proto(metric.kind()),
            degraded: false,
        }))
    }
}

// ── proto → domain ────────────────────────────────────────────────────────────

fn entity_from_proto(e: proto::EntityRef) -> Result<EntityRef, CounterError> {
    let kind = entity_kind_from_proto(e.entity_type).ok_or_else(|| CounterError::DomainViolation {
        field: "entity_type".to_owned(),
        message: "unspecified or unknown entity type".to_owned(),
    })?;
    Ok(EntityRef::new(kind, EntityId::new(e.id)?))
}

fn entity_kind_from_proto(v: i32) -> Option<EntityKind> {
    use proto::CounterEntityType as P;
    match P::try_from(v).ok()? {
        P::Post => Some(EntityKind::Post),
        P::Profile => Some(EntityKind::Profile),
        P::Media => Some(EntityKind::Media),
        P::Hashtag => Some(EntityKind::Hashtag),
        P::Comment => Some(EntityKind::Comment),
        P::Unspecified => None,
    }
}

fn metric_from_proto(v: i32) -> Option<Metric> {
    use proto::CounterMetric as P;
    match P::try_from(v).ok()? {
        P::View => Some(Metric::View),
        P::Impression => Some(Metric::Impression),
        P::Click => Some(Metric::Click),
        P::Like => Some(Metric::Like),
        P::Share => Some(Metric::Share),
        P::Comment => Some(Metric::Comment),
        P::Follower => Some(Metric::Follower),
        P::Following => Some(Metric::Following),
        P::UniqueViewer => Some(Metric::UniqueViewer),
        P::Reach => Some(Metric::Reach),
        P::Unspecified => None,
    }
}

fn scope_from_proto(v: i32) -> TrendingScope {
    use proto::TrendingScope as P;
    match P::try_from(v).unwrap_or(P::Global) {
        P::Hashtag => TrendingScope::Hashtag,
        P::Category => TrendingScope::Category,
        P::Region => TrendingScope::Region,
        _ => TrendingScope::Global,
    }
}

fn granularity_from_proto(v: i32) -> TimeGranularity {
    use proto::TimeGranularity as P;
    match P::try_from(v).unwrap_or(P::Day) {
        P::Hour => TimeGranularity::Hour,
        P::Week => TimeGranularity::Week,
        _ => TimeGranularity::Day,
    }
}

fn ts_to_dt(ts: Option<prost_types::Timestamp>) -> Option<DateTime<Utc>> {
    let ts = ts?;
    DateTime::from_timestamp(ts.seconds, ts.nanos.max(0) as u32)
}

fn optional(s: String) -> Option<String> {
    Some(s).filter(|s| !s.trim().is_empty())
}

// ── domain → proto ────────────────────────────────────────────────────────────

fn batch_to_proto(readout: BatchReadout) -> proto::BatchGetCountersResponse {
    proto::BatchGetCountersResponse {
        snapshots: readout
            .snapshots
            .into_iter()
            .map(|s| snapshot_to_proto(s, readout.degraded))
            .collect(),
        degraded: readout.degraded,
    }
}

fn snapshot_to_proto(snap: CountSnapshot, degraded: bool) -> proto::CounterSnapshot {
    proto::CounterSnapshot {
        entity: Some(entity_to_proto(&snap.entity)),
        values: snap
            .values
            .into_iter()
            .map(|v| proto::CounterValue {
                metric: metric_to_proto(v.metric),
                value: v.value,
                kind: value_kind_to_proto(v.kind),
            })
            .collect(),
        degraded,
    }
}

fn entity_to_proto(e: &EntityRef) -> proto::EntityRef {
    proto::EntityRef {
        entity_type: entity_kind_to_proto(e.kind),
        id: e.id.as_str().to_owned(),
    }
}

fn entity_kind_to_proto(kind: EntityKind) -> i32 {
    use proto::CounterEntityType as P;
    (match kind {
        EntityKind::Post => P::Post,
        EntityKind::Profile => P::Profile,
        EntityKind::Media => P::Media,
        EntityKind::Hashtag => P::Hashtag,
        EntityKind::Comment => P::Comment,
    }) as i32
}

fn metric_to_proto(m: Metric) -> i32 {
    use proto::CounterMetric as P;
    (match m {
        Metric::View => P::View,
        Metric::Impression => P::Impression,
        Metric::Click => P::Click,
        Metric::Like => P::Like,
        Metric::Share => P::Share,
        Metric::Comment => P::Comment,
        Metric::Follower => P::Follower,
        Metric::Following => P::Following,
        Metric::UniqueViewer => P::UniqueViewer,
        Metric::Reach => P::Reach,
    }) as i32
}

fn value_kind_to_proto(kind: MetricKind) -> i32 {
    use proto::CounterValueKind as P;
    (match kind {
        MetricKind::Exact => P::Exact,
        MetricKind::Approximate => P::Approximate,
    }) as i32
}

fn trending_to_proto(item: TrendingItem) -> proto::TrendingEntry {
    proto::TrendingEntry {
        entity: Some(entity_to_proto(&item.entity)),
        score: item.score,
        rank: item.rank as i32,
    }
}

fn bucket_to_proto(b: TimeSeriesBucket) -> proto::TimeSeriesBucket {
    proto::TimeSeriesBucket {
        bucket_start: Some(prost_types::Timestamp {
            seconds: b.bucket_start.timestamp(),
            nanos: b.bucket_start.timestamp_subsec_nanos() as i32,
        }),
        value: b.value,
    }
}

fn to_status(err: CounterError) -> Status {
    let message = err.to_string();
    match err.http_status().as_u16() {
        400 | 422 => Status::invalid_argument(message),
        404 => Status::not_found(message),
        503 => Status::unavailable(message),
        504 => Status::deadline_exceeded(message),
        _ => Status::internal(message),
    }
}
