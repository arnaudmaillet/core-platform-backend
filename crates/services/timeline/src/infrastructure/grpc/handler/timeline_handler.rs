use tonic::{Request, Response, Status};
use uuid::Uuid;

use cqrs::{Envelope, QueryBus};

use transport::grpc::edge;
use crate::application::query::get_audio_feed::GetAudioFeedQuery;
use crate::application::query::get_discovery_feed::GetDiscoveryFeedQuery;
use crate::domain::value_object::{ContentLevel, DiscoveryRanking, Viewer};
use crate::application::query::get_following_feed::GetFollowingFeedQuery;

// ── Proto inclusion ───────────────────────────────────────────────────────────

pub use timeline_api as proto;

pub use proto::timeline_service_server::TimelineServiceServer;

pub struct TimelineServiceHandler<QB>
where
    QB: QueryBus + Send + Sync + 'static,
{
    query_bus: QB,
}

impl<QB> TimelineServiceHandler<QB>
where
    QB: QueryBus + Send + Sync + 'static,
{
    pub fn new(query_bus: QB) -> Self {
        Self { query_bus }
    }
}

// ── RPC implementations ───────────────────────────────────────────────────────

impl<QB> TimelineServiceHandler<QB>
where
    QB: QueryBus + Send + Sync + 'static,
{
    pub async fn get_following_feed(
        &self,
        request: Request<proto::GetFollowingFeedRequest>,
    ) -> Result<Response<proto::GetFollowingFeedResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().profile_id)?;
        let req = request.into_inner();

        let query = GetFollowingFeedQuery {
            profile_id: req.profile_id,
            limit:      req.limit,
            page_token: if req.page_token.is_empty() { None } else { Some(req.page_token) },
        };

        let page = self
            .query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), query))
            .await
            .map_err(cqrs_to_status)?;

        let items = page
            .items
            .into_iter()
            .map(|e| proto::FeedItem {
                post_id:         e.post_id.to_string(),
                author_id:       e.author_id.to_string(),
                published_at_ms: e.published_at_ms,
            })
            .collect();

        Ok(Response::new(proto::GetFollowingFeedResponse {
            items,
            next_page_token: page.next_page_token.unwrap_or_default(),
            is_cold:         page.is_cold,
        }))
    }

    pub async fn get_audio_feed(
        &self,
        request: Request<proto::GetAudioFeedRequest>,
    ) -> Result<Response<proto::GetAudioFeedResponse>, Status> {
        let req = request.into_inner();

        let query = GetAudioFeedQuery {
            audio_id:   req.audio_id,
            limit:      req.limit,
            page_token: req.page_token,
        };

        let result = self
            .query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), query))
            .await
            .map_err(cqrs_to_status)?;

        let items = result
            .items
            .into_iter()
            .map(|item| proto::AudioFeedItem {
                post_id:         item.post_id,
                author_id:       item.author_id,
                published_at_ms: item.published_at_ms,
            })
            .collect();

        Ok(Response::new(proto::GetAudioFeedResponse {
            items,
            next_token: result.next_token,
        }))
    }
}

impl<QB> TimelineServiceHandler<QB>
where
    QB: QueryBus + Send + Sync + 'static,
{
    pub async fn get_discovery_feed(
        &self,
        request: Request<proto::GetDiscoveryFeedRequest>,
    ) -> Result<Response<proto::GetDiscoveryFeedResponse>, Status> {
        let (viewer, guest) = viewer_of(&request);
        let guest_principal = guest.then(|| edge::principal(&request).map(|p| p.account_id().to_owned()))
            .flatten();
        // 13–17 (the token's `age` bracket, #652) never see sensitive content.
        let minor = edge::principal(&request).is_some_and(|p| p.is_minor());
        let req = request.into_inner();

        let ranking = match proto::DiscoveryRanking::try_from(req.ranking) {
            Ok(proto::DiscoveryRanking::Trending) => DiscoveryRanking::Trending,
            Ok(proto::DiscoveryRanking::Recent) => DiscoveryRanking::Recent,
            Ok(proto::DiscoveryRanking::Nearby) => DiscoveryRanking::Nearby,
            _ => DiscoveryRanking::ForYou,
        };
        let content_level = content_level_for(req.content_level, guest, minor);

        let query = GetDiscoveryFeedQuery {
            ranking,
            viewer,
            guest: guest_principal,
            content_level,
            lat:        req.lat,
            lng:        req.lng,
            limit:      req.limit,
            page_token: if req.page_token.is_empty() { None } else { Some(req.page_token) },
        };

        let page = self
            .query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), query))
            .await
            .map_err(cqrs_to_status)?;

        let items = page
            .items
            .into_iter()
            .map(|e| proto::FeedItem {
                post_id:         e.post_id.to_string(),
                author_id:       e.author_id.to_string(),
                published_at_ms: e.published_at_ms,
            })
            .collect();

        Ok(Response::new(proto::GetDiscoveryFeedResponse {
            items,
            next_page_token:       page.next_page_token.unwrap_or_default(),
            region_applied:        String::new(), // v1: one global pool
            content_level_applied: match content_level {
                ContentLevel::Restricted => proto::ContentLevel::Restricted,
                ContentLevel::Standard => proto::ContentLevel::Standard,
            } as i32,
        }))
    }
}

/// The sensitive-content level a reader gets: a guest or a 13–17 reader never
/// more than RESTRICTED; anyone else what it asks for (the client sends the
/// profile's sensitive-content setting, #662), RESTRICTED by default.
fn content_level_for(requested: i32, guest: bool, minor: bool) -> ContentLevel {
    match proto::ContentLevel::try_from(requested) {
        Ok(proto::ContentLevel::Standard) if !guest && !minor => ContentLevel::Standard,
        _ => ContentLevel::Restricted,
    }
}

/// The reader, from how the request arrived (never a request field), and
/// whether it is a guest session (or an anonymous caller).
fn viewer_of<T>(request: &Request<T>) -> (Viewer, bool) {
    match edge::viewer(request) {
        edge::Viewer::Internal => (Viewer::Internal, false),
        edge::Viewer::Anonymous => (Viewer::Profiles(Vec::new()), true),
        edge::Viewer::Member { profile_ids, .. } => {
            let guest = edge::principal(request).is_some_and(|p| p.is_guest());
            (Viewer::Profiles(profile_ids), guest)
        }
    }
}

// ── Proto trait implementation ────────────────────────────────────────────────

#[tonic::async_trait]
impl<QB> proto::timeline_service_server::TimelineService for TimelineServiceHandler<QB>
where
    QB: QueryBus + Send + Sync + 'static,
{
    async fn get_following_feed(
        &self,
        request: Request<proto::GetFollowingFeedRequest>,
    ) -> Result<Response<proto::GetFollowingFeedResponse>, Status> {
        self.get_following_feed(request).await
    }

    async fn get_audio_feed(
        &self,
        request: Request<proto::GetAudioFeedRequest>,
    ) -> Result<Response<proto::GetAudioFeedResponse>, Status> {
        self.get_audio_feed(request).await
    }

    async fn get_discovery_feed(
        &self,
        request: Request<proto::GetDiscoveryFeedRequest>,
    ) -> Result<Response<proto::GetDiscoveryFeedResponse>, Status> {
        self.get_discovery_feed(request).await
    }
}

// ── Error mapping ─────────────────────────────────────────────────────────────

pub fn cqrs_to_status(err: cqrs::error::CqrsError) -> Status {
    use cqrs::error::CqrsError;
    match err {
        CqrsError::HandlerNotFound { type_name } => {
            Status::unimplemented(format!("no handler registered for {type_name}"))
        }
        CqrsError::DuplicateRegistration { type_name } => {
            Status::internal(format!("duplicate handler for {type_name}"))
        }
        CqrsError::Handler(boxed) => {
            use error::AppError as _;
            let msg       = boxed.to_string();
            let retryable = boxed.is_retryable();
            match boxed.http_status().as_u16() {
                403       => Status::permission_denied(msg),
                404       => Status::not_found(msg),
                409 if retryable => Status::aborted(msg),
                409       => Status::already_exists(msg),
                400 | 422 => Status::failed_precondition(msg),
                503 | 502 => Status::unavailable(msg),
                _         => Status::internal(msg),
            }
        }
    }
}

#[cfg(test)]
mod content_level_tests {
    use super::*;

    #[test]
    fn guests_and_teens_never_get_sensitive_content() {
        let standard = proto::ContentLevel::Standard as i32;
        assert_eq!(content_level_for(standard, false, false), ContentLevel::Standard);
        assert_eq!(content_level_for(standard, true, false), ContentLevel::Restricted, "guest");
        assert_eq!(content_level_for(standard, false, true), ContentLevel::Restricted, "13–17");
        assert_eq!(content_level_for(0, false, false), ContentLevel::Restricted, "unspecified");
    }
}
