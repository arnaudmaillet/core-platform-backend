use chrono::{DateTime, Utc};
use tonic::{Request, Response, Status};
use uuid::Uuid;

use cqrs::{CommandBus, Envelope, QueryBus};

use transport::grpc::edge;
use crate::application::command::{
    BlockProfileCommand, FollowProfileCommand, UnblockProfileCommand, UnfollowProfileCommand,
};
use crate::application::query::{
    CheckAccessQuery, GetRelationStatusQuery, ListBlocksQuery, ListFollowersQuery,
    ListFollowingQuery,
};
use crate::domain::access::{ContentAccess, Viewer};
use crate::application::query::get_relation_status::RelationStatusView;
use crate::domain::entity::{BlockEdge, FollowEdge};
use crate::domain::value_object::RelationStatus;

// ── Proto inclusion ───────────────────────────────────────────────────────────
// Generated stubs now come from the contracts tier (`social-graph-api`) instead
// of a local `build.rs`. Aliasing it as `proto` keeps every `proto::…` reference
// site below unchanged.

pub use social_graph_api as proto;

pub use proto::social_graph_service_server::SocialGraphServiceServer;

/// gRPC handler for the SocialGraph service.
///
/// Bridges Protobuf RPCs to CQRS command/query envelopes and back.
/// Zero domain logic lives here — all invariant enforcement is in the
/// domain aggregate and command handlers.
pub struct SocialGraphServiceHandler<CB, QB>
where
    CB: CommandBus + Send + Sync + 'static,
    QB: QueryBus + Send + Sync + 'static,
{
    command_bus: CB,
    query_bus:   QB,
}

impl<CB, QB> SocialGraphServiceHandler<CB, QB>
where
    CB: CommandBus + Send + Sync + 'static,
    QB: QueryBus + Send + Sync + 'static,
{
    pub fn new(command_bus: CB, query_bus: QB) -> Self {
        Self { command_bus, query_bus }
    }

    fn ok_response(actor_id: &str, target_id: &str) -> Response<proto::CommandResponse> {
        Response::new(proto::CommandResponse {
            success:   true,
            actor_id:  actor_id.to_owned(),
            target_id: target_id.to_owned(),
        })
    }
}

// ── Command implementations ───────────────────────────────────────────────────

impl<CB, QB> SocialGraphServiceHandler<CB, QB>
where
    CB: CommandBus + Send + Sync + 'static,
    QB: QueryBus + Send + Sync + 'static,
{
    pub async fn follow(
        &self,
        request: Request<proto::FollowRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().actor_id)?;
        let req = request.into_inner();
        let cmd = FollowProfileCommand {
            actor_id:  req.actor_id.clone(),
            target_id: req.target_id.clone(),
        };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| Self::ok_response(&req.actor_id, &req.target_id))
            .map_err(cqrs_to_status)
    }

    pub async fn unfollow(
        &self,
        request: Request<proto::UnfollowRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().actor_id)?;
        let req = request.into_inner();
        let cmd = UnfollowProfileCommand {
            actor_id:  req.actor_id.clone(),
            target_id: req.target_id.clone(),
        };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| Self::ok_response(&req.actor_id, &req.target_id))
            .map_err(cqrs_to_status)
    }

    pub async fn block(
        &self,
        request: Request<proto::BlockRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().actor_id)?;
        let req = request.into_inner();
        let cmd = BlockProfileCommand {
            actor_id:  req.actor_id.clone(),
            target_id: req.target_id.clone(),
        };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| Self::ok_response(&req.actor_id, &req.target_id))
            .map_err(cqrs_to_status)
    }

    pub async fn unblock(
        &self,
        request: Request<proto::UnblockRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().actor_id)?;
        let req = request.into_inner();
        let cmd = UnblockProfileCommand {
            actor_id:  req.actor_id.clone(),
            target_id: req.target_id.clone(),
        };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| Self::ok_response(&req.actor_id, &req.target_id))
            .map_err(cqrs_to_status)
    }
}

// ── Query implementations ─────────────────────────────────────────────────────

impl<CB, QB> SocialGraphServiceHandler<CB, QB>
where
    CB: CommandBus + Send + Sync + 'static,
    QB: QueryBus + Send + Sync + 'static,
{
    pub async fn get_relation_status(
        &self,
        request: Request<proto::GetRelationStatusRequest>,
    ) -> Result<Response<proto::RelationStatusView>, Status> {
        edge::require_profile(&request, &request.get_ref().actor_id)?;
        let req = request.into_inner();
        let query = GetRelationStatusQuery {
            actor_id:  req.actor_id,
            target_id: req.target_id,
        };
        let view: RelationStatusView = self
            .query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), query))
            .await
            .map_err(cqrs_to_status)?;

        Ok(Response::new(relation_status_view_to_proto(view)))
    }

    pub async fn list_followers(
        &self,
        request: Request<proto::ListFollowersRequest>,
    ) -> Result<Response<proto::ListFollowersResponse>, Status> {
        let viewer = viewer_of(&request);
        let req    = request.into_inner();
        let limit  = req.limit.clamp(1, 100) as u32;
        let query  = ListFollowersQuery {
            followee_id: req.followee_id,
            limit,
            page_token: Some(req.page_token).filter(|s| !s.is_empty()),
            viewer,
        };
        let (edges, next): (Vec<FollowEdge>, Option<String>) = self
            .query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), query))
            .await
            .map_err(cqrs_to_status)?;

        Ok(Response::new(proto::ListFollowersResponse {
            followers:       edges.into_iter().map(follow_edge_to_proto).collect(),
            next_page_token: next.unwrap_or_default(),
        }))
    }

    pub async fn list_following(
        &self,
        request: Request<proto::ListFollowingRequest>,
    ) -> Result<Response<proto::ListFollowingResponse>, Status> {
        let viewer = viewer_of(&request);
        let req    = request.into_inner();
        let limit  = req.limit.clamp(1, 100) as u32;
        let query  = ListFollowingQuery {
            follower_id: req.follower_id,
            limit,
            page_token: Some(req.page_token).filter(|s| !s.is_empty()),
            viewer,
        };
        let (edges, next): (Vec<FollowEdge>, Option<String>) = self
            .query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), query))
            .await
            .map_err(cqrs_to_status)?;

        Ok(Response::new(proto::ListFollowingResponse {
            following:       edges.into_iter().map(follow_edge_to_proto).collect(),
            next_page_token: next.unwrap_or_default(),
        }))
    }

    pub async fn list_blocks(
        &self,
        request: Request<proto::ListBlocksRequest>,
    ) -> Result<Response<proto::ListBlocksResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().blocker_id)?;
        let req   = request.into_inner();
        let limit = req.limit.clamp(1, 100) as u32;
        let query = ListBlocksQuery {
            blocker_id: req.blocker_id,
            limit,
            page_token: Some(req.page_token).filter(|s| !s.is_empty()),
        };
        let (edges, next): (Vec<BlockEdge>, Option<String>) = self
            .query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), query))
            .await
            .map_err(cqrs_to_status)?;

        Ok(Response::new(proto::ListBlocksResponse {
            blocks:          edges.into_iter().map(block_edge_to_proto).collect(),
            next_page_token: next.unwrap_or_default(),
        }))
    }

    /// Mesh-only (absent from `EDGE_POLICY`): the caller passes the reader it
    /// took from its own edge request.
    pub async fn check_access(
        &self,
        request: Request<proto::CheckAccessRequest>,
    ) -> Result<Response<proto::CheckAccessResponse>, Status> {
        let req   = request.into_inner();
        let query = CheckAccessQuery {
            viewer_profile_ids: req.viewer_profile_ids,
            target_profile_ids: req.target_profile_ids,
        };
        let access: Vec<(crate::domain::value_object::ProfileId, ContentAccess)> = self
            .query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), query))
            .await
            .map_err(cqrs_to_status)?;

        Ok(Response::new(proto::CheckAccessResponse {
            targets: access
                .into_iter()
                .map(|(target, access)| proto::TargetAccess {
                    target_profile_id: target.as_str(),
                    access:            content_access_to_proto(access) as i32,
                })
                .collect(),
        }))
    }
}

/// The reader of a viewer-aware RPC, from how the request arrived. A `pids`
/// entry that is not a profile id is dropped.
fn viewer_of<T>(request: &Request<T>) -> Viewer {
    match edge::viewer(request) {
        edge::Viewer::Internal => Viewer::Internal,
        edge::Viewer::Anonymous => Viewer::Profiles(Vec::new()),
        edge::Viewer::Member { profile_ids, .. } => Viewer::Profiles(
            profile_ids
                .iter()
                .filter_map(|id| crate::domain::value_object::ProfileId::try_from(id.as_str()).ok())
                .collect(),
        ),
    }
}

fn content_access_to_proto(access: ContentAccess) -> proto::ContentAccess {
    match access {
        ContentAccess::Visible    => proto::ContentAccess::Visible,
        ContentAccess::HeaderOnly => proto::ContentAccess::HeaderOnly,
        ContentAccess::Hidden     => proto::ContentAccess::Hidden,
    }
}

// ── Proto conversion helpers ──────────────────────────────────────────────────

fn dt_to_ts(dt: DateTime<Utc>) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: dt.timestamp(),
        nanos:   dt.timestamp_subsec_nanos() as i32,
    }
}

fn relation_status_to_i32(s: RelationStatus) -> i32 {
    match s {
        RelationStatus::None        => 1,
        RelationStatus::Following   => 2,
        RelationStatus::FollowedBy  => 3,
        RelationStatus::MutualFollow => 4,
        RelationStatus::Blocking    => 5,
        RelationStatus::BlockedBy   => 6,
    }
}

fn relation_status_view_to_proto(v: RelationStatusView) -> proto::RelationStatusView {
    proto::RelationStatusView {
        actor_id:               v.actor_id.as_str(),
        target_id:              v.target_id.as_str(),
        status:                 relation_status_to_i32(v.status),
        target_followers_count: v.target_followers_count,
        target_following_count: v.target_following_count,
    }
}

fn follow_edge_to_proto(e: FollowEdge) -> proto::EdgeSummary {
    proto::EdgeSummary {
        profile_id:  e.profile_id.as_str(),
        followed_at: Some(dt_to_ts(e.followed_at)),
    }
}

fn block_edge_to_proto(e: BlockEdge) -> proto::BlockSummary {
    proto::BlockSummary {
        blockee_id: e.blockee_id.as_str(),
        blocked_at: Some(dt_to_ts(e.blocked_at)),
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
            let msg      = boxed.to_string();
            let retryable = boxed.is_retryable();
            match boxed.http_status().as_u16() {
                404 => Status::not_found(msg),
                409 if retryable => Status::aborted(msg),
                409 => Status::already_exists(msg),
                400 | 422 => Status::failed_precondition(msg),
                503 | 502 => Status::unavailable(msg),
                _         => Status::internal(msg),
            }
        }
    }
}
