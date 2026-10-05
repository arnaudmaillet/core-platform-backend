use chrono::{DateTime, Utc};
use tonic::{Request, Response, Status};
use uuid::Uuid;

use cqrs::{CommandBus, Envelope, QueryBus};

use transport::grpc::edge;
use crate::application::command::{
    ApproveFollowRequestCommand, BlockProfileCommand, FollowProfileCommand, MuteProfileCommand,
    RestrictProfileCommand, SetListPrivacyCommand, UnblockProfileCommand, UnfollowProfileCommand,
    UnmuteProfileCommand, UnrestrictProfileCommand, WithdrawFollowRequestCommand,
};
use crate::application::query::{
    CheckAccessQuery, CheckInteractionQuery, FollowListPage, FollowRequestsPage, GetListPrivacyQuery, GetRelationStatusQuery,
    ListBlocksQuery, ListFollowRequestsQuery, ListFollowersQuery, ListFollowingQuery, ListMutesQuery,
    ListRestrictedQuery, MutedProfilesQuery, RestrictedAmongQuery,
};
use crate::domain::access::{ContentAccess, Viewer};
use crate::domain::interaction::{InteractionAudience, InteractionKind, InteractionVerdict};
use crate::domain::list_privacy::ListPrivacy;
use crate::domain::mute::{Mute, MuteScope, MuteScopes};
use crate::domain::value_object::ProfileId;
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
            requested: false,
        })
    }
}


fn interaction_kind_from_proto(v: i32) -> Result<InteractionKind, Status> {
    match proto::InteractionKind::try_from(v) {
        Ok(proto::InteractionKind::Comment) => Ok(InteractionKind::Comment),
        Ok(proto::InteractionKind::Mention) => Ok(InteractionKind::Mention),
        Ok(proto::InteractionKind::Message) => Ok(InteractionKind::Message),
        _ => Err(Status::invalid_argument("unknown interaction kind")),
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
            .map_err(cqrs_to_status)?;
        // A private target got a request rather than a follow: say which.
        let view: RelationStatusView = self
            .query_bus
            .dispatch(Envelope::new(
                Uuid::now_v7(),
                GetRelationStatusQuery { actor_id: req.actor_id.clone(), target_id: req.target_id.clone() },
            ))
            .await
            .map_err(cqrs_to_status)?;
        let mut response = Self::ok_response(&req.actor_id, &req.target_id);
        response.get_mut().requested = view.status == RelationStatus::Requested;
        Ok(response)
    }

    /// The owner's inbox of a private profile (edge: the caller's own profile).
    /// Mesh-only (absent from the edge policy): the owning service asks.
    pub async fn check_interaction(
        &self,
        request: Request<proto::CheckInteractionRequest>,
    ) -> Result<Response<proto::CheckInteractionResponse>, Status> {
        let req = request.into_inner();
        let query = CheckInteractionQuery {
            actor_id:  req.actor_profile_id,
            target_id: req.target_profile_id,
            kind:      interaction_kind_from_proto(req.kind)?,
        };
        let verdict: InteractionVerdict = self
            .query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), query))
            .await
            .map_err(cqrs_to_status)?;
        Ok(Response::new(proto::CheckInteractionResponse {
            allowed: verdict != InteractionVerdict::Refused,
            held:    verdict == InteractionVerdict::Held,
        }))
    }

    /// Mesh-only (absent from the edge policy): timeline asks with the reader
    /// it took from its own edge request.
    pub async fn list_muted_profiles(
        &self,
        request: Request<proto::ListMutedProfilesRequest>,
    ) -> Result<Response<proto::ListMutedProfilesResponse>, Status> {
        let req = request.into_inner();
        let scope = mute_scope_from_proto(req.scope)?;
        let query = MutedProfilesQuery { profile_ids: req.profile_ids, scope };
        let muted: std::collections::HashSet<ProfileId> = self
            .query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), query))
            .await
            .map_err(cqrs_to_status)?;
        Ok(Response::new(proto::ListMutedProfilesResponse {
            profile_ids: muted.iter().map(ProfileId::as_str).collect(),
        }))
    }

    /// Mesh-only (absent from the edge policy): comment asks with a page's
    /// commenters on the owner's post.
    pub async fn list_restricted_among(
        &self,
        request: Request<proto::ListRestrictedAmongRequest>,
    ) -> Result<Response<proto::ListRestrictedAmongResponse>, Status> {
        let req = request.into_inner();
        let query = RestrictedAmongQuery { owner_id: req.owner_id, candidate_ids: req.candidate_ids };
        let restricted: std::collections::HashSet<ProfileId> = self
            .query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), query))
            .await
            .map_err(cqrs_to_status)?;
        Ok(Response::new(proto::ListRestrictedAmongResponse {
            restricted_ids: restricted.iter().map(ProfileId::as_str).collect(),
        }))
    }

    pub async fn restrict(
        &self,
        request: Request<proto::RestrictRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().actor_id)?;
        let req = request.into_inner();
        let cmd = RestrictProfileCommand { actor_id: req.actor_id.clone(), target_id: req.target_id.clone() };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| Self::ok_response(&req.actor_id, &req.target_id))
            .map_err(cqrs_to_status)
    }

    pub async fn unrestrict(
        &self,
        request: Request<proto::UnrestrictRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().actor_id)?;
        let req = request.into_inner();
        let cmd = UnrestrictProfileCommand { actor_id: req.actor_id.clone(), target_id: req.target_id.clone() };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| Self::ok_response(&req.actor_id, &req.target_id))
            .map_err(cqrs_to_status)
    }

    pub async fn list_restricted(
        &self,
        request: Request<proto::ListRestrictedRequest>,
    ) -> Result<Response<proto::ListRestrictedResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().profile_id)?;
        let req = request.into_inner();
        let query = ListRestrictedQuery {
            profile_id: req.profile_id,
            limit:      req.limit.clamp(1, 100) as u32,
            page_token: Some(req.page_token).filter(|s| !s.is_empty()),
        };
        let (entries, next): (Vec<(ProfileId, DateTime<Utc>)>, Option<String>) = self
            .query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), query))
            .await
            .map_err(cqrs_to_status)?;
        Ok(Response::new(proto::ListRestrictedResponse {
            restricted: entries
                .into_iter()
                .map(|(profile_id, at)| proto::RestrictedSummary {
                    profile_id:    profile_id.as_str(),
                    restricted_at: Some(dt_to_ts(at)),
                })
                .collect(),
            next_page_token: next.unwrap_or_default(),
        }))
    }

    pub async fn mute(
        &self,
        request: Request<proto::MuteRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().actor_id)?;
        let req = request.into_inner();
        let cmd = MuteProfileCommand {
            actor_id:  req.actor_id.clone(),
            target_id: req.target_id.clone(),
            scopes:    req.scopes.map(mute_scopes_from_proto).unwrap_or_default(),
        };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| Self::ok_response(&req.actor_id, &req.target_id))
            .map_err(cqrs_to_status)
    }

    pub async fn unmute(
        &self,
        request: Request<proto::UnmuteRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().actor_id)?;
        let req = request.into_inner();
        let cmd = UnmuteProfileCommand { actor_id: req.actor_id.clone(), target_id: req.target_id.clone() };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| Self::ok_response(&req.actor_id, &req.target_id))
            .map_err(cqrs_to_status)
    }

    pub async fn list_mutes(
        &self,
        request: Request<proto::ListMutesRequest>,
    ) -> Result<Response<proto::ListMutesResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().profile_id)?;
        let req = request.into_inner();
        let query = ListMutesQuery {
            profile_id: req.profile_id,
            limit:      req.limit.clamp(1, 100) as u32,
            page_token: Some(req.page_token).filter(|s| !s.is_empty()),
        };
        let (mutes, next): (Vec<Mute>, Option<String>) = self
            .query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), query))
            .await
            .map_err(cqrs_to_status)?;
        Ok(Response::new(proto::ListMutesResponse {
            mutes: mutes
                .into_iter()
                .map(|m| proto::MuteSummary {
                    profile_id: m.profile_id.as_str(),
                    scopes:     Some(mute_scopes_to_proto(m.scopes)),
                    muted_at:   Some(dt_to_ts(m.muted_at)),
                })
                .collect(),
            next_page_token: next.unwrap_or_default(),
        }))
    }

    pub async fn list_follow_requests(
        &self,
        request: Request<proto::ListFollowRequestsRequest>,
    ) -> Result<Response<proto::ListFollowRequestsResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().owner_id)?;
        let req = request.into_inner();
        let query = ListFollowRequestsQuery {
            owner_id:   req.owner_id,
            limit:      req.limit.clamp(1, 100) as u32,
            page_token: Some(req.page_token).filter(|s| !s.is_empty()),
        };
        let page: FollowRequestsPage = self
            .query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), query))
            .await
            .map_err(cqrs_to_status)?;
        Ok(Response::new(proto::ListFollowRequestsResponse {
            requests:        page.requests.into_iter().map(follow_edge_to_proto).collect(),
            next_page_token: page.next_page_token.unwrap_or_default(),
            pending_count:   page.pending.map(|n| n as i64),
        }))
    }

    /// The owner approves (edge: `owner_id` is one of the caller's profiles).
    pub async fn approve_follow_request(
        &self,
        request: Request<proto::AnswerFollowRequestRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().owner_id)?;
        let req = request.into_inner();
        let cmd = ApproveFollowRequestCommand {
            owner_id:     req.owner_id.clone(),
            requester_id: req.requester_id.clone(),
        };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| Self::ok_response(&req.requester_id, &req.owner_id))
            .map_err(cqrs_to_status)
    }

    /// The owner declines (edge: `owner_id` is one of the caller's profiles).
    pub async fn decline_follow_request(
        &self,
        request: Request<proto::AnswerFollowRequestRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().owner_id)?;
        let req = request.into_inner();
        self.withdraw(req.requester_id, req.owner_id).await
    }

    /// The requester cancels (edge: `actor_id` is one of the caller's profiles).
    pub async fn cancel_follow_request(
        &self,
        request: Request<proto::CancelFollowRequestRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().actor_id)?;
        let req = request.into_inner();
        self.withdraw(req.actor_id, req.target_id).await
    }

    async fn withdraw(
        &self,
        requester_id: String,
        target_id: String,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        let cmd = WithdrawFollowRequestCommand {
            requester_id: requester_id.clone(),
            target_id:    target_id.clone(),
        };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| Self::ok_response(&requester_id, &target_id))
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

    /// The owner (`profile_id`) removes `follower_id`: the follower's follow
    /// is undone exactly as its own Unfollow would (counts, feeds, events).
    pub async fn remove_follower(
        &self,
        request: Request<proto::RemoveFollowerRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().profile_id)?;
        let req = request.into_inner();
        let cmd = UnfollowProfileCommand {
            actor_id:  req.follower_id.clone(),
            target_id: req.profile_id.clone(),
        };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| Self::ok_response(&req.profile_id, &req.follower_id))
            .map_err(cqrs_to_status)
    }

    pub async fn set_list_privacy(
        &self,
        request: Request<proto::SetListPrivacyRequest>,
    ) -> Result<Response<proto::ListPrivacy>, Status> {
        edge::require_profile(&request, &request.get_ref().profile_id)?;
        let req = request.into_inner();
        let cmd = SetListPrivacyCommand {
            profile_id: req.profile_id.clone(),
            followers:  list_audience_from_proto(req.followers)?,
            following:  list_audience_from_proto(req.following)?,
        };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map_err(cqrs_to_status)?;
        self.list_privacy_of(req.profile_id).await
    }

    pub async fn get_list_privacy(
        &self,
        request: Request<proto::GetListPrivacyRequest>,
    ) -> Result<Response<proto::ListPrivacy>, Status> {
        edge::require_profile(&request, &request.get_ref().profile_id)?;
        self.list_privacy_of(request.into_inner().profile_id).await
    }

    async fn list_privacy_of(&self, profile_id: String) -> Result<Response<proto::ListPrivacy>, Status> {
        let privacy: ListPrivacy = self
            .query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), GetListPrivacyQuery { profile_id }))
            .await
            .map_err(cqrs_to_status)?;
        Ok(Response::new(proto::ListPrivacy {
            followers: list_audience_to_proto(privacy.followers) as i32,
            following: list_audience_to_proto(privacy.following) as i32,
        }))
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
        let page: FollowListPage = self
            .query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), query))
            .await
            .map_err(cqrs_to_status)?;

        Ok(Response::new(proto::ListFollowersResponse {
            followers:       page.edges.into_iter().map(follow_edge_to_proto).collect(),
            next_page_token: page.next_page_token.unwrap_or_default(),
            hidden:          page.hidden,
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
        let page: FollowListPage = self
            .query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), query))
            .await
            .map_err(cqrs_to_status)?;

        Ok(Response::new(proto::ListFollowingResponse {
            following:       page.edges.into_iter().map(follow_edge_to_proto).collect(),
            next_page_token: page.next_page_token.unwrap_or_default(),
            hidden:          page.hidden,
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
        RelationStatus::Requested   => 7,
    }
}

fn relation_status_view_to_proto(v: RelationStatusView) -> proto::RelationStatusView {
    proto::RelationStatusView {
        actor_id:               v.actor_id.as_str(),
        target_id:              v.target_id.as_str(),
        status:                 relation_status_to_i32(v.status),
        target_followers_count: v.target_followers_count,
        target_following_count: v.target_following_count,
        muted:                  Some(mute_scopes_to_proto(v.muted)),
        restricted:             v.restricted,
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

// ── Mutes ─────────────────────────────────────────────────────────────────────

fn mute_scopes_from_proto(s: proto::MuteScopes) -> MuteScopes {
    MuteScopes { posts: s.posts, stories: s.stories, messages: s.messages }
}

fn mute_scopes_to_proto(s: MuteScopes) -> proto::MuteScopes {
    proto::MuteScopes { posts: s.posts, stories: s.stories, messages: s.messages }
}

fn mute_scope_from_proto(value: i32) -> Result<MuteScope, Status> {
    match proto::MuteScope::try_from(value) {
        Ok(proto::MuteScope::Posts) => Ok(MuteScope::Posts),
        Ok(proto::MuteScope::Stories) => Ok(MuteScope::Stories),
        Ok(proto::MuteScope::Messages) => Ok(MuteScope::Messages),
        _ => Err(Status::invalid_argument(format!("a mute scope is required (got {value})"))),
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

// ── List privacy ──────────────────────────────────────────────────────────────

/// `None` for UNSPECIFIED (keep the list's audience).
fn list_audience_from_proto(value: i32) -> Result<Option<InteractionAudience>, Status> {
    match proto::ListAudience::try_from(value) {
        Ok(proto::ListAudience::Unspecified) => Ok(None),
        Ok(proto::ListAudience::Everyone) => Ok(Some(InteractionAudience::Everyone)),
        Ok(proto::ListAudience::Followers) => Ok(Some(InteractionAudience::Followers)),
        Ok(proto::ListAudience::Mutuals) => Ok(Some(InteractionAudience::Mutuals)),
        Ok(proto::ListAudience::OnlyMe) => Ok(Some(InteractionAudience::NoOne)),
        Err(_) => Err(Status::invalid_argument(format!("unknown list audience {value}"))),
    }
}

fn list_audience_to_proto(audience: InteractionAudience) -> proto::ListAudience {
    match audience {
        InteractionAudience::Everyone => proto::ListAudience::Everyone,
        InteractionAudience::Followers => proto::ListAudience::Followers,
        InteractionAudience::Mutuals => proto::ListAudience::Mutuals,
        InteractionAudience::NoOne => proto::ListAudience::OnlyMe,
    }
}
