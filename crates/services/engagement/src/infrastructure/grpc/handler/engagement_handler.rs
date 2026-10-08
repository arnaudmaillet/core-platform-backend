use tonic::{Request, Response, Status};
use uuid::Uuid;

use cqrs::{CommandBus, Envelope, QueryBus};

use transport::grpc::edge;
use crate::application::command::{
    record_share::RecordShareCommand,
    record_view::RecordViewCommand,
    remove_reaction::RemoveReactionCommand,
    upsert_reaction::UpsertReactionCommand,
};
use crate::application::port::{PostEngagementSnapshot, ProfileReaction};
use crate::application::query::batch_get_likes::BatchGetLikesQuery;
use crate::application::query::get_post_engagement::{
    EngagementReader, GetPostEngagementQuery, LikeSummary, PostEngagement,
};
use crate::domain::value_object::LikeTarget;
use crate::application::query::list_reactions_by_profile::ListReactionsByProfileQuery;
use crate::domain::value_object::ReactionKind;

// ── Proto inclusion ───────────────────────────────────────────────────────────

pub use engagement_api as proto;

pub use proto::engagement_service_server::EngagementServiceServer;

pub struct EngagementServiceHandler<CB, QB>
where
    CB: CommandBus + Send + Sync + 'static,
    QB: QueryBus + Send + Sync + 'static,
{
    command_bus: CB,
    query_bus:   QB,
}

impl<CB, QB> EngagementServiceHandler<CB, QB>
where
    CB: CommandBus + Send + Sync + 'static,
    QB: QueryBus + Send + Sync + 'static,
{
    pub fn new(command_bus: CB, query_bus: QB) -> Self {
        Self { command_bus, query_bus }
    }
}

// ── RPC implementations ───────────────────────────────────────────────────────

impl<CB, QB> EngagementServiceHandler<CB, QB>
where
    CB: CommandBus + Send + Sync + 'static,
    QB: QueryBus + Send + Sync + 'static,
{
    pub async fn upsert_reaction(
        &self,
        request: Request<proto::UpsertReactionRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().profile_id)?;
        let req = request.into_inner();
        let cmd = UpsertReactionCommand {
            post_id:    req.post_id,
            profile_id: req.profile_id,
            kind:       req.kind,
        };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| ok_response())
            .map_err(cqrs_to_status)
    }

    pub async fn remove_reaction(
        &self,
        request: Request<proto::RemoveReactionRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().profile_id)?;
        let req = request.into_inner();
        let cmd = RemoveReactionCommand {
            post_id:    req.post_id,
            profile_id: req.profile_id,
        };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| ok_response())
            .map_err(cqrs_to_status)
    }

    pub async fn record_view(
        &self,
        request: Request<proto::RecordViewRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        let cmd = RecordViewCommand { post_id: request.into_inner().post_id };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| ok_response())
            .map_err(cqrs_to_status)
    }

    pub async fn record_share(
        &self,
        request: Request<proto::RecordShareRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        let cmd = RecordShareCommand { post_id: request.into_inner().post_id };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| ok_response())
            .map_err(cqrs_to_status)
    }

    pub async fn get_post_engagement(
        &self,
        request: Request<proto::GetPostEngagementRequest>,
    ) -> Result<Response<proto::PostEngagementView>, Status> {
        // The reader from how the request arrived: hidden likes (#809) reach
        // only the author (one of the caller's profiles) and the mesh.
        let (reader, account) = reader_of(&request);
        let req   = request.into_inner();
        let query = GetPostEngagementQuery { post_id: req.post_id.clone(), reader, account };

        let engagement: PostEngagement = self
            .query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), query))
            .await
            .map_err(cqrs_to_status)?;

        let mut view = snapshot_to_proto(req.post_id, engagement.snapshot);
        view.like_count = engagement.likes.count;
        view.my_likes = engagement.likes.mine;
        view.likes_hidden = engagement.likes.hidden;
        Ok(Response::new(view))
    }

    /// The likes of up to 100 posts and comments (#665), as the reader sees
    /// them.
    pub async fn batch_get_likes(
        &self,
        request: Request<proto::BatchGetLikesRequest>,
    ) -> Result<Response<proto::BatchGetLikesResponse>, Status> {
        let (reader, account) = reader_of(&request);
        let req = request.into_inner();
        let targets = req
            .targets
            .iter()
            .map(|t| match &t.target {
                Some(proto::like_target::Target::PostId(id)) => LikeTarget::parse("post", id),
                Some(proto::like_target::Target::CommentId(id)) => LikeTarget::parse("comment", id),
                None => LikeTarget::parse("", ""),
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let query = BatchGetLikesQuery { targets, reader, account };
        let likes: Vec<LikeSummary> = self
            .query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), query))
            .await
            .map_err(cqrs_to_status)?;
        Ok(Response::new(proto::BatchGetLikesResponse {
            likes: req
                .targets
                .into_iter()
                .zip(likes)
                .map(|(target, l)| proto::LikeView { target: Some(target), count: l.count, mine: l.mine, hidden: l.hidden })
                .collect(),
        }))
    }

    /// Mesh only (#653): a profile's reactions for the GDPR export.
    pub async fn list_reactions_by_profile(
        &self,
        request: Request<proto::ListReactionsByProfileRequest>,
    ) -> Result<Response<proto::ListReactionsByProfileResponse>, Status> {
        let req = request.into_inner();
        let limit = req.limit.clamp(1, 500);
        let query = ListReactionsByProfileQuery {
            profile_id: req.profile_id,
            limit,
            after:      Some(req.page_token).filter(|t| !t.is_empty()),
        };
        let reactions: Vec<ProfileReaction> = self
            .query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), query))
            .await
            .map_err(cqrs_to_status)?;
        let next_page_token = match reactions.last() {
            Some(last) if reactions.len() == limit as usize => last.post_id.as_str(),
            _ => String::new(),
        };
        Ok(Response::new(proto::ListReactionsByProfileResponse {
            reactions: reactions
                .into_iter()
                .map(|r| proto::ProfileReactionView {
                    post_id:       r.post_id.as_str(),
                    kind:          kind_to_proto(r.kind),
                    reacted_at_ms: r.reacted_at_ms,
                })
                .collect(),
            next_page_token,
        }))
    }
}

// ── Proto trait implementation ────────────────────────────────────────────────

#[tonic::async_trait]
impl<CB, QB> proto::engagement_service_server::EngagementService for EngagementServiceHandler<CB, QB>
where
    CB: CommandBus + Send + Sync + 'static,
    QB: QueryBus + Send + Sync + 'static,
{
    async fn list_reactions_by_profile(
        &self,
        request: Request<proto::ListReactionsByProfileRequest>,
    ) -> Result<Response<proto::ListReactionsByProfileResponse>, Status> {
        self.list_reactions_by_profile(request).await
    }

    async fn upsert_reaction(
        &self,
        request: Request<proto::UpsertReactionRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.upsert_reaction(request).await
    }

    async fn remove_reaction(
        &self,
        request: Request<proto::RemoveReactionRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.remove_reaction(request).await
    }

    async fn record_view(
        &self,
        request: Request<proto::RecordViewRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.record_view(request).await
    }

    async fn record_share(
        &self,
        request: Request<proto::RecordShareRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.record_share(request).await
    }

    async fn get_post_engagement(
        &self,
        request: Request<proto::GetPostEngagementRequest>,
    ) -> Result<Response<proto::PostEngagementView>, Status> {
        self.get_post_engagement(request).await
    }

    async fn batch_get_likes(
        &self,
        request: Request<proto::BatchGetLikesRequest>,
    ) -> Result<Response<proto::BatchGetLikesResponse>, Status> {
        self.batch_get_likes(request).await
    }
}

/// The reader from how the request arrived, and its account when it is a
/// member (its own likes; a guest has none).
fn reader_of<T>(request: &Request<T>) -> (EngagementReader, Option<String>) {
    match edge::viewer(request) {
        edge::Viewer::Internal => (EngagementReader::Internal, None),
        edge::Viewer::Anonymous => (EngagementReader::Profiles(Vec::new()), None),
        edge::Viewer::Member { profile_ids, account_id } => {
            let guest = edge::principal(request).is_some_and(|p| p.is_guest());
            (EngagementReader::Profiles(profile_ids), (!guest).then_some(account_id))
        }
    }
}

// ── Conversion helpers ────────────────────────────────────────────────────────

fn ok_response() -> Response<proto::CommandResponse> {
    Response::new(proto::CommandResponse { success: true, message: String::new() })
}

fn snapshot_to_proto(post_id: String, s: PostEngagementSnapshot) -> proto::PostEngagementView {
    let total = s.total_weighted_score();

    let reaction_scores = ReactionKind::all()
        .iter()
        .filter_map(|kind| {
            let score = s.reaction_scores.get(kind.as_redis_key()).copied().unwrap_or(0);
            if score == 0 { return None; }
            Some(proto::ReactionScoreEntry {
                kind:  kind_to_proto(*kind),
                score,
            })
        })
        .collect();

    proto::PostEngagementView {
        post_id,
        reaction_scores,
        total_weighted_score: total,
        view_count:    s.view_count,
        share_count:   s.share_count,
        comment_count: s.comment_count,
        // Filled by the caller (the likes are read alongside, #665).
        like_count:    0,
        my_likes:      0,
        likes_hidden:  false,
    }
}

fn kind_to_proto(kind: ReactionKind) -> i32 {
    match kind {
        ReactionKind::Heart  => 1,
        ReactionKind::Fire   => 2,
        ReactionKind::Rocket => 3,
        ReactionKind::Clap   => 4,
        ReactionKind::Sad    => 5,
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
