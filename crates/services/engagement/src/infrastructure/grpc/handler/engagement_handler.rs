use tonic::{Request, Response, Status};
use uuid::Uuid;

use cqrs::{CommandBus, Envelope, QueryBus};

use transport::grpc::edge;
use crate::application::command::{record_share::RecordShareCommand, record_view::RecordViewCommand};
use crate::application::port::{AccountLike, PostEngagementSnapshot};
use crate::application::likes::LikePosition;
use crate::application::query::batch_get_likes::BatchGetLikesQuery;
use crate::application::query::get_like_positions::GetLikePositionsQuery;
use crate::application::query::list_likes_by_profile::{LikedPosts, ListLikesByProfileQuery};
use crate::application::query::get_post_engagement::{
    EngagementReader, GetPostEngagementQuery, LikeSummary, PostEngagement,
};
use crate::application::query::list_likes_by_account::ListLikesByAccountQuery;
use crate::domain::value_object::LikeTarget;

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
            .map(target_from_proto)
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

    /// Mesh only (#665): an account's position on posts and comments, for
    /// the wallet's stake settlement.
    pub async fn get_like_positions(
        &self,
        request: Request<proto::GetLikePositionsRequest>,
    ) -> Result<Response<proto::GetLikePositionsResponse>, Status> {
        let req = request.into_inner();
        let targets = req
            .targets
            .iter()
            .map(target_from_proto)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let query = GetLikePositionsQuery { account_id: req.account_id, targets };
        let positions: Vec<LikePosition> = self
            .query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), query))
            .await
            .map_err(cqrs_to_status)?;
        Ok(Response::new(proto::GetLikePositionsResponse {
            positions: req
                .targets
                .into_iter()
                .zip(positions)
                .map(|(target, p)| proto::LikePosition {
                    target:           Some(target),
                    total:            p.total,
                    count_on_arrival: p.arrival,
                    count_now:        p.count,
                })
                .collect(),
        }))
    }

    /// A profile's Likes tab (#829), as the reader may see it.
    pub async fn list_likes_by_profile(
        &self,
        request: Request<proto::ListLikesByProfileRequest>,
    ) -> Result<Response<proto::ListLikesByProfileResponse>, Status> {
        let (reader, _) = reader_of(&request);
        let req = request.into_inner();
        let query = ListLikesByProfileQuery {
            profile_id: req.profile_id,
            limit:      req.limit,
            after:      Some(req.page_token).filter(|t| !t.is_empty()),
            reader,
        };
        let page: LikedPosts = self
            .query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), query))
            .await
            .map_err(cqrs_to_status)?;
        Ok(Response::new(proto::ListLikesByProfileResponse {
            post_ids:        page.post_ids,
            next_page_token: page.next.unwrap_or_default(),
        }))
    }

    /// Mesh only (#653, #665): what an account liked, for the GDPR export.
    pub async fn list_likes_by_account(
        &self,
        request: Request<proto::ListLikesByAccountRequest>,
    ) -> Result<Response<proto::ListLikesByAccountResponse>, Status> {
        let req = request.into_inner();
        let after = match req.page_token.split_once(':') {
            _ if req.page_token.is_empty() => None,
            Some((kind, id)) => Some(LikeTarget::parse(kind, id).map_err(|e| Status::invalid_argument(e.to_string()))?),
            None => return Err(Status::invalid_argument("malformed page_token")),
        };
        let query = ListLikesByAccountQuery { account_id: req.account_id, limit: req.limit, after };
        let likes: Vec<AccountLike> = self
            .query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), query))
            .await
            .map_err(cqrs_to_status)?;
        let limit = match req.limit {
            l if l <= 0 => crate::application::query::list_likes_by_account::DEFAULT_LIMIT,
            l => l.min(crate::application::query::list_likes_by_account::MAX_LIMIT),
        };
        let next_page_token = match likes.last() {
            Some(last) if likes.len() == limit as usize => format!("{}:{}", last.target.kind(), last.target.id()),
            _ => String::new(),
        };
        Ok(Response::new(proto::ListLikesByAccountResponse {
            likes: likes
                .into_iter()
                .map(|l| proto::AccountLikeView {
                    target:     Some(target_to_proto(&l.target)),
                    total:      l.total,
                    profile_id: l.profile_id,
                    liked_at:   Some(prost_types::Timestamp {
                        seconds: l.liked_at.timestamp(),
                        nanos:   l.liked_at.timestamp_subsec_nanos() as i32,
                    }),
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
    async fn list_likes_by_account(
        &self,
        request: Request<proto::ListLikesByAccountRequest>,
    ) -> Result<Response<proto::ListLikesByAccountResponse>, Status> {
        self.list_likes_by_account(request).await
    }

    async fn get_like_positions(
        &self,
        request: Request<proto::GetLikePositionsRequest>,
    ) -> Result<Response<proto::GetLikePositionsResponse>, Status> {
        self.get_like_positions(request).await
    }

    async fn list_likes_by_profile(
        &self,
        request: Request<proto::ListLikesByProfileRequest>,
    ) -> Result<Response<proto::ListLikesByProfileResponse>, Status> {
        self.list_likes_by_profile(request).await
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
    proto::PostEngagementView {
        post_id,
        view_count:    s.view_count,
        share_count:   s.share_count,
        comment_count: s.comment_count,
        // Filled by the caller (the likes are read alongside, #665).
        like_count:    0,
        my_likes:      0,
        likes_hidden:  false,
    }
}

fn target_from_proto(t: &proto::LikeTarget) -> Result<LikeTarget, crate::error::EngagementError> {
    match &t.target {
        Some(proto::like_target::Target::PostId(id)) => LikeTarget::parse("post", id),
        Some(proto::like_target::Target::CommentId(id)) => LikeTarget::parse("comment", id),
        None => LikeTarget::parse("", ""),
    }
}

fn target_to_proto(t: &LikeTarget) -> proto::LikeTarget {
    proto::LikeTarget {
        target: Some(match t {
            LikeTarget::Post(id) => proto::like_target::Target::PostId(id.clone()),
            LikeTarget::Comment(id) => proto::like_target::Target::CommentId(id.clone()),
        }),
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
