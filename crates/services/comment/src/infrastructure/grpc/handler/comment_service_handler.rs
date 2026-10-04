use tonic::{Request, Response, Status};
use uuid::Uuid;

use cqrs::{CommandBus, Envelope, QueryBus};

use transport::grpc::edge;
use crate::application::command::{
    create_comment::CreateCommentCommand,
    delete_comment::DeleteCommentCommand,
};
use crate::application::port::CommentSummary;
use crate::application::query::{
    get_comment::GetCommentQuery,
    list_replies::ListRepliesQuery,
    list_top_level::ListTopLevelQuery,
};
use crate::domain::aggregate::Comment;
use crate::domain::value_object::{CommentStatus, ProfileId, Viewer};

// ── Proto inclusion ───────────────────────────────────────────────────────────

pub use comment_api as proto;

pub use proto::comment_service_server::CommentServiceServer;

pub struct CommentServiceHandler<CB, QB>
where
    CB: CommandBus + Send + Sync + 'static,
    QB: QueryBus + Send + Sync + 'static,
{
    command_bus: CB,
    query_bus:   QB,
}

impl<CB, QB> CommentServiceHandler<CB, QB>
where
    CB: CommandBus + Send + Sync + 'static,
    QB: QueryBus + Send + Sync + 'static,
{
    pub fn new(command_bus: CB, query_bus: QB) -> Self {
        Self { command_bus, query_bus }
    }
}

// ── Command RPC helpers ───────────────────────────────────────────────────────

impl<CB, QB> CommentServiceHandler<CB, QB>
where
    CB: CommandBus + Send + Sync + 'static,
    QB: QueryBus + Send + Sync + 'static,
{
    pub async fn create_comment(
        &self,
        request: Request<proto::CreateCommentRequest>,
    ) -> Result<Response<proto::CreateCommentResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().author_id)?;
        let req = request.into_inner();

        let comment_id = if req.comment_id.is_empty() {
            Uuid::now_v7().to_string()
        } else {
            req.comment_id.clone()
        };

        let cmd = CreateCommentCommand {
            comment_id: comment_id.clone(),
            post_id:    req.post_id.clone(),
            author_id:  req.author_id,
            parent_id:  Some(req.parent_id).filter(|s| !s.is_empty()),
            body:       Some(req.body).filter(|s| !s.is_empty()),
            gif_id:     Some(req.gif_id).filter(|s| !s.is_empty()),
            gif_url:    Some(req.gif_url).filter(|s| !s.is_empty()),
            gif_width:  if req.gif_width == 0 { None } else { Some(req.gif_width) },
            gif_height: if req.gif_height == 0 { None } else { Some(req.gif_height) },
        };

        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| Response::new(proto::CreateCommentResponse {
                comment_id,
                post_id: req.post_id,
            }))
            .map_err(cqrs_to_status)
    }

    pub async fn delete_comment(
        &self,
        request: Request<proto::DeleteCommentRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().author_id)?;
        let req = request.into_inner();
        let cmd = DeleteCommentCommand {
            comment_id: req.comment_id,
            author_id:  req.author_id,
        };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| ok_response())
            .map_err(cqrs_to_status)
    }
}

// ── Query RPC helpers ─────────────────────────────────────────────────────────

impl<CB, QB> CommentServiceHandler<CB, QB>
where
    CB: CommandBus + Send + Sync + 'static,
    QB: QueryBus + Send + Sync + 'static,
{
    pub async fn get_comment(
        &self,
        request: Request<proto::GetCommentRequest>,
    ) -> Result<Response<proto::CommentView>, Status> {
        let viewer = viewer_of(&request);
        let query  = GetCommentQuery { comment_id: request.into_inner().comment_id, viewer };
        let comment: Comment = self
            .query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), query))
            .await
            .map_err(cqrs_to_status)?;

        Ok(Response::new(comment_to_proto(comment)))
    }

    pub async fn list_top_level(
        &self,
        request: Request<proto::ListTopLevelRequest>,
    ) -> Result<Response<proto::ListCommentsResponse>, Status> {
        let viewer = viewer_of(&request);
        let req    = request.into_inner();
        let query  = ListTopLevelQuery {
            post_id:    req.post_id,
            limit:      req.limit,
            page_token: Some(req.page_token).filter(|s| !s.is_empty()),
            viewer,
        };
        let (summaries, next): (Vec<CommentSummary>, Option<String>) = self
            .query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), query))
            .await
            .map_err(cqrs_to_status)?;

        Ok(Response::new(proto::ListCommentsResponse {
            comments:   summaries.into_iter().map(summary_to_proto).collect(),
            next_token: next.unwrap_or_default(),
        }))
    }

    pub async fn list_replies(
        &self,
        request: Request<proto::ListRepliesRequest>,
    ) -> Result<Response<proto::ListCommentsResponse>, Status> {
        let viewer = viewer_of(&request);
        let req    = request.into_inner();
        let query  = ListRepliesQuery {
            post_id:    req.post_id,
            comment_id: req.comment_id,
            limit:      req.limit,
            page_token: Some(req.page_token).filter(|s| !s.is_empty()),
            viewer,
        };
        let (summaries, next): (Vec<CommentSummary>, Option<String>) = self
            .query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), query))
            .await
            .map_err(cqrs_to_status)?;

        Ok(Response::new(proto::ListCommentsResponse {
            comments:   summaries.into_iter().map(summary_to_proto).collect(),
            next_token: next.unwrap_or_default(),
        }))
    }
}

// ── Proto trait implementation ────────────────────────────────────────────────

#[tonic::async_trait]
impl<CB, QB> proto::comment_service_server::CommentService for CommentServiceHandler<CB, QB>
where
    CB: CommandBus + Send + Sync + 'static,
    QB: QueryBus + Send + Sync + 'static,
{
    async fn create_comment(
        &self,
        request: Request<proto::CreateCommentRequest>,
    ) -> Result<Response<proto::CreateCommentResponse>, Status> {
        self.create_comment(request).await
    }

    async fn delete_comment(
        &self,
        request: Request<proto::DeleteCommentRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.delete_comment(request).await
    }

    async fn get_comment(
        &self,
        request: Request<proto::GetCommentRequest>,
    ) -> Result<Response<proto::CommentView>, Status> {
        self.get_comment(request).await
    }

    async fn list_top_level(
        &self,
        request: Request<proto::ListTopLevelRequest>,
    ) -> Result<Response<proto::ListCommentsResponse>, Status> {
        self.list_top_level(request).await
    }

    async fn list_replies(
        &self,
        request: Request<proto::ListRepliesRequest>,
    ) -> Result<Response<proto::ListCommentsResponse>, Status> {
        self.list_replies(request).await
    }
}

// ── Conversion helpers ────────────────────────────────────────────────────────

/// The reader of a viewer-aware RPC, from how the request arrived. A `pids`
/// entry that is not a profile id is dropped.
fn viewer_of<T>(request: &Request<T>) -> Viewer {
    match edge::viewer(request) {
        edge::Viewer::Internal => Viewer::Internal,
        edge::Viewer::Anonymous => Viewer::Profiles(Vec::new()),
        edge::Viewer::Member { profile_ids, .. } => Viewer::Profiles(
            profile_ids.iter().filter_map(|id| ProfileId::try_from(id.as_str()).ok()).collect(),
        ),
    }
}

fn comment_to_proto(c: Comment) -> proto::CommentView {
    let gif = c.gif().map(|g| proto::GifMetadata {
        gif_id:     g.gif_id.clone(),
        gif_url:    g.gif_url.clone(),
        gif_width:  g.gif_width,
        gif_height: g.gif_height,
    });

    proto::CommentView {
        comment_id:    c.id().as_str(),
        post_id:       c.post_id().as_str(),
        author_id:     c.author_id().as_str(),
        parent_id:     c.parent_id().map(|p| p.as_str()).unwrap_or_default(),
        status:        status_to_proto(c.status()),
        body:          c.body().map(|b| b.as_str().to_owned()).unwrap_or_default(),
        gif,
        created_at_ms: c.created_at().timestamp_millis(),
        updated_at_ms: c.updated_at().timestamp_millis(),
    }
}

fn summary_to_proto(s: CommentSummary) -> proto::CommentView {
    proto::CommentView {
        comment_id:    s.comment_id.as_str(),
        post_id:       String::new(),
        author_id:     s.author_id.as_str(),
        parent_id:     String::new(),
        status:        status_to_proto(s.status),
        body:          s.body.unwrap_or_default(),
        gif:           build_gif_proto(s.gif_url, s.gif_width, s.gif_height),
        created_at_ms: s.created_at.timestamp_millis(),
        updated_at_ms: 0,
    }
}

fn build_gif_proto(
    gif_url:    Option<String>,
    gif_width:  Option<u32>,
    gif_height: Option<u32>,
) -> Option<proto::GifMetadata> {
    match (gif_url, gif_width, gif_height) {
        (Some(url), Some(w), Some(h)) if !url.is_empty() => Some(proto::GifMetadata {
            gif_id:     String::new(),
            gif_url:    url,
            gif_width:  w,
            gif_height: h,
        }),
        _ => None,
    }
}

fn status_to_proto(s: CommentStatus) -> i32 {
    match s {
        CommentStatus::Published => 1,
        CommentStatus::Deleted   => 2,
    }
}

fn ok_response() -> Response<proto::CommandResponse> {
    Response::new(proto::CommandResponse { success: true, message: String::new() })
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
