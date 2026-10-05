use tonic::{Request, Response, Status};
use uuid::Uuid;

use cqrs::{CommandBus, Envelope, QueryBus};

use transport::grpc::edge;
use crate::application::command::{
    create_post::CreatePostCommand,
    delete_post::DeletePostCommand,
    publish_post::PublishPostCommand,
    restore_post::RestorePostCommand,
    update_post::UpdatePostCommand,
};
use crate::application::command::create_post::AttachmentInput;
use crate::application::port::PostSummary;
use crate::application::query::{
    get_post::GetPostQuery,
    list_posts_by_profile::ListPostsByProfileQuery,
    list_recently_deleted::ListRecentlyDeletedQuery,
};
use crate::domain::aggregate::{Post, ReuseOverrides};
use crate::domain::entity::MediaAttachment;
use crate::domain::value_object::{AudioId, AudioKind, AudioReference, PostId, ProfileId, Viewer};

// ── Proto inclusion ───────────────────────────────────────────────────────────

pub use post_api as proto;

pub use proto::post_service_server::PostServiceServer;

pub struct PostServiceHandler<CB, QB>
where
    CB: CommandBus + Send + Sync + 'static,
    QB: QueryBus + Send + Sync + 'static,
{
    command_bus: CB,
    query_bus:   QB,
}

impl<CB, QB> PostServiceHandler<CB, QB>
where
    CB: CommandBus + Send + Sync + 'static,
    QB: QueryBus + Send + Sync + 'static,
{
    pub fn new(command_bus: CB, query_bus: QB) -> Self {
        Self { command_bus, query_bus }
    }
}

// ── Command RPC helpers ───────────────────────────────────────────────────────

impl<CB, QB> PostServiceHandler<CB, QB>
where
    CB: CommandBus + Send + Sync + 'static,
    QB: QueryBus + Send + Sync + 'static,
{
    pub async fn create_post(
        &self,
        request: Request<proto::CreatePostRequest>,
    ) -> Result<Response<proto::CreatePostResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().profile_id)?;
        let req         = request.into_inner();
        let post_id     = PostId::new_v7();
        let post_id_str = post_id.as_str();
        let profile_id  = req.profile_id.clone();

        let audio_ref = proto_audio_ref_to_domain(req.audio_ref)?;

        let cmd = CreatePostCommand {
            post_id:     post_id_str.clone(),
            profile_id:  req.profile_id,
            kind:        req.kind,
            caption:     req.caption,
            attachments: req.attachments.into_iter().map(attachment_input_from_proto).collect(),
            parent_id:   Some(req.parent_id).filter(|s| !s.is_empty()),
            root_id:     Some(req.root_id).filter(|s| !s.is_empty()),
            audio_ref,
            location:    req.location.map(|g| (g.lat, g.lng)),
            reuse:       ReuseOverrides { allow_remix: req.allow_remix, allow_sound_reuse: req.allow_sound_reuse },
        };

        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| Response::new(proto::CreatePostResponse {
                post_id:    post_id_str,
                profile_id,
            }))
            .map_err(cqrs_to_status)
    }

    pub async fn publish_post(
        &self,
        request: Request<proto::PublishPostRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().profile_id)?;
        let req = request.into_inner();
        let cmd = PublishPostCommand {
            post_id:    req.post_id,
            profile_id: req.profile_id,
        };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| Response::new(proto::CommandResponse { success: true, message: String::new() }))
            .map_err(cqrs_to_status)
    }

    pub async fn update_post(
        &self,
        request: Request<proto::UpdatePostRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().profile_id)?;
        let req = request.into_inner();
        let cmd = UpdatePostCommand {
            post_id:     req.post_id,
            profile_id:  req.profile_id,
            caption:     req.caption,
            attachments: req.attachments.into_iter().map(attachment_input_from_proto).collect(),
        };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| Response::new(proto::CommandResponse { success: true, message: String::new() }))
            .map_err(cqrs_to_status)
    }

    pub async fn delete_post(
        &self,
        request: Request<proto::DeletePostRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().profile_id)?;
        let req = request.into_inner();
        let cmd = DeletePostCommand {
            post_id:    req.post_id,
            profile_id: req.profile_id,
        };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| Response::new(proto::CommandResponse { success: true, message: String::new() }))
            .map_err(cqrs_to_status)
    }

    pub async fn restore_post(
        &self,
        request: Request<proto::RestorePostRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().profile_id)?;
        let req = request.into_inner();
        let cmd = RestorePostCommand { post_id: req.post_id, profile_id: req.profile_id };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| Response::new(proto::CommandResponse { success: true, message: String::new() }))
            .map_err(cqrs_to_status)
    }

    pub async fn list_recently_deleted(
        &self,
        request: Request<proto::ListRecentlyDeletedRequest>,
    ) -> Result<Response<proto::ListRecentlyDeletedResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().profile_id)?;
        let req = request.into_inner();
        let query = ListRecentlyDeletedQuery {
            profile_id: req.profile_id,
            limit:      req.limit,
            page_token: Some(req.page_token).filter(|s| !s.is_empty()),
        };
        let (posts, next): (Vec<Post>, Option<String>) = self
            .query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), query))
            .await
            .map_err(cqrs_to_status)?;
        Ok(Response::new(proto::ListRecentlyDeletedResponse {
            posts:      posts.into_iter().map(post_to_proto).collect(),
            next_token: next.unwrap_or_default(),
        }))
    }
}

// ── Query RPC helpers ─────────────────────────────────────────────────────────

impl<CB, QB> PostServiceHandler<CB, QB>
where
    CB: CommandBus + Send + Sync + 'static,
    QB: QueryBus + Send + Sync + 'static,
{
    pub async fn get_post(
        &self,
        request: Request<proto::GetPostRequest>,
    ) -> Result<Response<proto::PostView>, Status> {
        let viewer = viewer_of(&request);
        let mature = mature_of(&request);
        let req    = request.into_inner();
        let query  = GetPostQuery { post_id: req.post_id, viewer, mature };
        let post: Post = self
            .query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), query))
            .await
            .map_err(cqrs_to_status)?;

        Ok(Response::new(post_to_proto(post)))
    }

    pub async fn list_posts_by_profile(
        &self,
        request: Request<proto::ListPostsByProfileRequest>,
    ) -> Result<Response<proto::ListPostsByProfileResponse>, Status> {
        let viewer = viewer_of(&request);
        let mature = mature_of(&request);
        let req    = request.into_inner();
        let query  = ListPostsByProfileQuery {
            mature,
            profile_id: req.profile_id,
            limit:      req.limit,
            page_token: Some(req.page_token).filter(|s| !s.is_empty()),
            viewer,
        };
        let (summaries, next): (Vec<PostSummary>, Option<String>) = self
            .query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), query))
            .await
            .map_err(cqrs_to_status)?;

        Ok(Response::new(proto::ListPostsByProfileResponse {
            posts:      summaries.into_iter().map(summary_to_proto).collect(),
            next_token: next.unwrap_or_default(),
        }))
    }
}

// ── Viewer ────────────────────────────────────────────────────────────────────

/// Whether the reader is cleared for mature content: the mesh is; an
/// anonymous client, a guest or a 13–17 holder (the token's `age`) is not.
fn mature_of<T>(request: &Request<T>) -> bool {
    match edge::viewer(request) {
        edge::Viewer::Internal => true,
        edge::Viewer::Anonymous => false,
        edge::Viewer::Member { .. } => edge::principal(request).is_some_and(|p| !p.is_guest() && !p.is_minor()),
    }
}

/// The reader of a viewer-aware RPC, from how the request arrived. A `pids`
/// entry that is not a profile id cannot author a post, so it is dropped.
fn viewer_of<T>(request: &Request<T>) -> Viewer {
    match edge::viewer(request) {
        edge::Viewer::Internal => Viewer::Internal,
        edge::Viewer::Anonymous => Viewer::Anonymous,
        edge::Viewer::Member { profile_ids, .. } => Viewer::Profiles(
            profile_ids.iter().filter_map(|id| ProfileId::try_from(id.as_str()).ok()).collect(),
        ),
    }
}

// ── Proto conversion helpers ──────────────────────────────────────────────────

fn attachment_input_from_proto(a: proto::MediaAttachmentInput) -> AttachmentInput {
    AttachmentInput {
        cdn_url:          a.cdn_url,
        mime_type:        a.mime_type,
        width:            a.width,
        height:           a.height,
        thumbnail_url:    Some(a.thumbnail_url).filter(|s| !s.is_empty()),
        duration_seconds: if a.duration_seconds == 0.0 { None } else { Some(a.duration_seconds) },
    }
}

fn attachment_to_proto(a: &MediaAttachment) -> proto::MediaAttachmentView {
    proto::MediaAttachmentView {
        cdn_url:          a.cdn_url.as_str().to_owned(),
        mime_type:        a.mime_type.as_str().to_owned(),
        width:            a.width,
        height:           a.height,
        thumbnail_url:    a.thumbnail_url.as_ref().map(|u| u.as_str().to_owned()).unwrap_or_default(),
        duration_seconds: a.duration_seconds.unwrap_or(0.0),
    }
}

fn post_to_proto(post: Post) -> proto::PostView {
    proto::PostView {
        post_id:       post.id().as_str(),
        profile_id:    post.profile_id().as_str(),
        kind:          post.kind().as_tinyint() as i32 + 1,
        status:        post.status().as_tinyint() as i32 + 1,
        caption:       post.caption().as_str().to_owned(),
        attachments:   post.attachments().iter().map(attachment_to_proto).collect(),
        parent_id:     post.parent_id().map(PostId::as_str).unwrap_or_default(),
        root_id:       post.root_id().map(PostId::as_str).unwrap_or_default(),
        created_at_ms:   post.created_at().timestamp_millis(),
        updated_at_ms:   post.updated_at().timestamp_millis(),
        published_at_ms: post.published_at().map(|d| d.timestamp_millis()).unwrap_or_default(),
        deleted_at_ms:   post.deleted_at().map(|d| d.timestamp_millis()).unwrap_or_default(),
        audio_ref:       domain_audio_ref_to_proto(post.audio_ref()),
        location:        post.location().map(|g| proto::GeoPoint { lat: g.lat(), lng: g.lng() }),
        moderation:      post.moderation().restriction.as_tinyint() as i32 + 1,
        allow_remix:       post.reuse().allow_remix,
        allow_sound_reuse: post.reuse().allow_sound_reuse,
        outside_window:    post.outside_window(),
    }
}

fn summary_to_proto(s: PostSummary) -> proto::PostSummary {
    proto::PostSummary {
        post_id:      s.post_id.as_str(),
        kind:         s.kind.as_tinyint() as i32 + 1,
        status:       s.status.as_tinyint() as i32 + 1,
        moderation:   s.moderation.as_tinyint() as i32 + 1,
        created_at_ms: s.created_at.timestamp_millis(),
    }
}

// ── Audio conversion helpers ──────────────────────────────────────────────────

fn proto_audio_ref_to_domain(
    proto: Option<proto::AudioReference>,
) -> Result<Option<AudioReference>, Status> {
    let Some(r) = proto else { return Ok(None) };
    if r.audio_id.is_empty() || r.audio_kind == 0 {
        return Ok(None);
    }
    let audio_id = AudioId::try_from(r.audio_id.as_str())
        .map_err(|e| Status::invalid_argument(e.to_string()))?;
    // Proto AudioKind uses +1 offset: ORIGINAL_SOUND=1 → domain 0, REUSED=2 → domain 1.
    let audio_kind = AudioKind::try_from((r.audio_kind - 1) as i8)
        .map_err(|e| Status::invalid_argument(e.to_string()))?;
    Ok(Some(AudioReference { audio_id, audio_kind }))
}

fn domain_audio_ref_to_proto(audio_ref: Option<&AudioReference>) -> Option<proto::AudioReference> {
    audio_ref.map(|r| proto::AudioReference {
        audio_id:   r.audio_id.as_str(),
        // Domain 0=OriginalSound → proto 1, domain 1=Reused → proto 2.
        audio_kind: r.audio_kind.as_tinyint() as i32 + 1,
    })
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
