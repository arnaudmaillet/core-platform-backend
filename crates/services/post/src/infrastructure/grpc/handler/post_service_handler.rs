use std::sync::Arc;

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
use crate::application::command::create_post::{AttachmentInput, CreatePosts};
use crate::application::port::PostSummary;
use crate::application::query::{
    get_post::GetPostQuery,
    like_visibility::{GetLikeVisibilityQuery, LikeVisibility},
    list_posts_by_profile::{ListPostsByProfileQuery, ProfileTab},
    list_recently_deleted::ListRecentlyDeletedQuery,
};
use crate::domain::aggregate::{Post, ReuseOverrides};
use crate::domain::entity::MediaAttachment;
use crate::domain::value_object::{AudioId, AudioKind, AudioReference, PostId, ProfileId, Viewer};
use crate::infrastructure::client::MediaDelivery;

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
    /// CreatePost, which answers the post it created or replayed (#876).
    creator:     Arc<dyn CreatePosts>,
    /// Delivers attachments' media assets on read (#907); `None` when media
    /// is not configured.
    media:       Option<Arc<dyn MediaDelivery>>,
    /// Card styles (#914): applied by authors, withheld during the experiment
    /// window; `None`: SetPostCardStyle is UNAVAILABLE and styles show as
    /// stored.
    card_styles: Option<Arc<crate::application::card_styles::CardStyles>>,
}

impl<CB, QB> PostServiceHandler<CB, QB>
where
    CB: CommandBus + Send + Sync + 'static,
    QB: QueryBus + Send + Sync + 'static,
{
    pub fn new(command_bus: CB, query_bus: QB, creator: Arc<dyn CreatePosts>) -> Self {
        Self { command_bus, query_bus, creator, media: None, card_styles: None }
    }

    /// Card styles (#914).
    pub fn with_card_styles(mut self, card_styles: Arc<crate::application::card_styles::CardStyles>) -> Self {
        self.card_styles = Some(card_styles);
        self
    }

    /// Applies a card style to one of the caller's own posts (#914). Edge:
    /// the post's author.
    pub async fn set_post_card_style(
        &self,
        request: Request<proto::SetPostCardStyleRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        edge::require_profile(&request, &request.get_ref().profile_id)?;
        let account = account_of(&request);
        let req = request.into_inner();
        let styles = self.card_styles.as_ref().ok_or_else(|| Status::unavailable("card styles are not configured"))?;
        let post_id = PostId::try_from(req.post_id.as_str()).map_err(|e| app_err_to_status(&e))?;
        let author = ProfileId::try_from(req.profile_id.as_str()).map_err(|e| app_err_to_status(&e))?;
        let style = Some(req.card_style_id).filter(|s| !s.is_empty());
        styles.apply(&post_id, &author, account.as_deref(), style).await.map_err(|e| app_err_to_status(&e))?;
        Ok(Response::new(proto::CommandResponse { success: true, message: String::new() }))
    }

    /// Attachments carry their media asset's delivery (#907).
    pub fn with_media(mut self, media: Arc<dyn MediaDelivery>) -> Self {
        self.media = Some(media);
        self
    }

    /// Only for clients: a mesh reader (search's hydration, the export) has
    /// no use for renditions, and would only load media.
    async fn deliver_media(&self, viewer: &Viewer, posts: &mut [proto::PostView]) {
        if *viewer != Viewer::Internal {
            deliver_media(self.media.as_deref(), posts).await;
        }
    }
}

/// Adds each attachment's delivery (#907): one media call for all the posts.
/// Fail-open — media unreachable or slow, the posts go out with their URLs
/// only.
async fn deliver_media(media: Option<&dyn MediaDelivery>, posts: &mut [proto::PostView]) {
    let Some(media) = media else { return };
    let mut ids: Vec<String> = posts
        .iter()
        .flat_map(|p| &p.attachments)
        .map(|a| a.asset_id.clone())
        .filter(|id| !id.is_empty())
        .collect();
    ids.sort();
    ids.dedup();
    if ids.is_empty() {
        return;
    }
    let delivered = match media.deliver(&ids).await {
        Ok(delivered) => delivered,
        Err(status) => {
            tracing::warn!(%status, assets = ids.len(), "media delivery unavailable; attachments served by URL");
            return;
        }
    };
    let by_id: std::collections::HashMap<&str, &media_api::DeliveredMedia> =
        delivered.iter().map(|m| (m.asset_id.as_str(), m)).collect();
    for attachment in posts.iter_mut().flat_map(|p| &mut p.attachments) {
        attachment.media = by_id.get(attachment.asset_id.as_str()).map(|m| (*m).clone());
    }
}

/// The edge caller's account (#907), `None` on the mesh.
fn account_of<T>(request: &Request<T>) -> Option<String> {
    edge::principal(request).map(|p| p.account_id().to_owned())
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
        let account_id = account_of(&request);
        let req        = request.into_inner();
        let profile_id = req.profile_id.clone();

        let audio_ref = proto_audio_ref_to_domain(req.audio_ref)?;

        let cmd = CreatePostCommand {
            post_id:     PostId::new_v7().as_str(),
            profile_id:  req.profile_id,
            kind:        req.kind,
            caption:     req.caption,
            attachments: req.attachments.into_iter().map(attachment_input_from_proto).collect(),
            parent_id:   Some(req.parent_id).filter(|s| !s.is_empty()),
            root_id:     Some(req.root_id).filter(|s| !s.is_empty()),
            audio_ref,
            location:    req.location.map(|g| (g.lat, g.lng)),
            reuse:       ReuseOverrides { allow_remix: req.allow_remix, allow_sound_reuse: req.allow_sound_reuse },
            idempotency_key: Some(req.idempotency_key).filter(|k| !k.is_empty()),
            account_id,
        };

        let created = self.creator.create(&cmd).await.map_err(|e| app_err_to_status(&e))?;
        Ok(Response::new(proto::CreatePostResponse { post_id: created.post_id.as_str(), profile_id }))
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
        let account_id = account_of(&request);
        let req = request.into_inner();
        let cmd = UpdatePostCommand {
            post_id:     req.post_id,
            profile_id:  req.profile_id,
            caption:     req.caption,
            attachments: req.attachments.into_iter().map(attachment_input_from_proto).collect(),
            account_id,
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
        let viewer = viewer_of(&request);
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
        let mut posts: Vec<proto::PostView> = posts.into_iter().map(post_to_proto).collect();
        self.deliver_media(&viewer, &mut posts).await;
        Ok(Response::new(proto::ListRecentlyDeletedResponse { posts, next_token: next.unwrap_or_default() }))
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
        // On the author's behalf: the mesh only; an edge caller's field is
        // dropped here, before the query.
        let as_author = (viewer == Viewer::Internal).then_some(req.as_author_id).filter(|a| !a.is_empty());
        let read_as_author = as_author.is_some();
        let query  = GetPostQuery { post_id: req.post_id, viewer: viewer.clone(), mature, as_author };
        let mut post: Post = self
            .query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), query))
            .await
            .map_err(cqrs_to_status)?;
        // #914: other readers see the default card during the experiment
        // window, but on the post detail.
        let author = read_as_author
            || matches!(&viewer, Viewer::Profiles(own) if own.iter().any(|p| p.as_uuid() == post.profile_id().as_uuid()));
        if let Some(styles) = &self.card_styles
            && !author
            && !req.detail
            && !styles.shown_to_others(&post, chrono::Utc::now())
        {
            post.withhold_card_style();
        }

        let mut view = [post_to_proto(post)];
        self.deliver_media(&viewer, &mut view).await;
        let [view] = view;
        Ok(Response::new(view))
    }

    /// Mesh only (#809): not in `EDGE_POLICY`.
    pub async fn batch_get_like_visibility(
        &self,
        request: Request<proto::BatchGetLikeVisibilityRequest>,
    ) -> Result<Response<proto::BatchGetLikeVisibilityResponse>, Status> {
        let query = GetLikeVisibilityQuery { post_ids: request.into_inner().post_ids };
        let found: Vec<LikeVisibility> = self
            .query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), query))
            .await
            .map_err(cqrs_to_status)?;
        Ok(Response::new(proto::BatchGetLikeVisibilityResponse {
            posts: found
                .into_iter()
                .map(|v| proto::PostLikeVisibility {
                    post_id:            v.post_id.as_str(),
                    author_id:          v.author_id.as_str(),
                    like_counts_hidden: v.like_counts_hidden,
                })
                .collect(),
        }))
    }

    pub async fn list_posts_by_profile(
        &self,
        request: Request<proto::ListPostsByProfileRequest>,
    ) -> Result<Response<proto::ListPostsByProfileResponse>, Status> {
        let viewer = viewer_of(&request);
        let mature = mature_of(&request);
        let req    = request.into_inner();
        let tab    = match proto::ProfilePostsTab::try_from(req.tab) {
            Ok(proto::ProfilePostsTab::Reposts) => ProfileTab::Reposts,
            Ok(proto::ProfilePostsTab::Places) => ProfileTab::Places,
            Ok(proto::ProfilePostsTab::Unspecified) => ProfileTab::All,
            Err(_) => return Err(Status::invalid_argument("unknown tab")),
        };
        let query  = ListPostsByProfileQuery {
            tab,
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
        asset_id:         Some(a.asset_id).filter(|s| !s.is_empty()),
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
        asset_id:         a.asset_id.map(|id| id.as_str()).unwrap_or_default(),
        media:            None,
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
        visible_until_ms:  post.visible_until().map(|t| t.timestamp_millis()).unwrap_or_default(),
        downloads_disabled: post.downloads_disabled(),
        like_counts_hidden: post.like_counts_hidden(),
        card_style_id:      post.card_style_id().map(str::to_owned).unwrap_or_default(),
    }
}

fn summary_to_proto(s: PostSummary) -> proto::PostSummary {
    proto::PostSummary {
        post_id:      s.post_id.as_str(),
        kind:         s.kind.as_tinyint() as i32 + 1,
        status:       s.status.as_tinyint() as i32 + 1,
        moderation:   s.moderation.as_tinyint() as i32 + 1,
        created_at_ms: s.created_at.timestamp_millis(),
        is_repost:    s.is_repost,
        has_place:    s.has_place,
        card_style_id: s.card_style_id.unwrap_or_default(),
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
        CqrsError::Handler(boxed) => app_err_to_status(&boxed),
    }
}

/// An application error's status: its HTTP class, `ABORTED` for a retryable
/// conflict.
fn app_err_to_status(err: &impl error::AppError) -> Status {
    let msg = err.to_string();
    match err.http_status().as_u16() {
        403       => Status::permission_denied(msg),
        404       => Status::not_found(msg),
        409 if err.is_retryable() => Status::aborted(msg),
        409       => Status::already_exists(msg),
        400 | 422 => Status::failed_precondition(msg),
        503 | 502 => Status::unavailable(msg),
        _         => Status::internal(msg),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use media_api::DeliveredMedia;

    struct FakeMedia(Result<Vec<DeliveredMedia>, tonic::Status>);

    #[async_trait]
    impl MediaDelivery for FakeMedia {
        async fn deliver(&self, _asset_ids: &[String]) -> Result<Vec<DeliveredMedia>, tonic::Status> {
            self.0.clone()
        }
    }

    fn view(asset_ids: &[&str]) -> proto::PostView {
        proto::PostView {
            attachments: asset_ids
                .iter()
                .map(|id| proto::MediaAttachmentView { asset_id: (*id).to_owned(), ..Default::default() })
                .collect(),
            ..Default::default()
        }
    }

    fn delivered(id: &str) -> DeliveredMedia {
        DeliveredMedia { asset_id: id.to_owned(), blurhash: format!("bh-{id}"), ..Default::default() }
    }

    /// #907: each attachment naming an asset carries its delivery; one
    /// without an asset, or one media did not resolve, carries none.
    #[tokio::test]
    async fn attachments_carry_their_assets_delivery() {
        let media = FakeMedia(Ok(vec![delivered("a"), delivered("b")]));
        let mut posts = [view(&["a", ""]), view(&["b", "unknown"])];
        deliver_media(Some(&media), &mut posts).await;
        let blurhashes: Vec<Option<String>> = posts
            .iter()
            .flat_map(|p| &p.attachments)
            .map(|a| a.media.as_ref().map(|m| m.blurhash.clone()))
            .collect();
        assert_eq!(blurhashes, [Some("bh-a".into()), None, Some("bh-b".into()), None]);
    }

    /// #907: media down, the posts go out as before (fail-open).
    #[tokio::test]
    async fn a_media_outage_serves_the_posts_without_delivery() {
        let media = FakeMedia(Err(tonic::Status::unavailable("down")));
        let mut posts = [view(&["a"])];
        deliver_media(Some(&media), &mut posts).await;
        assert_eq!(posts[0].attachments[0].asset_id, "a");
        assert!(posts[0].attachments[0].media.is_none());
    }

    #[test]
    fn an_attachments_asset_id_round_trips_to_its_view() {
        let input = proto::MediaAttachmentInput {
            cdn_url: "https://cdn.example/a.jpg".into(),
            mime_type: "image/jpeg".into(),
            width: 10,
            height: 10,
            asset_id: "0199b8c4-0000-7000-8000-000000000001".into(),
            ..Default::default()
        };
        let parsed = crate::application::command::create_post::parse_attachments(&[attachment_input_from_proto(input)])
            .unwrap();
        let view = attachment_to_proto(&parsed[0]);
        assert_eq!(view.asset_id, "0199b8c4-0000-7000-8000-000000000001");
        assert!(view.media.is_none(), "delivery is resolved per read");
    }
}
