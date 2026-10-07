//! gRPC request handler for `media.v1`. Each method translates an inbound Protobuf
//! request into a validated application command/query, runs it with a fresh
//! correlation id, and maps the outcome (or [`MediaError`]) back to Protobuf /
//! [`Status`]. No bytes ever cross this surface — only tickets, metadata, and URLs.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use cqrs::{Envelope, QueryHandler};
use error::AppError;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use transport::grpc::edge;
use crate::application::command::{
    CommitUploadCommand, CommitUploadHandler, DeleteAssetCommand, DeleteAssetHandler,
    IssueUploadTicketCommand, IssueUploadTicketHandler, IssueUploadTicketOutcome,
    ProcessAssetCommand, ProcessAssetHandler,
};
use crate::application::query::{
    DeliveredMediaView, GetAssetHandler, GetAssetQuery, ListAssetsByOwnerHandler, ListAssetsByOwnerQuery,
    PrivateDocumentUrlHandler, ResolveDeliveryHandler, ResolveDeliveryQuery,
};
use crate::domain::aggregate::{Asset, Rendition};
use crate::domain::value_object::{
    AssetId, AssetState, DeliveryVisibility, MediaKind, MimeType, OwnerId, RenditionKind,
};
use crate::error::MediaError;

pub use media_api as proto;

/// gRPC handler for `media.v1.MediaService`. Holds the application handlers as
/// `Arc`s (so it is cheaply `Clone`).
#[derive(Clone)]
pub struct MediaServiceHandler {
    issue: Arc<IssueUploadTicketHandler>,
    commit: Arc<CommitUploadHandler>,
    delete: Arc<DeleteAssetHandler>,
    process: Arc<ProcessAssetHandler>,
    get: Arc<GetAssetHandler>,
    resolve: Arc<ResolveDeliveryHandler>,
    /// The GDPR export's listing (#653); `None` answers UNIMPLEMENTED.
    by_owner: Option<Arc<ListAssetsByOwnerHandler>>,
    /// Staff links to private documents (#777); `None` answers UNIMPLEMENTED.
    private_documents: Option<Arc<PrivateDocumentUrlHandler>>,
}

impl MediaServiceHandler {
    pub fn new(
        issue: Arc<IssueUploadTicketHandler>,
        commit: Arc<CommitUploadHandler>,
        delete: Arc<DeleteAssetHandler>,
        process: Arc<ProcessAssetHandler>,
        get: Arc<GetAssetHandler>,
        resolve: Arc<ResolveDeliveryHandler>,
    ) -> Self {
        Self { issue, commit, delete, process, get, resolve, by_owner: None, private_documents: None }
    }

    /// Enables `ListAssetsByOwner` (#653).
    pub fn with_list_by_owner(mut self, handler: Arc<ListAssetsByOwnerHandler>) -> Self {
        self.by_owner = Some(handler);
        self
    }

    pub fn with_private_documents(mut self, handler: Arc<PrivateDocumentUrlHandler>) -> Self {
        self.private_documents = Some(handler);
        self
    }

    /// Mesh only (#777): a staff link to a private document.
    pub async fn get_private_document_url(
        &self,
        request: Request<proto::GetPrivateDocumentUrlRequest>,
    ) -> Result<Response<proto::GetPrivateDocumentUrlResponse>, Status> {
        let handler = self
            .private_documents
            .as_ref()
            .ok_or_else(|| Status::unimplemented("private documents are not wired"))?;
        let asset_id = AssetId::try_from(request.into_inner().asset_id.as_str()).map_err(to_status)?;
        let resolved = handler.handle_at(asset_id, Utc::now()).await.map_err(to_status)?;
        Ok(Response::new(proto::GetPrivateDocumentUrlResponse {
            url:        resolved.url,
            expires_at: resolved.expires_at.map(to_timestamp),
        }))
    }

    /// Mesh only (#653): an account's assets with signed downloads.
    pub async fn list_assets_by_owner(
        &self,
        request: Request<proto::ListAssetsByOwnerRequest>,
    ) -> Result<Response<proto::ListAssetsByOwnerResponse>, Status> {
        let handler = self.by_owner.as_ref().ok_or_else(|| Status::unimplemented("listing by owner is not enabled"))?;
        let req = request.into_inner();
        let limit = i64::from(req.limit.clamp(1, 500));
        let after = Some(req.page_token).filter(|t| !t.is_empty());
        let query = ListAssetsByOwnerQuery {
            owner_id: OwnerId::try_from(req.owner_id.as_str()).map_err(to_status)?,
            limit,
            after: after.as_deref().map(AssetId::try_from).transpose().map_err(to_status)?,
            url_ttl: chrono::Duration::seconds(req.url_ttl_secs),
        };
        let owned = handler.handle(Envelope::new(Uuid::now_v7(), query)).await.map_err(to_status)?;
        let next_page_token = match owned.last() {
            Some(last) if owned.len() == limit as usize => last.asset.id().as_str(),
            _ => String::new(),
        };
        Ok(Response::new(proto::ListAssetsByOwnerResponse {
            assets: owned
                .iter()
                .map(|o| proto::OwnedAsset {
                    asset: Some(asset_to_proto(&o.asset)),
                    download_url: o.download.as_ref().map(|d| d.url.clone()).unwrap_or_default(),
                    download_expires_at_ms: o
                        .download
                        .as_ref()
                        .and_then(|d| d.expires_at)
                        .map(|at| at.timestamp_millis())
                        .unwrap_or_default(),
                })
                .collect(),
            next_page_token,
        }))
    }

    pub async fn issue_upload_ticket(
        &self,
        request: Request<proto::IssueUploadTicketRequest>,
    ) -> Result<Response<proto::IssueUploadTicketResponse>, Status> {
        edge::require_account(&request, &request.get_ref().owner_id)?;
        let req = request.into_inner();
        let cmd = IssueUploadTicketCommand {
            owner_id: OwnerId::try_from(req.owner_id.as_str()).map_err(to_status)?,
            kind: kind_from_proto(req.kind)?,
            declared_mime: MimeType::new(req.declared_mime_type).map_err(to_status)?,
            declared_size: req.declared_size_bytes,
            content_sha256: optional(req.content_sha256),
            idempotency_key: optional(req.idempotency_key),
        };
        let outcome = self
            .issue
            .handle(Envelope::new(Uuid::now_v7(), cmd), Utc::now())
            .await
            .map_err(to_status)?;
        Ok(Response::new(issue_outcome_to_proto(outcome)))
    }

    pub async fn commit_upload(
        &self,
        request: Request<proto::CommitUploadRequest>,
    ) -> Result<Response<proto::CommitUploadResponse>, Status> {
        let req = request.into_inner();
        let cmd = CommitUploadCommand {
            asset_id: AssetId::try_from(req.asset_id.as_str()).map_err(to_status)?,
            etag: optional(req.etag),
            content_sha256: optional(req.content_sha256),
        };
        let asset = self
            .commit
            .handle(Envelope::new(Uuid::now_v7(), cmd), Utc::now())
            .await
            .map_err(to_status)?;
        Ok(Response::new(proto::CommitUploadResponse { asset: Some(asset_to_proto(&asset)) }))
    }

    pub async fn abort_upload(
        &self,
        request: Request<proto::AbortUploadRequest>,
    ) -> Result<Response<proto::AbortUploadResponse>, Status> {
        edge::require_account(&request, &request.get_ref().owner_id)?;
        let req = request.into_inner();
        // Abort = delete the (still-pending) reservation: purges staging + tombstones.
        let cmd = DeleteAssetCommand {
            asset_id: AssetId::try_from(req.asset_id.as_str()).map_err(to_status)?,
            owner_id: OwnerId::try_from(req.owner_id.as_str()).map_err(to_status)?,
        };
        self.delete
            .handle(Envelope::new(Uuid::now_v7(), cmd), Utc::now())
            .await
            .map_err(to_status)?;
        Ok(Response::new(proto::AbortUploadResponse {}))
    }

    pub async fn get_asset(
        &self,
        request: Request<proto::GetAssetRequest>,
    ) -> Result<Response<proto::GetAssetResponse>, Status> {
        // A private document (#777) is its owner's only: the edge caller's
        // account must own it; the mesh reads it.
        let caller = match edge::principal(&request) {
            Some(principal) => Some(OwnerId::try_from(principal.account_id()).map_err(to_status)?),
            None => None,
        };
        let req = request.into_inner();
        let query = GetAssetQuery {
            asset_id: AssetId::try_from(req.asset_id.as_str()).map_err(to_status)?,
            caller,
        };
        let asset = self
            .get
            .handle(Envelope::new(Uuid::now_v7(), query))
            .await
            .map_err(to_status)?;
        Ok(Response::new(proto::GetAssetResponse { asset: Some(asset_to_proto(&asset)) }))
    }

    pub async fn delete_asset(
        &self,
        request: Request<proto::DeleteAssetRequest>,
    ) -> Result<Response<proto::DeleteAssetResponse>, Status> {
        edge::require_account(&request, &request.get_ref().owner_id)?;
        let req = request.into_inner();
        let cmd = DeleteAssetCommand {
            asset_id: AssetId::try_from(req.asset_id.as_str()).map_err(to_status)?,
            owner_id: OwnerId::try_from(req.owner_id.as_str()).map_err(to_status)?,
        };
        self.delete
            .handle(Envelope::new(Uuid::now_v7(), cmd), Utc::now())
            .await
            .map_err(to_status)?;
        Ok(Response::new(proto::DeleteAssetResponse {}))
    }

    pub async fn resolve_delivery(
        &self,
        request: Request<proto::ResolveDeliveryRequest>,
    ) -> Result<Response<proto::ResolveDeliveryResponse>, Status> {
        let req = request.into_inner();
        let query = ResolveDeliveryQuery {
            asset_id: AssetId::try_from(req.asset_id.as_str()).map_err(to_status)?,
            preferred: rendition_from_proto(req.preferred),
            visibility: visibility_from_proto(req.visibility),
        };
        let view = self
            .resolve
            .handle(Envelope::new(Uuid::now_v7(), query))
            .await
            .map_err(to_status)?;
        Ok(Response::new(proto::ResolveDeliveryResponse {
            media: Some(delivered_to_proto(view)),
        }))
    }

    pub async fn batch_resolve_delivery(
        &self,
        request: Request<proto::BatchResolveDeliveryRequest>,
    ) -> Result<Response<proto::BatchResolveDeliveryResponse>, Status> {
        let req = request.into_inner();
        // Unparseable ids are dropped (the batch fails open for feed hydration).
        let ids: Vec<AssetId> = req
            .asset_ids
            .iter()
            .filter_map(|id| AssetId::try_from(id.as_str()).ok())
            .collect();
        let views = self
            .resolve
            .resolve_batch(
                &ids,
                rendition_from_proto(req.preferred),
                visibility_from_proto(req.visibility),
                Utc::now(),
            )
            .await
            .map_err(to_status)?;
        Ok(Response::new(proto::BatchResolveDeliveryResponse {
            media: views.into_iter().map(delivered_to_proto).collect(),
        }))
    }

    pub async fn reprocess(
        &self,
        request: Request<proto::ReprocessRequest>,
    ) -> Result<Response<proto::ReprocessResponse>, Status> {
        let req = request.into_inner();
        let asset_id = AssetId::try_from(req.asset_id.as_str()).map_err(to_status)?;
        // Retrigger the pipeline (a no-op unless the asset is awaiting processing),
        // then return the current metadata.
        self.process
            .handle(Envelope::new(Uuid::now_v7(), ProcessAssetCommand { asset_id }), Utc::now())
            .await
            .map_err(to_status)?;
        let asset = self
            .get
            .handle(Envelope::new(Uuid::now_v7(), GetAssetQuery { asset_id, caller: None }))
            .await
            .map_err(to_status)?;
        Ok(Response::new(proto::ReprocessResponse { asset: Some(asset_to_proto(&asset)) }))
    }
}

// ── proto → domain ────────────────────────────────────────────────────────────

fn optional(s: String) -> Option<String> {
    if s.is_empty() { None } else { Some(s) }
}

fn kind_from_proto(value: i32) -> Result<MediaKind, Status> {
    match value {
        1 => Ok(MediaKind::Avatar),
        2 => Ok(MediaKind::PostImage),
        // MEDIA_KIND_VIDEO — ingest accepted: the video probe validates the bytes
        // (real stream, allowed codec) at finalize. Transcode/delivery renditions
        // still land in later phases.
        3 => Ok(MediaKind::Video),
        4 => Ok(MediaKind::PrivateDocument),
        other => Err(Status::invalid_argument(format!("unsupported media kind: {other}"))),
    }
}

/// `UNSPECIFIED`/unknown ⇒ `None` (all renditions); otherwise the specific kind.
fn rendition_from_proto(value: i32) -> Option<RenditionKind> {
    match value {
        1 => Some(RenditionKind::Original),
        2 => Some(RenditionKind::Thumbnail),
        3 => Some(RenditionKind::Small),
        4 => Some(RenditionKind::Medium),
        5 => Some(RenditionKind::Large),
        6 => Some(RenditionKind::Manifest),
        7 => Some(RenditionKind::Poster),
        _ => None,
    }
}

/// `UNSPECIFIED`/unknown ⇒ `None` (the asset's default visibility).
fn visibility_from_proto(value: i32) -> Option<DeliveryVisibility> {
    match value {
        1 => Some(DeliveryVisibility::Public),
        2 => Some(DeliveryVisibility::Signed),
        _ => None,
    }
}

// ── domain → proto ────────────────────────────────────────────────────────────

fn kind_to_proto(kind: MediaKind) -> i32 {
    match kind {
        MediaKind::Avatar => 1,
        MediaKind::PostImage => 2,
        MediaKind::Video => 3,
        MediaKind::PrivateDocument => 4,
    }
}

fn state_to_proto(state: AssetState) -> i32 {
    match state {
        AssetState::Pending => 1,
        AssetState::Uploaded => 2,
        AssetState::Processing => 3,
        AssetState::Ready => 4,
        AssetState::Failed => 5,
        AssetState::Quarantined => 6,
        AssetState::Deleted => 7,
    }
}

fn rendition_kind_to_proto(kind: RenditionKind) -> i32 {
    match kind {
        RenditionKind::Original => 1,
        RenditionKind::Thumbnail => 2,
        RenditionKind::Small => 3,
        RenditionKind::Medium => 4,
        RenditionKind::Large => 5,
        RenditionKind::Manifest => 6,
        RenditionKind::Poster => 7,
    }
}

fn visibility_to_proto(v: DeliveryVisibility) -> i32 {
    match v {
        DeliveryVisibility::Public => 1,
        DeliveryVisibility::Signed => 2,
    }
}

fn issue_outcome_to_proto(outcome: IssueUploadTicketOutcome) -> proto::IssueUploadTicketResponse {
    proto::IssueUploadTicketResponse {
        asset_id: outcome.asset_id.as_str(),
        ticket: outcome.upload.map(|u| proto::UploadTicket {
            upload_url: u.presigned.url,
            method: u.presigned.method,
            required_headers: u.presigned.required_headers,
            max_size_bytes: u.max_size_bytes,
            expires_at: Some(to_timestamp(u.expires_at)),
        }),
        deduplicated: outcome.deduplicated,
    }
}

fn asset_to_proto(a: &Asset) -> proto::Asset {
    proto::Asset {
        id: a.id().as_str(),
        owner_id: a.owner_id().as_str(),
        kind: kind_to_proto(a.kind()),
        state: state_to_proto(a.state()),
        mime_type: a.mime_type().map(|m| m.as_str().to_owned()).unwrap_or_default(),
        byte_size: a.byte_size().unwrap_or(0),
        width: a.dimensions().map(|d| d.width()).unwrap_or(0),
        height: a.dimensions().map(|d| d.height()).unwrap_or(0),
        content_hash: a.content_hash().map(|h| h.as_str().to_owned()).unwrap_or_default(),
        blurhash: a.blurhash().map(|b| b.as_str().to_owned()).unwrap_or_default(),
        renditions: a.renditions().iter().map(rendition_to_proto).collect(),
        created_at: Some(to_timestamp(a.created_at())),
        updated_at: Some(to_timestamp(a.updated_at())),
    }
}

fn rendition_to_proto(r: &Rendition) -> proto::Rendition {
    proto::Rendition {
        kind: rendition_kind_to_proto(r.kind()),
        mime_type: r.mime_type().as_str().to_owned(),
        storage_key: r.storage_key().as_str().to_owned(),
        width: r.dimensions().width(),
        height: r.dimensions().height(),
        byte_size: r.byte_size(),
    }
}

fn delivered_to_proto(view: DeliveredMediaView) -> proto::DeliveredMedia {
    proto::DeliveredMedia {
        asset_id: view.asset_id.as_str(),
        state: state_to_proto(view.state),
        blurhash: view.blurhash.unwrap_or_default(),
        renditions: view
            .renditions
            .into_iter()
            .map(|r| proto::DeliveredRendition {
                kind: rendition_kind_to_proto(r.kind),
                url: r.url,
                visibility: visibility_to_proto(r.visibility),
                url_expires_at: r.expires_at.map(to_timestamp),
                width: r.width,
                height: r.height,
            })
            .collect(),
        degraded: view.degraded,
    }
}

fn to_timestamp(dt: DateTime<Utc>) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: dt.timestamp(),
        nanos: dt.timestamp_subsec_nanos() as i32,
    }
}

fn to_status(err: MediaError) -> Status {
    let message = err.to_string();
    match err.http_status().as_u16() {
        400 | 410 | 413 | 415 | 422 => Status::invalid_argument(message),
        404 => Status::not_found(message),
        409 => Status::failed_precondition(message),
        // 451 — quarantine / legal hold.
        451 => Status::permission_denied(message),
        503 => Status::unavailable(message),
        504 => Status::deadline_exceeded(message),
        _ => Status::internal(message),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::Code;

    #[test]
    fn all_known_kinds_map_to_their_domain_variants() {
        assert_eq!(kind_from_proto(1).unwrap(), MediaKind::Avatar);
        assert_eq!(kind_from_proto(2).unwrap(), MediaKind::PostImage);
        // VIDEO ingest is now accepted (the probe validates the bytes at finalize).
        assert_eq!(kind_from_proto(3).unwrap(), MediaKind::Video);
    }

    #[test]
    fn unknown_kinds_are_invalid_argument() {
        assert_eq!(kind_from_proto(0).unwrap_err().code(), Code::InvalidArgument);
        assert_eq!(kind_from_proto(99).unwrap_err().code(), Code::InvalidArgument);
    }
}
