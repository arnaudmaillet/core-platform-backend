use std::sync::Arc;

use chrono::{DateTime, Utc};
use cqrs::Envelope;

use crate::application::policy::MediaPolicy;
use crate::application::port::{AssetRepository, ObjectStore, PresignedUpload};
use crate::domain::aggregate::{Asset, ReserveParams};
use crate::domain::value_object::{
    AssetId, ContentHash, MediaKind, MimeType, OwnerId, UploadConstraints, UploadTicket,
};
use crate::error::MediaError;

/// Plane A — broker a pre-signed upload. No bytes; the client PUTs them straight to
/// the object store using the returned URL.
#[derive(Debug, Clone)]
pub struct IssueUploadTicketCommand {
    pub owner_id: OwnerId,
    pub kind: MediaKind,
    pub declared_mime: MimeType,
    pub declared_size: u64,
    /// Optional client-declared SHA-256 (hex). Used for the dedup short-circuit when
    /// dedup is enabled; otherwise ignored.
    pub content_sha256: Option<String>,
    /// Optional caller idempotency key (honored by the Phase-4 cache adapter).
    pub idempotency_key: Option<String>,
}

/// The pre-signed plan returned to the client (absent when the upload deduped onto
/// existing bytes).
#[derive(Debug, Clone)]
pub struct PreparedUpload {
    pub presigned: PresignedUpload,
    pub max_size_bytes: u64,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct IssueUploadTicketOutcome {
    pub asset_id: AssetId,
    /// `None` when `deduplicated` — the asset already exists READY, no upload needed.
    pub upload: Option<PreparedUpload>,
    pub deduplicated: bool,
}

/// Reserves a `Pending` asset (validating the declared MIME/size against the kind's
/// constraints) and mints a pre-signed upload URL for it.
pub struct IssueUploadTicketHandler {
    assets: Arc<dyn AssetRepository>,
    store: Arc<dyn ObjectStore>,
    policy: MediaPolicy,
}

impl IssueUploadTicketHandler {
    pub fn new(
        assets: Arc<dyn AssetRepository>,
        store: Arc<dyn ObjectStore>,
        policy: MediaPolicy,
    ) -> Self {
        Self { assets, store, policy }
    }

    pub async fn handle(
        &self,
        envelope: Envelope<IssueUploadTicketCommand>,
        now: DateTime<Utc>,
    ) -> Result<IssueUploadTicketOutcome, MediaError> {
        let cmd = envelope.payload;
        let constraints = UploadConstraints::for_kind(cmd.kind);

        // Dedup short-circuit (fork B) — only when enabled and a hash is supplied.
        // Never for a private document (#777), either way: its bytes are its
        // owner's alone, and no other upload is handed its asset.
        if self.policy.dedup_enabled
            && !cmd.kind.is_private()
            && let Some(sha) = cmd.content_sha256.as_deref()
        {
            let hash = ContentHash::new(sha)?;
            if let Some(existing) =
                self.assets.find_ready_by_content_hash(&hash).await?.filter(|a| !a.kind().is_private())
            {
                return Ok(IssueUploadTicketOutcome {
                    asset_id: existing.id(),
                    upload: None,
                    deduplicated: true,
                });
            }
        }

        let id = AssetId::new();
        let asset = Asset::reserve(
            ReserveParams {
                id,
                owner_id: cmd.owner_id,
                kind: cmd.kind,
                declared_mime: cmd.declared_mime.clone(),
                declared_size: cmd.declared_size,
            },
            &constraints,
            now,
        )?;
        self.assets.save(&asset).await?;

        let ticket = UploadTicket::issue(id, constraints, self.policy.upload_ticket_ttl, now)?;
        let presigned = self
            .store
            .presign_put(
                ticket.storage_key(),
                &cmd.declared_mime,
                ticket.constraints().max_bytes(),
                self.policy.upload_ticket_ttl,
            )
            .await?;

        Ok(IssueUploadTicketOutcome {
            asset_id: id,
            upload: Some(PreparedUpload {
                presigned,
                max_size_bytes: ticket.constraints().max_bytes(),
                expires_at: ticket.expires_at(),
            }),
            deduplicated: false,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::fakes::{t0, Fixture, TEST_HASH};
    use uuid::Uuid;

    fn cmd() -> IssueUploadTicketCommand {
        IssueUploadTicketCommand {
            owner_id: OwnerId::from_uuid(Uuid::from_u128(7)),
            kind: MediaKind::PostImage,
            declared_mime: MimeType::new("image/jpeg").unwrap(),
            declared_size: 2_000_000,
            content_sha256: None,
            idempotency_key: None,
        }
    }

    fn env(c: IssueUploadTicketCommand) -> Envelope<IssueUploadTicketCommand> {
        Envelope::new(Uuid::now_v7(), c)
    }

    #[tokio::test]
    async fn issues_a_ticket_and_persists_a_pending_asset() {
        let fx = Fixture::new();
        let out = fx.issue_ticket_handler().handle(env(cmd()), t0()).await.unwrap();

        assert!(!out.deduplicated);
        let upload = out.upload.expect("a fresh upload plan");
        assert_eq!(upload.presigned.method, "PUT");
        assert!(upload.presigned.url.contains(&out.asset_id.as_str()));
        // The asset is persisted Pending.
        let stored = fx.assets.find_by_id(&out.asset_id).await.unwrap().unwrap();
        assert_eq!(stored.state(), crate::domain::value_object::AssetState::Pending);
    }

    #[tokio::test]
    async fn rejects_an_oversize_declaration_before_any_upload() {
        let fx = Fixture::new();
        let mut c = cmd();
        c.declared_size = MediaKind::PostImage.max_bytes() + 1;
        let err = fx.issue_ticket_handler().handle(env(c), t0()).await.unwrap_err();
        assert!(matches!(err, MediaError::UploadSizeExceeded { .. }));
    }

    #[tokio::test]
    async fn dedup_off_by_default_always_issues_a_fresh_ticket() {
        let fx = Fixture::new();
        // Even with a matching READY asset present, dedup is disabled by default.
        fx.seed_ready_asset(TEST_HASH).await;
        let mut c = cmd();
        c.content_sha256 = Some(TEST_HASH.to_owned());
        let out = fx.issue_ticket_handler().handle(env(c), t0()).await.unwrap();
        assert!(!out.deduplicated);
        assert!(out.upload.is_some());
    }

    #[tokio::test]
    async fn dedup_when_enabled_short_circuits_onto_existing_bytes() {
        let mut fx = Fixture::new();
        fx.policy.dedup_enabled = true;
        let existing = fx.seed_ready_asset(TEST_HASH).await;

        let mut c = cmd();
        c.content_sha256 = Some(TEST_HASH.to_owned());
        let out = fx.issue_ticket_handler().handle(env(c), t0()).await.unwrap();

        assert!(out.deduplicated);
        assert!(out.upload.is_none(), "no upload needed on a dedup hit");
        assert_eq!(out.asset_id, existing, "reuses the existing asset");
    }
}
