use std::sync::Arc;

use chrono::{DateTime, Utc};
use cqrs::Envelope;

use crate::application::policy::MediaPolicy;
use crate::application::port::{AssetRepository, ObjectStore, PresignedUpload};
use crate::domain::aggregate::{Asset, ReserveParams};
use crate::domain::value_object::{
    AssetId, AssetState, ContentHash, DeliveryVisibility, MediaKind, MimeType, OwnerId, UploadConstraints, UploadKey,
    UploadTicket,
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
    /// Optional caller idempotency key (#876): a retried ticket under the same
    /// key answers the asset the first one reserved.
    pub idempotency_key: Option<String>,
    /// How the owner wants it delivered (#919); `None` ⇒ the kind's default.
    /// Never laxer than the kind's.
    pub delivery_visibility: Option<crate::domain::value_object::DeliveryVisibility>,
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
///
/// Under an idempotency key (#876) the reservation is keyed: a key that already
/// reserved a live asset of this owner answers that asset instead — a fresh
/// upload URL while it is still `Pending` (the bytes never landed, or the commit
/// was lost), no upload (`deduplicated`) once it is past `Pending`. The same key
/// over another kind, type or content is refused (`UploadKeyConflict`): it would
/// hand back other bytes.
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
        let key = cmd.idempotency_key.as_deref().map(UploadKey::new).transpose()?;
        // Private documents (#777) only where their private home exists.
        if cmd.kind.is_private() && !self.policy.private_documents_enabled {
            return Err(MediaError::PrivateDocumentsDisabled);
        }
        let constraints = UploadConstraints::for_kind(cmd.kind);

        // Dedup short-circuit (fork B) — only when enabled and a hash is supplied.
        // Never for a private document (#777), either way: its bytes are its
        // owner's alone, and no other upload is handed its asset. Nor for an
        // upload its owner wants SIGNED (#919): it must not be handed another
        // owner's public asset.
        let signed = cmd.delivery_visibility == Some(DeliveryVisibility::Signed);
        if self.policy.dedup_enabled
            && !cmd.kind.is_private()
            && !signed
            && let Some(sha) = cmd.content_sha256.as_deref()
        {
            // A key that already reserved an asset answers it first: a READY
            // match appearing since must not leave that reservation orphaned.
            if let Some(key) = &key
                && let Some(holder) = self.assets.find_by_upload_key(&cmd.owner_id, key).await?
            {
                return self.replay(holder, &cmd, now).await;
            }
            let hash = ContentHash::new(sha)?;
            // Only onto public bytes of the same kind: a declared hash is no
            // proof of possession, and a SIGNED asset is its owner's alone
            // (#919). The uploader gets an asset of its own over those bytes
            // (#924), never someone else's id: attaching it, or changing its
            // visibility, is the uploader's business only.
            if let Some(existing) = self
                .assets
                .find_ready_by_content_hash(&hash)
                .await?
                .filter(|a| a.kind() == cmd.kind && a.delivery_visibility() == DeliveryVisibility::Public)
                && let Ok(asset) = Asset::share_bytes(AssetId::new(), cmd.owner_id, &existing, now)
            {
                match &key {
                    Some(key) => {
                        if let Some(holder) = self.assets.insert_keyed(&asset, key).await? {
                            return self.replay(holder, &cmd, now).await;
                        }
                    }
                    None => self.assets.save(&asset).await?,
                }
                return Ok(IssueUploadTicketOutcome { asset_id: asset.id(), upload: None, deduplicated: true });
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
                delivery_visibility: cmd.delivery_visibility,
            },
            &constraints,
            now,
        )?;
        match &key {
            Some(key) => {
                if let Some(holder) = self.assets.insert_keyed(&asset, key).await? {
                    return self.replay(holder, &cmd, now).await;
                }
            }
            None => self.assets.save(&asset).await?,
        }
        self.upload_for(id, constraints, &cmd.declared_mime, now).await
    }

    /// Answers a repeated key with the asset it reserved (#876).
    async fn replay(
        &self,
        holder: AssetId,
        cmd: &IssueUploadTicketCommand,
        now: DateTime<Utc>,
    ) -> Result<IssueUploadTicketOutcome, MediaError> {
        let asset = self.assets.find_by_id(&holder).await?.ok_or(MediaError::AssetNotFound { id: holder.as_str() })?;
        let other_content = cmd
            .content_sha256
            .as_deref()
            .zip(asset.content_hash())
            .is_some_and(|(declared, stored)| !declared.eq_ignore_ascii_case(stored.as_str()));
        // Another visibility is another upload too (#919): a retry must never
        // turn a SIGNED request into the PUBLIC asset the key reserved first.
        let other_visibility = asset.delivery_visibility() != cmd.kind.default_visibility().stricter(cmd.delivery_visibility);
        if asset.kind() != cmd.kind || asset.declared_mime() != &cmd.declared_mime || other_content || other_visibility {
            return Err(MediaError::UploadKeyConflict);
        }
        if asset.state() != AssetState::Pending {
            return Ok(IssueUploadTicketOutcome { asset_id: holder, upload: None, deduplicated: true });
        }
        self.upload_for(holder, UploadConstraints::for_kind(cmd.kind), &cmd.declared_mime, now).await
    }

    /// A fresh pre-signed upload for the `Pending` asset `id`.
    async fn upload_for(
        &self,
        id: AssetId,
        constraints: UploadConstraints,
        mime: &MimeType,
        now: DateTime<Utc>,
    ) -> Result<IssueUploadTicketOutcome, MediaError> {
        let ticket = UploadTicket::issue(id, constraints, self.policy.upload_ticket_ttl, now)?;
        let presigned = self
            .store
            .presign_put(ticket.storage_key(), mime, ticket.constraints().max_bytes(), self.policy.upload_ticket_ttl)
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
            delivery_visibility: None,
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
        // #924: an asset of the uploader's own, over the same bytes.
        assert_ne!(out.asset_id, existing, "never someone else's asset id");
        let (mine, theirs) = (
            fx.assets.find_by_id(&out.asset_id).await.unwrap().unwrap(),
            fx.assets.find_by_id(&existing).await.unwrap().unwrap(),
        );
        assert_eq!(mine.owner_id(), cmd().owner_id);
        assert_eq!(mine.state(), AssetState::Ready);
        assert_eq!(mine.renditions(), theirs.renditions(), "the shared content-addressed objects");
        assert_eq!(mine.content_hash(), theirs.content_hash());
        assert_eq!(mine.delivery_visibility(), DeliveryVisibility::Public);
    }

    /// #924: another kind's bytes are not shared (another rendition ladder and
    /// path), and a keyed dedup answers its own asset on retry.
    #[tokio::test]
    async fn dedup_shares_only_the_same_kind_and_replays_under_its_key() {
        let mut fx = Fixture::new();
        fx.policy.dedup_enabled = true;
        fx.seed_ready_asset(TEST_HASH).await; // a POST_IMAGE

        let mut avatar = cmd();
        avatar.kind = MediaKind::Avatar;
        avatar.content_sha256 = Some(TEST_HASH.to_owned());
        assert!(!fx.issue_ticket_handler().handle(env(avatar), t0()).await.unwrap().deduplicated);

        let mut keyed = keyed("dedup-key-0001");
        keyed.content_sha256 = Some(TEST_HASH.to_owned());
        let first = fx.issue_ticket_handler().handle(env(keyed.clone()), t0()).await.unwrap();
        let again = fx.issue_ticket_handler().handle(env(keyed), t0()).await.unwrap();
        assert!(first.deduplicated && again.deduplicated);
        assert_eq!(again.asset_id, first.asset_id, "the key answers the asset it got");
    }

    /// #919: an upload its owner wants SIGNED is never handed another
    /// owner's existing (public) asset, even with dedup on; it is reserved
    /// SIGNED.
    #[tokio::test]
    async fn a_signed_upload_never_dedups_onto_existing_bytes() {
        let mut fx = Fixture::new();
        fx.policy.dedup_enabled = true;
        let existing = fx.seed_ready_asset(TEST_HASH).await;

        let mut c = cmd();
        c.content_sha256 = Some(TEST_HASH.to_owned());
        c.delivery_visibility = Some(DeliveryVisibility::Signed);
        let out = fx.issue_ticket_handler().handle(env(c), t0()).await.unwrap();

        assert!(!out.deduplicated);
        assert_ne!(out.asset_id, existing);
        let reserved = fx.assets.find_by_id(&out.asset_id).await.unwrap().unwrap();
        assert_eq!(reserved.delivery_visibility(), DeliveryVisibility::Signed);
    }

    /// #919: a declared hash is no proof of possession — a public upload never
    /// dedups onto someone's SIGNED asset (it would hand its id, and so its
    /// signed URLs, to whoever knows the hash).
    #[tokio::test]
    async fn a_public_upload_never_dedups_onto_a_signed_asset() {
        let mut fx = Fixture::new();
        fx.policy.dedup_enabled = true;
        let existing = fx.seed_ready_asset(TEST_HASH).await;
        let mut held = fx.assets.find_by_id(&existing).await.unwrap().unwrap();
        held.change_visibility(DeliveryVisibility::Signed, t0()).unwrap();
        fx.assets.save(&held).await.unwrap();

        let mut c = cmd();
        c.content_sha256 = Some(TEST_HASH.to_owned());
        let out = fx.issue_ticket_handler().handle(env(c), t0()).await.unwrap();
        assert!(!out.deduplicated);
        assert_ne!(out.asset_id, existing);
    }

    /// #919: the same idempotency key with another visibility is another
    /// upload (MED-1007), never the asset the key reserved first.
    #[tokio::test]
    async fn the_same_key_with_another_visibility_is_a_conflict() {
        let fx = Fixture::new();
        let first = fx.issue_ticket_handler().handle(env(keyed("vis-key-0001")), t0()).await.unwrap();
        let mut signed = keyed("vis-key-0001");
        signed.delivery_visibility = Some(DeliveryVisibility::Signed);
        let err = fx.issue_ticket_handler().handle(env(signed), t0()).await.unwrap_err();
        assert!(matches!(err, MediaError::UploadKeyConflict), "{err:?}");
        let again = fx.issue_ticket_handler().handle(env(keyed("vis-key-0001")), t0()).await.unwrap();
        assert_eq!(again.asset_id, first.asset_id, "the same request still replays");
    }

    /// #777: off by default, a private document cannot be uploaded at all —
    /// nothing reaches the bucket before the infra keeps `private/` private.
    #[tokio::test]
    async fn private_documents_are_refused_while_not_enabled() {
        let fx = Fixture::new();
        let mut c = cmd();
        c.kind = MediaKind::PrivateDocument;
        c.declared_mime = MimeType::new("application/pdf").unwrap();
        let off = IssueUploadTicketHandler::new(
            Arc::clone(&fx.assets) as _,
            Arc::clone(&fx.store) as _,
            crate::application::policy::MediaPolicy::standard(),
        );
        let err = off.handle(env(c.clone()), t0()).await.unwrap_err();
        assert!(matches!(err, MediaError::PrivateDocumentsDisabled), "{err:?}");
        assert!(fx.store.keys().is_empty() && fx.assets.find_by_content_hash(&ContentHash::new(TEST_HASH).unwrap()).await.unwrap().is_empty());
        // Enabled (as the fixture is), the same ticket is issued.
        assert!(fx.issue_ticket_handler().handle(env(c), t0()).await.is_ok());
    }

    // ── Idempotency keys (#876) ─────────────────────────────────────────────

    fn keyed(key: &str) -> IssueUploadTicketCommand {
        IssueUploadTicketCommand { idempotency_key: Some(key.to_owned()), ..cmd() }
    }

    #[tokio::test]
    async fn a_repeated_key_answers_the_same_pending_asset_with_a_fresh_upload() {
        let fx = Fixture::new();
        let handler = fx.issue_ticket_handler();

        let first = handler.handle(env(keyed("bubble-7:9f86d081884c7d65")), t0()).await.unwrap();
        let retry = handler.handle(env(keyed("bubble-7:9f86d081884c7d65")), t0()).await.unwrap();

        assert_eq!(retry.asset_id, first.asset_id, "no second asset");
        assert!(!retry.deduplicated, "still pending: the bytes may never have landed");
        let upload = retry.upload.expect("a fresh upload plan for the same asset");
        assert!(upload.presigned.url.contains(&first.asset_id.as_str()));
    }

    #[tokio::test]
    async fn a_repeated_key_past_pending_needs_no_upload() {
        let fx = Fixture::new();
        let handler = fx.issue_ticket_handler();
        let first = handler.handle(env(keyed("clip-3")), t0()).await.unwrap();
        fx.store.put_object(&crate::domain::value_object::StorageKey::staging(first.asset_id), 2_000_000, "etag-1");
        fx.commit_handler()
            .handle(
                env_commit(first.asset_id),
                t0(),
            )
            .await
            .unwrap();

        let retry = handler.handle(env(keyed("clip-3")), t0()).await.unwrap();
        assert_eq!(retry.asset_id, first.asset_id);
        assert!(retry.deduplicated);
        assert!(retry.upload.is_none(), "the bytes are in: no second upload");
    }

    #[tokio::test]
    async fn another_key_or_no_key_reserves_a_new_asset() {
        let fx = Fixture::new();
        let handler = fx.issue_ticket_handler();
        let a = handler.handle(env(keyed("k-1")), t0()).await.unwrap().asset_id;
        let b = handler.handle(env(keyed("k-2")), t0()).await.unwrap().asset_id;
        let c = handler.handle(env(cmd()), t0()).await.unwrap().asset_id;
        let d = handler.handle(env(cmd()), t0()).await.unwrap().asset_id;
        let mut other_owner = keyed("k-1");
        other_owner.owner_id = OwnerId::from_uuid(Uuid::from_u128(8));
        let e = handler.handle(env(other_owner), t0()).await.unwrap().asset_id;
        let ids: std::collections::HashSet<_> = [a, b, c, d, e].into_iter().collect();
        assert_eq!(ids.len(), 5);
    }

    #[tokio::test]
    async fn the_same_key_for_another_upload_is_refused() {
        let fx = Fixture::new();
        let handler = fx.issue_ticket_handler();
        handler.handle(env(keyed("k-1")), t0()).await.unwrap();

        let mut png = keyed("k-1");
        png.declared_mime = MimeType::new("image/png").unwrap();
        assert!(matches!(handler.handle(env(png), t0()).await, Err(MediaError::UploadKeyConflict)));
        let mut avatar = keyed("k-1");
        avatar.kind = MediaKind::Avatar;
        assert!(matches!(handler.handle(env(avatar), t0()).await, Err(MediaError::UploadKeyConflict)));
    }

    #[tokio::test]
    async fn an_aborted_upload_frees_its_key() {
        let fx = Fixture::new();
        let handler = fx.issue_ticket_handler();
        let first = handler.handle(env(keyed("k-1")), t0()).await.unwrap().asset_id;
        fx.delete_handler()
            .handle(
                Envelope::new(Uuid::now_v7(), crate::application::command::DeleteAssetCommand {
                    asset_id: first,
                    owner_id: OwnerId::from_uuid(Uuid::from_u128(7)),
                }),
                t0(),
            )
            .await
            .unwrap();

        let again = handler.handle(env(keyed("k-1")), t0()).await.unwrap();
        assert_ne!(again.asset_id, first);
        assert!(again.upload.is_some());
    }

    #[tokio::test]
    async fn a_malformed_key_is_refused() {
        let fx = Fixture::new();
        let err = fx.issue_ticket_handler().handle(env(keyed("has space")), t0()).await.unwrap_err();
        assert!(matches!(err, MediaError::InvalidUploadKey));
    }

    /// With dedup on, a READY match appearing after the first ticket does not
    /// orphan the key's reservation.
    #[tokio::test]
    async fn a_keyed_retry_answers_its_reservation_before_dedup() {
        let mut fx = Fixture::new();
        fx.policy.dedup_enabled = true;
        let mut first = keyed("k-1");
        first.content_sha256 = Some(TEST_HASH.to_owned());
        let reserved = fx.issue_ticket_handler().handle(env(first.clone()), t0()).await.unwrap();
        fx.seed_ready_asset(TEST_HASH).await;

        let retry = fx.issue_ticket_handler().handle(env(first), t0()).await.unwrap();
        assert_eq!(retry.asset_id, reserved.asset_id);
        assert!(retry.upload.is_some(), "still pending: upload it");
    }

    fn env_commit(asset_id: AssetId) -> Envelope<crate::application::command::CommitUploadCommand> {
        Envelope::new(Uuid::now_v7(), crate::application::command::CommitUploadCommand {
            asset_id,
            etag: None,
            content_sha256: None,
        })
    }
}
