use std::collections::BTreeSet;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::domain::event::{
    AssetDeleted, AssetFailed, AssetQuarantined, AssetReady, AssetRestored, AssetUploaded,
    AssetVariantReady, DomainEvent,
};
use crate::domain::value_object::{
    AssetId, AssetState, Blurhash, ContentHash, Dimensions, MediaKind, MimeType, OwnerId,
    RenditionKind, StorageKey, UploadConstraints,
};
use crate::error::MediaError;

/// One derivative in an asset's catalog. A content-addressed object in the byte
/// store; callers receive a delivery URL for its `storage_key`, never the bytes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Rendition {
    kind: RenditionKind,
    mime_type: MimeType,
    storage_key: StorageKey,
    dimensions: Dimensions,
    byte_size: u64,
}

impl Rendition {
    pub fn new(
        kind: RenditionKind,
        mime_type: MimeType,
        storage_key: StorageKey,
        dimensions: Dimensions,
        byte_size: u64,
    ) -> Self {
        Self {
            kind,
            mime_type,
            storage_key,
            dimensions,
            byte_size,
        }
    }

    pub fn kind(&self) -> RenditionKind {
        self.kind
    }
    pub fn mime_type(&self) -> &MimeType {
        &self.mime_type
    }
    pub fn storage_key(&self) -> &StorageKey {
        &self.storage_key
    }
    pub fn dimensions(&self) -> Dimensions {
        self.dimensions
    }
    pub fn byte_size(&self) -> u64 {
        self.byte_size
    }
}

/// Inputs to reserve a new asset (issued at upload-ticket time).
#[derive(Debug, Clone)]
pub struct ReserveParams {
    pub id: AssetId,
    pub owner_id: OwnerId,
    pub kind: MediaKind,
    pub declared_mime: MimeType,
    pub declared_size: u64,
}

/// The verified facts established when an upload is finalized (from the server-side
/// probe — never the client's declarations).
#[derive(Debug, Clone)]
pub struct FinalizeParams {
    pub mime_type: MimeType,
    pub byte_size: u64,
    /// `None` for a document (a PDF has none).
    pub dimensions: Option<Dimensions>,
    pub content_hash: ContentHash,
}

/// How long a READY private document lives when no decision moves its purge
/// (#777): an upload never attached to a request, or a request never decided.
pub const PRIVATE_DOCUMENT_BACKSTOP: chrono::Duration = chrono::Duration::days(90);

/// A full snapshot for reconstructing an asset from storage (no events emitted).
#[derive(Debug, Clone)]
pub struct AssetSnapshot {
    pub id: AssetId,
    pub owner_id: OwnerId,
    pub kind: MediaKind,
    pub state: AssetState,
    pub declared_mime: MimeType,
    pub declared_size: u64,
    pub mime_type: Option<MimeType>,
    pub byte_size: Option<u64>,
    pub dimensions: Option<Dimensions>,
    pub content_hash: Option<ContentHash>,
    pub blurhash: Option<Blurhash>,
    pub renditions: Vec<Rendition>,
    pub legal_hold: bool,
    pub prior_state: Option<AssetState>,
    pub enforcements: BTreeSet<String>,
    /// When a private document is purged (#777); `None` for other kinds.
    pub purge_after: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// The **Asset** aggregate — the lifecycle-bearing System-of-Record for one piece
/// of media. It owns the state machine and the rendition catalog; the bytes live
/// in object storage, keyed by the content-addressed [`StorageKey`].
///
/// # Invariants (enforced here)
/// 1. Reservation rejects an upload that violates the kind's constraints (MIME
///    allowlist / size ceiling) before any byte is accepted.
/// 2. The forward path is `Pending → Uploaded → Processing → Ready`, with
///    `Uploaded`/`Processing → Failed`; illegal jumps are rejected.
/// 3. `mark_ready` requires a content hash and the `Original` rendition — a READY
///    asset is always resolvable.
/// 4. Quarantine/restore are reversible and cross-state; quarantine remembers the
///    prior state so restore returns to it.
/// 5. A legal hold blocks hard-delete (compliance preservation overrides erasure).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Asset {
    id: AssetId,
    owner_id: OwnerId,
    kind: MediaKind,
    state: AssetState,
    declared_mime: MimeType,
    declared_size: u64,
    mime_type: Option<MimeType>,
    byte_size: Option<u64>,
    dimensions: Option<Dimensions>,
    content_hash: Option<ContentHash>,
    blurhash: Option<Blurhash>,
    renditions: Vec<Rendition>,
    legal_hold: bool,
    prior_state: Option<AssetState>,
    /// The moderation enforcements this asset's quarantine answers to (ids);
    /// empty for a screen block. A reversal lifts only its own.
    #[serde(default)]
    enforcements: BTreeSet<String>,
    /// A private document's purge time (#777): a backstop from READY, moved to
    /// the decision + retention once its verification request is decided.
    #[serde(default)]
    purge_after: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,

    #[serde(skip)]
    pending_events: Vec<DomainEvent>,
}

impl Asset {
    /// Invariant 1: reserves a `Pending` asset after checking the declared MIME and
    /// size against the kind's constraints. No event — reservation is internal;
    /// `AssetUploaded` fires on finalize.
    pub fn reserve(
        params: ReserveParams,
        constraints: &UploadConstraints,
        now: DateTime<Utc>,
    ) -> Result<Self, MediaError> {
        constraints.validate(&params.declared_mime, params.declared_size)?;
        Ok(Self {
            id: params.id,
            owner_id: params.owner_id,
            kind: params.kind,
            state: AssetState::Pending,
            declared_mime: params.declared_mime,
            declared_size: params.declared_size,
            mime_type: None,
            byte_size: None,
            dimensions: None,
            content_hash: None,
            blurhash: None,
            renditions: Vec::new(),
            legal_hold: false,
            prior_state: None,
            enforcements: BTreeSet::new(),
            purge_after: None,
            created_at: now,
            updated_at: now,
            pending_events: Vec::new(),
        })
    }

    /// Reconstructs from storage (no events emitted).
    pub fn reconstitute(s: AssetSnapshot) -> Self {
        Self {
            id: s.id,
            owner_id: s.owner_id,
            kind: s.kind,
            state: s.state,
            declared_mime: s.declared_mime,
            declared_size: s.declared_size,
            mime_type: s.mime_type,
            byte_size: s.byte_size,
            dimensions: s.dimensions,
            content_hash: s.content_hash,
            blurhash: s.blurhash,
            renditions: s.renditions,
            legal_hold: s.legal_hold,
            prior_state: s.prior_state,
            enforcements: s.enforcements,
            purge_after: s.purge_after,
            created_at: s.created_at,
            updated_at: s.updated_at,
            pending_events: Vec::new(),
        }
    }

    // ─── Queries ─────────────────────────────────────────────────────────────

    pub fn id(&self) -> AssetId {
        self.id
    }
    pub fn owner_id(&self) -> OwnerId {
        self.owner_id
    }
    pub fn kind(&self) -> MediaKind {
        self.kind
    }
    pub fn state(&self) -> AssetState {
        self.state
    }
    /// The client-declared MIME from reservation (used to cross-check the probe).
    pub fn declared_mime(&self) -> &MimeType {
        &self.declared_mime
    }
    pub fn mime_type(&self) -> Option<&MimeType> {
        self.mime_type.as_ref()
    }
    pub fn byte_size(&self) -> Option<u64> {
        self.byte_size
    }
    pub fn dimensions(&self) -> Option<Dimensions> {
        self.dimensions
    }
    pub fn content_hash(&self) -> Option<&ContentHash> {
        self.content_hash.as_ref()
    }
    pub fn blurhash(&self) -> Option<&Blurhash> {
        self.blurhash.as_ref()
    }
    pub fn renditions(&self) -> &[Rendition] {
        &self.renditions
    }
    pub fn rendition(&self, kind: RenditionKind) -> Option<&Rendition> {
        self.renditions.iter().find(|r| r.kind() == kind)
    }
    pub fn legal_hold(&self) -> bool {
        self.legal_hold
    }
    pub fn is_deliverable(&self) -> bool {
        self.state.is_deliverable()
    }
    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }
    pub fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at
    }

    /// A private document's purge time (#777).
    pub fn purge_after(&self) -> Option<DateTime<Utc>> {
        self.purge_after
    }

    // ─── Commands ────────────────────────────────────────────────────────────

    /// A private document's verification request was decided (#777): it is
    /// purged at `at` (the decision + the retention). Other kinds refuse.
    pub fn schedule_purge(&mut self, at: DateTime<Utc>, now: DateTime<Utc>) -> Result<(), MediaError> {
        if !self.kind.is_private() {
            return Err(MediaError::DomainViolation {
                field:   "purge_after".into(),
                message: "only a private document is purged on schedule".into(),
            });
        }
        self.purge_after = Some(at);
        self.updated_at = now;
        Ok(())
    }

    /// Invariant 2: finalizes a pending upload with the server-verified facts,
    /// re-checking the *actual* MIME/size against the constraints, then transitions
    /// to `Uploaded` and emits [`AssetUploaded`].
    pub fn finalize(
        &mut self,
        params: FinalizeParams,
        constraints: &UploadConstraints,
        now: DateTime<Utc>,
    ) -> Result<(), MediaError> {
        self.ensure_state(AssetState::Pending, AssetState::Uploaded)?;
        constraints.validate(&params.mime_type, params.byte_size)?;

        self.mime_type = Some(params.mime_type);
        self.byte_size = Some(params.byte_size);
        self.dimensions = params.dimensions;
        self.content_hash = Some(params.content_hash.clone());
        self.transition(AssetState::Uploaded, now);

        self.emit(DomainEvent::AssetUploaded(AssetUploaded {
            asset_id: self.id,
            owner_id: self.owner_id,
            kind: self.kind,
            content_hash: params.content_hash,
            byte_size: params.byte_size,
            occurred_at: now,
        }));
        Ok(())
    }

    /// Invariant 2: enters the transformation pipeline. No event — internal.
    pub fn begin_processing(&mut self, now: DateTime<Utc>) -> Result<(), MediaError> {
        self.ensure_state(AssetState::Uploaded, AssetState::Processing)?;
        self.transition(AssetState::Processing, now);
        Ok(())
    }

    /// Records the progressive-render placeholder. Allowed while the bytes exist but
    /// the asset is not yet READY (Uploaded/Processing).
    pub fn set_blurhash(&mut self, blurhash: Blurhash, now: DateTime<Utc>) -> Result<(), MediaError> {
        if !matches!(self.state, AssetState::Uploaded | AssetState::Processing) {
            return Err(MediaError::DomainViolation {
                field: "blurhash".into(),
                message: format!("cannot set a blurhash while '{}'", self.state),
            });
        }
        self.blurhash = Some(blurhash);
        self.updated_at = now;
        Ok(())
    }

    /// Adds a derivative to the catalog (during processing) and emits
    /// [`AssetVariantReady`]. Rejects a duplicate rendition kind.
    pub fn attach_rendition(&mut self, rendition: Rendition, now: DateTime<Utc>) -> Result<(), MediaError> {
        if self.state != AssetState::Processing {
            return Err(MediaError::InvalidStateTransition {
                from: self.state.to_string(),
                to: "attach_rendition(processing)".into(),
            });
        }
        if self.rendition(rendition.kind()).is_some() {
            return Err(MediaError::DomainViolation {
                field: "rendition".into(),
                message: format!("a '{}' rendition already exists", rendition.kind()),
            });
        }
        let kind = rendition.kind();
        self.renditions.push(rendition);
        self.updated_at = now;
        self.emit(DomainEvent::AssetVariantReady(AssetVariantReady {
            asset_id: self.id,
            owner_id: self.owner_id,
            rendition: kind,
            occurred_at: now,
        }));
        Ok(())
    }

    /// Invariant 3: completes processing. Requires a content hash and the
    /// `Original` rendition, then transitions to `Ready` and emits [`AssetReady`].
    pub fn mark_ready(&mut self, now: DateTime<Utc>) -> Result<(), MediaError> {
        self.ensure_state(AssetState::Processing, AssetState::Ready)?;
        if self.content_hash.is_none() {
            return Err(MediaError::DomainViolation {
                field: "content_hash".into(),
                message: "cannot mark ready without a finalized content hash".into(),
            });
        }
        // The master rendition a READY asset must carry: images keep the validated
        // Original; video carries the playback Manifest (its HLS entry point); a
        // private document has none (its bytes live at its private key).
        let master = match self.kind {
            MediaKind::Video => Some(RenditionKind::Manifest),
            MediaKind::PrivateDocument => None,
            _ => Some(RenditionKind::Original),
        };
        if let Some(master) = master
            && self.rendition(master).is_none()
        {
            return Err(MediaError::DomainViolation {
                field: "renditions".into(),
                message: format!("cannot mark ready without the {master} rendition"),
            });
        }
        self.transition(AssetState::Ready, now);
        if self.kind.is_private() {
            // An orphan (never attached, or never decided) still goes.
            self.purge_after = Some(now + PRIVATE_DOCUMENT_BACKSTOP);
        }
        self.emit(DomainEvent::AssetReady(AssetReady {
            asset_id: self.id,
            owner_id: self.owner_id,
            occurred_at: now,
        }));
        Ok(())
    }

    /// Invariant 2: fails processing terminally and emits [`AssetFailed`].
    pub fn mark_failed(&mut self, reason: impl Into<String>, now: DateTime<Utc>) -> Result<(), MediaError> {
        if !matches!(self.state, AssetState::Uploaded | AssetState::Processing) {
            return Err(MediaError::InvalidStateTransition {
                from: self.state.to_string(),
                to: AssetState::Failed.to_string(),
            });
        }
        self.transition(AssetState::Failed, now);
        self.emit(DomainEvent::AssetFailed(AssetFailed {
            asset_id: self.id,
            owner_id: self.owner_id,
            reason: reason.into(),
            occurred_at: now,
        }));
        Ok(())
    }

    /// Invariant 4: revokes delivery (moderation takedown / compliance hold),
    /// remembering the prior state for a later restore. Idempotent: re-quarantining
    /// an already-quarantined asset is a no-op `Ok`. Rejected on a deleted asset.
    pub fn quarantine(&mut self, now: DateTime<Utc>) -> Result<(), MediaError> {
        match self.state {
            AssetState::Quarantined => return Ok(()),
            AssetState::Deleted => {
                return Err(MediaError::InvalidStateTransition {
                    from: self.state.to_string(),
                    to: AssetState::Quarantined.to_string(),
                });
            }
            _ => {}
        }
        self.prior_state = Some(self.state);
        self.transition(AssetState::Quarantined, now);
        self.emit(DomainEvent::AssetQuarantined(AssetQuarantined {
            asset_id: self.id,
            owner_id: self.owner_id,
            occurred_at: now,
        }));
        Ok(())
    }

    /// Records that moderation enforcement `id` covers this (quarantined) asset.
    pub fn cover(&mut self, enforcement_id: &str, now: DateTime<Utc>) {
        if self.enforcements.insert(enforcement_id.to_owned()) {
            self.updated_at = now;
        }
    }

    /// Lifts enforcement `id`; `true` if others still cover the asset.
    pub fn uncover(&mut self, enforcement_id: &str, now: DateTime<Utc>) -> bool {
        if self.enforcements.remove(enforcement_id) {
            self.updated_at = now;
        }
        !self.enforcements.is_empty()
    }

    /// The moderation enforcements this asset's quarantine answers to.
    pub fn enforcements(&self) -> &BTreeSet<String> {
        &self.enforcements
    }

    /// Invariant 4: reinstates a quarantined asset to its prior state (defaulting to
    /// `Ready`) and emits [`AssetRestored`].
    pub fn restore(&mut self, now: DateTime<Utc>) -> Result<(), MediaError> {
        if self.state != AssetState::Quarantined {
            return Err(MediaError::InvalidStateTransition {
                from: self.state.to_string(),
                to: "restore".into(),
            });
        }
        let target = self.prior_state.take().unwrap_or(AssetState::Ready);
        self.enforcements.clear();
        self.transition(target, now);
        self.emit(DomainEvent::AssetRestored(AssetRestored {
            asset_id: self.id,
            owner_id: self.owner_id,
            occurred_at: now,
        }));
        Ok(())
    }

    /// Places a legal hold (e.g. CSAM evidence preservation). Blocks deletion. No
    /// event — it is an internal compliance flag, not a lifecycle fact.
    pub fn place_legal_hold(&mut self, now: DateTime<Utc>) {
        self.legal_hold = true;
        self.updated_at = now;
    }

    /// Lifts a legal hold.
    pub fn lift_legal_hold(&mut self, now: DateTime<Utc>) {
        self.legal_hold = false;
        self.updated_at = now;
    }

    /// Invariant 5: hard-deletes the asset. Refused while a legal hold is active
    /// (`LegalHoldActive`, MED-7003) — compliance preservation overrides erasure.
    /// Idempotent: deleting an already-deleted asset is a no-op `Ok`.
    pub fn delete(&mut self, now: DateTime<Utc>) -> Result<(), MediaError> {
        if self.legal_hold {
            return Err(MediaError::LegalHoldActive);
        }
        if self.state == AssetState::Deleted {
            return Ok(());
        }
        self.transition(AssetState::Deleted, now);
        self.emit(DomainEvent::AssetDeleted(AssetDeleted {
            asset_id: self.id,
            owner_id: self.owner_id,
            occurred_at: now,
        }));
        Ok(())
    }

    /// Drains accumulated events for the unit-of-work to publish.
    pub fn drain_events(&mut self) -> Vec<DomainEvent> {
        std::mem::take(&mut self.pending_events)
    }

    // ─── Internals ───────────────────────────────────────────────────────────

    /// Guards a forward-path transition (Invariant 2).
    fn ensure_state(&self, expected: AssetState, target: AssetState) -> Result<(), MediaError> {
        if self.state != expected {
            return Err(MediaError::InvalidStateTransition {
                from: self.state.to_string(),
                to: target.to_string(),
            });
        }
        Ok(())
    }

    fn transition(&mut self, next: AssetState, now: DateTime<Utc>) {
        self.state = next;
        self.updated_at = now;
    }

    fn emit(&mut self, event: DomainEvent) {
        self.pending_events.push(event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn t0() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-06-26T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    fn constraints() -> UploadConstraints {
        UploadConstraints::for_kind(MediaKind::PostImage)
    }

    fn hash() -> ContentHash {
        ContentHash::new("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855").unwrap()
    }

    fn reserved() -> Asset {
        Asset::reserve(
            ReserveParams {
                id: AssetId::from_uuid(Uuid::from_u128(1)),
                owner_id: OwnerId::from_uuid(Uuid::from_u128(9)),
                kind: MediaKind::PostImage,
                declared_mime: MimeType::new("image/jpeg").unwrap(),
                declared_size: 2_000_000,
            },
            &constraints(),
            t0(),
        )
        .unwrap()
    }

    fn finalize_params() -> FinalizeParams {
        FinalizeParams {
            mime_type: MimeType::new("image/jpeg").unwrap(),
            byte_size: 2_000_000,
            dimensions: Some(Dimensions::new(1920, 1080).unwrap()),
            content_hash: hash(),
        }
    }

    fn original_rendition() -> Rendition {
        Rendition::new(
            RenditionKind::Original,
            MimeType::new("image/jpeg").unwrap(),
            StorageKey::rendition(MediaKind::PostImage, &hash(), RenditionKind::Original, "jpg"),
            Dimensions::new(1920, 1080).unwrap(),
            2_000_000,
        )
    }

    /// Drives an asset all the way to READY and returns it.
    fn ready_asset() -> Asset {
        let mut a = reserved();
        a.finalize(finalize_params(), &constraints(), t0()).unwrap();
        a.begin_processing(t0()).unwrap();
        a.attach_rendition(original_rendition(), t0()).unwrap();
        a.mark_ready(t0()).unwrap();
        a.drain_events();
        a
    }

    #[test]
    fn reserve_rejects_oversize_and_off_allowlist() {
        let oversize = Asset::reserve(
            ReserveParams {
                id: AssetId::new(),
                owner_id: OwnerId::from_uuid(Uuid::nil()),
                kind: MediaKind::Avatar,
                declared_mime: MimeType::new("image/png").unwrap(),
                declared_size: MediaKind::Avatar.max_bytes() + 1,
            },
            &UploadConstraints::for_kind(MediaKind::Avatar),
            t0(),
        );
        assert!(matches!(oversize.unwrap_err(), MediaError::UploadSizeExceeded { .. }));

        let bad_mime = Asset::reserve(
            ReserveParams {
                id: AssetId::new(),
                owner_id: OwnerId::from_uuid(Uuid::nil()),
                kind: MediaKind::Avatar,
                declared_mime: MimeType::new("application/zip").unwrap(),
                declared_size: 10,
            },
            &UploadConstraints::for_kind(MediaKind::Avatar),
            t0(),
        );
        assert!(matches!(bad_mime.unwrap_err(), MediaError::UnsupportedMimeType { .. }));
    }

    #[test]
    fn happy_path_emits_uploaded_variant_and_ready_in_order() {
        let mut a = reserved();
        assert_eq!(a.state(), AssetState::Pending);

        a.finalize(finalize_params(), &constraints(), t0()).unwrap();
        assert_eq!(a.state(), AssetState::Uploaded);
        assert_eq!(a.content_hash(), Some(&hash()));

        a.begin_processing(t0()).unwrap();
        a.set_blurhash(Blurhash::new("LEHV6nWB2yk8pyo0adR*").unwrap(), t0()).unwrap();
        a.attach_rendition(original_rendition(), t0()).unwrap();
        a.mark_ready(t0()).unwrap();
        assert!(a.is_deliverable());

        let events = a.drain_events();
        let kinds: Vec<&str> = events.iter().map(|e| e.event_type()).collect();
        assert_eq!(
            kinds,
            ["media.asset_uploaded", "media.asset_variant_ready", "media.asset_ready"]
        );
        assert_eq!(events[0].asset_id(), a.id());
        assert!(a.drain_events().is_empty(), "events drain once");
    }

    #[test]
    fn finalize_is_rejected_from_a_non_pending_state() {
        let mut a = ready_asset();
        assert!(matches!(
            a.finalize(finalize_params(), &constraints(), t0()).unwrap_err(),
            MediaError::InvalidStateTransition { .. }
        ));
    }

    #[test]
    fn finalize_revalidates_actual_size_against_the_ceiling() {
        let mut a = reserved();
        let mut p = finalize_params();
        p.byte_size = MediaKind::PostImage.max_bytes() + 1; // probe found it bigger than declared
        assert!(matches!(
            a.finalize(p, &constraints(), t0()).unwrap_err(),
            MediaError::UploadSizeExceeded { .. }
        ));
        assert_eq!(a.state(), AssetState::Pending, "rejected finalize leaves state untouched");
    }

    #[test]
    fn mark_ready_requires_the_original_rendition() {
        let mut a = reserved();
        a.finalize(finalize_params(), &constraints(), t0()).unwrap();
        a.begin_processing(t0()).unwrap();
        // Only a thumbnail attached — no original.
        let thumb = Rendition::new(
            RenditionKind::Thumbnail,
            MimeType::new("image/webp").unwrap(),
            StorageKey::rendition(MediaKind::PostImage, &hash(), RenditionKind::Thumbnail, "webp"),
            Dimensions::new(320, 180).unwrap(),
            20_000,
        );
        a.attach_rendition(thumb, t0()).unwrap();
        assert!(matches!(
            a.mark_ready(t0()).unwrap_err(),
            MediaError::DomainViolation { .. }
        ));
    }

    #[test]
    fn duplicate_rendition_kind_is_rejected() {
        let mut a = reserved();
        a.finalize(finalize_params(), &constraints(), t0()).unwrap();
        a.begin_processing(t0()).unwrap();
        a.attach_rendition(original_rendition(), t0()).unwrap();
        assert!(matches!(
            a.attach_rendition(original_rendition(), t0()).unwrap_err(),
            MediaError::DomainViolation { .. }
        ));
    }

    #[test]
    fn quarantine_remembers_prior_state_and_restore_returns_to_it() {
        let mut a = ready_asset();
        a.quarantine(t0()).unwrap();
        assert_eq!(a.state(), AssetState::Quarantined);
        assert!(!a.is_deliverable());
        assert_eq!(a.drain_events().len(), 1, "first quarantine emits one event");

        // Idempotent re-quarantine: no extra event.
        a.quarantine(t0()).unwrap();
        assert!(a.drain_events().is_empty());

        a.restore(t0()).unwrap();
        assert_eq!(a.state(), AssetState::Ready, "restored to the pre-quarantine state");
        let events = a.drain_events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type(), "media.asset_restored");
    }

    #[test]
    fn quarantine_then_delete_is_allowed_but_legal_hold_blocks_delete() {
        let mut a = ready_asset();
        a.place_legal_hold(t0());
        a.quarantine(t0()).unwrap();
        // Legal hold blocks erasure even though it is quarantined.
        assert!(matches!(a.delete(t0()).unwrap_err(), MediaError::LegalHoldActive));

        // Lift the hold → delete succeeds and is idempotent.
        a.lift_legal_hold(t0());
        a.drain_events();
        a.delete(t0()).unwrap();
        assert_eq!(a.state(), AssetState::Deleted);
        let events = a.drain_events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type(), "media.asset_deleted");
        // Idempotent second delete: no event.
        a.delete(t0()).unwrap();
        assert!(a.drain_events().is_empty());
    }

    #[test]
    fn cannot_quarantine_a_deleted_asset() {
        let mut a = ready_asset();
        a.delete(t0()).unwrap();
        a.drain_events();
        assert!(matches!(
            a.quarantine(t0()).unwrap_err(),
            MediaError::InvalidStateTransition { .. }
        ));
    }

    #[test]
    fn failed_processing_is_terminal_for_the_forward_path() {
        let mut a = reserved();
        a.finalize(finalize_params(), &constraints(), t0()).unwrap();
        a.begin_processing(t0()).unwrap();
        a.mark_failed("unsupported codec", t0()).unwrap();
        assert_eq!(a.state(), AssetState::Failed);
        assert!(matches!(
            a.mark_ready(t0()).unwrap_err(),
            MediaError::InvalidStateTransition { .. }
        ));
        let events = a.drain_events();
        assert_eq!(events.last().unwrap().event_type(), "media.asset_failed");
    }

    #[test]
    fn snapshot_round_trips_through_reconstitute() {
        let a = ready_asset();
        let snapshot = AssetSnapshot {
            id: a.id(),
            owner_id: a.owner_id(),
            kind: a.kind(),
            state: a.state(),
            declared_mime: MimeType::new("image/jpeg").unwrap(),
            declared_size: 2_000_000,
            mime_type: a.mime_type().cloned(),
            byte_size: a.byte_size(),
            dimensions: a.dimensions(),
            content_hash: a.content_hash().cloned(),
            blurhash: a.blurhash().cloned(),
            renditions: a.renditions().to_vec(),
            legal_hold: a.legal_hold(),
            prior_state: None,
            enforcements: Default::default(),
            purge_after: None,
            created_at: a.created_at(),
            updated_at: a.updated_at(),
        };
        let mut restored = Asset::reconstitute(snapshot);
        assert_eq!(restored.state(), AssetState::Ready);
        assert_eq!(restored.content_hash(), Some(&hash()));
        assert!(restored.drain_events().is_empty(), "reconstitute emits nothing");
    }
}
