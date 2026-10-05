use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::domain::value_object::{
    NotificationId, NotificationKind, ProfileId, SubjectId, SubjectKind,
};

/// The Notification aggregate root.
///
/// Invariants enforced at creation time:
/// - sender and target must be distinct profiles (enforced in workers via BlockCache).
/// - `sample_sender_ids` is capped to `MAX_SAMPLE_SENDERS` (see config).
/// - `sender_count >= 1` always.
pub struct Notification {
    id:                NotificationId,
    target_profile_id: ProfileId,
    sender_profile_id: ProfileId,
    sample_sender_ids: Vec<Uuid>,
    sender_count:      i32,
    kind:              NotificationKind,
    subject_kind:      SubjectKind,
    subject_id:        SubjectId,
    created_at:        DateTime<Utc>,
    is_read:           bool,
}

impl Notification {
    /// Creates a new individual (non-collapsed) notification.
    ///
    /// `created_at` is supplied by the caller (the source event's timestamp) rather
    /// than read from the wall clock, so the value is deterministic across Kafka
    /// redeliveries — a prerequisite for the idempotent, deterministically-keyed
    /// `notifications_by_profile` INSERT.
    pub fn create(
        id:                NotificationId,
        target_profile_id: ProfileId,
        sender_profile_id: ProfileId,
        kind:              NotificationKind,
        subject_kind:      SubjectKind,
        subject_id:        SubjectId,
        created_at:        DateTime<Utc>,
    ) -> Self {
        Self {
            id,
            target_profile_id,
            sender_profile_id,
            sample_sender_ids: vec![sender_profile_id.as_uuid()],
            sender_count: 1,
            kind,
            subject_kind,
            subject_id,
            created_at,
            is_read: false,
        }
    }

    /// A notice from the platform itself (an appeal's outcome, #744): no
    /// sender — the nil profile id, no sample senders, a count of 0.
    pub fn from_platform(
        id:                NotificationId,
        target_profile_id: ProfileId,
        kind:              NotificationKind,
        subject_kind:      SubjectKind,
        subject_id:        SubjectId,
        created_at:        DateTime<Utc>,
    ) -> Self {
        Self {
            id,
            target_profile_id,
            sender_profile_id: ProfileId::from_uuid(Uuid::nil()),
            sample_sender_ids: Vec::new(),
            sender_count: 0,
            kind,
            subject_kind,
            subject_id,
            created_at,
            is_read: false,
        }
    }

    /// Reconstitutes a collapsed notification produced by the CollapseFlushWorker.
    /// `sender_profile_id` is the most-recent sender in the window.
    #[allow(clippy::too_many_arguments)] // aggregate/worker constructor — same precedent as chat
    pub fn create_collapsed(
        id:                NotificationId,
        target_profile_id: ProfileId,
        sender_profile_id: ProfileId,
        sample_sender_ids: Vec<Uuid>,
        sender_count:      i32,
        kind:              NotificationKind,
        subject_kind:      SubjectKind,
        subject_id:        SubjectId,
        created_at:        DateTime<Utc>,
    ) -> Self {
        Self {
            id,
            target_profile_id,
            sender_profile_id,
            sample_sender_ids,
            sender_count,
            kind,
            subject_kind,
            subject_id,
            created_at,
            is_read: false,
        }
    }

    /// Reconstitutes an aggregate from a ScyllaDB row (for query projections).
    #[allow(clippy::too_many_arguments)] // aggregate/worker constructor — same precedent as chat
    pub fn reconstitute(
        id:                NotificationId,
        target_profile_id: ProfileId,
        sender_profile_id: ProfileId,
        sample_sender_ids: Vec<Uuid>,
        sender_count:      i32,
        kind:              NotificationKind,
        subject_kind:      SubjectKind,
        subject_id:        SubjectId,
        created_at:        DateTime<Utc>,
        is_read:           bool,
    ) -> Self {
        Self {
            id,
            target_profile_id,
            sender_profile_id,
            sample_sender_ids,
            sender_count,
            kind,
            subject_kind,
            subject_id,
            created_at,
            is_read,
        }
    }

    pub fn id(&self)                -> NotificationId  { self.id }
    pub fn target_profile_id(&self) -> ProfileId       { self.target_profile_id }
    pub fn sender_profile_id(&self) -> ProfileId       { self.sender_profile_id }
    pub fn sample_sender_ids(&self) -> &[Uuid]         { &self.sample_sender_ids }
    pub fn sender_count(&self)      -> i32             { self.sender_count }
    pub fn kind(&self)              -> NotificationKind { self.kind }
    pub fn subject_kind(&self)      -> SubjectKind     { self.subject_kind }
    pub fn subject_id(&self)        -> SubjectId       { self.subject_id }
    pub fn created_at(&self)        -> DateTime<Utc>   { self.created_at }
    pub fn is_read(&self)           -> bool            { self.is_read }
}
