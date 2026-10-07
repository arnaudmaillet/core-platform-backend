//! Family supervision storage (#670).
//!
//! Invites live on the shard of their code (found from the code alone); a
//! supervision lives on the **teen's** shard (where "at most two
//! supervisors" holds atomically, and coming of age is read), with a
//! reverse index on the supervisor's shard. Writes are idempotent and
//! ordered teen-first, so a retry completes a half-done pairing or ending.

use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, Utc};

use crate::domain::supervision::{InviteCode, Supervision, SupervisionInvite};
use crate::domain::value_object::{AccountId, AgeBracket};
use crate::error::AccountError;

/// Whether a pairing was made now or already existed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Linked {
    Created,
    Existing,
}

#[async_trait]
pub trait SupervisionStore: Send + Sync + 'static {
    async fn put_invite(&self, invite: &SupervisionInvite) -> Result<(), AccountError>;
    async fn find_invite(&self, code: &InviteCode) -> Result<Option<SupervisionInvite>, AccountError>;
    async fn delete_invite(&self, code: &InviteCode) -> Result<(), AccountError>;

    /// Pairs `link.teen` with `link.supervisor` (teen's shard, then the
    /// reverse index), unless the teen already has `max` supervisors
    /// ([`AccountError::SupervisorLimitReached`]). Idempotent.
    async fn link(&self, link: &Supervision, max: usize) -> Result<Linked, AccountError>;

    /// Ends the pairing; `false` when there was none. Idempotent.
    async fn unlink(&self, teen: &AccountId, supervisor: &AccountId) -> Result<bool, AccountError>;

    /// A teen's supervisors.
    async fn supervisors_of(&self, teen: &AccountId) -> Result<Vec<Supervision>, AccountError>;

    /// The teens a supervisor supervises (index entries the teen's shard no
    /// longer confirms are dropped).
    async fn teens_of(&self, supervisor: &AccountId) -> Result<Vec<Supervision>, AccountError>;

    /// The supervisions whose teen is 18 on `today` (up to `limit`).
    async fn came_of_age(&self, today: NaiveDate, limit: i64) -> Result<Vec<Supervision>, AccountError>;

    /// Drops the invites expired at `now`; returns how many.
    async fn purge_expired_invites(&self, now: DateTime<Utc>) -> Result<u64, AccountError>;
}

/// An account's age bracket on `today` (`None`: unknown, or no such account).
#[async_trait]
pub trait AccountAges: Send + Sync + 'static {
    async fn age_bracket(&self, account: &AccountId, today: NaiveDate) -> Result<Option<AgeBracket>, AccountError>;
}
