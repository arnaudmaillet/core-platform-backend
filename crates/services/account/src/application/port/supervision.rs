//! Family supervision storage (#670).
//!
//! Invites live on the shard of their code (found from the code alone); a
//! supervision lives on the **teen's** shard (where "at most two
//! supervisors" holds atomically, and coming of age is read), with a
//! reverse index on the supervisor's shard. Writes are idempotent and
//! ordered teen-first, so a retry completes a half-done pairing or ending.

use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, Utc};

use crate::domain::supervision::{InviteCode, LimitsRecord, Supervision, SupervisionInvite};
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

    /// Claims the invite for `acceptor`, atomically: `true` when it is
    /// unclaimed or already `acceptor`'s (a retry), `false` when another
    /// account claimed it (or it is gone).
    async fn claim_invite(&self, code: &InviteCode, acceptor: &AccountId) -> Result<bool, AccountError>;

    /// `account`'s failed accepts in the hour starting at `hour`.
    async fn failed_accepts(&self, account: &AccountId, hour: DateTime<Utc>) -> Result<i64, AccountError>;

    /// Counts one failed accept for `account` in the hour starting at `hour`.
    async fn record_failed_accept(&self, account: &AccountId, hour: DateTime<Utc>) -> Result<(), AccountError>;
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

    /// A teen's limits (on the teen's shard), if any.
    async fn limits(&self, teen: &AccountId) -> Result<Option<LimitsRecord>, AccountError>;

    /// Sets a teen's limits (replacing the previous ones: one shared set).
    async fn put_limits(&self, teen: &AccountId, record: &LimitsRecord) -> Result<(), AccountError>;

    /// Lifts a teen's limits; `false` when there were none.
    async fn clear_limits(&self, teen: &AccountId) -> Result<bool, AccountError>;

    /// Adds `minutes` to `account`'s time on `day` (its local day); returns
    /// the day's total.
    async fn add_usage(&self, account: &AccountId, day: NaiveDate, minutes: i32) -> Result<i32, AccountError>;

    /// Drops the invites expired at `now`; returns how many.
    async fn purge_expired_invites(&self, now: DateTime<Utc>) -> Result<u64, AccountError>;
}

/// An account's age bracket on `today` (`None`: unknown, or no such account).
#[async_trait]
pub trait AccountAges: Send + Sync + 'static {
    async fn age_bracket(&self, account: &AccountId, today: NaiveDate) -> Result<Option<AgeBracket>, AccountError>;
}
