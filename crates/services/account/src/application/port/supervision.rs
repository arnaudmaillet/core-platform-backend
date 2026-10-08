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

    /// `account`'s last `days` days with time counted, most recent first.
    async fn recent_usage(&self, account: &AccountId, days: i64) -> Result<Vec<(NaiveDate, i32)>, AccountError>;

    /// Drops the invites expired at `now`; returns how many.
    async fn purge_expired_invites(&self, now: DateTime<Utc>) -> Result<u64, AccountError>;
}

/// An account's age bracket on `today` (`None`: unknown, or no such account).
#[async_trait]
pub trait AccountAges: Send + Sync + 'static {
    async fn age_bracket(&self, account: &AccountId, today: NaiveDate) -> Result<Option<AgeBracket>, AccountError>;
}

/// Which of a teen's profile connections a supervisor looks at (#670 part 3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionKind {
    /// The profiles it follows.
    Following,
    /// The profiles following it.
    Followers,
    /// The profiles it blocked.
    Blocked,
}

/// One profile on the other end of a connection, and since when.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Connection {
    pub profile_id: String,
    pub since:      Option<DateTime<Utc>>,
}

/// What became of a report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReportOutcome {
    UnderReview,
    ActionTaken,
    NoViolation,
}

/// A report a teen made: who or what, when, and the decision — never the
/// teen's own words.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReportSummary {
    /// `post`, `comment`, `chat_message`, `media`, `account` or `profile`.
    pub entity_type: String,
    pub entity_id:   String,
    /// The policy category chosen (`spam`, `harassment`, …).
    pub category:    String,
    pub outcome:     ReportOutcome,
    pub reported_at: Option<DateTime<Utc>>,
}

/// A page of a listing; `next_page_token` is `None` on the last.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivityPage<T> {
    pub items:           Vec<T>,
    pub next_page_token: Option<String>,
}

/// A teen's activity elsewhere in the fleet, for their supervisors (#670
/// part 3): social-graph connections and moderation reports, read as the
/// mesh. Failures are [`AccountError::SupervisionActivityUnavailable`].
#[async_trait]
pub trait SupervisedActivity: Send + Sync + 'static {
    async fn connections(
        &self,
        profile_id: &str,
        kind: ConnectionKind,
        limit: u32,
        page_token: &str,
    ) -> Result<ActivityPage<Connection>, AccountError>;

    /// The reporter's reports, never a self-harm / CSAM / NCII one, nor one
    /// about content of `hidden_accounts` (the teen's supervisors).
    async fn reports(
        &self,
        reporter: &AccountId,
        hidden_accounts: &[AccountId],
        limit: u32,
        page_token: &str,
    ) -> Result<ActivityPage<ReportSummary>, AccountError>;
}
