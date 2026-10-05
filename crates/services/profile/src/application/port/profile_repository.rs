use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::domain::aggregate::Profile;
use crate::domain::value_object::{AccountId, Handle, ProfileId};
use crate::error::ProfileError;

/// Days a released handle stays reserved before anyone may claim it again.
pub const HANDLE_RESERVATION_DAYS: i64 = 30;

/// Lightweight read model used by the `profiles_by_account` index table.
///
/// Deliberately excludes bio, website, custom_links, and masking details
/// to keep the listing payload lean for high-cardinality feed renders.
#[derive(Debug, Clone)]
pub struct ProfileSummary {
    pub profile_id: ProfileId,
    pub handle: String,
    pub display_name: String,
    pub avatar_url: Option<String>,
    pub profile_kind: String,
    pub visibility: String,
    pub status: String,
    pub created_at: DateTime<Utc>,
}

/// Persistence port for the Profile aggregate.
///
/// Three underlying ScyllaDB tables back this trait:
/// - `profile.profiles` — full aggregate, keyed by `profile_id`.
/// - `profile.profiles_by_account` — lightweight index for account-level listing.
/// - `profile.profile_handles` — globally unique handle → profile_id index with LWT semantics.
///
/// Implementations must enforce handle uniqueness via ScyllaDB LWT (`IF NOT EXISTS`) and
/// optimistic concurrency on the main table via LWT (`IF version = ?`).
#[async_trait]
pub trait ProfileRepository: Send + Sync + 'static {
    /// Persist the profile aggregate.
    ///
    /// Version == 0 triggers an INSERT; any other version triggers a LWT UPDATE.
    /// Returns [`ProfileError::ConcurrentModification`] when the LWT is not applied.
    async fn save(&self, profile: &Profile) -> Result<(), ProfileError>;

    async fn find_by_id(&self, id: &ProfileId) -> Result<Option<Profile>, ProfileError>;

    async fn find_by_handle(&self, handle: &Handle) -> Result<Option<Profile>, ProfileError>;

    /// Returns a page of lightweight summaries and the next page token.
    /// `page_token` is an opaque cursor; `None` starts from the beginning.
    async fn list_by_account(
        &self,
        account_id: &AccountId,
        limit: i32,
        page_token: Option<&str>,
    ) -> Result<(Vec<ProfileSummary>, Option<String>), ProfileError>;

    /// Attempts to atomically claim `handle` via ScyllaDB LWT: a free handle, or
    /// one released more than [`HANDLE_RESERVATION_DAYS`] ago and not held (its
    /// tombstone is taken over), or `profile_id`'s own released handle. Returns
    /// `false` if it is taken, held or still reserved.
    async fn claim_handle(
        &self,
        handle: &Handle,
        profile_id: ProfileId,
        account_id: AccountId,
    ) -> Result<bool, ProfileError>;

    /// Marks a handle as tombstoned (30-day reservation after release). A
    /// `held` handle — released by a verified profile — is never claimable again.
    async fn tombstone_handle(&self, handle: &Handle, held: bool) -> Result<(), ProfileError>;

    /// Undoes [`claim_handle`](Self::claim_handle) when what followed it failed:
    /// drops the claim only if `profile_id` still holds it (LWT).
    async fn release_handle_claim(&self, handle: &Handle, profile_id: ProfileId) -> Result<(), ProfileError>;

    /// Returns `true` if the handle is available for claiming by `claimant`
    /// (`None`: someone new).
    ///
    /// Available means: no row exists, OR the existing row is tombstoned, not
    /// held, and its 30-day reservation has expired, OR `claimant` is the
    /// profile that released it (a rename back, held or not).
    async fn handle_is_available(&self, handle: &Handle, claimant: Option<ProfileId>) -> Result<bool, ProfileError>;

    /// Writes a row into `profile.profiles_by_account` for the given profile.
    async fn save_account_index(&self, profile: &Profile) -> Result<(), ProfileError>;

    /// Removes the row from `profile.profiles_by_account`.
    async fn delete_account_index(&self, profile: &Profile) -> Result<(), ProfileError>;
}
