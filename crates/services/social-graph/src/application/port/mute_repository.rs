use std::collections::HashSet;

use async_trait::async_trait;

use crate::domain::mute::{Mute, MuteScope, MuteScopes};
use crate::domain::value_object::ProfileId;
use crate::error::SocialGraphError;

/// At most this many mutes per muter are read for a feed filter.
pub const MAX_MUTES_READ: i32 = 5_000;

/// Persistence port for mutes (`social_graph.mutes`).
#[async_trait]
pub trait MuteRepository: Send + Sync + 'static {
    /// Records (or replaces) `muter`'s mute of `muted`.
    async fn upsert(&self, muter: &ProfileId, mute: &Mute) -> Result<(), SocialGraphError>;

    /// Removes `muter`'s mute of `muted` (absent is fine).
    async fn delete(&self, muter: &ProfileId, muted: &ProfileId) -> Result<(), SocialGraphError>;

    /// How `muter` mutes `muted`; the empty scopes when it does not.
    async fn scopes(&self, muter: &ProfileId, muted: &ProfileId) -> Result<MuteScopes, SocialGraphError>;

    /// `muter`'s mutes in profile-id order. The page token is the last
    /// profile id returned.
    async fn list(
        &self,
        muter: &ProfileId,
        limit: i32,
        page_token: Option<&str>,
    ) -> Result<(Vec<Mute>, Option<String>), SocialGraphError>;

    /// The profiles any of `muters` mutes for `scope` (up to
    /// [`MAX_MUTES_READ`] mutes per muter, in one query).
    async fn muted_by(&self, muters: &[ProfileId], scope: MuteScope) -> Result<HashSet<ProfileId>, SocialGraphError>;
}
