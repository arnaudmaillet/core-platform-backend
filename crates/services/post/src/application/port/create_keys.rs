use async_trait::async_trait;

use crate::domain::value_object::{IdempotencyKey, PostId, ProfileId};
use crate::error::PostError;

/// What a claim found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CreateClaim {
    /// Unused: the caller now holds it, for the post id it claimed with.
    Fresh,
    /// Already used: this is the post the first call created.
    Created(PostId),
    /// Another call holds it and has not finished.
    InFlight,
}

/// CreatePost's idempotency keys (#876), scoped to one author.
///
/// A key is claimed for a post id before the post is written (pending, for
/// 60 s), completed once it is stored (24 h), and released if the call fails
/// first, so the client's retry can create again. A pending claim whose call
/// died expires on its own.
#[async_trait]
pub trait CreateKeys: Send + Sync + 'static {
    /// Claims `key` for `post_id` unless it is already claimed or completed.
    async fn claim(&self, author: &ProfileId, key: &IdempotencyKey, post_id: PostId) -> Result<CreateClaim, PostError>;

    /// Marks the claim of `post_id` created. A no-op when the key no longer
    /// holds that claim.
    async fn complete(&self, author: &ProfileId, key: &IdempotencyKey, post_id: PostId) -> Result<(), PostError>;

    /// Drops the pending claim of `post_id`. A no-op when the key no longer
    /// holds that claim.
    async fn release(&self, author: &ProfileId, key: &IdempotencyKey, post_id: PostId) -> Result<(), PostError>;
}
