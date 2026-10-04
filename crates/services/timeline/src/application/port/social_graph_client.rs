use std::collections::HashMap;

use async_trait::async_trait;

use crate::domain::value_object::{AuthorId, ContentAccess, ProfileId};
use crate::error::TimelineError;

/// Port for cross-service communication with services/social-graph via gRPC.
///
/// Used in two scenarios:
///   1. `PostPublishedWorker`: fetches all followers of a regular author to
///      perform fan-out writes to Redis + ScyllaDB.
///   2. `GetFollowingFeedQuery` cold-start: fetches all followings of a profile
///      to rebuild `timeline:following:{profile_id}` in Redis.
///
/// Both operations paginate through the social-graph `ListFollowers` /
/// `ListFollowing` RPCs internally; callers receive the full flattened list.
/// This is acceptable because:
///   - Fan-out (followers) is a background operation with no latency SLA.
///   - Cold-start rebuild (following) is a one-time O(1) event per eviction.
#[async_trait]
pub trait SocialGraphClient: Send + Sync + 'static {
    /// Returns the complete list of followers for `author_id`.
    ///
    /// Paginates through `ListFollowers` RPCs until `next_page_token` is empty.
    /// `page_size` controls the number of items per gRPC call.
    async fn list_all_followers(
        &self,
        author_id: &AuthorId,
        page_size: i32,
    ) -> Result<Vec<ProfileId>, TimelineError>;

    /// Returns the complete list of followees for `profile_id`.
    ///
    /// Paginates through `ListFollowing` RPCs until `next_page_token` is empty.
    async fn list_all_following(
        &self,
        profile_id: &ProfileId,
        page_size:  i32,
    ) -> Result<Vec<AuthorId>, TimelineError>;

    /// What `viewers` (a reader's profile ids; empty for a guest) may see of
    /// each author's content (`CheckAccess`). One answer per author; a missing
    /// one counts as hidden. Errors are `AccessCheckUnavailable`.
    async fn check_access(
        &self,
        viewers: &[String],
        authors: &[AuthorId],
    ) -> Result<HashMap<AuthorId, ContentAccess>, TimelineError>;
}
