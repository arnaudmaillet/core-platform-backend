use crate::domain::value_object::{AuthorId, PostId};

/// A single chronological slot in a user's following feed.
///
/// Invariants:
/// - `published_at_ms` is a Unix epoch millisecond timestamp sourced from
///   the `PostPublished` event. It is never mutated after ingestion.
/// - `post_id` is a UUID v7 sourced from services/post.
/// - No post content (text, media URLs) is stored. The BFF hydrates
///   all rendering metadata from services/post and services/profile.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedEntry {
    pub post_id:        PostId,
    pub author_id:      AuthorId,
    pub published_at_ms: i64,
}

impl FeedEntry {
    pub fn new(post_id: PostId, author_id: AuthorId, published_at_ms: i64) -> Self {
        Self { post_id, author_id, published_at_ms }
    }
}
