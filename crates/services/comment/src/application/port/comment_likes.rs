use std::collections::HashMap;

use async_trait::async_trait;

use crate::domain::value_object::CommentId;
use crate::error::CommentError;

/// Comments' like counts (#832): a like is a point staked in the wallet
/// (#665), counted by engagement. Read to rank a thread's replies by
/// popularity; a comment with none may be absent.
#[async_trait]
pub trait CommentLikes: Send + Sync + 'static {
    async fn counts(&self, comments: &[CommentId]) -> Result<HashMap<CommentId, i64>, CommentError>;
}
