use std::collections::HashMap;

use async_trait::async_trait;

use crate::domain::ContentAccess;
use crate::error::SearchError;

/// social-graph's access check (`CheckAccess`): what `viewers` (a reader's
/// profile ids; empty when anonymous) may see of each target author.
#[async_trait]
pub trait AudienceGate: Send + Sync + 'static {
    /// One answer per target; a target missing from the map counts as hidden.
    async fn access(
        &self,
        viewers: &[String],
        targets: &[String],
    ) -> Result<HashMap<String, ContentAccess>, SearchError>;
}
