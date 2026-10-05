use async_trait::async_trait;

use crate::domain::value_object::{ActorId, EntityType};
use crate::error::ModerationError;

/// Tells whose content a client report targets: the **account** responsible
/// for a post, comment or profile. A client only knows content ids; the account
/// a decision would penalise must never come from the reporter. Also lists an
/// account's profiles, the other way round.
#[async_trait]
pub trait SubjectResolver: Send + Sync + 'static {
    /// `None` when the content does not exist. Errors are
    /// `ContentDirectoryUnavailable` (transient) or `UnsupportedReportTarget`.
    async fn responsible_account(
        &self,
        entity_type: EntityType,
        entity_id: &str,
    ) -> Result<Option<ActorId>, ModerationError>;

    /// The account's profiles (deleted ones left out): who is told of an
    /// appeal's outcome (#744). Empty when the account has none. Errors are
    /// `ContentDirectoryUnavailable` (transient).
    async fn profiles_of(&self, account: &ActorId) -> Result<Vec<String>, ModerationError>;
}
