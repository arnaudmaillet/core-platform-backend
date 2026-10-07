use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::domain::value_object::{AccountId, ProfileId};

/// Staff decided a profile's verification request (#668). Carries the
/// requester's private documents (#777) so media purges them after the
/// retention.
#[derive(Debug, Clone)]
pub struct VerificationDecided {
    pub profile_id: ProfileId,
    pub account_id: AccountId,
    pub approved: bool,
    /// Media asset ids of the request's private documents.
    pub private_documents: Vec<String>,
    pub occurred_at: DateTime<Utc>,
    pub correlation_id: Uuid,
}
