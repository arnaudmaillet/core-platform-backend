use std::sync::Arc;

use cqrs::{Envelope, Query, QueryHandler};

use crate::application::port::{ProfileReaction, ReactionLedger};
use crate::domain::value_object::{PostId, ProfileId};
use crate::error::EngagementError;

/// A profile's reactions — mesh only: the GDPR data export (#653) reads them
/// for the account that owns the profile. Paged by post id.
pub struct ListReactionsByProfileQuery {
    pub profile_id: String,
    pub limit:      i32,
    /// The last post id of the previous page.
    pub after:      Option<String>,
}

impl Query for ListReactionsByProfileQuery {
    type Response = Vec<ProfileReaction>;
}

pub struct ListReactionsByProfileHandler {
    /// `None` on an instance without the durable ledger (no write-behind).
    pub ledger: Option<Arc<dyn ReactionLedger>>,
}

impl QueryHandler<ListReactionsByProfileQuery> for ListReactionsByProfileHandler {
    type Error = EngagementError;

    async fn handle(&self, envelope: Envelope<ListReactionsByProfileQuery>) -> Result<Vec<ProfileReaction>, EngagementError> {
        let ledger = self.ledger.as_ref().ok_or(EngagementError::LedgerUnavailable)?;
        let query = &envelope.payload;
        let profile = ProfileId::try_from(query.profile_id.as_str())?;
        let after = query.after.as_deref().map(PostId::try_from).transpose()?;
        ledger.list_by_profile(&profile, query.limit, after.as_ref()).await
    }
}
