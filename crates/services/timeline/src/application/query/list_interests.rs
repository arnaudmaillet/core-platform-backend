use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use cqrs::{Envelope, Query, QueryHandler};
use validate_core::{FieldViolation, Validate};

use crate::application::port::InterestStore;
use crate::domain::value_object::interest::MAX_INTERESTS;
use crate::domain::value_object::{Interest, ProfileId};
use crate::error::TimelineError;

/// The profile's interest tags (#662), heaviest first, with today's weight.
pub struct ListInterestsQuery {
    pub profile_id: String,
}

impl Query for ListInterestsQuery {
    type Response = Vec<Interest>;
}

impl Validate for ListInterestsQuery {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        if ProfileId::try_from(self.profile_id.as_str()).is_err() {
            return Err(vec![FieldViolation::new("profile_id", "TML-VAL-040", "profile_id must be a UUID")]);
        }
        Ok(())
    }
}

pub struct ListInterestsHandler {
    pub interests: Arc<dyn InterestStore>,
}

impl QueryHandler<ListInterestsQuery> for ListInterestsHandler {
    type Error = TimelineError;

    async fn handle(&self, envelope: Envelope<ListInterestsQuery>) -> Result<Vec<Interest>, TimelineError> {
        let profile = ProfileId::try_from(envelope.payload.profile_id.as_str())?;
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or_default();
        self.interests.top(&profile, now_ms, MAX_INTERESTS).await
    }
}
