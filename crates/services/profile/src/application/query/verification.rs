use std::sync::Arc;

use cqrs::{Envelope, Query, QueryHandler};

use crate::application::port::VerificationStore;
use crate::domain::entity::VerificationRequest;
use crate::domain::value_object::ProfileId;
use crate::error::ProfileError;

/// The profile's latest verification request, if any (its owner asks).
#[derive(Debug, Clone)]
pub struct GetVerificationRequestQuery {
    pub profile_id: String,
}

impl Query for GetVerificationRequestQuery {
    type Response = Option<VerificationRequest>;
}

pub struct GetVerificationRequestHandler {
    pub verifications: Arc<dyn VerificationStore>,
}

impl QueryHandler<GetVerificationRequestQuery> for GetVerificationRequestHandler {
    type Error = ProfileError;

    async fn handle(&self, envelope: Envelope<GetVerificationRequestQuery>) -> Result<Option<VerificationRequest>, ProfileError> {
        let id = ProfileId::try_from(envelope.payload.profile_id.as_str())?;
        self.verifications.get(&id).await
    }
}

/// The pending requests, oldest first (staff queue; mesh-only).
#[derive(Debug, Clone)]
pub struct ListPendingVerificationsQuery {
    pub limit:      i32,
    pub page_token: Option<String>,
}

impl Query for ListPendingVerificationsQuery {
    type Response = (Vec<(ProfileId, VerificationRequest)>, Option<String>);
}

pub struct ListPendingVerificationsHandler {
    pub verifications: Arc<dyn VerificationStore>,
}

impl QueryHandler<ListPendingVerificationsQuery> for ListPendingVerificationsHandler {
    type Error = ProfileError;

    async fn handle(
        &self,
        envelope: Envelope<ListPendingVerificationsQuery>,
    ) -> Result<(Vec<(ProfileId, VerificationRequest)>, Option<String>), ProfileError> {
        let q = &envelope.payload;
        let (ids, next) = self.verifications.pending(q.limit, q.page_token.as_deref()).await?;
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(request) = self.verifications.get(&id).await? {
                out.push((id, request));
            }
        }
        Ok((out, next))
    }
}
