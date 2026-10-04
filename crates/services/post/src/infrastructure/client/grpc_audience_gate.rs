//! The audience check over gRPC: social-graph's mesh-only `CheckAccess`.

use async_trait::async_trait;
use social_graph_api::social_graph_service_client::SocialGraphServiceClient;
use social_graph_api::{CheckAccessRequest, ContentAccess as ProtoAccess};
use tonic::transport::Channel;

use crate::application::port::AudienceGate;
use crate::domain::value_object::{ContentAccess, ProfileId};
use crate::error::PostError;

/// `CheckAccess` client. The channel is `Arc`-backed, so each call clones the
/// client. It must carry a request timeout: tonic has none by default.
pub struct GrpcAudienceGate {
    social_graph: SocialGraphServiceClient<Channel>,
}

impl GrpcAudienceGate {
    pub fn new(social_graph: Channel) -> Self {
        Self { social_graph: SocialGraphServiceClient::new(social_graph) }
    }
}

/// Maps the answer for one target. Anything but an explicit VISIBLE or
/// HEADER_ONLY (an unknown value, a missing target) is HIDDEN: fail closed.
fn map_access(answer: Option<i32>) -> ContentAccess {
    match answer.and_then(|a| ProtoAccess::try_from(a).ok()) {
        Some(ProtoAccess::Visible) => ContentAccess::Visible,
        Some(ProtoAccess::HeaderOnly) => ContentAccess::HeaderOnly,
        _ => ContentAccess::Hidden,
    }
}

#[async_trait]
impl AudienceGate for GrpcAudienceGate {
    async fn access(&self, viewers: &[ProfileId], author: &ProfileId) -> Result<ContentAccess, PostError> {
        let author_id = author.as_str();
        let request = CheckAccessRequest {
            viewer_profile_ids: viewers.iter().map(ProfileId::as_str).collect(),
            target_profile_ids: vec![author_id.clone()],
        };
        let response = self
            .social_graph
            .clone()
            .check_access(request)
            .await
            .map_err(|status| PostError::AccessCheckUnavailable { reason: status.to_string() })?
            .into_inner();
        let answer = response
            .targets
            .iter()
            .find(|t| t.target_profile_id == author_id)
            .map(|t| t.access);
        Ok(map_access(answer))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_explicit_answers_open_access() {
        assert_eq!(map_access(Some(ProtoAccess::Visible as i32)), ContentAccess::Visible);
        assert_eq!(map_access(Some(ProtoAccess::HeaderOnly as i32)), ContentAccess::HeaderOnly);
        assert_eq!(map_access(Some(ProtoAccess::Hidden as i32)), ContentAccess::Hidden);
        assert_eq!(map_access(Some(ProtoAccess::Unspecified as i32)), ContentAccess::Hidden);
        assert_eq!(map_access(Some(99)), ContentAccess::Hidden);
        assert_eq!(map_access(None), ContentAccess::Hidden, "a missing target");
    }
}
