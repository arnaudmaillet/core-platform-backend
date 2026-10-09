//! [`ProfileAccess`] over the mesh (#829): social-graph's `CheckAccess` — may
//! the reader see a profile's content (a private profile it doesn't follow, a
//! block either way, a hidden profile). Any failure fails closed.

use async_trait::async_trait;
use tonic::transport::Channel;

use social_graph_api::social_graph_service_client::SocialGraphServiceClient;

use crate::application::port::ProfileAccess;
use crate::error::EngagementError;

/// social-graph `CheckAccess` caps the reader's profiles per call.
const MAX_VIEWERS: usize = 20;

pub struct GrpcProfileAccess {
    social: SocialGraphServiceClient<Channel>,
}

impl GrpcProfileAccess {
    /// `channel` must carry request and connect timeouts.
    pub fn new(channel: Channel) -> Self {
        Self { social: SocialGraphServiceClient::new(channel) }
    }
}

#[async_trait]
impl ProfileAccess for GrpcProfileAccess {
    async fn visible(&self, viewers: &[String], profile_id: &str) -> Result<bool, EngagementError> {
        let answer = self
            .social
            .clone()
            .check_access(social_graph_api::CheckAccessRequest {
                viewer_profile_ids: viewers.iter().take(MAX_VIEWERS).cloned().collect(),
                target_profile_ids: vec![profile_id.to_owned()],
            })
            .await
            .map_err(|status| EngagementError::SocialGraphUnavailable { message: status.to_string() })?
            .into_inner();
        // A profile missing from the answer is not visible (fail closed).
        Ok(answer
            .targets
            .iter()
            .any(|t| t.target_profile_id == profile_id && t.access == social_graph_api::ContentAccess::Visible as i32))
    }
}
