//! social-graph `CheckAccess` over gRPC, split to the RPC's caps (≤ 20 viewer
//! profiles × ≤ 100 targets per call) and recombined per target exactly as one
//! call would answer.

use std::collections::HashMap;

use async_trait::async_trait;
use social_graph_api::social_graph_service_client::SocialGraphServiceClient;
use social_graph_api::{CheckAccessRequest, ContentAccess as ProtoAccess};
use tonic::transport::Channel;

use crate::application::port::AudienceGate;
use crate::domain::ContentAccess;
use crate::error::SearchError;

const MAX_VIEWERS_PER_CALL: usize = 20;
const MAX_TARGETS_PER_CALL: usize = 100;

pub struct GrpcAudienceGate {
    social_graph: SocialGraphServiceClient<Channel>,
}

impl GrpcAudienceGate {
    /// `social_graph` must carry request and connect timeouts.
    pub fn new(social_graph: Channel) -> Self {
        Self { social_graph: SocialGraphServiceClient::new(social_graph) }
    }
}

/// Folds one more chunk's answer into a target's running answer: a block from
/// any profile hides it; otherwise any profile that may see it opens it.
fn fold(current: Option<ContentAccess>, answer: ContentAccess) -> ContentAccess {
    match (current, answer) {
        (Some(ContentAccess::Hidden), _) | (_, ContentAccess::Hidden) => ContentAccess::Hidden,
        (Some(ContentAccess::Visible), _) | (_, ContentAccess::Visible) => ContentAccess::Visible,
        _ => ContentAccess::HeaderOnly,
    }
}

fn from_proto(value: i32) -> ContentAccess {
    match ProtoAccess::try_from(value) {
        Ok(ProtoAccess::Visible) => ContentAccess::Visible,
        Ok(ProtoAccess::HeaderOnly) => ContentAccess::HeaderOnly,
        _ => ContentAccess::Hidden, // unknown → fail closed
    }
}

#[async_trait]
impl AudienceGate for GrpcAudienceGate {
    async fn access(
        &self,
        viewers: &[String],
        targets: &[String],
    ) -> Result<HashMap<String, ContentAccess>, SearchError> {
        let viewer_chunks: Vec<&[String]> =
            if viewers.is_empty() { vec![&[]] } else { viewers.chunks(MAX_VIEWERS_PER_CALL).collect() };
        let mut answers: HashMap<String, ContentAccess> = HashMap::new();
        for v in &viewer_chunks {
            for t in targets.chunks(MAX_TARGETS_PER_CALL) {
                let response = self
                    .social_graph
                    .clone()
                    .check_access(CheckAccessRequest {
                        viewer_profile_ids: v.to_vec(),
                        target_profile_ids: t.to_vec(),
                    })
                    .await
                    .map_err(|status| {
                        tracing::debug!(%status, "CheckAccess failed");
                        SearchError::EngineUnavailable
                    })?
                    .into_inner();
                for target in response.targets {
                    let current = answers.get(&target.target_profile_id).copied();
                    answers.insert(target.target_profile_id, fold(current, from_proto(target.access)));
                }
            }
        }
        Ok(answers)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunked_answers_fold_like_one_call() {
        use ContentAccess::*;
        assert_eq!(fold(Some(HeaderOnly), Visible), Visible);
        assert_eq!(fold(Some(Visible), Hidden), Hidden);
        assert_eq!(fold(Some(Hidden), Visible), Hidden);
        assert_eq!(fold(None, HeaderOnly), HeaderOnly);
        assert_eq!(from_proto(ProtoAccess::Unspecified as i32), Hidden);
        assert_eq!(from_proto(42), Hidden);
    }
}
