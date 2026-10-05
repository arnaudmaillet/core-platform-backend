//! social-graph `CheckAccess` over gRPC, split to the RPC's caps (≤ 20 viewer
//! profiles × ≤ 100 authors per call; a viewport can hold hundreds of pins) and
//! recombined per author exactly as one call would answer.

use std::collections::HashMap;

use async_trait::async_trait;
use social_graph_api::social_graph_service_client::SocialGraphServiceClient;
use social_graph_api::{CheckAccessRequest, ContentAccess as ProtoAccess};
use tonic::transport::Channel;
use uuid::Uuid;

use crate::application::port::AudienceGate;
use crate::domain::value_object::{AuthorAccess, ContentAccess};
use crate::error::GeoDiscoveryError;

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

/// Folds one more chunk's answer into an author's running answer: a block from
/// any profile hides; otherwise any profile that may see it opens it.
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
        authors: &[Uuid],
    ) -> Result<HashMap<Uuid, AuthorAccess>, GeoDiscoveryError> {
        let viewer_chunks: Vec<&[String]> =
            if viewers.is_empty() { vec![&[]] } else { viewers.chunks(MAX_VIEWERS_PER_CALL).collect() };
        let mut answers: HashMap<Uuid, AuthorAccess> = HashMap::new();
        for v in &viewer_chunks {
            for t in authors.chunks(MAX_TARGETS_PER_CALL) {
                let response = self
                    .social_graph
                    .clone()
                    .check_access(CheckAccessRequest {
                        viewer_profile_ids: v.to_vec(),
                        target_profile_ids: t.iter().map(Uuid::to_string).collect(),
                    })
                    .await
                    .map_err(|status| GeoDiscoveryError::AccessCheckUnavailable { reason: status.to_string() })?
                    .into_inner();
                for target in response.targets {
                    let Ok(author) = Uuid::parse_str(&target.target_profile_id) else { continue };
                    // Any viewer chunk following (or mutual with) the author counts.
                    let current = answers.get(&author).copied();
                    answers.insert(author, AuthorAccess {
                        content: fold(current.map(|c| c.content), from_proto(target.access)),
                        follows: current.is_some_and(|c| c.follows) || target.follows,
                        mutual:  current.is_some_and(|c| c.mutual) || target.mutual,
                    });
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
    }
}
