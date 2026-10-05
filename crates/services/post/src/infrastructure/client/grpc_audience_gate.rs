//! The audience check over gRPC: social-graph's mesh-only `CheckAccess`.

use async_trait::async_trait;
use social_graph_api::social_graph_service_client::SocialGraphServiceClient;
use social_graph_api::{CheckAccessRequest, CheckInteractionRequest, ContentAccess as ProtoAccess, InteractionKind};
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

/// `CheckAccess` accepts at most this many viewer profiles per call
/// (social-graph `MAX_VIEWERS`); an account can own more, so larger sets are
/// split and the answers combined.
const MAX_VIEWERS_PER_CALL: usize = 20;

/// Combines one target's answers across viewer chunks, as one call over the
/// whole set would decide it: a block from any profile hides it; otherwise any
/// profile that may see it (a follower of a private target) opens it.
fn combine(answers: impl IntoIterator<Item = ContentAccess>) -> ContentAccess {
    let mut combined = ContentAccess::HeaderOnly;
    for answer in answers {
        match answer {
            ContentAccess::Hidden => return ContentAccess::Hidden,
            ContentAccess::Visible => combined = ContentAccess::Visible,
            ContentAccess::HeaderOnly => {}
        }
    }
    combined
}

#[async_trait]
impl AudienceGate for GrpcAudienceGate {
    async fn access(&self, viewers: &[ProfileId], author: &ProfileId) -> Result<ContentAccess, PostError> {
        let author_id = author.as_str();
        // An anonymous reader is one call with no profiles.
        let chunks: Vec<&[ProfileId]> = if viewers.is_empty() {
            vec![&[]]
        } else {
            viewers.chunks(MAX_VIEWERS_PER_CALL).collect()
        };
        let mut answers = Vec::with_capacity(chunks.len());
        for chunk in chunks {
            let request = CheckAccessRequest {
                viewer_profile_ids: chunk.iter().map(ProfileId::as_str).collect(),
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
            answers.push(map_access(answer));
        }
        Ok(combine(answers))
    }

    async fn may_mention(&self, author: &ProfileId, mentioned: &ProfileId) -> Result<bool, PostError> {
        let answer = self
            .social_graph
            .clone()
            .check_interaction(CheckInteractionRequest {
                actor_profile_id:  author.as_str(),
                target_profile_id: mentioned.as_str(),
                kind:              InteractionKind::Mention as i32,
            })
            .await
            .map_err(|status| PostError::AccessCheckUnavailable { reason: status.to_string() })?
            .into_inner();
        Ok(answer.allowed)
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

    #[test]
    fn chunked_answers_combine_like_one_call() {
        use ContentAccess::*;
        assert_eq!(combine([HeaderOnly, Visible]), Visible, "a follower in any chunk");
        assert_eq!(combine([Visible, Hidden]), Hidden, "a block in any chunk");
        assert_eq!(combine([HeaderOnly, HeaderOnly]), HeaderOnly);
        assert_eq!(combine([Visible]), Visible);
    }
}
