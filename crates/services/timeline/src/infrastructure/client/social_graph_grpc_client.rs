use std::collections::HashMap;

use async_trait::async_trait;
use transport::grpc::client::ResilientChannel;

use crate::application::port::SocialGraphClient;
use crate::domain::value_object::{AuthorId, ContentAccess, ProfileId};
use crate::error::TimelineError;

// ── Generated social-graph client stubs ──────────────────────────────────────
// Stubs come from the contracts tier (`social-graph-api`) — no cross-service
// proto recompilation. Aliased as `sg_proto` to keep the references below stable.

use social_graph_api as sg_proto;

use sg_proto::{
    social_graph_service_client::SocialGraphServiceClient,
    CheckAccessRequest, ContentAccess as ProtoAccess, ListFollowersRequest, ListFollowingRequest,
};

/// `CheckAccess` caps: viewer profiles and target authors per call.
const MAX_VIEWERS_PER_CALL: usize = 20;
const MAX_TARGETS_PER_CALL: usize = 100;

/// Folds one more chunk's answer into an author's running answer: a block from
/// any profile hides; otherwise any profile that may see it opens it — exactly
/// what one unchunked call would answer.
fn fold(current: Option<ContentAccess>, answer: ContentAccess) -> ContentAccess {
    match (current, answer) {
        (Some(ContentAccess::Hidden), _) | (_, ContentAccess::Hidden) => ContentAccess::Hidden,
        (Some(ContentAccess::Visible), _) | (_, ContentAccess::Visible) => ContentAccess::Visible,
        _ => ContentAccess::HeaderOnly,
    }
}

fn access_from_proto(value: i32) -> ContentAccess {
    match ProtoAccess::try_from(value) {
        Ok(ProtoAccess::Visible) => ContentAccess::Visible,
        Ok(ProtoAccess::HeaderOnly) => ContentAccess::HeaderOnly,
        _ => ContentAccess::Hidden, // unknown → fail closed
    }
}

// ── Client ────────────────────────────────────────────────────────────────────

/// tonic gRPC client adapter for services/social-graph.
///
/// The channel is a [`ResilientChannel`]: trace injection + circuit breaker + timeout,
/// configured from the `social-graph` resilience binding and hot-reloaded with it (see
/// [`crate::service`]). The breaker/timeout therefore wrap every paginated call below.
///
/// Both `list_all_followers` and `list_all_following` paginate internally
/// until `next_page_token` is empty, returning the complete flattened list.
/// This is safe because:
///   - Fan-out (followers): called from a background Kafka worker — no latency SLA.
///   - Following list: called at most once per cold-start rebuild — amortized over
///     the `TIMELINE_WARM_TTL_SECS` (default 24h) window.
pub struct SocialGraphGrpcClient {
    channel: ResilientChannel,
}

impl SocialGraphGrpcClient {
    pub fn new(channel: ResilientChannel) -> Self {
        Self { channel }
    }

    fn client(&self) -> SocialGraphServiceClient<ResilientChannel> {
        SocialGraphServiceClient::new(self.channel.clone())
    }
}

#[async_trait]
impl SocialGraphClient for SocialGraphGrpcClient {
    async fn list_all_followers(
        &self,
        author_id: &AuthorId,
        page_size: i32,
    ) -> Result<Vec<ProfileId>, TimelineError> {
        let mut client      = self.client();
        let mut all_ids     = Vec::new();
        let mut page_token  = String::new();
        let followee_id_str = author_id.to_string();

        loop {
            let resp = client
                .list_followers(ListFollowersRequest {
                    followee_id: followee_id_str.clone(),
                    limit:       page_size,
                    page_token:  page_token.clone(),
                })
                .await
                .map_err(|e| TimelineError::SocialGraphClientError {
                    message: e.to_string(),
                })?
                .into_inner();

            for edge in &resp.followers {
                let pid = ProfileId::try_from(edge.profile_id.as_str())
                    .map_err(|_| TimelineError::SocialGraphInvalidId(edge.profile_id.clone()))?;
                all_ids.push(pid);
            }

            if resp.next_page_token.is_empty() {
                break;
            }
            page_token = resp.next_page_token;
        }

        Ok(all_ids)
    }

    async fn list_all_following(
        &self,
        profile_id: &ProfileId,
        page_size:  i32,
    ) -> Result<Vec<AuthorId>, TimelineError> {
        let mut client       = self.client();
        let mut all_ids      = Vec::new();
        let mut page_token   = String::new();
        let follower_id_str  = profile_id.to_string();

        loop {
            let resp = client
                .list_following(ListFollowingRequest {
                    follower_id: follower_id_str.clone(),
                    limit:       page_size,
                    page_token:  page_token.clone(),
                })
                .await
                .map_err(|e| TimelineError::SocialGraphClientError {
                    message: e.to_string(),
                })?
                .into_inner();

            for edge in &resp.following {
                let aid = AuthorId::try_from(edge.profile_id.as_str())
                    .map_err(|_| TimelineError::SocialGraphInvalidId(edge.profile_id.clone()))?;
                all_ids.push(aid);
            }

            if resp.next_page_token.is_empty() {
                break;
            }
            page_token = resp.next_page_token;
        }

        Ok(all_ids)
    }

    /// Split to the RPC's caps (≤ 20 viewers × ≤ 100 authors per call) and
    /// recombined per author.
    async fn check_access(
        &self,
        viewers: &[String],
        authors: &[AuthorId],
    ) -> Result<HashMap<AuthorId, ContentAccess>, TimelineError> {
        let mut client = self.client();
        let viewer_chunks: Vec<&[String]> =
            if viewers.is_empty() { vec![&[]] } else { viewers.chunks(MAX_VIEWERS_PER_CALL).collect() };
        let mut answers: HashMap<AuthorId, ContentAccess> = HashMap::new();
        for v in &viewer_chunks {
            for t in authors.chunks(MAX_TARGETS_PER_CALL) {
                let response = client
                    .check_access(CheckAccessRequest {
                        viewer_profile_ids: v.to_vec(),
                        target_profile_ids: t.iter().map(AuthorId::to_string).collect(),
                    })
                    .await
                    .map_err(|status| TimelineError::AccessCheckUnavailable { reason: status.to_string() })?
                    .into_inner();
                for target in response.targets {
                    let Ok(author) = AuthorId::try_from(target.target_profile_id.as_str()) else { continue };
                    let current = answers.get(&author).copied();
                    answers.insert(author, fold(current, access_from_proto(target.access)));
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
        assert_eq!(access_from_proto(ProtoAccess::Unspecified as i32), Hidden);
    }
}
