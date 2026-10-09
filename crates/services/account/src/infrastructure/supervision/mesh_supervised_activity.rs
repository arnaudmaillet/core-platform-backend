use std::time::Duration as StdDuration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tonic::transport::{Channel, Endpoint};

use moderation_api::moderation_service_client::ModerationServiceClient;
use tonic::service::interceptor::InterceptedService;
use transport::grpc::mesh::MeshTokenInterceptor;
use social_graph_api::social_graph_service_client::SocialGraphServiceClient;

use crate::application::port::{
    ActivityPage, Connection, ConnectionKind, ReportOutcome, ReportSummary, SupervisedActivity,
};
use crate::domain::value_object::AccountId;
use crate::error::AccountError;

/// [`SupervisedActivity`] over social-graph (`ListFollowing`,
/// `ListFollowers`, `ListBlocks`) and moderation (`ListReportsByReporter`,
/// which leaves out self-harm / CSAM / NCII reports and those about the
/// hidden accounts' content), read as the mesh: the lists in full, whatever
/// their privacy. Lazy channels with request deadlines.
pub struct MeshSupervisedActivity {
    social:     SocialGraphServiceClient<Channel>,
    /// Carries this pod's mesh token: moderation checks who lists a
    /// member's reports (#852).
    moderation: ModerationServiceClient<InterceptedService<Channel, MeshTokenInterceptor>>,
}

fn unavailable(service: &'static str) -> impl Fn(tonic::Status) -> AccountError {
    move |status| AccountError::SupervisionActivityUnavailable { reason: format!("{service}: {status}") }
}

impl MeshSupervisedActivity {
    pub fn new(social_graph_endpoint: &str, moderation_endpoint: &str) -> Result<Self, AccountError> {
        let channel = |uri: &str| -> Result<Channel, AccountError> {
            Ok(Endpoint::from_shared(uri.to_owned())
                .map_err(|e| AccountError::SupervisionActivityUnavailable { reason: format!("endpoint {uri}: {e}") })?
                .timeout(StdDuration::from_secs(3))
                .connect_timeout(StdDuration::from_secs(2))
                .connect_lazy())
        };
        Ok(Self {
            social:     SocialGraphServiceClient::new(channel(social_graph_endpoint)?),
            moderation: ModerationServiceClient::with_interceptor(channel(moderation_endpoint)?, MeshTokenInterceptor::from_env()),
        })
    }
}

#[async_trait]
impl SupervisedActivity for MeshSupervisedActivity {
    async fn connections(
        &self,
        profile_id: &str,
        kind: ConnectionKind,
        limit: u32,
        page_token: &str,
    ) -> Result<ActivityPage<Connection>, AccountError> {
        let (profile_id, limit, page_token) = (profile_id.to_owned(), i32::try_from(limit).unwrap_or(i32::MAX), page_token.to_owned());
        let mut social = self.social.clone();
        let (items, next): (Vec<Connection>, String) = match kind {
            ConnectionKind::Following => {
                let page = social
                    .list_following(social_graph_api::ListFollowingRequest { follower_id: profile_id, limit, page_token })
                    .await
                    .map_err(unavailable("social-graph"))?
                    .into_inner();
                (page.following.into_iter().map(|e| connection(e.profile_id, e.followed_at)).collect(), page.next_page_token)
            }
            ConnectionKind::Followers => {
                let page = social
                    .list_followers(social_graph_api::ListFollowersRequest { followee_id: profile_id, limit, page_token })
                    .await
                    .map_err(unavailable("social-graph"))?
                    .into_inner();
                (page.followers.into_iter().map(|e| connection(e.profile_id, e.followed_at)).collect(), page.next_page_token)
            }
            ConnectionKind::Blocked => {
                let page = social
                    .list_blocks(social_graph_api::ListBlocksRequest { blocker_id: profile_id, limit, page_token })
                    .await
                    .map_err(unavailable("social-graph"))?
                    .into_inner();
                (page.blocks.into_iter().map(|b| connection(b.blockee_id, b.blocked_at)).collect(), page.next_page_token)
            }
        };
        Ok(ActivityPage { items, next_page_token: Some(next).filter(|t| !t.is_empty()) })
    }

    async fn reports(
        &self,
        reporter: &AccountId,
        hidden_accounts: &[AccountId],
        limit: u32,
        page_token: &str,
    ) -> Result<ActivityPage<ReportSummary>, AccountError> {
        let page = self
            .moderation
            .clone()
            .list_reports_by_reporter(moderation_api::ListReportsByReporterRequest {
                reporter_id: reporter.as_uuid().to_string(),
                page_size:   i32::try_from(limit).unwrap_or(i32::MAX),
                page_token:  page_token.to_owned(),
                hidden_account_ids: hidden_accounts.iter().map(|a| a.as_uuid().to_string()).collect(),
            })
            .await
            .map_err(unavailable("moderation"))?
            .into_inner();
        // Only who or what, when and the decision: the free text (empty on
        // this RPC anyway) and the decision id are not carried over.
        let items = page
            .reports
            .into_iter()
            .map(|r| ReportSummary {
                entity_type: name(moderation_api::EntityType::try_from(r.entity_type).map(|e| e.as_str_name()), "ENTITY_TYPE_"),
                entity_id:   r.entity_id,
                category:    name(moderation_api::PolicyCategory::try_from(r.category).map(|c| c.as_str_name()), "POLICY_CATEGORY_"),
                outcome:     match moderation_api::ReportStatus::try_from(r.status) {
                    Ok(moderation_api::ReportStatus::ActionTaken) => ReportOutcome::ActionTaken,
                    Ok(moderation_api::ReportStatus::NoViolation) => ReportOutcome::NoViolation,
                    _ => ReportOutcome::UnderReview,
                },
                reported_at: r.reported_at.and_then(timestamp),
            })
            .collect();
        Ok(ActivityPage { items, next_page_token: Some(page.next_page_token).filter(|t| !t.is_empty()) })
    }
}

fn connection(profile_id: String, since: Option<prost_types::Timestamp>) -> Connection {
    Connection { profile_id, since: since.and_then(timestamp) }
}

fn timestamp(ts: prost_types::Timestamp) -> Option<DateTime<Utc>> {
    DateTime::from_timestamp(ts.seconds, u32::try_from(ts.nanos).unwrap_or(0))
}

/// `ENTITY_TYPE_CHAT_MESSAGE` → `chat_message`; unknown values: `other`.
fn name<E>(wire: Result<&str, E>, prefix: &str) -> String {
    match wire {
        Ok(n) => n.strip_prefix(prefix).unwrap_or(n).to_ascii_lowercase(),
        Err(_) => "other".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_enum_names_become_snake_case_values() {
        assert_eq!(name::<()>(Ok("ENTITY_TYPE_CHAT_MESSAGE"), "ENTITY_TYPE_"), "chat_message");
        assert_eq!(name::<()>(Ok("POLICY_CATEGORY_VIOLENT_EXTREMISM"), "POLICY_CATEGORY_"), "violent_extremism");
        assert_eq!(name(Err(()), "ENTITY_TYPE_"), "other");
    }
}
