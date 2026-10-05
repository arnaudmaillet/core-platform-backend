use std::collections::HashSet;
use std::time::Duration as StdDuration;

use async_trait::async_trait;
use tonic::transport::{Channel, Endpoint};
use tonic::Code;

use profile_api::profile_service_client::ProfileServiceClient;
use social_graph_api::social_graph_service_client::SocialGraphServiceClient;

use crate::application::port::{DirectoryProfile, ProfileDirectory};
use crate::domain::value_object::AccountId;
use crate::error::AccountError;

/// social-graph `CheckAccess` caps per call.
const MAX_VIEWERS: usize = 20;
const MAX_TARGETS: usize = 100;
const PAGE: i32 = 100;

/// [`ProfileDirectory`] over profile (`ListProfilesByAccount` +
/// `GetProfileById`, read as the mesh: every field) and social-graph
/// (`CheckAccess`). Lazy channels with request deadlines.
pub struct MeshProfileDirectory {
    profile: ProfileServiceClient<Channel>,
    social:  SocialGraphServiceClient<Channel>,
}

fn unavailable(service: &'static str) -> impl Fn(tonic::Status) -> AccountError {
    move |status| AccountError::DirectoryUnavailable { reason: format!("{service}: {status}") }
}

impl MeshProfileDirectory {
    pub fn new(profile_endpoint: &str, social_graph_endpoint: &str) -> Result<Self, AccountError> {
        let channel = |uri: &str| -> Result<Channel, AccountError> {
            Ok(Endpoint::from_shared(uri.to_owned())
                .map_err(|e| AccountError::DirectoryUnavailable { reason: format!("endpoint {uri}: {e}") })?
                .timeout(StdDuration::from_secs(3))
                .connect_timeout(StdDuration::from_secs(2))
                .connect_lazy())
        };
        Ok(Self {
            profile: ProfileServiceClient::new(channel(profile_endpoint)?),
            social:  SocialGraphServiceClient::new(channel(social_graph_endpoint)?),
        })
    }
}

#[async_trait]
impl ProfileDirectory for MeshProfileDirectory {
    async fn profiles_of(&self, account_id: &AccountId) -> Result<Vec<DirectoryProfile>, AccountError> {
        let (mut ids, mut token) = (Vec::new(), String::new());
        loop {
            let page = self
                .profile
                .clone()
                .list_profiles_by_account(profile_api::ListProfilesByAccountRequest {
                    account_id: account_id.as_uuid().to_string(),
                    limit: PAGE,
                    page_token: token,
                })
                .await
                .map_err(unavailable("profile"))?
                .into_inner();
            ids.extend(page.profiles.into_iter().map(|p| p.profile_id));
            if page.next_page_token.is_empty() {
                break;
            }
            token = page.next_page_token;
        }
        let mut profiles = Vec::with_capacity(ids.len());
        for profile_id in ids {
            let view = match self
                .profile
                .clone()
                .get_profile_by_id(profile_api::GetProfileByIdRequest { profile_id })
                .await
            {
                Ok(view) => view.into_inner(),
                // Gone between the two reads: not findable.
                Err(status) if status.code() == Code::NotFound => continue,
                Err(status) => return Err(unavailable("profile")(status)),
            };
            // Absent discovery settings (an older profile server): not findable.
            let discovery = view.discovery_settings.unwrap_or_default();
            profiles.push(DirectoryProfile {
                profile_id:   view.profile_id,
                handle:       view.handle,
                display_name: view.display_name,
                avatar_url:   Some(view.avatar_url).filter(|u| !u.is_empty()),
                active:       view.status == profile_api::ProfileStatus::Active as i32,
                by_email:     discovery.by_email,
                by_phone:     discovery.by_phone,
            });
        }
        Ok(profiles)
    }

    async fn hidden_from(&self, viewers: &[String], targets: &[String]) -> Result<HashSet<String>, AccountError> {
        let viewer_chunks: Vec<&[String]> =
            if viewers.is_empty() { vec![&[]] } else { viewers.chunks(MAX_VIEWERS).collect() };
        let mut hidden = HashSet::new();
        for v in &viewer_chunks {
            for t in targets.chunks(MAX_TARGETS) {
                let answer = self
                    .social
                    .clone()
                    .check_access(social_graph_api::CheckAccessRequest {
                        viewer_profile_ids: v.to_vec(),
                        target_profile_ids: t.to_vec(),
                    })
                    .await
                    .map_err(unavailable("social-graph"))?
                    .into_inner();
                // A block from any of the caller's profiles hides the target;
                // a target missing from the answer is hidden (fail closed).
                let answered: HashSet<&str> = answer.targets.iter().map(|a| a.target_profile_id.as_str()).collect();
                hidden.extend(
                    answer
                        .targets
                        .iter()
                        .filter(|a| a.access == social_graph_api::ContentAccess::Hidden as i32)
                        .map(|a| a.target_profile_id.clone()),
                );
                hidden.extend(t.iter().filter(|id| !answered.contains(id.as_str())).cloned());
            }
        }
        Ok(hidden)
    }
}
