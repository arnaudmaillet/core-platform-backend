//! What likes land on, over the mesh: post (`GetPost`), comment
//! (`GetComment`), and whether the one who likes may see it (social-graph
//! `CheckAccess`). Any failure but NOT_FOUND is `WAL-6001` (fail closed).

use async_trait::async_trait;
use tonic::transport::Channel;
use tonic::Code;

use comment_api::comment_service_client::CommentServiceClient;
use post_api::post_service_client::PostServiceClient;
use social_graph_api::social_graph_service_client::SocialGraphServiceClient;

use crate::application::port::{AudienceCheck, TargetDirectory, TargetInfo};
use crate::domain::StakeTarget;
use crate::error::WalletError;

pub struct GrpcTargetDirectory {
    posts:    PostServiceClient<Channel>,
    comments: CommentServiceClient<Channel>,
}

impl GrpcTargetDirectory {
    /// Both channels must carry request and connect timeouts.
    pub fn new(posts: Channel, comments: Channel) -> Self {
        Self { posts: PostServiceClient::new(posts), comments: CommentServiceClient::new(comments) }
    }
}

fn unavailable(service: &'static str) -> impl Fn(tonic::Status) -> WalletError {
    move |status| WalletError::PeerUnavailable { service, reason: status.to_string() }
}

#[async_trait]
impl TargetDirectory for GrpcTargetDirectory {
    async fn target(&self, target: &StakeTarget) -> Result<Option<TargetInfo>, WalletError> {
        match target {
            StakeTarget::Post(id) => {
                let request = post_api::GetPostRequest { post_id: id.clone(), ..Default::default() };
                match self.posts.clone().get_post(request).await {
                    Ok(view) => {
                        let view = view.into_inner();
                        let stakeable = view.status == post_api::PostStatus::Published as i32
                            && view.moderation != post_api::ModerationRestriction::Removed as i32;
                        Ok(Some(TargetInfo { author_profile_id: view.profile_id, stakeable }))
                    }
                    Err(status) if matches!(status.code(), Code::NotFound | Code::InvalidArgument) => Ok(None),
                    Err(status) => Err(unavailable("post")(status)),
                }
            }
            StakeTarget::Comment(id) => {
                let request = comment_api::GetCommentRequest { comment_id: id.clone() };
                match self.comments.clone().get_comment(request).await {
                    Ok(view) => {
                        let view = view.into_inner();
                        let stakeable = view.status == comment_api::CommentStatus::Published as i32;
                        Ok(Some(TargetInfo { author_profile_id: view.author_id, stakeable }))
                    }
                    Err(status) if matches!(status.code(), Code::NotFound | Code::InvalidArgument) => Ok(None),
                    Err(status) => Err(unavailable("comment")(status)),
                }
            }
        }
    }
}

/// social-graph `CheckAccess` caps the reader's profiles per call.
const MAX_VIEWERS: usize = 20;

pub struct GrpcAudienceCheck {
    social: SocialGraphServiceClient<Channel>,
}

impl GrpcAudienceCheck {
    /// `channel` must carry request and connect timeouts.
    pub fn new(channel: Channel) -> Self {
        Self { social: SocialGraphServiceClient::new(channel) }
    }
}

#[async_trait]
impl AudienceCheck for GrpcAudienceCheck {
    async fn visible(&self, viewers: &[String], author_profile_id: &str) -> Result<bool, WalletError> {
        let viewers: Vec<String> = viewers.iter().take(MAX_VIEWERS).cloned().collect();
        let answer = self
            .social
            .clone()
            .check_access(social_graph_api::CheckAccessRequest {
                viewer_profile_ids: viewers,
                target_profile_ids: vec![author_profile_id.to_owned()],
            })
            .await
            .map_err(unavailable("social-graph"))?
            .into_inner();
        // An author missing from the answer is not visible (fail closed).
        Ok(answer.targets.iter().any(|t| {
            t.target_profile_id == author_profile_id && t.access == social_graph_api::ContentAccess::Visible as i32
        }))
    }
}
