//! What likes land on, over the mesh: post (`GetPost`) and comment
//! (`GetComment`). Any failure but NOT_FOUND is `WAL-6001` (fail closed).

use async_trait::async_trait;
use tonic::transport::Channel;
use tonic::Code;

use comment_api::comment_service_client::CommentServiceClient;
use post_api::post_service_client::PostServiceClient;

use crate::application::port::{TargetDirectory, TargetInfo};
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
