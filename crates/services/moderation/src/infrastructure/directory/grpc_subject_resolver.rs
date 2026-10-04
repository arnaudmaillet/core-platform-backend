//! Whose content a client report targets, over the mesh: a post or a comment
//! resolves to its author profile, a profile to its owning account. Mesh reads
//! are unfiltered (moderation is a trusted internal caller), so a draft or a
//! hidden profile can still be reported and resolved.

use async_trait::async_trait;
use comment_api::comment_service_client::CommentServiceClient;
use comment_api::GetCommentRequest;
use post_api::post_service_client::PostServiceClient;
use post_api::GetPostRequest;
use profile_api::profile_service_client::ProfileServiceClient;
use profile_api::GetProfileByIdRequest;
use tonic::transport::Channel;
use tonic::{Code, Status};
use uuid::Uuid;

use crate::application::port::SubjectResolver;
use crate::domain::value_object::{ActorId, EntityType};
use crate::error::ModerationError;

/// The three channels must carry request and connect timeouts.
pub struct GrpcSubjectResolver {
    posts: PostServiceClient<Channel>,
    comments: CommentServiceClient<Channel>,
    profiles: ProfileServiceClient<Channel>,
}

impl GrpcSubjectResolver {
    pub fn new(posts: Channel, comments: Channel, profiles: Channel) -> Self {
        Self {
            posts: PostServiceClient::new(posts),
            comments: CommentServiceClient::new(comments),
            profiles: ProfileServiceClient::new(profiles),
        }
    }

    /// The account owning `profile_id`, or `None` when the profile is gone.
    async fn profile_owner(&self, profile_id: &str) -> Result<Option<ActorId>, ModerationError> {
        let view = match self
            .profiles
            .clone()
            .get_profile_by_id(GetProfileByIdRequest { profile_id: profile_id.to_owned() })
            .await
        {
            Ok(r) => r.into_inner(),
            Err(status) => return not_found_or_unavailable(status),
        };
        Uuid::parse_str(&view.account_id)
            .map(|id| Some(ActorId::from_uuid(id)))
            .map_err(|_| ModerationError::ContentDirectoryUnavailable)
    }
}

fn not_found_or_unavailable<T>(status: Status) -> Result<Option<T>, ModerationError> {
    match status.code() {
        Code::NotFound | Code::InvalidArgument | Code::FailedPrecondition => Ok(None),
        _ => Err(ModerationError::ContentDirectoryUnavailable),
    }
}

#[async_trait]
impl SubjectResolver for GrpcSubjectResolver {
    async fn responsible_account(
        &self,
        entity_type: EntityType,
        entity_id: &str,
    ) -> Result<Option<ActorId>, ModerationError> {
        let author_profile = match entity_type {
            EntityType::Profile => entity_id.to_owned(),
            EntityType::Post => {
                match self.posts.clone().get_post(GetPostRequest { post_id: entity_id.to_owned() }).await {
                    Ok(r) => r.into_inner().profile_id,
                    Err(status) => return not_found_or_unavailable(status),
                }
            }
            EntityType::Comment => {
                match self
                    .comments
                    .clone()
                    .get_comment(GetCommentRequest { comment_id: entity_id.to_owned() })
                    .await
                {
                    Ok(r) => r.into_inner().author_id,
                    Err(status) => return not_found_or_unavailable(status),
                }
            }
            other => {
                return Err(ModerationError::UnsupportedReportTarget {
                    entity_type: other.as_str().to_owned(),
                });
            }
        };
        self.profile_owner(&author_profile).await
    }
}
