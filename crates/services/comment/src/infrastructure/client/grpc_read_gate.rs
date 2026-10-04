//! The comment read gate over gRPC: post `GetPost` (the post's own state) then
//! social-graph's mesh-only `CheckAccess` (the authors' audience).

use std::collections::HashSet;

use async_trait::async_trait;
use post_api::post_service_client::PostServiceClient;
use post_api::{GetPostRequest, ModerationRestriction, PostStatus};
use social_graph_api::social_graph_service_client::SocialGraphServiceClient;
use social_graph_api::{CheckAccessRequest, ContentAccess};
use tonic::transport::Channel;
use tonic::Code;

use crate::application::port::ReadGate;
use crate::domain::value_object::{PostId, ProfileId, Viewer};
use crate::error::CommentError;

/// Both channels are `Arc`-backed and must carry request timeouts (tonic has
/// none by default).
pub struct GrpcReadGate {
    post:         PostServiceClient<Channel>,
    social_graph: SocialGraphServiceClient<Channel>,
}

impl GrpcReadGate {
    pub fn new(post: Channel, social_graph: Channel) -> Self {
        Self {
            post:         PostServiceClient::new(post),
            social_graph: SocialGraphServiceClient::new(social_graph),
        }
    }
}

fn unavailable(status: tonic::Status) -> CommentError {
    CommentError::AccessCheckUnavailable { reason: status.to_string() }
}

/// Whether the post's own state lets anyone but its author read it.
fn post_is_public(view: &post_api::PostView) -> bool {
    view.status == PostStatus::Published as i32
        && view.moderation != ModerationRestriction::Removed as i32
}

/// Applies the access answers: `None` when the post author is not VISIBLE to
/// the reader; otherwise the comment authors that are HIDDEN (or unanswered:
/// fail closed). One's own profiles are never hidden from oneself.
fn decide(
    answers: &[(String, i32)],
    viewers: &[ProfileId],
    post_author: &str,
    comment_authors: &[ProfileId],
) -> Option<HashSet<ProfileId>> {
    let access = |id: &str| {
        answers
            .iter()
            .find(|(target, _)| target == id)
            .and_then(|(_, a)| ContentAccess::try_from(*a).ok())
    };
    let own = |id: &str| viewers.iter().any(|v| v.as_str() == id);
    if !own(post_author) && access(post_author) != Some(ContentAccess::Visible) {
        return None;
    }
    Some(
        comment_authors
            .iter()
            .filter(|a| {
                let id = a.as_str();
                !own(&id)
                    && !matches!(
                        access(&id),
                        Some(ContentAccess::Visible) | Some(ContentAccess::HeaderOnly)
                    )
            })
            .cloned()
            .collect(),
    )
}

#[async_trait]
impl ReadGate for GrpcReadGate {
    async fn check(
        &self,
        viewer: &Viewer,
        post_id: &PostId,
        comment_authors: &[ProfileId],
    ) -> Result<Option<HashSet<ProfileId>>, CommentError> {
        let viewers: &[ProfileId] = match viewer {
            Viewer::Internal => return Ok(Some(HashSet::new())),
            Viewer::Profiles(ids) => ids,
        };

        // The post's own state, read over the mesh (unfiltered), then judged
        // here for this reader: only its author reads a draft or a takedown.
        let view = match self
            .post
            .clone()
            .get_post(GetPostRequest { post_id: post_id.as_str() })
            .await
        {
            Ok(response) => response.into_inner(),
            Err(status) if status.code() == Code::NotFound => return Ok(None),
            Err(status) => return Err(unavailable(status)),
        };
        let reader_is_author = viewers.iter().any(|v| v.as_str() == view.profile_id);
        if !reader_is_author && !post_is_public(&view) {
            return Ok(None);
        }

        // One CheckAccess for the post author and every comment author.
        let mut targets: Vec<String> = comment_authors.iter().map(ProfileId::as_str).collect();
        targets.push(view.profile_id.clone());
        targets.sort();
        targets.dedup();
        let response = self
            .social_graph
            .clone()
            .check_access(CheckAccessRequest {
                viewer_profile_ids: viewers.iter().map(ProfileId::as_str).collect(),
                target_profile_ids: targets,
            })
            .await
            .map_err(unavailable)?
            .into_inner();
        let answers: Vec<(String, i32)> = response
            .targets
            .into_iter()
            .map(|t| (t.target_profile_id, t.access))
            .collect();
        Ok(decide(&answers, viewers, &view.profile_id, comment_authors))
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;

    fn id() -> ProfileId {
        ProfileId::try_from(Uuid::now_v7().to_string().as_str()).unwrap()
    }

    fn answer(p: &ProfileId, a: ContentAccess) -> (String, i32) {
        (p.as_str(), a as i32)
    }

    #[test]
    fn the_post_author_must_be_visible() {
        let (author, reader) = (id(), id());
        for a in [ContentAccess::HeaderOnly, ContentAccess::Hidden] {
            assert_eq!(decide(&[answer(&author, a)], std::slice::from_ref(&reader), &author.as_str(), &[]), None);
        }
        assert_eq!(decide(&[], std::slice::from_ref(&reader), &author.as_str(), &[]), None, "no answer");
        assert!(decide(&[answer(&author, ContentAccess::Visible)], &[reader], &author.as_str(), &[]).is_some());
        assert!(decide(&[], std::slice::from_ref(&author), &author.as_str(), &[]).is_some(), "the author");
    }

    #[test]
    fn hidden_or_unanswered_comment_authors_are_dropped_private_ones_kept() {
        let (author, reader, blocked, private, silent) = (id(), id(), id(), id(), id());
        let answers = [
            answer(&author, ContentAccess::Visible),
            answer(&blocked, ContentAccess::Hidden),
            answer(&private, ContentAccess::HeaderOnly),
        ];
        let hidden = decide(
            &answers,
            std::slice::from_ref(&reader),
            &author.as_str(),
            &[blocked.clone(), private, silent.clone(), reader.clone()],
        )
        .unwrap();
        assert_eq!(hidden, HashSet::from([blocked, silent]), "the reader's own comments stay");
    }

    #[test]
    fn only_published_and_not_removed_posts_are_public() {
        let view = |status: PostStatus, moderation: ModerationRestriction| post_api::PostView {
            status: status as i32,
            moderation: moderation as i32,
            ..Default::default()
        };
        assert!(post_is_public(&view(PostStatus::Published, ModerationRestriction::None)));
        assert!(post_is_public(&view(PostStatus::Published, ModerationRestriction::Limited)));
        assert!(!post_is_public(&view(PostStatus::Published, ModerationRestriction::Removed)));
        assert!(!post_is_public(&view(PostStatus::Draft, ModerationRestriction::None)));
        assert!(!post_is_public(&view(PostStatus::Deleted, ModerationRestriction::None)));
    }
}
