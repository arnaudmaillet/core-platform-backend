//! The comment read gate over gRPC: post `GetPost` (the post's own state) then
//! social-graph's mesh-only `CheckAccess` (the authors' audience) and
//! `ListRestrictedAmong` (commenters the post's owner restricted).

use std::collections::HashSet;

use async_trait::async_trait;
use post_api::post_service_client::PostServiceClient;
use post_api::{GetPostRequest, ModerationRestriction, PostStatus};
use social_graph_api::social_graph_service_client::SocialGraphServiceClient;
use social_graph_api::{
    CheckAccessRequest, CheckInteractionRequest, ContentAccess, InteractionKind, ListRestrictedAmongRequest,
};
use tonic::transport::Channel;
use tonic::Code;

use crate::application::port::{CommentAdmission, ReadGate};
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

/// `CheckAccess` caps per call (social-graph `MAX_VIEWERS` / `MAX_TARGETS`).
const MAX_VIEWERS_PER_CALL: usize = 20;
const MAX_TARGETS_PER_CALL: usize = 100;

/// The calls covering `viewers` × `targets` within the caps: one per (viewer
/// chunk, target chunk). An anonymous reader is one viewer chunk with no
/// profiles.
fn requests(viewers: &[ProfileId], targets: &[String]) -> Vec<CheckAccessRequest> {
    let viewer_chunks: Vec<&[ProfileId]> = if viewers.is_empty() {
        vec![&[]]
    } else {
        viewers.chunks(MAX_VIEWERS_PER_CALL).collect()
    };
    let mut out = Vec::new();
    for v in &viewer_chunks {
        for t in targets.chunks(MAX_TARGETS_PER_CALL) {
            out.push(CheckAccessRequest {
                viewer_profile_ids: v.iter().map(ProfileId::as_str).collect(),
                target_profile_ids: t.to_vec(),
            });
        }
    }
    out
}

/// One answer per target from the split calls, as one call over the whole
/// viewer set would give: HIDDEN if any viewer chunk says HIDDEN (or none
/// answered for it: fail closed), else VISIBLE if any says VISIBLE (any profile
/// follows), else HEADER_ONLY.
fn combine(targets: &[String], responses: &[Vec<(String, i32)>]) -> Vec<(String, i32)> {
    targets
        .iter()
        .map(|target| {
            let mut answered = false;
            let mut combined = ContentAccess::HeaderOnly;
            for (id, access) in responses.iter().flatten() {
                if id != target {
                    continue;
                }
                answered = true;
                match ContentAccess::try_from(*access) {
                    Ok(ContentAccess::Visible) => combined = ContentAccess::Visible,
                    Ok(ContentAccess::HeaderOnly) => {}
                    _ => return (target.clone(), ContentAccess::Hidden as i32),
                }
            }
            let combined = if answered { combined } else { ContentAccess::Hidden };
            (target.clone(), combined as i32)
        })
        .collect()
}

/// `ListRestrictedAmong` cap: candidates per call.
const MAX_RESTRICTION_CANDIDATES: usize = 100;

/// The commenters whose restriction by the post's owner must be checked for
/// this reader: none when the reader owns the post (the owner sees every
/// comment); otherwise every commenter not already hidden and not one of the
/// reader's own profiles (a restricted profile still sees its own comments).
fn restriction_candidates(
    viewers: &[ProfileId],
    post_author: &str,
    comment_authors: &[ProfileId],
    hidden: &HashSet<ProfileId>,
) -> Vec<String> {
    if viewers.iter().any(|v| v.as_str() == post_author) {
        return Vec::new();
    }
    comment_authors
        .iter()
        .filter(|a| !hidden.contains(*a) && !viewers.contains(a) && a.as_str() != post_author)
        .map(ProfileId::as_str)
        .collect()
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

        // CheckAccess for the post author and every comment author, split to
        // the RPC's caps (a full page of 100 commenters + the post author is
        // already over), then recombined per target.
        let mut targets: Vec<String> = comment_authors.iter().map(ProfileId::as_str).collect();
        targets.push(view.profile_id.clone());
        targets.sort();
        targets.dedup();
        let mut responses = Vec::new();
        for request in requests(viewers, &targets) {
            let response = self
                .social_graph
                .clone()
                .check_access(request)
                .await
                .map_err(unavailable)?
                .into_inner();
            responses.push(response.targets.into_iter().map(|t| (t.target_profile_id, t.access)).collect());
        }
        let answers = combine(&targets, &responses);
        let Some(mut hidden) = decide(&answers, viewers, &view.profile_id, comment_authors) else {
            return Ok(None);
        };

        // A commenter the post's owner restricted is seen by itself and the
        // owner only (fail closed like the rest of the gate).
        let candidates = restriction_candidates(viewers, &view.profile_id, comment_authors, &hidden);
        for chunk in candidates.chunks(MAX_RESTRICTION_CANDIDATES) {
            let response = self
                .social_graph
                .clone()
                .list_restricted_among(ListRestrictedAmongRequest {
                    owner_id:      view.profile_id.clone(),
                    candidate_ids: chunk.to_vec(),
                })
                .await
                .map_err(unavailable)?
                .into_inner();
            hidden.extend(response.restricted_ids.iter().filter_map(|id| ProfileId::try_from(id.as_str()).ok()));
        }
        Ok(Some(hidden))
    }

    async fn may_comment(&self, author: &ProfileId, post_id: &PostId) -> Result<CommentAdmission, CommentError> {
        let view = match self
            .post
            .clone()
            .get_post(GetPostRequest { post_id: post_id.as_str() })
            .await
        {
            Ok(response) => response.into_inner(),
            Err(status) if status.code() == Code::NotFound => return Ok(CommentAdmission::PostUnavailable),
            Err(status) => return Err(unavailable(status)),
        };
        // Commenting on your own post is always allowed.
        if view.profile_id == author.as_str() {
            return Ok(CommentAdmission::Allowed);
        }
        if !post_is_public(&view) {
            return Ok(CommentAdmission::PostUnavailable);
        }
        // Readable for this commenter (a private author they don't follow, a
        // block or a hidden author ⇒ not)…
        let access = self
            .social_graph
            .clone()
            .check_access(CheckAccessRequest {
                viewer_profile_ids: vec![author.as_str()],
                target_profile_ids: vec![view.profile_id.clone()],
            })
            .await
            .map_err(unavailable)?
            .into_inner();
        let visible = access
            .targets
            .iter()
            .any(|t| t.target_profile_id == view.profile_id && t.access == ContentAccess::Visible as i32);
        if !visible {
            return Ok(CommentAdmission::PostUnavailable);
        }
        // …and its author takes comments from them.
        let allowed = self
            .social_graph
            .clone()
            .check_interaction(CheckInteractionRequest {
                actor_profile_id: author.as_str(),
                target_profile_id: view.profile_id,
                kind: InteractionKind::Comment as i32,
            })
            .await
            .map_err(unavailable)?
            .into_inner()
            .allowed;
        Ok(if allowed { CommentAdmission::Allowed } else { CommentAdmission::Restricted })
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
    fn restrictions_are_checked_for_other_readers_only_and_never_hide_ones_own_comments() {
        let (owner, reader, restricted, blocked) = (id(), id(), id(), id());
        let authors = [owner.clone(), reader.clone(), restricted.clone(), blocked.clone()];
        let hidden: HashSet<ProfileId> = [blocked].into();

        // The post's owner sees every comment: nothing to check.
        assert!(restriction_candidates(std::slice::from_ref(&owner), &owner.as_str(), &authors, &hidden).is_empty());

        // Another reader: every commenter but itself, the owner and those
        // already hidden.
        let candidates = restriction_candidates(std::slice::from_ref(&reader), &owner.as_str(), &authors, &hidden);
        assert_eq!(candidates, vec![restricted.as_str()]);

        // The restricted profile reading: its own comments are not candidates.
        let own = restriction_candidates(std::slice::from_ref(&restricted), &owner.as_str(), &authors, &hidden);
        assert_eq!(own, vec![reader.as_str()]);
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
    fn a_full_page_of_commenters_stays_within_the_rpc_caps() {
        // 100 distinct commenters + the post author, read by an account with 25
        // profiles: 2 viewer chunks × 2 target chunks, each within the caps.
        let viewers: Vec<ProfileId> = (0..25).map(|_| id()).collect();
        let targets: Vec<String> = (0..101).map(|_| id().as_str()).collect();
        let calls = requests(&viewers, &targets);
        assert_eq!(calls.len(), 4);
        for call in &calls {
            assert!(call.viewer_profile_ids.len() <= MAX_VIEWERS_PER_CALL);
            assert!(call.target_profile_ids.len() <= MAX_TARGETS_PER_CALL);
        }
        let covered: HashSet<&String> = calls.iter().flat_map(|c| &c.target_profile_ids).collect();
        assert_eq!(covered.len(), 101, "every target asked");
        // Anonymous: one viewer chunk with no profiles.
        assert_eq!(requests(&[], &targets).len(), 2);
    }

    #[test]
    fn split_answers_combine_like_one_call() {
        let t = |s: &str| s.to_owned();
        let (v, h, ho) = (ContentAccess::Visible as i32, ContentAccess::Hidden as i32, ContentAccess::HeaderOnly as i32);
        let targets = [t("a"), t("b"), t("c"), t("d")];
        let responses = vec![
            vec![(t("a"), ho), (t("b"), v), (t("c"), ho)],
            vec![(t("a"), v), (t("b"), h), (t("c"), ho)],
        ];
        let combined: std::collections::HashMap<String, i32> = combine(&targets, &responses).into_iter().collect();
        assert_eq!(combined["a"], v, "a follower in another chunk");
        assert_eq!(combined["b"], h, "a block in any chunk");
        assert_eq!(combined["c"], ho);
        assert_eq!(combined["d"], h, "never answered: fail closed");
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
