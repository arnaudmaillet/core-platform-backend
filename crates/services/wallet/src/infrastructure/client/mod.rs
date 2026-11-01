//! What likes land on, over the mesh: post (`GetPost`), comment
//! (`GetComment`), and whether the one who likes may see it (social-graph
//! `CheckAccess`). Any failure but NOT_FOUND is `WAL-6001` (fail closed).
//! What they came to, for the settlement: engagement's `GetLikePositions`.
//! Uploaded loaders' assets (#975): media's `GetAsset` / `DeleteAsset`.

use async_trait::async_trait;
use tonic::transport::Channel;
use tonic::Code;

use comment_api::comment_service_client::CommentServiceClient;
use engagement_api::engagement_service_client::EngagementServiceClient;
use media_api::media_service_client::MediaServiceClient;
use post_api::post_service_client::PostServiceClient;
use social_graph_api::social_graph_service_client::SocialGraphServiceClient;

use crate::application::port::{AudienceCheck, LikePositions, LoaderAssetStatus, LoaderAssets, TargetDirectory, TargetInfo};
use crate::domain::{AccountId, Observed, StakeTarget};
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

pub struct GrpcLikePositions {
    engagement: EngagementServiceClient<Channel>,
}

impl GrpcLikePositions {
    /// `channel` must carry request and connect timeouts.
    pub fn new(channel: Channel) -> Self {
        Self { engagement: EngagementServiceClient::new(channel) }
    }
}

fn like_target(target: &StakeTarget) -> engagement_api::LikeTarget {
    use engagement_api::like_target::Target;
    engagement_api::LikeTarget {
        target: Some(match target {
            StakeTarget::Post(id) => Target::PostId(id.clone()),
            StakeTarget::Comment(id) => Target::CommentId(id.clone()),
        }),
    }
}

#[async_trait]
impl LikePositions for GrpcLikePositions {
    async fn positions(&self, account: &AccountId, targets: &[StakeTarget]) -> Result<Vec<Observed>, WalletError> {
        let answer = self
            .engagement
            .clone()
            .get_like_positions(engagement_api::GetLikePositionsRequest {
                account_id: account.as_uuid().to_string(),
                targets:    targets.iter().map(like_target).collect(),
            })
            .await
            .map_err(unavailable("engagement"))?
            .into_inner();
        Ok(answer
            .positions
            .into_iter()
            .map(|p| Observed { total: p.total, count_on_arrival: p.count_on_arrival, count_now: p.count_now })
            .collect())
    }
}

pub struct GrpcLoaderAssets {
    media: MediaServiceClient<Channel>,
}

impl GrpcLoaderAssets {
    /// `channel` must carry request and connect timeouts.
    pub fn new(channel: Channel) -> Self {
        Self { media: MediaServiceClient::new(channel) }
    }
}

/// Where a media asset stands as `account`'s loader.
fn loader_status(asset: &media_api::Asset, account: &AccountId) -> LoaderAssetStatus {
    use media_api::AssetState;
    if asset.kind != media_api::MediaKind::Loader as i32 || asset.owner_id != account.as_uuid().to_string() {
        return LoaderAssetStatus::NotALoader;
    }
    match AssetState::try_from(asset.state) {
        Ok(AssetState::MediaAssetStateReady) => LoaderAssetStatus::Ready,
        Ok(AssetState::MediaAssetStatePending | AssetState::MediaAssetStateUploaded | AssetState::MediaAssetStateProcessing) => LoaderAssetStatus::NotReady,
        Ok(AssetState::MediaAssetStateFailed | AssetState::MediaAssetStateQuarantined) => LoaderAssetStatus::Rejected,
        _ => LoaderAssetStatus::NotALoader,
    }
}

#[async_trait]
impl LoaderAssets for GrpcLoaderAssets {
    async fn status(&self, account: &AccountId, asset_id: &uuid::Uuid) -> Result<LoaderAssetStatus, WalletError> {
        let request = media_api::GetAssetRequest { asset_id: asset_id.to_string() };
        match self.media.clone().get_asset(request).await {
            Ok(answer) => Ok(answer.into_inner().asset.map_or(LoaderAssetStatus::NotALoader, |a| loader_status(&a, account))),
            Err(status) if matches!(status.code(), Code::NotFound | Code::InvalidArgument) => Ok(LoaderAssetStatus::NotALoader),
            Err(status) => Err(unavailable("media")(status)),
        }
    }

    async fn delete(&self, account: &AccountId, asset_id: &uuid::Uuid) -> Result<(), WalletError> {
        let request = media_api::DeleteAssetRequest { asset_id: asset_id.to_string(), owner_id: account.as_uuid().to_string() };
        match self.media.clone().delete_asset(request).await {
            // Gone already, or kept as evidence by a legal hold (MED-7003):
            // the loader goes either way.
            Ok(_) => Ok(()),
            Err(status) if matches!(status.code(), Code::NotFound | Code::PermissionDenied) => Ok(()),
            Err(status) => Err(unavailable("media")(status)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// #975: only the account's own LOADER counts; READY imports, the
    /// pipeline's states wait, a failed or blocked one is rejected.
    #[test]
    fn a_media_asset_is_a_loader_of_its_owner_only() {
        use media_api::{AssetState, MediaKind};
        let account = AccountId::from_uuid(uuid::Uuid::now_v7());
        let asset = |kind: MediaKind, state: AssetState, owner: String| media_api::Asset {
            kind: kind as i32,
            state: state as i32,
            owner_id: owner,
            ..Default::default()
        };
        let mine = || account.as_uuid().to_string();
        assert_eq!(loader_status(&asset(MediaKind::Loader, AssetState::MediaAssetStateReady, mine()), &account), LoaderAssetStatus::Ready);
        assert_eq!(loader_status(&asset(MediaKind::Loader, AssetState::MediaAssetStateProcessing, mine()), &account), LoaderAssetStatus::NotReady);
        assert_eq!(loader_status(&asset(MediaKind::Loader, AssetState::MediaAssetStateQuarantined, mine()), &account), LoaderAssetStatus::Rejected);
        assert_eq!(loader_status(&asset(MediaKind::Loader, AssetState::MediaAssetStateDeleted, mine()), &account), LoaderAssetStatus::NotALoader);
        assert_eq!(loader_status(&asset(MediaKind::Avatar, AssetState::MediaAssetStateReady, mine()), &account), LoaderAssetStatus::NotALoader);
        let stranger = uuid::Uuid::now_v7().to_string();
        assert_eq!(loader_status(&asset(MediaKind::Loader, AssetState::MediaAssetStateReady, stranger), &account), LoaderAssetStatus::NotALoader);
    }

    #[test]
    fn a_stake_target_is_engagements_like_target() {
        use engagement_api::like_target::Target;
        assert_eq!(like_target(&StakeTarget::Post("p".into())).target, Some(Target::PostId("p".into())));
        assert_eq!(like_target(&StakeTarget::Comment("c".into())).target, Some(Target::CommentId("c".into())));
    }
}
