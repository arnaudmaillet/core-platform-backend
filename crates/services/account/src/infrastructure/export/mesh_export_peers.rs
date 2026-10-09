//! The export's sources over the mesh (#653): each service's mesh RPCs, every
//! page, each message transcoded to JSON through the service's own descriptor
//! set (field names as in the proto) — the archive shows the data as the
//! service holds it. Any RPC failure fails the export (retried next pass).

use std::time::Duration as StdDuration;

use async_trait::async_trait;
use chrono::Duration;
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage, SerializeOptions};
use serde_json::json;
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Status};

use crate::application::port::{ConversationExport, ExportPeers, MessageExport};
use crate::domain::value_object::AccountId;
use crate::error::AccountError;

use chat_api::chat_service_client::ChatServiceClient;
use comment_api::comment_service_client::CommentServiceClient;
use engagement_api::engagement_service_client::EngagementServiceClient;
use media_api::media_service_client::MediaServiceClient;
use post_api::post_service_client::PostServiceClient;
use profile_api::profile_service_client::ProfileServiceClient;
use search_api::search_service_client::SearchServiceClient;
use social_graph_api::social_graph_service_client::SocialGraphServiceClient;
use tonic::service::interceptor::InterceptedService;
use transport::grpc::mesh::MeshTokenInterceptor;
use wallet_api::wallet_service_client::WalletServiceClient;

/// Items per page when walking a listing.
const PAGE: i32 = 100;

/// The mesh endpoints (`ACCOUNT_<SERVICE>_GRPC_ENDPOINT`).
#[derive(Debug, Clone)]
pub struct MeshEndpoints {
    pub profile: String,
    pub post: String,
    pub comment: String,
    pub engagement: String,
    pub social_graph: String,
    pub chat: String,
    pub media: String,
    pub search: String,
    /// Family supervision's view of a teen's reports (#670).
    pub moderation: String,
    /// The wallet (#665); `None` until the mesh route exists: the export goes
    /// without `wallet.json`.
    pub wallet: Option<String>,
}

impl MeshEndpoints {
    pub fn from_env() -> Self {
        let var = |name: &str, default: &str| {
            std::env::var(name).ok().filter(|v| !v.trim().is_empty()).unwrap_or_else(|| default.to_owned())
        };
        Self {
            profile: var("ACCOUNT_PROFILE_GRPC_ENDPOINT", "http://localhost:50052"),
            post: var("ACCOUNT_POST_GRPC_ENDPOINT", "http://localhost:50056"),
            comment: var("ACCOUNT_COMMENT_GRPC_ENDPOINT", "http://localhost:50057"),
            engagement: var("ACCOUNT_ENGAGEMENT_GRPC_ENDPOINT", "http://localhost:50058"),
            social_graph: var("ACCOUNT_SOCIAL_GRAPH_GRPC_ENDPOINT", "http://localhost:50053"),
            chat: var("ACCOUNT_CHAT_GRPC_ENDPOINT", "http://localhost:50051"),
            media: var("ACCOUNT_MEDIA_GRPC_ENDPOINT", "http://localhost:50063"),
            search: var("ACCOUNT_SEARCH_GRPC_ENDPOINT", "http://localhost:50062"),
            moderation: var("ACCOUNT_MODERATION_GRPC_ENDPOINT", "http://localhost:50061"),
            wallet: std::env::var("ACCOUNT_WALLET_GRPC_ENDPOINT").ok().filter(|v| !v.trim().is_empty()),
        }
    }
}

/// One service's descriptors, to transcode its messages.
struct Descriptors(DescriptorPool);

impl Descriptors {
    fn of(set: &[u8]) -> Result<Self, AccountError> {
        DescriptorPool::decode(set).map(Self).map_err(|e| unavailable("descriptors", e))
    }

    /// `message` (of proto type `full_name`) as JSON, proto field names.
    fn json<M: Message>(&self, full_name: &str, message: &M) -> serde_json::Value {
        let Some(descriptor) = self.0.get_message_by_name(full_name) else {
            return json!({ "unreadable": full_name });
        };
        match DynamicMessage::decode(descriptor, message.encode_to_vec().as_slice()) {
            Ok(dynamic) => {
                let options = SerializeOptions::new().use_proto_field_name(true).skip_default_fields(false);
                dynamic.serialize_with_options(serde_json::value::Serializer, &options).unwrap_or_default()
            }
            Err(_) => json!({ "unreadable": full_name }),
        }
    }
}

fn unavailable(what: &str, e: impl std::fmt::Display) -> AccountError {
    AccountError::DataExportUnavailable { reason: format!("{what}: {e}") }
}

fn rpc(service: &'static str) -> impl Fn(Status) -> AccountError {
    move |status| unavailable(service, format!("{} {}", status.code(), status.message()))
}

fn page_token(next: String) -> Option<String> {
    Some(next).filter(|t| !t.is_empty())
}

pub struct MeshExportPeers {
    profile: ProfileServiceClient<Channel>,
    post: PostServiceClient<Channel>,
    comment: CommentServiceClient<Channel>,
    engagement: EngagementServiceClient<Channel>,
    social: SocialGraphServiceClient<Channel>,
    chat: ChatServiceClient<Channel>,
    media: MediaServiceClient<Channel>,
    search: SearchServiceClient<Channel>,
    /// Carries this pod's mesh token: the wallet checks who exports (#852).
    wallet: Option<WalletServiceClient<InterceptedService<Channel, MeshTokenInterceptor>>>,
    profile_d: Descriptors,
    post_d: Descriptors,
    comment_d: Descriptors,
    engagement_d: Descriptors,
    social_d: Descriptors,
    chat_d: Descriptors,
    media_d: Descriptors,
    search_d: Descriptors,
    wallet_d: Descriptors,
}

impl MeshExportPeers {
    /// Lazily connected channels, with deadlines: an unreachable service
    /// fails its section, the export retries.
    pub fn new(endpoints: &MeshEndpoints) -> Result<Self, AccountError> {
        let channel = |uri: &str| -> Result<Channel, AccountError> {
            Ok(Endpoint::from_shared(uri.to_owned())
                .map_err(|e| unavailable("endpoint", e))?
                .timeout(StdDuration::from_secs(10))
                .connect_timeout(StdDuration::from_secs(3))
                .connect_lazy())
        };
        Ok(Self {
            profile: ProfileServiceClient::new(channel(&endpoints.profile)?),
            post: PostServiceClient::new(channel(&endpoints.post)?),
            comment: CommentServiceClient::new(channel(&endpoints.comment)?),
            engagement: EngagementServiceClient::new(channel(&endpoints.engagement)?),
            social: SocialGraphServiceClient::new(channel(&endpoints.social_graph)?),
            chat: ChatServiceClient::new(channel(&endpoints.chat)?),
            media: MediaServiceClient::new(channel(&endpoints.media)?),
            search: SearchServiceClient::new(channel(&endpoints.search)?),
            wallet: endpoints
                .wallet
                .as_deref()
                .map(channel)
                .transpose()?
                .map(|c| WalletServiceClient::with_interceptor(c, MeshTokenInterceptor::from_env())),
            profile_d: Descriptors::of(profile_api::FILE_DESCRIPTOR_SET)?,
            post_d: Descriptors::of(post_api::FILE_DESCRIPTOR_SET)?,
            comment_d: Descriptors::of(comment_api::FILE_DESCRIPTOR_SET)?,
            engagement_d: Descriptors::of(engagement_api::FILE_DESCRIPTOR_SET)?,
            social_d: Descriptors::of(social_graph_api::FILE_DESCRIPTOR_SET)?,
            chat_d: Descriptors::of(chat_api::FILE_DESCRIPTOR_SET)?,
            media_d: Descriptors::of(media_api::FILE_DESCRIPTOR_SET)?,
            search_d: Descriptors::of(search_api::FILE_DESCRIPTOR_SET)?,
            wallet_d: Descriptors::of(wallet_api::FILE_DESCRIPTOR_SET)?,
        })
    }
}

#[async_trait]
impl ExportPeers for MeshExportPeers {
    async fn profiles(&self, account_id: &AccountId) -> Result<Vec<(String, serde_json::Value)>, AccountError> {
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
                .map_err(rpc("profile"))?
                .into_inner();
            ids.extend(page.profiles.into_iter().map(|p| p.profile_id));
            match page_token(page.next_page_token) {
                Some(next) => token = next,
                None => break,
            }
        }
        let mut profiles = Vec::with_capacity(ids.len());
        for id in ids {
            let view = self
                .profile
                .clone()
                .get_profile_by_id(profile_api::GetProfileByIdRequest { profile_id: id.clone() })
                .await;
            match view {
                Ok(view) => profiles.push((id, self.profile_d.json("profile.v1.ProfileView", &view.into_inner()))),
                // Deleted meanwhile: nothing to export.
                Err(status) if status.code() == Code::NotFound => {}
                Err(status) => return Err(rpc("profile")(status)),
            }
        }
        Ok(profiles)
    }

    async fn posts(&self, profile_id: &str) -> Result<Vec<serde_json::Value>, AccountError> {
        let (mut ids, mut token) = (Vec::new(), String::new());
        loop {
            let page = self
                .post
                .clone()
                .list_posts_by_profile(post_api::ListPostsByProfileRequest {
                    profile_id: profile_id.to_owned(),
                    limit: PAGE,
                    page_token: token,
                })
                .await
                .map_err(rpc("post"))?
                .into_inner();
            ids.extend(page.posts.into_iter().map(|p| p.post_id));
            match page_token(page.next_token) {
                Some(next) => token = next,
                None => break,
            }
        }
        let mut posts = Vec::with_capacity(ids.len());
        for post_id in ids {
            // On the holder's behalf: their own locations, whatever their
            // sharing (#657).
            let request = post_api::GetPostRequest { post_id, as_author_id: profile_id.to_owned() };
            match self.post.clone().get_post(request).await {
                Ok(view) => posts.push(self.post_d.json("post.v1.PostView", &view.into_inner())),
                Err(status) if status.code() == Code::NotFound => {}
                Err(status) => return Err(rpc("post")(status)),
            }
        }
        Ok(posts)
    }

    async fn comments(&self, profile_id: &str) -> Result<Vec<serde_json::Value>, AccountError> {
        let (mut comments, mut token) = (Vec::new(), String::new());
        loop {
            let page = self
                .comment
                .clone()
                .list_comments_by_author(comment_api::ListCommentsByAuthorRequest {
                    author_id: profile_id.to_owned(),
                    limit: PAGE,
                    page_token: token,
                })
                .await
                .map_err(rpc("comment"))?
                .into_inner();
            comments.extend(page.comments.iter().map(|c| self.comment_d.json("comment.v1.CommentView", c)));
            match page_token(page.next_token) {
                Some(next) => token = next,
                None => return Ok(comments),
            }
        }
    }

    /// search's `ListRecentSearches` over the mesh (#816): `require_profile`
    /// accepts a mesh caller, so no separate RPC is needed.
    async fn recent_searches(&self, profile_id: &str) -> Result<Vec<serde_json::Value>, AccountError> {
        let response = self
            .search
            .clone()
            .list_recent_searches(search_api::ListRecentSearchesRequest { profile_id: profile_id.to_owned() })
            .await
            .map_err(rpc("search"))?
            .into_inner();
        Ok(response.searches.iter().map(|s| self.search_d.json("search.v1.RecentSearch", s)).collect())
    }

    async fn social(&self, profile_id: &str) -> Result<serde_json::Value, AccountError> {
        let mut following = Vec::new();
        let mut token = String::new();
        loop {
            let page = self
                .social
                .clone()
                .list_following(social_graph_api::ListFollowingRequest {
                    follower_id: profile_id.to_owned(),
                    limit: PAGE,
                    page_token: token,
                })
                .await
                .map_err(rpc("social-graph"))?
                .into_inner();
            following.extend(page.following.iter().map(|e| self.social_d.json("social_graph.v1.EdgeSummary", e)));
            match page_token(page.next_page_token) {
                Some(next) => token = next,
                None => break,
            }
        }
        let mut followers = Vec::new();
        let mut token = String::new();
        loop {
            let page = self
                .social
                .clone()
                .list_followers(social_graph_api::ListFollowersRequest {
                    followee_id: profile_id.to_owned(),
                    limit: PAGE,
                    page_token: token,
                })
                .await
                .map_err(rpc("social-graph"))?
                .into_inner();
            followers.extend(page.followers.iter().map(|e| self.social_d.json("social_graph.v1.EdgeSummary", e)));
            match page_token(page.next_page_token) {
                Some(next) => token = next,
                None => break,
            }
        }
        let mut blocks = Vec::new();
        let mut token = String::new();
        loop {
            let page = self
                .social
                .clone()
                .list_blocks(social_graph_api::ListBlocksRequest {
                    blocker_id: profile_id.to_owned(),
                    limit: PAGE,
                    page_token: token,
                })
                .await
                .map_err(rpc("social-graph"))?
                .into_inner();
            blocks.extend(page.blocks.iter().map(|b| self.social_d.json("social_graph.v1.BlockSummary", b)));
            match page_token(page.next_page_token) {
                Some(next) => token = next,
                None => break,
            }
        }
        Ok(json!({ "following": following, "followers": followers, "blocks": blocks }))
    }

    async fn conversations(&self, profile_id: &str) -> Result<Vec<ConversationExport>, AccountError> {
        let (mut conversations, mut token) = (Vec::new(), String::new());
        loop {
            let page = self
                .chat
                .clone()
                .list_conversations_by_member(chat_api::ListConversationsByMemberRequest {
                    member_id: profile_id.to_owned(),
                    limit: PAGE,
                    page_token: token,
                })
                .await
                .map_err(rpc("chat"))?
                .into_inner();
            conversations.extend(page.memberships.iter().map(|m| ConversationExport {
                conversation_id: m.conversation_id.clone(),
                membership: self.chat_d.json("chat.v1.MembershipView", m),
                direct: m.kind == chat_api::ConversationKind::Direct as i32,
                left: m.left_at_ms > 0,
            }));
            match page_token(page.next_page_token) {
                Some(next) => token = next,
                None => return Ok(conversations),
            }
        }
    }

    async fn members(&self, conversation_id: &str, as_member: &str) -> Result<Vec<String>, AccountError> {
        let members = self
            .chat
            .clone()
            .list_members(chat_api::ListMembersRequest {
                conversation_id: conversation_id.to_owned(),
                requester_id: as_member.to_owned(),
            })
            .await
            .map_err(rpc("chat"))?
            .into_inner();
        Ok(members.members.into_iter().map(|m| m.profile_id).collect())
    }

    async fn messages(&self, conversation: &ConversationExport, as_member: &str) -> Result<Vec<MessageExport>, AccountError> {
        let conversation_id = &conversation.conversation_id;
        let (mut messages, mut token) = (Vec::new(), String::new());
        loop {
            // A direct conversation whole; anything else as chat reduces it
            // for the export (others' messages to their time, up to the
            // departure for a group the holder left).
            let page = if conversation.direct {
                self.chat
                    .clone()
                    .get_history(chat_api::GetHistoryRequest {
                        conversation_id: conversation_id.clone(),
                        requester_id: as_member.to_owned(),
                        limit: PAGE,
                        page_token: token,
                    })
                    .await
            } else {
                self.chat
                    .clone()
                    .get_former_member_history(chat_api::GetFormerMemberHistoryRequest {
                        conversation_id: conversation_id.clone(),
                        member_id: as_member.to_owned(),
                        limit: PAGE,
                        page_token: token,
                    })
                    .await
            }
            .map_err(rpc("chat"))?
            .into_inner();
            messages.extend(page.messages.iter().map(|m| MessageExport {
                sender_id: m.sender_id.clone(),
                created_at_ms: m.created_at_ms,
                message: self.chat_d.json("chat.v1.MessageView", m),
            }));
            match page_token(page.next_page_token) {
                Some(next) => token = next,
                None => return Ok(messages),
            }
        }
    }

    /// engagement's `ListLikesByAccount` (#665): mesh only.
    async fn likes(&self, account_id: &AccountId) -> Result<Vec<serde_json::Value>, AccountError> {
        let (mut likes, mut token) = (Vec::new(), String::new());
        loop {
            let page = self
                .engagement
                .clone()
                .list_likes_by_account(engagement_api::ListLikesByAccountRequest {
                    account_id: account_id.as_uuid().to_string(),
                    limit: PAGE,
                    page_token: token,
                })
                .await
                .map_err(rpc("engagement"))?
                .into_inner();
            likes.extend(page.likes.iter().map(|l| self.engagement_d.json("engagement.v1.AccountLikeView", l)));
            match page_token(page.next_page_token) {
                Some(next) => token = next,
                None => return Ok(likes),
            }
        }
    }

    /// wallet's mesh-only `ExportWallet` and `ListStakePositions`, and
    /// `ListWalletTransactions` (read-only; the mesh passes its account
    /// binding), every page (#665).
    async fn wallet(&self, account_id: &AccountId) -> Result<Option<serde_json::Value>, AccountError> {
        let Some(client) = &self.wallet else { return Ok(None) };
        let account = account_id.as_uuid().to_string();
        let wallet = client
            .clone()
            .export_wallet(wallet_api::ExportWalletRequest { account_id: account.clone() })
            .await
            .map_err(rpc("wallet"))?
            .into_inner()
            .wallet
            .map(|w| self.wallet_d.json("wallet.v1.Wallet", &w));
        let (mut transactions, mut token) = (Vec::new(), String::new());
        loop {
            let page = client
                .clone()
                .list_wallet_transactions(wallet_api::ListWalletTransactionsRequest {
                    account_id: account.clone(),
                    page_size: PAGE,
                    page_token: token,
                    ..Default::default()
                })
                .await
                .map_err(rpc("wallet"))?
                .into_inner();
            transactions.extend(page.transactions.iter().map(|t| self.wallet_d.json("wallet.v1.WalletTransaction", t)));
            match page_token(page.next_page_token) {
                Some(next) => token = next,
                None => break,
            }
        }
        let (mut positions, mut token) = (Vec::new(), String::new());
        loop {
            let page = client
                .clone()
                .list_stake_positions(wallet_api::ListStakePositionsRequest {
                    account_id: account.clone(),
                    page_size: PAGE * 5,
                    page_token: token,
                })
                .await
                .map_err(rpc("wallet"))?
                .into_inner();
            positions.extend(page.positions.iter().map(|p| self.wallet_d.json("wallet.v1.StakePosition", p)));
            match page_token(page.next_page_token) {
                Some(next) => token = next,
                None => break,
            }
        }
        Ok(Some(json!({ "wallet": wallet, "transactions": transactions, "stake_positions": positions })))
    }

    async fn media(&self, account_id: &AccountId, ttl: Duration) -> Result<Vec<serde_json::Value>, AccountError> {
        let (mut media, mut token) = (Vec::new(), String::new());
        loop {
            let page = self
                .media
                .clone()
                .list_assets_by_owner(media_api::ListAssetsByOwnerRequest {
                    owner_id: account_id.as_uuid().to_string(),
                    limit: PAGE,
                    page_token: token,
                    url_ttl_secs: ttl.num_seconds(),
                })
                .await
                .map_err(rpc("media"))?
                .into_inner();
            media.extend(page.assets.iter().map(|a| self.media_d.json("media.v1.OwnedAsset", a)));
            match page_token(page.next_page_token) {
                Some(next) => token = next,
                None => return Ok(media),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every service's messages transcode with their proto field names.
    #[test]
    fn messages_transcode_to_json_with_proto_field_names() {
        let chat = Descriptors::of(chat_api::FILE_DESCRIPTOR_SET).unwrap();
        let message = chat_api::MessageView {
            message_id: "m1".into(),
            sender_id: "p1".into(),
            body: "hello".into(),
            created_at_ms: 42,
            ..Default::default()
        };
        let json = chat.json("chat.v1.MessageView", &message);
        assert_eq!(json["sender_id"], "p1");
        assert_eq!(json["body"], "hello");
        assert_eq!(json["created_at_ms"], "42", "int64 as a JSON string (proto3 JSON mapping)");
        for set in [
            profile_api::FILE_DESCRIPTOR_SET,
            post_api::FILE_DESCRIPTOR_SET,
            comment_api::FILE_DESCRIPTOR_SET,
            engagement_api::FILE_DESCRIPTOR_SET,
            social_graph_api::FILE_DESCRIPTOR_SET,
            media_api::FILE_DESCRIPTOR_SET,
        ] {
            assert!(Descriptors::of(set).is_ok(), "every descriptor set decodes");
        }
        assert_eq!(chat.json("chat.v1.Nope", &message), json!({ "unreadable": "chat.v1.Nope" }));
    }
}
