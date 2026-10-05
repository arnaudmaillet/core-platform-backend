//! In-process `post` and `social-graph` gRPC servers for the read-gate suite:
//! the real `GrpcReadGate` talks to them over real tonic transport, so the
//! wire contract (fields, codes, caps) is exercised, not a Rust fake. Only the
//! RPCs the gate calls answer; the rest are `UNIMPLEMENTED`.
#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Channel, Server};
use tonic::{Request, Response, Status};

use post_api::post_service_server::{PostService, PostServiceServer};
use social_graph_api::social_graph_service_server::{SocialGraphService, SocialGraphServiceServer};

/// What `post` holds: the views `GetPost` answers with (mesh reads), or an
/// outage.
#[derive(Default)]
pub struct Posts {
    pub views: Mutex<HashMap<String, post_api::PostView>>,
    pub down:  Mutex<bool>,
}

impl Posts {
    pub fn put(&self, view: post_api::PostView) {
        self.views.lock().unwrap().insert(view.post_id.clone(), view);
    }
}

/// What `social-graph` holds: per (viewer, target) access overrides (default
/// VISIBLE), restrictions (owner, restricted), and interaction answers per
/// actor (default allowed).
#[derive(Default)]
pub struct Graph {
    pub access:       Mutex<HashMap<(String, String), social_graph_api::ContentAccess>>,
    pub restrictions: Mutex<HashSet<(String, String)>>,
    /// actor → (allowed, held).
    pub interaction:  Mutex<HashMap<String, (bool, bool)>>,
    /// Every CheckAccess call's (viewer count, target count), to check the caps.
    pub access_calls: Mutex<Vec<(usize, usize)>>,
    pub down:         Mutex<bool>,
}

struct PostServer(Arc<Posts>);

#[tonic::async_trait]
impl PostService for PostServer {
    async fn get_post(&self, request: Request<post_api::GetPostRequest>) -> Result<Response<post_api::PostView>, Status> {
        if *self.0.down.lock().unwrap() {
            return Err(Status::unavailable("post is down"));
        }
        let id = request.into_inner().post_id;
        self.0.views.lock().unwrap().get(&id).cloned().map(Response::new).ok_or_else(|| Status::not_found(id))
    }
    async fn create_post(&self, _: Request<post_api::CreatePostRequest>) -> Result<Response<post_api::CreatePostResponse>, Status> {
        Err(Status::unimplemented("CreatePost"))
    }
    async fn publish_post(&self, _: Request<post_api::PublishPostRequest>) -> Result<Response<post_api::CommandResponse>, Status> {
        Err(Status::unimplemented("PublishPost"))
    }
    async fn update_post(&self, _: Request<post_api::UpdatePostRequest>) -> Result<Response<post_api::CommandResponse>, Status> {
        Err(Status::unimplemented("UpdatePost"))
    }
    async fn delete_post(&self, _: Request<post_api::DeletePostRequest>) -> Result<Response<post_api::CommandResponse>, Status> {
        Err(Status::unimplemented("DeletePost"))
    }
    async fn restore_post(&self, _: Request<post_api::RestorePostRequest>) -> Result<Response<post_api::CommandResponse>, Status> {
        Err(Status::unimplemented("RestorePost"))
    }
    async fn list_recently_deleted(&self, _: Request<post_api::ListRecentlyDeletedRequest>) -> Result<Response<post_api::ListRecentlyDeletedResponse>, Status> {
        Err(Status::unimplemented("ListRecentlyDeleted"))
    }
    async fn list_posts_by_profile(&self, _: Request<post_api::ListPostsByProfileRequest>) -> Result<Response<post_api::ListPostsByProfileResponse>, Status> {
        Err(Status::unimplemented("ListPostsByProfile"))
    }
}

struct GraphServer(Arc<Graph>);

#[tonic::async_trait]
impl SocialGraphService for GraphServer {
    async fn check_access(
        &self,
        request: Request<social_graph_api::CheckAccessRequest>,
    ) -> Result<Response<social_graph_api::CheckAccessResponse>, Status> {
        if *self.0.down.lock().unwrap() {
            return Err(Status::unavailable("social-graph is down"));
        }
        let req = request.into_inner();
        if req.viewer_profile_ids.len() > 20 || req.target_profile_ids.len() > 100 {
            return Err(Status::invalid_argument("over the CheckAccess caps"));
        }
        self.0.access_calls.lock().unwrap().push((req.viewer_profile_ids.len(), req.target_profile_ids.len()));
        let access = self.0.access.lock().unwrap();
        let targets = req
            .target_profile_ids
            .iter()
            .map(|target| {
                // The most restrictive answer over the reader's profiles.
                let answer = req
                    .viewer_profile_ids
                    .iter()
                    .filter_map(|viewer| access.get(&(viewer.clone(), target.clone())).copied())
                    .min_by_key(|a| match a {
                        social_graph_api::ContentAccess::Hidden => 0,
                        social_graph_api::ContentAccess::HeaderOnly => 1,
                        _ => 2,
                    })
                    .unwrap_or(social_graph_api::ContentAccess::Visible);
                social_graph_api::TargetAccess { target_profile_id: target.clone(), access: answer as i32, ..Default::default() }
            })
            .collect();
        Ok(Response::new(social_graph_api::CheckAccessResponse { targets }))
    }

    async fn check_interaction(
        &self,
        request: Request<social_graph_api::CheckInteractionRequest>,
    ) -> Result<Response<social_graph_api::CheckInteractionResponse>, Status> {
        let actor = request.into_inner().actor_profile_id;
        let (allowed, held) = self.0.interaction.lock().unwrap().get(&actor).copied().unwrap_or((true, false));
        Ok(Response::new(social_graph_api::CheckInteractionResponse { allowed, held, ..Default::default() }))
    }

    async fn list_restricted_among(
        &self,
        request: Request<social_graph_api::ListRestrictedAmongRequest>,
    ) -> Result<Response<social_graph_api::ListRestrictedAmongResponse>, Status> {
        let req = request.into_inner();
        if req.candidate_ids.len() > 100 {
            return Err(Status::invalid_argument("over the ListRestrictedAmong cap"));
        }
        let restrictions = self.0.restrictions.lock().unwrap();
        let restricted_ids =
            req.candidate_ids.into_iter().filter(|c| restrictions.contains(&(req.owner_id.clone(), c.clone()))).collect();
        Ok(Response::new(social_graph_api::ListRestrictedAmongResponse { restricted_ids }))
    }
    async fn follow(&self, _: Request<social_graph_api::FollowRequest>) -> Result<Response<social_graph_api::CommandResponse>, Status> {
        Err(Status::unimplemented("Follow"))
    }
    async fn list_follow_requests(&self, _: Request<social_graph_api::ListFollowRequestsRequest>) -> Result<Response<social_graph_api::ListFollowRequestsResponse>, Status> {
        Err(Status::unimplemented("ListFollowRequests"))
    }
    async fn approve_follow_request(&self, _: Request<social_graph_api::AnswerFollowRequestRequest>) -> Result<Response<social_graph_api::CommandResponse>, Status> {
        Err(Status::unimplemented("ApproveFollowRequest"))
    }
    async fn decline_follow_request(&self, _: Request<social_graph_api::AnswerFollowRequestRequest>) -> Result<Response<social_graph_api::CommandResponse>, Status> {
        Err(Status::unimplemented("DeclineFollowRequest"))
    }
    async fn cancel_follow_request(&self, _: Request<social_graph_api::CancelFollowRequestRequest>) -> Result<Response<social_graph_api::CommandResponse>, Status> {
        Err(Status::unimplemented("CancelFollowRequest"))
    }
    async fn unfollow(&self, _: Request<social_graph_api::UnfollowRequest>) -> Result<Response<social_graph_api::CommandResponse>, Status> {
        Err(Status::unimplemented("Unfollow"))
    }
    async fn remove_follower(&self, _: Request<social_graph_api::RemoveFollowerRequest>) -> Result<Response<social_graph_api::CommandResponse>, Status> {
        Err(Status::unimplemented("RemoveFollower"))
    }
    async fn set_list_privacy(&self, _: Request<social_graph_api::SetListPrivacyRequest>) -> Result<Response<social_graph_api::ListPrivacy>, Status> {
        Err(Status::unimplemented("SetListPrivacy"))
    }
    async fn block(&self, _: Request<social_graph_api::BlockRequest>) -> Result<Response<social_graph_api::CommandResponse>, Status> {
        Err(Status::unimplemented("Block"))
    }
    async fn unblock(&self, _: Request<social_graph_api::UnblockRequest>) -> Result<Response<social_graph_api::CommandResponse>, Status> {
        Err(Status::unimplemented("Unblock"))
    }
    async fn mute(&self, _: Request<social_graph_api::MuteRequest>) -> Result<Response<social_graph_api::CommandResponse>, Status> {
        Err(Status::unimplemented("Mute"))
    }
    async fn unmute(&self, _: Request<social_graph_api::UnmuteRequest>) -> Result<Response<social_graph_api::CommandResponse>, Status> {
        Err(Status::unimplemented("Unmute"))
    }
    async fn restrict(&self, _: Request<social_graph_api::RestrictRequest>) -> Result<Response<social_graph_api::CommandResponse>, Status> {
        Err(Status::unimplemented("Restrict"))
    }
    async fn unrestrict(&self, _: Request<social_graph_api::UnrestrictRequest>) -> Result<Response<social_graph_api::CommandResponse>, Status> {
        Err(Status::unimplemented("Unrestrict"))
    }
    async fn get_relation_status(&self, _: Request<social_graph_api::GetRelationStatusRequest>) -> Result<Response<social_graph_api::RelationStatusView>, Status> {
        Err(Status::unimplemented("GetRelationStatus"))
    }
    async fn list_followers(&self, _: Request<social_graph_api::ListFollowersRequest>) -> Result<Response<social_graph_api::ListFollowersResponse>, Status> {
        Err(Status::unimplemented("ListFollowers"))
    }
    async fn list_following(&self, _: Request<social_graph_api::ListFollowingRequest>) -> Result<Response<social_graph_api::ListFollowingResponse>, Status> {
        Err(Status::unimplemented("ListFollowing"))
    }
    async fn get_list_privacy(&self, _: Request<social_graph_api::GetListPrivacyRequest>) -> Result<Response<social_graph_api::ListPrivacy>, Status> {
        Err(Status::unimplemented("GetListPrivacy"))
    }
    async fn list_mutes(&self, _: Request<social_graph_api::ListMutesRequest>) -> Result<Response<social_graph_api::ListMutesResponse>, Status> {
        Err(Status::unimplemented("ListMutes"))
    }
    async fn list_restricted(&self, _: Request<social_graph_api::ListRestrictedRequest>) -> Result<Response<social_graph_api::ListRestrictedResponse>, Status> {
        Err(Status::unimplemented("ListRestricted"))
    }
    async fn list_blocks(&self, _: Request<social_graph_api::ListBlocksRequest>) -> Result<Response<social_graph_api::ListBlocksResponse>, Status> {
        Err(Status::unimplemented("ListBlocks"))
    }
    async fn list_muted_profiles(&self, _: Request<social_graph_api::ListMutedProfilesRequest>) -> Result<Response<social_graph_api::ListMutedProfilesResponse>, Status> {
        Err(Status::unimplemented("ListMutedProfiles"))
    }
}

/// Both peers, served on loopback ports, and channels to them.
pub struct Peers {
    pub posts:        Arc<Posts>,
    pub graph:        Arc<Graph>,
    pub post_channel: Channel,
    pub graph_channel: Channel,
}

async fn listener() -> (TcpListener, SocketAddr) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind a loopback port");
    let addr = listener.local_addr().unwrap();
    (listener, addr)
}

impl Peers {
    pub async fn start() -> Self {
        let (posts, graph) = (Arc::new(Posts::default()), Arc::new(Graph::default()));
        let (post_listener, post_addr) = listener().await;
        let post_service = PostServiceServer::new(PostServer(Arc::clone(&posts)));
        tokio::spawn(async move {
            Server::builder()
                .add_service(post_service)
                .serve_with_incoming(TcpListenerStream::new(post_listener))
                .await
                .expect("post peer");
        });
        let (graph_listener, graph_addr) = listener().await;
        let graph_service = SocialGraphServiceServer::new(GraphServer(Arc::clone(&graph)));
        tokio::spawn(async move {
            Server::builder()
                .add_service(graph_service)
                .serve_with_incoming(TcpListenerStream::new(graph_listener))
                .await
                .expect("social-graph peer");
        });
        let channel = |addr: SocketAddr| Channel::from_shared(format!("http://{addr}")).unwrap().connect_lazy();
        Self { posts, graph, post_channel: channel(post_addr), graph_channel: channel(graph_addr) }
    }
}
