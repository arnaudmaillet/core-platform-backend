//! In-process fake for the social-graph gRPC dependency.
//!
//! Timeline's [`SocialGraphClient`] port is the seam between the read/fan-out
//! paths and the social-graph service. Rather than boot a second microservice,
//! the suite injects [`FakeSocialGraph`]: it *is* the follow graph (tests declare
//! edges with [`add_follow`](FakeSocialGraph::add_follow)) and it counts calls, so
//! a scenario can assert that, e.g., a warmed following-set is not rebuilt from
//! gRPC again.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use async_trait::async_trait;

use timeline::application::port::{NearbyPosts, SocialGraphClient};
use timeline::domain::value_object::{AuthorId, ContentAccess, PostId, ProfileId};
use timeline::error::TimelineError;

/// A deterministic, call-counting stand-in for the social-graph service.
#[derive(Default)]
pub struct FakeSocialGraph {
    /// author → its followers (drives fan-out-on-write).
    followers: Mutex<HashMap<AuthorId, Vec<ProfileId>>>,
    /// profile → who it follows (drives cold-start following-set rebuild).
    following: Mutex<HashMap<ProfileId, Vec<AuthorId>>>,
    followers_calls: AtomicUsize,
    following_calls: AtomicUsize,
    /// Authors whose content no reader may see (a private profile the reader
    /// does not follow, say); every other author is visible.
    private:         Mutex<HashSet<AuthorId>>,
}

impl FakeSocialGraph {
    pub fn new() -> Self {
        Self::default()
    }

    /// Declares that `follower` follows `author`, updating both projections.
    pub fn add_follow(&self, follower: ProfileId, author: AuthorId) {
        self.followers.lock().unwrap().entry(author).or_default().push(follower);
        self.following.lock().unwrap().entry(follower).or_default().push(author);
    }

    /// Makes `author`'s content invisible to every reader (`HEADER_ONLY`).
    pub fn make_private(&self, author: AuthorId) {
        self.private.lock().unwrap().insert(author);
    }

    /// Number of `list_all_followers` calls observed so far.
    pub fn followers_calls(&self) -> usize {
        self.followers_calls.load(Ordering::SeqCst)
    }

    /// Number of `list_all_following` calls observed so far.
    pub fn following_calls(&self) -> usize {
        self.following_calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl SocialGraphClient for FakeSocialGraph {
    async fn list_all_followers(
        &self,
        author_id: &AuthorId,
        _page_size: i32,
    ) -> Result<Vec<ProfileId>, TimelineError> {
        self.followers_calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.followers.lock().unwrap().get(author_id).cloned().unwrap_or_default())
    }

    async fn list_all_following(
        &self,
        profile_id: &ProfileId,
        _page_size: i32,
    ) -> Result<Vec<AuthorId>, TimelineError> {
        self.following_calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.following.lock().unwrap().get(profile_id).cloned().unwrap_or_default())
    }

    async fn check_access(
        &self,
        _viewers: &[String],
        authors:  &[AuthorId],
    ) -> Result<HashMap<AuthorId, ContentAccess>, TimelineError> {
        let private = self.private.lock().unwrap();
        Ok(authors
            .iter()
            .map(|a| (*a, if private.contains(a) { ContentAccess::HeaderOnly } else { ContentAccess::Visible }))
            .collect())
    }
}

/// geo-discovery's map index, as a fixed list of posts around any point.
#[derive(Default)]
pub struct FakeNearby {
    pub posts: Mutex<Vec<PostId>>,
}

#[async_trait]
impl NearbyPosts for FakeNearby {
    async fn around(&self, _lat: f64, _lng: f64, _guest: Option<&str>) -> Result<Vec<PostId>, TimelineError> {
        Ok(self.posts.lock().unwrap().clone())
    }
}
