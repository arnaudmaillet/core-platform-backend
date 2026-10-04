//! Content hydration — the seam between a thin notification and a fat document.
//!
//! `post.v1.events` and `profile.v1.events` are notifications (ids + timestamps),
//! so a publish/update can't be projected directly: the consumer first resolves the
//! authoritative snapshot from the owning service over gRPC (`GetPost` /
//! `GetProfileById`). This runs on the **ingestion** path only; the query path stays
//! self-contained.

use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use post_api::GetPostRequest;
use post_api::post_service_client::PostServiceClient;
use profile_api::GetProfileByIdRequest;
use profile_api::profile_service_client::ProfileServiceClient;
use tonic::Code;
use tonic::transport::Channel;

use crate::domain::{
    EntityDeletion, EntityKind, PostEvent, PostSnapshot, ProfileEvent, ProfileSnapshot, SourceEvent,
};
use crate::error::SearchError;
use crate::infrastructure::decode::ContentRef;

/// Turns a [`ContentRef`] (from the decode layer) into a projectable
/// [`SourceEvent`] by fetching the source-of-record snapshot.
#[async_trait]
pub trait SourceHydrator: Send + Sync + 'static {
    async fn hydrate(
        &self,
        content_ref: ContentRef,
        now: DateTime<Utc>,
    ) -> Result<SourceEvent, SearchError>;
}

/// The production hydrator: `post` + `profile` gRPC clients, routed by kind.
pub struct GrpcSourceHydrator {
    post: PostServiceClient<Channel>,
    profile: ProfileServiceClient<Channel>,
}

impl GrpcSourceHydrator {
    pub fn new(post: Channel, profile: Channel) -> Self {
        Self {
            post: PostServiceClient::new(post),
            profile: ProfileServiceClient::new(profile),
        }
    }

    async fn hydrate_post(&self, content_ref: ContentRef) -> Result<SourceEvent, SearchError> {
        let mut client = self.post.clone();
        let view = match client
            .get_post(GetPostRequest {
                post_id: content_ref.id.clone(),
            })
            .await
        {
            Ok(resp) => resp.into_inner(),
            // Deleted between the event and our fetch — converge by removing it.
            Err(status) if status.code() == Code::NotFound => {
                return Ok(deleted(EntityKind::Post, content_ref.id));
            }
            Err(status) if is_transient(status.code()) => {
                return Err(SearchError::EngineUnavailable);
            }
            Err(status) => {
                return Err(SearchError::UnmappedSource {
                    reason: format!("get_post failed: {status}"),
                });
            }
        };

        Ok(post_event(view, content_ref.revision))
    }

    async fn hydrate_profile(&self, content_ref: ContentRef) -> Result<SourceEvent, SearchError> {
        let mut client = self.profile.clone();
        let view = match client
            .get_profile_by_id(GetProfileByIdRequest {
                profile_id: content_ref.id.clone(),
            })
            .await
        {
            Ok(resp) => resp.into_inner(),
            Err(status) if status.code() == Code::NotFound => {
                return Ok(deleted(EntityKind::Profile, content_ref.id));
            }
            Err(status) if is_transient(status.code()) => {
                return Err(SearchError::EngineUnavailable);
            }
            Err(status) => {
                return Err(SearchError::UnmappedSource {
                    reason: format!("get_profile_by_id failed: {status}"),
                });
            }
        };

        let snapshot = ProfileSnapshot {
            profile_id: view.profile_id,
            handle: view.handle,
            display_name: view.display_name,
            bio: view.bio,
            avatar_key: view.avatar_url,
            verified: view.verified,
            created_at: view.created_at.map(ts_to_dt).unwrap_or_else(Utc::now),
            // The profile's own monotonic version is the authoritative content version.
            revision: view.version.max(0) as u64,
        };
        Ok(SourceEvent::Profile(ProfileEvent::Upserted(snapshot)))
    }
}

#[async_trait]
impl SourceHydrator for GrpcSourceHydrator {
    async fn hydrate(
        &self,
        content_ref: ContentRef,
        _now: DateTime<Utc>,
    ) -> Result<SourceEvent, SearchError> {
        match content_ref.kind {
            EntityKind::Post => self.hydrate_post(content_ref).await,
            EntityKind::Profile => self.hydrate_profile(content_ref).await,
            other => Err(SearchError::UnmappedSource {
                reason: format!("no hydrator for {other:?}"),
            }),
        }
    }
}

/// The search event for a hydrated post. Only a **published** post is indexed: a
/// draft (a `PostUpdated` fires for drafts too) or a soft-deleted post converges
/// to "absent", so a stale or out-of-order event can never index one. The status
/// is read at hydration time, so whatever order events arrive in, the document
/// follows the post's current state. A post moderation removed or limited
/// converges to "absent" too: a takedown survives a re-announce (a delete then
/// restore, #663), whatever moderation's own stream did to the document.
fn post_event(view: post_api::PostView, revision: u64) -> SourceEvent {
    // Age-gated too: search has no per-reader content level, so mature
    // content stays out for everyone (13–17 and guests are never served it).
    let hidden_by_moderation = view.moderation == post_api::ModerationRestriction::Removed as i32
        || view.moderation == post_api::ModerationRestriction::Limited as i32
        || view.moderation == post_api::ModerationRestriction::AgeGated as i32;
    if view.status != post_api::PostStatus::Published as i32 || hidden_by_moderation {
        return deleted(EntityKind::Post, view.post_id);
    }
    let snapshot = PostSnapshot {
        post_id: view.post_id,
        // The post's author is its profile; `author_handle` is a display field
        // left to a future secondary profile lookup.
        author_id: view.profile_id,
        author_handle: String::new(),
        hashtags: extract_hashtags(&view.caption),
        caption: view.caption,
        // Thumbnail derivation from `attachments` is deferred (display-only).
        thumbnail_key: String::new(),
        created_at: ms_to_dt(view.created_at_ms),
        revision,
    };
    SourceEvent::Post(PostEvent::Published(snapshot))
}

fn deleted(kind: EntityKind, id: String) -> SourceEvent {
    match kind {
        EntityKind::Post => SourceEvent::Post(PostEvent::Deleted(EntityDeletion { id })),
        EntityKind::Profile => SourceEvent::Profile(ProfileEvent::Deleted(EntityDeletion { id })),
        EntityKind::Hashtag => SourceEvent::Post(PostEvent::Deleted(EntityDeletion { id })),
    }
}

/// Extract `#hashtags` from caption text (a search-side responsibility — posts do
/// not model hashtags). Normalized to lowercase, de-duplicated.
fn extract_hashtags(caption: &str) -> Vec<String> {
    let mut tags: Vec<String> = caption
        .split_whitespace()
        .filter_map(|token| token.strip_prefix('#'))
        .map(|tag| {
            tag.trim_matches(|c: char| !c.is_alphanumeric())
                .to_lowercase()
        })
        .filter(|tag| !tag.is_empty())
        .collect();
    tags.sort();
    tags.dedup();
    tags
}

fn ms_to_dt(ms: i64) -> DateTime<Utc> {
    Utc.timestamp_millis_opt(ms).single().unwrap_or_else(Utc::now)
}

fn ts_to_dt(ts: prost_types::Timestamp) -> DateTime<Utc> {
    Utc.timestamp_opt(ts.seconds, ts.nanos.max(0) as u32)
        .single()
        .unwrap_or_else(Utc::now)
}

fn is_transient(code: Code) -> bool {
    matches!(
        code,
        Code::Unavailable | Code::DeadlineExceeded | Code::ResourceExhausted | Code::Aborted
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_and_normalizes_hashtags() {
        let tags = extract_hashtags("Loving #Rust and #rust and #OpenSearch! no_tag");
        assert_eq!(tags, vec!["opensearch".to_owned(), "rust".to_owned()]);
    }

    #[test]
    fn no_hashtags_is_empty() {
        assert!(extract_hashtags("just a plain caption").is_empty());
    }

    fn view(status: post_api::PostStatus) -> post_api::PostView {
        post_api::PostView {
            post_id: "post-1".into(),
            profile_id: "author-1".into(),
            caption: "hello #world".into(),
            status: status as i32,
            ..Default::default()
        }
    }

    #[test]
    fn a_published_post_is_indexed() {
        match post_event(view(post_api::PostStatus::Published), 7) {
            SourceEvent::Post(PostEvent::Published(snap)) => {
                assert_eq!(snap.post_id, "post-1");
                assert_eq!(snap.hashtags, vec!["world".to_owned()]);
                assert_eq!(snap.revision, 7);
            }
            other => panic!("expected an upsert, got {other:?}"),
        }
    }

    #[test]
    fn a_post_moderation_removed_or_limited_converges_to_absent() {
        for restriction in [post_api::ModerationRestriction::Removed, post_api::ModerationRestriction::Limited] {
            let mut v = view(post_api::PostStatus::Published);
            v.moderation = restriction as i32;
            assert_eq!(
                post_event(v, 7),
                SourceEvent::Post(PostEvent::Deleted(EntityDeletion { id: "post-1".into() })),
                "{restriction:?}"
            );
        }
        let mut gated = view(post_api::PostStatus::Published);
        gated.moderation = post_api::ModerationRestriction::AgeGated as i32;
        assert!(matches!(post_event(gated, 7), SourceEvent::Post(PostEvent::Deleted(_))), "age-gated: absent");
    }

    #[test]
    fn drafts_and_deleted_posts_converge_to_absent() {
        for status in [post_api::PostStatus::Draft, post_api::PostStatus::Deleted] {
            assert_eq!(
                post_event(view(status), 7),
                SourceEvent::Post(PostEvent::Deleted(EntityDeletion { id: "post-1".into() })),
                "{status:?}"
            );
        }
    }
}
