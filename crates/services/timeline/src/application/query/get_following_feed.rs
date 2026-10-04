use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use cqrs::{Envelope, Query, QueryHandler};
use tokio::sync::Semaphore;

use crate::application::port::{
    muted_authors, AuthorPostRepository, FeedRepository, FeedStore, FollowingStore,
    SocialGraphClient, TierCache, VipRegistry,
};
use crate::domain::aggregate::FeedEntry;
use crate::domain::value_object::{AuthorId, AuthorTier, FeedCursor, ProfileId};
use crate::error::TimelineError;

/// A single page of the user's following feed.
pub struct FollowingFeedPage {
    pub items:           Vec<FeedEntry>,
    pub next_page_token: Option<String>,
    /// True when the response was assembled from ScyllaDB cold storage rather
    /// than Redis. The BFF may use this to show a "feed is loading" indicator.
    pub is_cold:         bool,
}

pub struct GetFollowingFeedQuery {
    pub profile_id: String,
    pub limit:      i32,
    pub page_token: Option<String>,
}

impl Query for GetFollowingFeedQuery {
    type Response = FollowingFeedPage;
}

pub struct GetFollowingFeedHandler<FS, VR, FR, AR, TC, FO, SG> {
    pub feed_store:         Arc<FS>,
    pub vip_registry:       Arc<VR>,
    pub feed_repository:    Arc<FR>,
    pub author_post_repo:   Arc<AR>,
    pub tier_cache:         Arc<TC>,
    pub following_store:    Arc<FO>,
    pub social_graph:       Arc<SG>,
    pub max_page_size:      i32,
    pub feed_cap:           u16,
    pub vip_registry_cap:   u16,
    pub vip_registry_ttl_secs: u64,
    pub warm_ttl_secs:      u64,
    pub social_graph_page_size: i32,
    pub max_vip_merge_sources: usize,
    /// Caps concurrent background warm-ups (cold-start ScyllaDB rebuilds).
    pub warm_semaphore:     Arc<Semaphore>,
    /// Singleflight set: profiles with an in-flight warm-up, so a stampede on one
    /// cold profile spawns at most one rebuild task.
    pub warming:            Arc<Mutex<HashSet<ProfileId>>>,
}

impl<FS, VR, FR, AR, TC, FO, SG> QueryHandler<GetFollowingFeedQuery>
    for GetFollowingFeedHandler<FS, VR, FR, AR, TC, FO, SG>
where
    FS: FeedStore,
    VR: VipRegistry,
    FR: FeedRepository,
    AR: AuthorPostRepository,
    TC: TierCache,
    FO: FollowingStore,
    SG: SocialGraphClient,
{
    type Error = TimelineError;

    async fn handle(
        &self,
        envelope: Envelope<GetFollowingFeedQuery>,
    ) -> Result<FollowingFeedPage, TimelineError> {
        let query = &envelope.payload;

        let profile_id = ProfileId::try_from(query.profile_id.as_str())?;
        let limit      = query.limit.min(self.max_page_size).max(1) as usize;
        let cursor     = query
            .page_token
            .as_deref()
            .filter(|s| !s.is_empty())
            .map(FeedCursor::decode)
            .transpose()?;

        let max_score = cursor
            .map(|c| c.published_at_ms)
            .unwrap_or(i64::MAX);

        // Ensure the following set is warm in Redis; the reader's mutes in
        // parallel (fail open).
        let reader = [profile_id.to_string()];
        let (following_ids, muted) = tokio::join!(
            self.ensure_following_set(&profile_id),
            muted_authors(self.social_graph.as_ref(), &reader),
        );
        let following_ids = following_ids?;

        if following_ids.is_empty() {
            return Ok(FollowingFeedPage {
                items:           Vec::new(),
                next_page_token: None,
                is_cold:         false,
            });
        }

        // Split following list into regular (materialized) vs VIP (at-read
        // merge); a muted VIP is not even read.
        let (_regular_ids, mut vip_ids) = self.split_by_tier(&following_ids).await;
        vip_ids.retain(|id| !muted.contains(id));

        // Check warm flag to route to Redis vs cold storage.
        let is_warm = self.tier_cache.is_warm(&profile_id).await?;

        if !is_warm {
            // Cold path: ScyllaDB → return immediately, warm Redis asynchronously.
            let page = self
                .serve_cold(&profile_id, &vip_ids, max_score, limit, &muted)
                .await?;

            // Trigger a bounded, de-duplicated async warm-up of the regular feed.
            self.try_spawn_warm(profile_id);

            return Ok(page);
        }

        // Hot path: Redis merge.
        self.serve_hot(&profile_id, &vip_ids, max_score, limit, cursor, &muted)
            .await
    }
}

impl<FS, VR, FR, AR, TC, FO, SG> GetFollowingFeedHandler<FS, VR, FR, AR, TC, FO, SG>
where
    FS: FeedStore,
    VR: VipRegistry,
    FR: FeedRepository,
    AR: AuthorPostRepository,
    TC: TierCache,
    FO: FollowingStore,
    SG: SocialGraphClient,
{
    /// Resolves the caller's following list from Redis cache.
    /// On cache miss, rebuilds from social-graph gRPC and persists to Redis.
    async fn ensure_following_set(
        &self,
        profile_id: &ProfileId,
    ) -> Result<Vec<AuthorId>, TimelineError> {
        let cache_hit = self.following_store.exists(profile_id).await?;

        if cache_hit {
            return self.following_store.get_all(profile_id).await;
        }

        // Cold following set: rebuild from social-graph gRPC.
        tracing::info!(
            profile_id = %profile_id,
            "following set cache miss — rebuilding from social-graph"
        );

        let all_following = self
            .social_graph
            .list_all_following(profile_id, self.social_graph_page_size)
            .await?;

        if !all_following.is_empty() {
            self.following_store
                .set_all(profile_id, &all_following)
                .await?;
        }

        Ok(all_following)
    }

    /// Splits a following list into regular and VIP subsets by consulting the
    /// tier cache. On cache miss the author is conservatively routed to regular
    /// (avoids blocking; the tier cache will be populated on next post.published).
    async fn split_by_tier(
        &self,
        following_ids: &[AuthorId],
    ) -> (Vec<AuthorId>, Vec<AuthorId>) {
        // One batched (pipelined) tier lookup for the whole following list instead
        // of a serial Redis round-trip per followee — the latter is a p99 cliff for
        // users following thousands of accounts. On a lookup error, fall back to all
        // misses (→ Standard), matching the previous per-author error handling.
        let tiers = self
            .tier_cache
            .get_tiers(following_ids)
            .await
            .unwrap_or_else(|_| vec![None; following_ids.len()]);

        let mut regular = Vec::new();
        let mut vip     = Vec::new();

        for (author_id, tier) in following_ids.iter().zip(tiers) {
            // Cache miss → conservatively route to regular (avoids blocking; the
            // tier cache is populated on the author's next post.published).
            let tier = tier.unwrap_or(AuthorTier::Standard);

            if tier.is_vip() {
                if vip.len() < self.max_vip_merge_sources {
                    vip.push(*author_id);
                }
            } else {
                regular.push(*author_id);
            }
        }

        (regular, vip)
    }

    /// Spawns a background feed warm-up, bounded two ways so a cold-cache stampede
    /// cannot overwhelm ScyllaDB:
    ///   1. **Singleflight** — at most one in-flight warm-up per profile.
    ///   2. **Concurrency cap** — a global semaphore bounds total concurrent
    ///      warm-ups; when exhausted the warm-up is skipped (cold reads still return
    ///      correct data, and a later request retries once a permit frees).
    fn try_spawn_warm(&self, profile_id: ProfileId) {
        // Singleflight guard: bail if a warm-up for this profile is already running.
        if !self.warming.lock().unwrap().insert(profile_id) {
            return;
        }

        // Concurrency cap. On exhaustion, release the singleflight slot and bail.
        let permit = match Arc::clone(&self.warm_semaphore).try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                self.warming.lock().unwrap().remove(&profile_id);
                return;
            }
        };

        let feed_store      = Arc::clone(&self.feed_store);
        let feed_repository = Arc::clone(&self.feed_repository);
        let tier_cache      = Arc::clone(&self.tier_cache);
        let warming         = Arc::clone(&self.warming);
        let warm_ttl        = self.warm_ttl_secs;
        let cap             = self.feed_cap;

        tokio::spawn(async move {
            // Held for the duration of the warm-up; released (permit + slot) on exit.
            let _permit = permit;

            if let Err(e) =
                warm_feed(&profile_id, &feed_store, &feed_repository, &tier_cache, cap, warm_ttl)
                    .await
            {
                tracing::warn!(
                    profile_id = %profile_id,
                    error      = %e,
                    "background feed warm-up failed"
                );
            }

            warming.lock().unwrap().remove(&profile_id);
        });
    }

    /// Hot path: pipeline Redis reads and merge in-process.
    async fn serve_hot(
        &self,
        profile_id: &ProfileId,
        vip_ids:    &[AuthorId],
        max_score:  i64,
        limit:      usize,
        cursor:     Option<FeedCursor>,
        muted:      &HashSet<AuthorId>,
    ) -> Result<FollowingFeedPage, TimelineError> {
        // Overscan by 2× to absorb dedup + cursor exclusion losses.
        let overscan = (limit * 2).max(50);

        // Collect materialized regular feed.
        let mut all_entries: Vec<FeedEntry> = self
            .feed_store
            .range_desc(profile_id, max_score, overscan)
            .await?;

        // Merge VIP slices (futures run concurrently via try_join_all).
        let vip_cap = self.vip_registry_cap as usize;
        let vip_slices: Vec<Vec<FeedEntry>> = futures::future::try_join_all(
            vip_ids.iter().map(|vid| {
                let vip_registry = Arc::clone(&self.vip_registry);
                let vid = *vid;
                async move {
                    vip_registry
                        .range_desc(&vid, max_score, vip_cap)
                        .await
                }
            }),
        )
        .await?;

        for slice in vip_slices {
            all_entries.extend(slice);
        }

        Ok(build_page(all_entries, cursor, limit, muted))
    }

    /// Cold path: read from ScyllaDB and merge VIP registries (or their cold-start
    /// equivalents from `posts_by_author`).
    async fn serve_cold(
        &self,
        profile_id: &ProfileId,
        vip_ids:    &[AuthorId],
        max_score:  i64,
        limit:      usize,
        muted:      &HashSet<AuthorId>,
    ) -> Result<FollowingFeedPage, TimelineError> {
        let cold_limit = (limit * 2).max(50) as i32;

        // Read regular feed from ScyllaDB.
        let mut all_entries = self
            .feed_repository
            .list_recent(profile_id, max_score, cold_limit)
            .await?;

        // Merge VIP cold-start entries from posts_by_author.
        let vip_cap = self.vip_registry_cap as i32;
        let vip_slices: Vec<Vec<FeedEntry>> = futures::future::try_join_all(
            vip_ids.iter().map(|vid| {
                let author_post_repo = Arc::clone(&self.author_post_repo);
                let vid = *vid;
                async move {
                    author_post_repo.list_recent(&vid, max_score, vip_cap).await
                }
            }),
        )
        .await?;

        for slice in vip_slices {
            all_entries.extend(slice);
        }

        let mut page = build_page(all_entries, None, limit, muted);
        page.is_cold = true;
        Ok(page)
    }
}

/// Merges, deduplicates, applies cursor exclusion, sorts DESC, and paginates,
/// leaving out muted authors' posts. The next cursor sits after the last entry
/// *examined*, muted or not, so a run of muted posts is skipped, not re-read.
fn build_page(
    mut entries:   Vec<FeedEntry>,
    cursor:        Option<FeedCursor>,
    limit:         usize,
    muted:         &HashSet<AuthorId>,
) -> FollowingFeedPage {
    // Sort newest-first.
    entries.sort_unstable_by(|a, b| {
        b.published_at_ms
            .cmp(&a.published_at_ms)
            .then_with(|| b.post_id.as_uuid().cmp(&a.post_id.as_uuid()))
    });

    // Deduplicate by post_id preserving order.
    let mut seen = std::collections::HashSet::new();
    entries.retain(|e| seen.insert(e.post_id));

    // Apply cursor exclusion: skip any entry that is at or after the cursor.
    if let Some(c) = cursor {
        // Parse the cursor's post_id once. An unparseable cursor falls back to a
        // pure-timestamp exclusion instead of silently substituting the nil UUID
        // (which would corrupt the tie-break at equal timestamps).
        let cursor_post_id = uuid::Uuid::parse_str(c.post_id_str()).ok();
        entries.retain(|e| match cursor_post_id {
            Some(cid) => {
                e.published_at_ms < c.published_at_ms
                    || (e.published_at_ms == c.published_at_ms && e.post_id.as_uuid() < cid)
            }
            None => e.published_at_ms < c.published_at_ms,
        });
    }

    let mut items: Vec<FeedEntry> = Vec::with_capacity(limit);
    let mut examined = 0;
    for entry in &entries {
        if items.len() == limit {
            break;
        }
        examined += 1;
        if !muted.contains(&entry.author_id) {
            items.push(entry.clone());
        }
    }
    let has_more = examined < entries.len();

    let next_page_token = if has_more {
        entries[..examined].last().map(|last| {
            FeedCursor::new(last.published_at_ms, &last.post_id.to_string()).encode()
        })
    } else {
        None
    };

    FollowingFeedPage { items, next_page_token, is_cold: false }
}

/// Warms the regular Redis feed for a profile from ScyllaDB.
async fn warm_feed<FS, FR, TC>(
    profile_id:      &ProfileId,
    feed_store:      &Arc<FS>,
    feed_repository: &Arc<FR>,
    tier_cache:      &Arc<TC>,
    cap:             u16,
    warm_ttl_secs:   u64,
) -> Result<(), TimelineError>
where
    FS: FeedStore,
    FR: FeedRepository,
    TC: TierCache,
{
    let entries = feed_repository
        .list_recent(profile_id, i64::MAX, cap as i32)
        .await?;

    if !entries.is_empty() {
        feed_store.push_batch(profile_id, &entries, cap).await?;
    }

    tier_cache.set_warm(profile_id, warm_ttl_secs).await?;

    tracing::debug!(
        profile_id = %profile_id,
        entries    = entries.len(),
        "feed warmed from ScyllaDB"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;
    use crate::domain::value_object::PostId;

    fn entry(author: AuthorId, at: i64) -> FeedEntry {
        FeedEntry::new(PostId::from_uuid(Uuid::now_v7()), author, at)
    }

    #[test]
    fn muted_posts_are_skipped_and_the_cursor_moves_past_them() {
        let (kept, muted) = (AuthorId::from_uuid(Uuid::now_v7()), AuthorId::from_uuid(Uuid::now_v7()));
        let mutes: HashSet<AuthorId> = [muted].into();
        // Newest first: a run of muted posts between two kept ones.
        let entries = vec![entry(kept, 50), entry(muted, 40), entry(muted, 30), entry(kept, 20), entry(muted, 10)];

        let page = build_page(entries.clone(), None, 2, &mutes);
        assert_eq!(page.items.iter().map(|e| e.published_at_ms).collect::<Vec<_>>(), vec![50, 20]);
        // The page examined down to 20; one muted entry is left, so there is more.
        let cursor = FeedCursor::decode(page.next_page_token.as_deref().unwrap()).unwrap();
        assert_eq!(cursor.published_at_ms, 20);

        let rest = build_page(entries, Some(cursor), 2, &mutes);
        assert!(rest.items.is_empty());
        assert_eq!(rest.next_page_token, None);
    }

    #[test]
    fn without_mutes_the_page_is_unchanged() {
        let a = AuthorId::from_uuid(Uuid::now_v7());
        let entries = vec![entry(a, 3), entry(a, 2), entry(a, 1)];
        let page = build_page(entries, None, 2, &HashSet::new());
        assert_eq!(page.items.iter().map(|e| e.published_at_ms).collect::<Vec<_>>(), vec![3, 2]);
        assert!(page.next_page_token.is_some());
    }
}
