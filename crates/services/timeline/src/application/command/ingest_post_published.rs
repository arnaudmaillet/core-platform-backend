use std::sync::Arc;

use cqrs::{Command, CommandHandler, Envelope};
use validate_core::{FieldViolation, Validate};

use crate::application::port::{
    AudioFeedRepository, AudioFeedStore, AuthorPostRepository, FeedRepository, FeedStore,
    SocialGraphClient, TierCache, VipRegistry,
};
use crate::domain::value_object::{AudioId, AuthorId, AuthorTier, FanOutMode, PostId};
use crate::error::TimelineError;

pub struct IngestPostPublishedCommand {
    pub post_id:         String,
    pub author_id:       String,
    pub author_tier:     u8,
    pub published_at_ms: i64,
    pub audio_id:        Option<String>,
}

impl Command for IngestPostPublishedCommand {}

impl Validate for IngestPostPublishedCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        let mut v = Vec::new();
        if self.post_id.trim().is_empty() {
            v.push(FieldViolation::new("post_id", "TML-VAL-001", "post_id must not be empty"));
        }
        if self.author_id.trim().is_empty() {
            v.push(FieldViolation::new("author_id", "TML-VAL-002", "author_id must not be empty"));
        }
        if self.published_at_ms <= 0 {
            v.push(FieldViolation::new(
                "published_at_ms",
                "TML-VAL-003",
                "published_at_ms must be a positive Unix epoch millisecond timestamp",
            ));
        }
        if v.is_empty() { Ok(()) } else { Err(v) }
    }
}

pub struct IngestPostPublishedHandler<FS, VR, FR, AR, TC, SG, AFR, AFS> {
    pub feed_store:            Arc<FS>,
    pub vip_registry:          Arc<VR>,
    pub feed_repository:       Arc<FR>,
    pub author_post_repo:      Arc<AR>,
    pub tier_cache:            Arc<TC>,
    pub social_graph:          Arc<SG>,
    pub audio_feed_repo:       Arc<AFR>,
    pub audio_feed_store:      Arc<AFS>,
    pub feed_cap:              u16,
    pub vip_registry_cap:      u16,
    pub vip_registry_ttl_secs: u64,
    pub tier_cache_ttl_secs:   u64,
    pub social_graph_page_size: i32,
    pub audio_feed_cap:        u16,
}

impl<FS, VR, FR, AR, TC, SG, AFR, AFS> CommandHandler<IngestPostPublishedCommand>
    for IngestPostPublishedHandler<FS, VR, FR, AR, TC, SG, AFR, AFS>
where
    FS:  FeedStore,
    VR:  VipRegistry,
    FR:  FeedRepository,
    AR:  AuthorPostRepository,
    TC:  TierCache,
    SG:  SocialGraphClient,
    AFR: AudioFeedRepository,
    AFS: AudioFeedStore,
{
    type Error = TimelineError;

    async fn handle(
        &self,
        envelope: Envelope<IngestPostPublishedCommand>,
    ) -> Result<(), TimelineError> {
        let cmd = &envelope.payload;

        let post_id   = PostId::try_from(cmd.post_id.as_str())?;
        let author_id = AuthorId::try_from(cmd.author_id.as_str())?;
        let tier      = AuthorTier::from_u8(cmd.author_tier);

        let entry = crate::domain::aggregate::FeedEntry::new(post_id, author_id, cmd.published_at_ms);

        self.tier_cache
            .set_tier(&author_id, tier, self.tier_cache_ttl_secs)
            .await?;

        self.author_post_repo
            .insert(&author_id, &post_id, tier, cmd.published_at_ms)
            .await?;

        match tier.fan_out_mode() {
            FanOutMode::Read  => self.handle_vip_fanout(&entry).await?,
            FanOutMode::Write => self.handle_write_fanout(&entry).await?,
        }

        if let Some(ref aid_str) = cmd.audio_id {
            match AudioId::try_from(aid_str.as_str()) {
                Ok(audio_id) => {
                    if let Err(e) = self
                        .audio_feed_repo
                        .insert(&audio_id, &post_id, &author_id, cmd.published_at_ms)
                        .await
                    {
                        tracing::warn!(
                            audio_id = %audio_id,
                            post_id  = %post_id,
                            error    = %e,
                            "audio feed ScyllaDB insert failed"
                        );
                    }

                    if let Err(e) = self
                        .audio_feed_store
                        .push(&audio_id, &post_id, &author_id, cmd.published_at_ms, self.audio_feed_cap)
                        .await
                    {
                        tracing::warn!(
                            audio_id = %audio_id,
                            post_id  = %post_id,
                            error    = %e,
                            "audio feed Redis push failed"
                        );
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        audio_id = %aid_str,
                        post_id  = %post_id,
                        error    = %e,
                        "invalid audio_id in PostPublished event — skipping audio index"
                    );
                }
            }
        }

        Ok(())
    }
}

impl<FS, VR, FR, AR, TC, SG, AFR, AFS> IngestPostPublishedHandler<FS, VR, FR, AR, TC, SG, AFR, AFS>
where
    FS:  FeedStore,
    VR:  VipRegistry,
    FR:  FeedRepository,
    AR:  AuthorPostRepository,
    TC:  TierCache,
    SG:  SocialGraphClient,
    AFR: AudioFeedRepository,
    AFS: AudioFeedStore,
{
    async fn handle_vip_fanout(
        &self,
        entry: &crate::domain::aggregate::FeedEntry,
    ) -> Result<(), TimelineError> {
        self.vip_registry
            .register(entry, self.vip_registry_cap, self.vip_registry_ttl_secs)
            .await
            .map_err(|e| TimelineError::VipRegistryWriteFailed {
                author_id: entry.author_id.to_string(),
                message:   e.to_string(),
            })?;

        tracing::debug!(
            author_id = %entry.author_id,
            post_id   = %entry.post_id,
            "VIP post registered in ZSET registry"
        );
        Ok(())
    }

    async fn handle_write_fanout(
        &self,
        entry: &crate::domain::aggregate::FeedEntry,
    ) -> Result<(), TimelineError> {
        let author_id = &entry.author_id;

        // `list_all_followers` paginates the social-graph RPC internally and returns
        // the complete follower set, so one call covers every follower — no outer
        // pagination loop is needed. This path runs only for Standard/Premium authors
        // (VIP authors fan out at read time via the registry), so the follower count
        // is bounded and safe to materialize.
        let followers = self
            .social_graph
            .list_all_followers(author_id, self.social_graph_page_size)
            .await
            .map_err(|e| TimelineError::FanOutFailed {
                author_id: author_id.to_string(),
                message:   e.to_string(),
            })?;

        // Redis first (user-visible feed), then ScyllaDB (durable write-behind).
        for profile_id in &followers {
            if let Err(e) = self
                .feed_store
                .push(profile_id, entry, self.feed_cap)
                .await
            {
                tracing::warn!(
                    profile_id = %profile_id,
                    post_id    = %entry.post_id,
                    error      = %e,
                    "Redis feed push failed — continuing fan-out"
                );
            }
        }

        for profile_id in &followers {
            if let Err(e) = self
                .feed_repository
                .insert(profile_id, entry)
                .await
            {
                tracing::warn!(
                    profile_id = %profile_id,
                    post_id    = %entry.post_id,
                    error      = %e,
                    "ScyllaDB feed insert failed — Redis is authoritative"
                );
            }
        }

        tracing::debug!(
            author_id       = %author_id,
            post_id         = %entry.post_id,
            total_followers = followers.len(),
            "fan-out-on-write completed"
        );
        Ok(())
    }
}

fn _assert_send_sync() {
    fn _check<T: Send + Sync>() {}
    _check::<TimelineError>();
}
