use std::sync::Arc;

use serde::Deserialize;
use transport::kafka::consumer::builder::KafkaConsumerBuilder;
use transport::kafka::consumer::{run_consumer, ProcessOutcome, RetryPolicy};
use transport::kafka::config::client::KafkaClientConfig;
use transport::kafka::config::consumer::{AutoOffsetReset, ConsumerConfig};
use transport::kafka::producer::KafkaProducerHandle;

use crate::application::command::IndexPostCommand;
use crate::application::port::{CardStore, PinStore, SpatialIndex, TileRepository};
use crate::infrastructure::cache::{RedisCardStore, RedisGeoSpatialIndex, RedisPinStore};
use crate::infrastructure::persistence::ScyllaTileRepository;
use crate::infrastructure::worker::build_dlq_producer;

const TOPIC: &str = "post.published";

/// Kafka event schema for `post.published`.
///
/// Published by `services/post` when a post transitions to Published status.
/// `services/post` only emits POST-owned data: it does not carry the author's
/// display name or avatar (those are profile-owned and joined separately from
/// `profile.v1.events`). This struct mirrors that contract — every field beyond
/// the post identity and timestamp is optional/defaulted, so a payload that
/// omits a location, caption, thumbnail, or score decodes cleanly instead of
/// being rejected to the DLQ.
#[derive(Debug, Deserialize)]
pub struct PostPublishedEvent {
    pub post_id:         String,
    /// Author identity. `services/post` emits this as `profile_id`; geo-discovery
    /// treats it as the author id for the card projection.
    pub profile_id:      String,
    /// Post caption. Empty when the post has none. Stored on the card for the
    /// Focus-mode read path.
    #[serde(default)]
    pub caption:         String,
    /// Cover thumbnail for the map pin. Absent for text-only posts.
    #[serde(default)]
    pub thumbnail_url:   Option<String>,
    /// Post location (WGS-84). `lat`/`lng` are emitted together by `services/post`
    /// or not at all. Absent → the post is NOT spatially indexed (skipped).
    #[serde(default)]
    pub lat:             Option<f64>,
    #[serde(default)]
    pub lng:             Option<f64>,
    /// Initial virality score. Typically 0.0 for brand-new posts. `services/post`
    /// does not emit it; virality is geo-discovery's own concern.
    #[serde(default)]
    pub virality_score:  f64,
    /// Unix epoch milliseconds of the publication timestamp.
    pub published_at_ms: i64,
    /// Optional retention override in seconds, counted from `published_at_ms`.
    /// Absent → 172 800 s (48 h).
    #[serde(default)]
    pub retention_secs:  Option<u64>,
    /// Author tier at publish time. 0=Standard, 1=Premium, 2=VIP.
    /// Denormalized by services/post from services/profile. Absent → 0 (Standard).
    #[serde(default)]
    pub author_tier:     u8,
}

/// Long-lived background worker that consumes `post.published` events and
/// indexes each post into the spatial index and card store.
///
/// Delivery semantics: at-least-once (auto-commit enabled). All writes are
/// idempotent (ZADD + cap Lua, ScyllaDB INSERT with no IF conditions), so
/// duplicate deliveries are safe.
pub struct PostIndexerWorker<SI, CS, TR, PS> {
    kafka_config:        KafkaClientConfig,
    spatial_index:       Arc<SI>,
    card_store:          Arc<CS>,
    tile_repository:     Arc<TR>,
    pin_store:           Arc<PS>,
    group_id:            String,
    card_cache_threshold: f64,
    /// The country ladder's post counts (#665), when wired.
    country_activity:    Option<(Arc<dyn crate::application::port::CountryActivityStore>, &'static crate::domain::country_atlas::CountryAtlas)>,
}

impl PostIndexerWorker<RedisGeoSpatialIndex, RedisCardStore, ScyllaTileRepository, RedisPinStore> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        kafka_config:        KafkaClientConfig,
        spatial_index:       Arc<RedisGeoSpatialIndex>,
        card_store:          Arc<RedisCardStore>,
        tile_repository:     Arc<ScyllaTileRepository>,
        pin_store:           Arc<RedisPinStore>,
        group_id:            impl Into<String>,
        card_cache_threshold: f64,
    ) -> Self {
        Self {
            kafka_config,
            spatial_index,
            card_store,
            tile_repository,
            pin_store,
            group_id: group_id.into(),
            card_cache_threshold,
            country_activity: None,
        }
    }
}

impl<SI, CS, TR, PS> PostIndexerWorker<SI, CS, TR, PS> {
    /// Counts each indexed post for the country it was published in (#665).
    pub fn with_country_activity(
        mut self,
        activity: Arc<dyn crate::application::port::CountryActivityStore>,
        atlas: &'static crate::domain::country_atlas::CountryAtlas,
    ) -> Self {
        self.country_activity = Some((activity, atlas));
        self
    }
}

impl<SI, CS, TR, PS> PostIndexerWorker<SI, CS, TR, PS>
where
    SI: SpatialIndex + 'static,
    CS: CardStore + 'static,
    TR: TileRepository + 'static,
    PS: PinStore + 'static,
{
    pub async fn run(self) {
        let producer = match build_dlq_producer(&self.kafka_config) {
            Ok(producer) => producer,
            Err(e) => {
                tracing::error!(topic = TOPIC, error = %e, "failed to build DLQ producer — post indexer consumer not started");
                return;
            }
        };

        let worker = Arc::new(self);
        loop {
            match worker.clone().run_once(&producer).await {
                Ok(()) => {
                    tracing::warn!(topic = TOPIC, "post indexer consumer exited cleanly — restarting");
                }
                Err(e) => {
                    tracing::error!(topic = TOPIC, error = %e, "post indexer consumer error — restarting after 5 s");
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }
            }
        }
    }

    async fn run_once(self: Arc<Self>, producer: &KafkaProducerHandle) -> Result<(), String> {
        let mut config = ConsumerConfig::new(self.kafka_config.clone(), &self.group_id);
        config.auto_offset_reset  = AutoOffsetReset::Earliest;
        config.enable_auto_commit = false;

        let handle = KafkaConsumerBuilder::new(config)
            .subscribe(TOPIC)
            .build()
            .map_err(|e| e.to_string())?;

        tracing::info!(topic = TOPIC, group = %self.group_id, "post indexer consumer started");

        let policy = RetryPolicy::default();
        run_consumer::<PostPublishedEvent, _>(&handle, producer, &policy, move |event| {
            let worker = Arc::clone(&self);
            Box::pin(async move { ProcessOutcome::from_result(worker.process(event).await) })
        })
        .await
        .map_err(|e| e.to_string())
    }

    async fn process(&self, event: &PostPublishedEvent) -> Result<(), crate::error::GeoDiscoveryError> {
        use cqrs::{CommandHandler, Envelope};
        use uuid::Uuid;

        // A post without a location is not a map post — skip spatial indexing.
        // lat/lng are emitted together by services/post, so a partial pair is
        // treated as "no location" rather than an error.
        let (Some(lat), Some(lng)) = (event.lat, event.lng) else {
            tracing::debug!(
                post_id = %event.post_id,
                "post.published carried no location — skipping geo indexing"
            );
            return Ok(());
        };

        let handler = crate::application::command::IndexPostHandler {
            spatial_index:        Arc::clone(&self.spatial_index),
            card_store:           Arc::clone(&self.card_store),
            tile_repository:      Arc::clone(&self.tile_repository),
            pin_store:            Arc::clone(&self.pin_store),
            card_cache_threshold: self.card_cache_threshold,
        };

        let cmd = IndexPostCommand {
            post_id:           event.post_id.clone(),
            // services/post emits the author identity as `profile_id`.
            author_id:         event.profile_id.clone(),
            // Display name + avatar are NOT carried on post.published (profile-owned).
            // They are backfilled from profile.v1.events by a separate consumer;
            // until that join runs, the card renders them empty.
            author_handle:     String::new(),
            author_avatar_url: String::new(),
            thumbnail_url:     event.thumbnail_url.clone().unwrap_or_default(),
            caption:           event.caption.clone(),
            lat,
            lng,
            virality_score:    event.virality_score,
            published_at_ms:   event.published_at_ms,
            retention_secs:    event.retention_secs,
            author_tier:       event.author_tier,
        };

        handler.handle(Envelope::new(Uuid::now_v7(), cmd)).await?;

        // The country ladder (#665): one more post where it was published (at
        // sea or off every border: nowhere).
        if let Some((activity, atlas)) = &self.country_activity {
            let day = chrono::DateTime::from_timestamp_millis(event.published_at_ms).map(|at| at.date_naive());
            if let (Some(country), Some(day)) = (atlas.country_at(lat, lng), day) {
                activity.add(country, day, 0, 1).await?;
            }
        }
        Ok(())
    }
}
