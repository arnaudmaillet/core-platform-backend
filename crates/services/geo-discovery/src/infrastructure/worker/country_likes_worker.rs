//! The country ladder's likes (#665): `engagement.reactions` → each heart
//! counted for the country of the post it lands on (from the post's map card
//! and the shared borders), on the reaction's UTC day. A post off the map, at
//! sea, or whose card has expired counts nowhere. Other kinds are skipped.

use std::sync::Arc;

use chrono::{DateTime, NaiveDate};
use serde::Deserialize;
use transport::kafka::config::client::KafkaClientConfig;
use transport::kafka::config::consumer::{AutoOffsetReset, ConsumerConfig};
use transport::kafka::consumer::builder::KafkaConsumerBuilder;
use transport::kafka::consumer::{run_consumer, ProcessOutcome, RetryPolicy};
use transport::kafka::producer::KafkaProducerHandle;
use uuid::Uuid;

use crate::application::port::{CountryActivityStore, TileRepository};
use crate::domain::country_atlas::CountryAtlas;
use crate::domain::value_object::PostId;
use crate::error::GeoDiscoveryError;
use crate::infrastructure::worker::build_dlq_producer;

const TOPIC: &str = "engagement.reactions";
const HEART: &str = "heart";

/// Lenient read of engagement's `ReactionKafkaEvent` (tagged `event_type`).
#[derive(Debug, Deserialize)]
pub struct ReactionEvent {
    event_type:  String,
    #[serde(default)]
    post_id:     String,
    #[serde(default)]
    new_kind:    Option<String>,
    #[serde(default)]
    old_kind:    Option<String>,
    /// `removed`'s kind.
    #[serde(default)]
    kind:        Option<String>,
    #[serde(default)]
    event_at_ms: i64,
}

/// What a reaction does to its post's likes.
#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    Skip,
    Count { post: Uuid, delta: i64, day: NaiveDate },
    Poison(String),
}

fn outcome(event: &ReactionEvent) -> Outcome {
    let is_heart = |k: &Option<String>| k.as_deref() == Some(HEART);
    let delta = match event.event_type.as_str() {
        "upserted" if is_heart(&event.new_kind) && !is_heart(&event.old_kind) => 1,
        "upserted" if !is_heart(&event.new_kind) && is_heart(&event.old_kind) => -1,
        "removed" if is_heart(&event.kind) => -1,
        _ => return Outcome::Skip,
    };
    let Ok(post) = Uuid::parse_str(&event.post_id) else {
        return Outcome::Poison(format!("bad post_id {:?}", event.post_id));
    };
    let Some(at) = DateTime::from_timestamp_millis(event.event_at_ms) else {
        return Outcome::Poison(format!("bad event_at_ms {}", event.event_at_ms));
    };
    Outcome::Count { post, delta, day: at.date_naive() }
}

pub struct CountryLikesWorker<TR> {
    kafka_config:    KafkaClientConfig,
    tile_repository: Arc<TR>,
    activity:        Arc<dyn CountryActivityStore>,
    atlas:           &'static CountryAtlas,
    group_id:        String,
}

impl<TR: TileRepository + 'static> CountryLikesWorker<TR> {
    pub fn new(
        kafka_config: KafkaClientConfig,
        tile_repository: Arc<TR>,
        activity: Arc<dyn CountryActivityStore>,
        atlas: &'static CountryAtlas,
        group_id: impl Into<String>,
    ) -> Self {
        Self { kafka_config, tile_repository, activity, atlas, group_id: group_id.into() }
    }

    pub async fn run(self) {
        let producer = match build_dlq_producer(&self.kafka_config) {
            Ok(producer) => producer,
            Err(e) => {
                tracing::error!(error = %e, "failed to build DLQ producer — country likes consumer not started");
                return;
            }
        };
        let worker = Arc::new(self);
        loop {
            match worker.clone().run_once(&producer).await {
                Ok(()) => tracing::warn!("country likes consumer exited cleanly — restarting"),
                Err(e) => {
                    tracing::error!(error = %e, "country likes consumer error — restarting after 5 s");
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }
            }
        }
    }

    async fn run_once(self: Arc<Self>, producer: &KafkaProducerHandle) -> Result<(), String> {
        let mut config = ConsumerConfig::new(self.kafka_config.clone(), &self.group_id);
        config.auto_offset_reset = AutoOffsetReset::Latest;
        config.enable_auto_commit = false;
        let handle = KafkaConsumerBuilder::new(config).subscribe(TOPIC).build().map_err(|e| e.to_string())?;
        tracing::info!(group = %self.group_id, "country likes consumer started");
        let policy = RetryPolicy::default();
        run_consumer::<ReactionEvent, _>(&handle, producer, &policy, move |event| {
            let worker = Arc::clone(&self);
            Box::pin(async move { worker.process(event).await })
        })
        .await
        .map_err(|e| e.to_string())
    }

    async fn process(&self, event: &ReactionEvent) -> ProcessOutcome {
        match outcome(event) {
            Outcome::Skip => ProcessOutcome::Done,
            Outcome::Poison(reason) => ProcessOutcome::Reject(reason),
            Outcome::Count { post, delta, day } => ProcessOutcome::from_result(self.count(post, delta, day).await),
        }
    }

    async fn count(&self, post: Uuid, delta: i64, day: NaiveDate) -> Result<(), GeoDiscoveryError> {
        let Some(card) = self.tile_repository.get_card(&PostId::from(post)).await? else { return Ok(()) };
        let (Some(lat), Some(lng)) = (card.lat, card.lng) else { return Ok(()) };
        match self.atlas.country_at(lat, lng) {
            Some(country) => self.activity.add(country, day, delta, 0).await,
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use engagement::domain::event::reaction_event::ReactionKafkaEvent;
    use engagement::domain::event::{ReactionRemovedEvent, ReactionUpsertedEvent};
    use engagement::domain::value_object::ReactionKind;

    use super::*;

    /// Serialized with engagement's own types, read back as the worker does.
    fn wire(event: ReactionKafkaEvent) -> ReactionEvent {
        serde_json::from_slice(&serde_json::to_vec(&event).unwrap()).unwrap()
    }

    fn upserted(new: ReactionKind, old: Option<ReactionKind>) -> ReactionEvent {
        wire(ReactionKafkaEvent::Upserted(ReactionUpsertedEvent {
            post_id: Uuid::nil().to_string(),
            profile_id: Uuid::now_v7().to_string(),
            new_kind: new,
            new_weight: 1,
            old_kind: old,
            old_weight: old.map(|_| 1),
            event_at_ms: 1_791_000_000_000,
        }))
    }

    #[test]
    fn hearts_count_up_and_down_other_kinds_skip() {
        let day = DateTime::from_timestamp_millis(1_791_000_000_000).unwrap().date_naive();
        let count = |delta| Outcome::Count { post: Uuid::nil(), delta, day };
        assert_eq!(outcome(&upserted(ReactionKind::Heart, None)), count(1));
        assert_eq!(outcome(&upserted(ReactionKind::Fire, Some(ReactionKind::Heart))), count(-1));
        assert_eq!(outcome(&upserted(ReactionKind::Heart, Some(ReactionKind::Heart))), Outcome::Skip);
        assert_eq!(outcome(&upserted(ReactionKind::Fire, None)), Outcome::Skip);
        let removed = wire(ReactionKafkaEvent::Removed(ReactionRemovedEvent {
            post_id: Uuid::nil().to_string(),
            profile_id: Uuid::now_v7().to_string(),
            kind: ReactionKind::Heart,
            weight: 1,
            event_at_ms: 1_791_000_000_000,
        }));
        assert_eq!(outcome(&removed), count(-1));
    }
}
