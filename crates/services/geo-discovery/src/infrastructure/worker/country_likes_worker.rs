//! The country ladder's likes (#665: a like is a point): `wallet.v1.events`
//! `stake_committed` on a post → its points counted for the country of the
//! post (from the post's map card and the shared borders), on the stake's UTC
//! day. A post off the map, at sea, or whose card has expired counts nowhere;
//! likes on comments and other wallet events are skipped.
//!
//! Idempotent: each stake is counted once, keyed by its account and batch key
//! — a redelivery or the wallet outbox's at-least-once counts nothing more.

use std::sync::Arc;

use chrono::{DateTime, NaiveDate, Utc};
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

const TOPIC: &str = "wallet.v1.events";

/// Lenient read of the wallet's `WalletEvent` (tagged `type`, snake_case).
#[derive(Debug, Deserialize)]
pub struct ReactionEvent {
    #[serde(rename = "type")]
    kind:        String,
    #[serde(default)]
    account_id:  String,
    #[serde(default)]
    target_kind: String,
    #[serde(default)]
    target_id:   String,
    #[serde(default)]
    points:      i64,
    #[serde(default)]
    stake_key:   String,
    #[serde(default)]
    staked_at:   Option<DateTime<Utc>>,
}

/// What a stake does to its post's likes.
#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    Skip,
    Count { post: Uuid, delta: i64, day: NaiveDate, event: String },
    Poison(String),
}

fn outcome(event: &ReactionEvent) -> Outcome {
    if event.kind != "stake_committed" || event.target_kind != "post" || event.points <= 0 {
        return Outcome::Skip;
    }
    let Ok(post) = Uuid::parse_str(&event.target_id) else {
        return Outcome::Poison(format!("bad post id {:?}", event.target_id));
    };
    let Some(at) = event.staked_at else {
        return Outcome::Poison("stake_committed without staked_at".into());
    };
    if event.account_id.is_empty() || event.stake_key.is_empty() {
        return Outcome::Poison("stake_committed without account or key".into());
    }
    // A batch key is unique per account: together they name the stake.
    let event_key = format!("l:{}:{}", event.account_id, event.stake_key);
    Outcome::Count { post, delta: event.points, day: at.date_naive(), event: event_key }
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
            Outcome::Count { post, delta, day, event } => {
                ProcessOutcome::from_result(self.count(post, delta, day, &event).await)
            }
        }
    }

    async fn count(&self, post: Uuid, delta: i64, day: NaiveDate, event: &str) -> Result<(), GeoDiscoveryError> {
        let Some(card) = self.tile_repository.get_card(&PostId::from(post)).await? else { return Ok(()) };
        let (Some(lat), Some(lng)) = (card.lat, card.lng) else { return Ok(()) };
        match self.atlas.country_at(lat, lng) {
            Some(country) => self.activity.add(country, day, delta, 0, event).await,
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use wallet::domain::event::{StakeCommitted, WalletEvent};

    use super::*;

    /// Serialized with the wallet's own types, read back as the worker does.
    fn wire(event: WalletEvent) -> ReactionEvent {
        serde_json::from_slice(&serde_json::to_vec(&event).unwrap()).unwrap()
    }

    fn stake(target_kind: &str, target_id: &str, points: i64) -> ReactionEvent {
        wire(WalletEvent::StakeCommitted(StakeCommitted {
            account_id: "acc".into(),
            profile_id: "liker".into(),
            target_kind: target_kind.into(),
            target_id: target_id.into(),
            author_profile_id: "author".into(),
            points,
            total: points,
            first: true,
            stake_key: "stake:k1".into(),
            staked_at: Utc.with_ymd_and_hms(2026, 10, 8, 12, 0, 0).unwrap(),
        }))
    }

    #[test]
    fn a_stake_on_a_post_counts_its_points_once_per_stake() {
        let post = Uuid::nil().to_string();
        let day = NaiveDate::from_ymd_opt(2026, 10, 8).unwrap();
        assert_eq!(
            outcome(&stake("post", &post, 30)),
            Outcome::Count { post: Uuid::nil(), delta: 30, day, event: "l:acc:stake:k1".into() }
        );
        assert_eq!(outcome(&stake("comment", "c1", 30)), Outcome::Skip, "comments have no country");
        let other: ReactionEvent = serde_json::from_str(r#"{"type":"something_new"}"#).unwrap();
        assert_eq!(outcome(&other), Outcome::Skip);
    }
}
