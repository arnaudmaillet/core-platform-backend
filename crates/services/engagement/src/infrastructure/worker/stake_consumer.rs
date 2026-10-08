//! Likes are points (#665): `wallet.v1.events` `stake_committed` → the
//! target's likes. Each event carries the account's total on the target, so
//! applying it is idempotent and order-proof (a total no larger than the one
//! held changes nothing): redeliveries and the outbox's at-least-once are
//! absorbed without a dedup marker. Redis first (what readers see), then the
//! durable copy. Other wallet events are skipped. A deleted account's late
//! stakes are dropped (its likes were erased, the count kept).

use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::Deserialize;
use transport::kafka::config::client::KafkaClientConfig;
use transport::kafka::config::consumer::{AutoOffsetReset, ConsumerConfig};
use transport::kafka::consumer::builder::KafkaConsumerBuilder;
use transport::kafka::consumer::{run_consumer, ProcessOutcome, RetryPolicy};
use transport::kafka::producer::KafkaProducerHandle;

use crate::application::erasure::anonymous_liker;
use crate::application::port::{ForgottenLike, LikeLedger, LikeStore};
use crate::domain::value_object::LikeTarget;
use crate::error::EngagementError;
use crate::infrastructure::worker::build_dlq_producer;

const TOPIC: &str = "wallet.v1.events";

/// Lenient read of the wallet's `WalletEvent` (tagged `type`, snake_case).
#[derive(Debug, Deserialize)]
pub struct WalletEvent {
    #[serde(rename = "type")]
    kind:        String,
    #[serde(default)]
    account_id:  String,
    #[serde(default)]
    profile_id:  String,
    #[serde(default)]
    target_kind: String,
    #[serde(default)]
    target_id:   String,
    #[serde(default)]
    total:       i64,
    #[serde(default)]
    staked_at:   Option<DateTime<Utc>>,
}

/// What an event asks of the likes.
#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    Skip,
    Apply { target: LikeTarget, account: String, profile: String, total: i64, at_micros: i64 },
    Poison(String),
}

fn outcome(event: &WalletEvent) -> Outcome {
    if event.kind != "stake_committed" {
        return Outcome::Skip;
    }
    let target = match LikeTarget::parse(&event.target_kind, &event.target_id) {
        Ok(target) => target,
        Err(e) => return Outcome::Poison(e.to_string()),
    };
    if uuid::Uuid::parse_str(&event.account_id).is_err() || event.total <= 0 {
        return Outcome::Poison(format!("stake_committed with account {:?}, total {}", event.account_id, event.total));
    }
    let Some(at) = event.staked_at else {
        return Outcome::Poison("stake_committed without staked_at".into());
    };
    Outcome::Apply {
        target,
        account: event.account_id.clone(),
        profile: event.profile_id.clone(),
        total: event.total,
        at_micros: at.timestamp_micros(),
    }
}

pub struct StakeConsumer {
    kafka_config: KafkaClientConfig,
    likes:        Arc<dyn LikeStore>,
    ledger:       Arc<dyn LikeLedger>,
    group_id:     String,
}

impl StakeConsumer {
    pub fn new(
        kafka_config: KafkaClientConfig,
        likes: Arc<dyn LikeStore>,
        ledger: Arc<dyn LikeLedger>,
        group_id: impl Into<String>,
    ) -> Self {
        Self { kafka_config, likes, ledger, group_id: group_id.into() }
    }

    pub async fn run(self) {
        let producer = match build_dlq_producer(&self.kafka_config) {
            Ok(producer) => producer,
            Err(e) => {
                tracing::error!(error = %e, "failed to build DLQ producer — stake consumer not started");
                return;
            }
        };
        let worker = Arc::new(self);
        loop {
            match worker.clone().run_once(&producer).await {
                Ok(()) => tracing::warn!("stake consumer exited cleanly — restarting"),
                Err(e) => {
                    tracing::error!(error = %e, "stake consumer error — restarting after 5 s");
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }
            }
        }
    }

    async fn run_once(self: Arc<Self>, producer: &KafkaProducerHandle) -> Result<(), String> {
        let mut config = ConsumerConfig::new(self.kafka_config.clone(), &self.group_id);
        config.auto_offset_reset = AutoOffsetReset::Earliest;
        config.enable_auto_commit = false;
        let handle = KafkaConsumerBuilder::new(config).subscribe(TOPIC).build().map_err(|e| e.to_string())?;
        tracing::info!(topic = TOPIC, group = %self.group_id, "stake consumer started");
        let policy = RetryPolicy::default();
        run_consumer::<WalletEvent, _>(&handle, producer, &policy, move |event| {
            let worker = Arc::clone(&self);
            Box::pin(async move {
                match outcome(event) {
                    Outcome::Skip => ProcessOutcome::Done,
                    Outcome::Poison(reason) => ProcessOutcome::Reject(reason),
                    Outcome::Apply { target, account, profile, total, at_micros } => {
                        ProcessOutcome::from_result(worker.apply(&target, &account, &profile, total, at_micros).await)
                    }
                }
            })
        })
        .await
        .map_err(|e| e.to_string())
    }

    async fn apply(&self, target: &LikeTarget, account: &str, profile: &str, total: i64, at_micros: i64) -> Result<(), EngagementError> {
        apply_stake(self.likes.as_ref(), self.ledger.as_ref(), target, account, profile, total, at_micros).await
    }
}

/// One stake: dropped when its account was deleted; otherwise applied, and
/// forgotten again if the deletion landed meanwhile (its erasure may have
/// listed the account's likes before this one was written).
async fn apply_stake(
    likes: &dyn LikeStore,
    ledger: &dyn LikeLedger,
    target: &LikeTarget,
    account: &str,
    profile: &str,
    total: i64,
    at_micros: i64,
) -> Result<(), EngagementError> {
    if ledger.erased_at(account).await?.is_some() {
        tracing::info!(target = %target, "stake of a deleted account dropped");
        return Ok(());
    }
    crate::application::likes::apply_total(likes, ledger, target, account, total).await?;
    ledger.record(target, account, profile, total, at_micros).await?;
    if let Some(erased_at) = ledger.erased_at(account).await? {
        likes.forget(account, std::slice::from_ref(target)).await?;
        let like = ForgottenLike { target: target.clone(), total, anonymous_id: anonymous_liker(account, target, erased_at) };
        ledger.forget(account, &[like], chrono::Utc::now().timestamp_micros()).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;
    use wallet::domain::event::{StakeCommitted, WalletEvent as Wallet};

    use super::*;

    /// Serialized with the wallet's own types, read back as the worker does.
    fn wire(event: Wallet) -> WalletEvent {
        serde_json::from_slice(&serde_json::to_vec(&event).unwrap()).unwrap()
    }

    #[test]
    fn a_stake_applies_the_accounts_total_on_its_target() {
        let account = Uuid::now_v7().to_string();
        let at = Utc::now();
        let event = wire(Wallet::StakeCommitted(StakeCommitted {
            account_id:        account.clone(),
            profile_id:        "liker".into(),
            target_kind:       "comment".into(),
            target_id:         "c1".into(),
            author_profile_id: "author".into(),
            points:            5,
            total:             35,
            first:             false,
            stake_key:         "stake:k".into(),
            staked_at:         at,
        }));
        assert_eq!(
            outcome(&event),
            Outcome::Apply {
                target: LikeTarget::Comment("c1".into()),
                account,
                profile: "liker".into(),
                total: 35,
                at_micros: at.timestamp_micros(),
            }
        );
        let other: WalletEvent = serde_json::from_str(r#"{"type":"something_else"}"#).unwrap();
        assert_eq!(outcome(&other), Outcome::Skip);
    }

    #[tokio::test]
    async fn a_deleted_accounts_late_stake_is_dropped() {
        use crate::application::fakes::Likes;
        let likes = Likes::default();
        let post = LikeTarget::Post("p1".into());
        apply_stake(&likes, &likes, &post, "a", "liker", 4, 1).await.unwrap();
        likes.mark_erased("gone", 1).await.unwrap();
        apply_stake(&likes, &likes, &post, "gone", "liker", 9, 1).await.unwrap();
        assert_eq!(likes.counts(std::slice::from_ref(&post)).await.unwrap(), vec![4]);
        assert_eq!(likes.mine("gone", &[post]).await.unwrap(), vec![Some(0)]);
    }
}
