//! `auth.v1.events` → wallet (#827): `guest_upgraded` (a guest's device became
//! a member) credits the guest's welcome gift, once per device. Other auth
//! events are skipped.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::Deserialize;
use tracing::{error, info};

use transport::kafka::consumer::{run_consumer, KafkaConsumerHandle, ProcessOutcome, RetryPolicy};
use transport::kafka::producer::KafkaProducerHandle;

use crate::application::Wallets;
use crate::domain::AccountId;

/// auth's `DomainEvent`, internally tagged on `type`, snake_case; only what
/// the wallet acts on is read.
#[derive(Debug, Deserialize)]
pub struct AuthEvent {
    #[serde(rename = "type")]
    kind:            String,
    #[serde(default)]
    account_id:      String,
    #[serde(default)]
    device_key_hash: String,
    #[serde(default)]
    guest_since:     Option<DateTime<Utc>>,
    #[serde(default)]
    occurred_at:     Option<DateTime<Utc>>,
}

/// What an event asks of the wallet.
#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    Skip,
    Welcome { account: AccountId, device: String, since: DateTime<Utc>, at: DateTime<Utc> },
    Poison(String),
}

fn outcome(event: &AuthEvent) -> Outcome {
    if event.kind != "guest_upgraded" {
        return Outcome::Skip;
    }
    let Ok(account) = AccountId::parse(&event.account_id) else {
        return Outcome::Poison(format!("guest_upgraded with a bad account_id {:?}", event.account_id));
    };
    match (event.device_key_hash.len() == 64, event.guest_since, event.occurred_at) {
        (true, Some(since), Some(at)) => Outcome::Welcome { account, device: event.device_key_hash.clone(), since, at },
        _ => Outcome::Poison("guest_upgraded without its device or times".into()),
    }
}

/// Runs until the stream ends or the broker / dead-letter path fails; the
/// supervising task respawns it.
pub async fn run_auth_consumer(consumer: KafkaConsumerHandle, wallets: Arc<Wallets>, producer: KafkaProducerHandle) {
    info!("wallet auth consumer started");
    let policy = RetryPolicy::default();
    let result = run_consumer::<AuthEvent, _>(&consumer, &producer, &policy, move |event| {
        let wallets = Arc::clone(&wallets);
        Box::pin(async move {
            match outcome(event) {
                Outcome::Skip => ProcessOutcome::Done,
                Outcome::Poison(reason) => ProcessOutcome::Reject(reason),
                Outcome::Welcome { account, device, since, at } => {
                    ProcessOutcome::from_result(wallets.welcome_gift(&account, &device, since, at).await.map(|_| ()))
                }
            }
        })
    })
    .await;
    if let Err(e) = result {
        error!(error = %e, "wallet auth consumer stopped");
    }
}

#[cfg(test)]
mod tests {
    use auth::domain::event::{DomainEvent, GuestUpgraded, SessionRevoked};
    use uuid::Uuid;

    use super::*;

    /// Serialized with auth's own types, read back as the consumer does.
    fn wire(event: &DomainEvent) -> AuthEvent {
        serde_json::from_slice(&serde_json::to_vec(event).unwrap()).unwrap()
    }

    #[test]
    fn guest_upgraded_is_read_from_auths_own_wire() {
        let account = Uuid::now_v7();
        let since = Utc::now() - chrono::TimeDelta::days(2);
        let at = Utc::now();
        let event = DomainEvent::GuestUpgraded(GuestUpgraded {
            account_id:      auth::domain::value_object::AccountId::from_uuid(account),
            device_key_hash: "a".repeat(64),
            guest_since:     since,
            occurred_at:     at,
            correlation_id:  Uuid::now_v7(),
        });
        assert_eq!(
            outcome(&wire(&event)),
            Outcome::Welcome { account: AccountId::from_uuid(account), device: "a".repeat(64), since, at }
        );
    }

    #[test]
    fn other_auth_events_are_skipped_and_a_malformed_upgrade_is_poison() {
        let revoked = serde_json::json!({ "type": "session_revoked", "account_id": Uuid::now_v7().to_string() });
        assert_eq!(outcome(&serde_json::from_value(revoked).unwrap()), Outcome::Skip);
        let _ = std::any::type_name::<SessionRevoked>();
        let bad = serde_json::json!({ "type": "guest_upgraded", "account_id": Uuid::now_v7().to_string(), "device_key_hash": "short" });
        assert!(matches!(outcome(&serde_json::from_value(bad).unwrap()), Outcome::Poison(_)));
    }
}
