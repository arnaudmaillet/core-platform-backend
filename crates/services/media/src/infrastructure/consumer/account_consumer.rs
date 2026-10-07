//! `account.v1.events` → media (#777): `account_deleted` erases every asset
//! the account owns ([`OwnerErasure`]). Other account events are skipped.

use std::sync::Arc;

use chrono::Utc;
use serde::Deserialize;
use tracing::{error, info};

use transport::kafka::consumer::{run_consumer, KafkaConsumerHandle, ProcessOutcome, RetryPolicy};
use transport::kafka::producer::KafkaProducerHandle;

use crate::application::command::OwnerErasure;
use crate::domain::value_object::OwnerId;

/// account's `DomainEvent`, internally tagged on `type`, snake_case; only what
/// media acts on is read.
#[derive(Debug, Deserialize)]
pub struct AccountEvent {
    #[serde(rename = "type")]
    kind:       String,
    #[serde(default)]
    account_id: String,
}

/// What an event asks of media.
#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    Skip,
    Erase(OwnerId),
    Poison(String),
}

fn outcome(event: &AccountEvent) -> Outcome {
    if event.kind != "account_deleted" {
        return Outcome::Skip;
    }
    match OwnerId::try_from(event.account_id.as_str()) {
        Ok(owner) => Outcome::Erase(owner),
        Err(_) => Outcome::Poison(format!("account_deleted with a bad account_id {:?}", event.account_id)),
    }
}

/// Runs until the stream ends or the broker / dead-letter path fails; the
/// supervising task respawns it.
pub async fn run_account_consumer(
    consumer: KafkaConsumerHandle,
    erasure: Arc<OwnerErasure>,
    producer: KafkaProducerHandle,
) {
    info!("media account consumer started");
    let policy = RetryPolicy::default();
    let result = run_consumer::<AccountEvent, _>(&consumer, &producer, &policy, move |event| {
        let erasure = Arc::clone(&erasure);
        Box::pin(async move {
            match outcome(event) {
                Outcome::Skip => ProcessOutcome::Done,
                Outcome::Poison(reason) => ProcessOutcome::Reject(reason),
                Outcome::Erase(owner) => ProcessOutcome::from_result(erasure.erase(&owner, Utc::now()).await.map(|_| ())),
            }
        })
    })
    .await;
    if let Err(e) = result {
        error!(error = %e, "media account consumer stopped");
    }
}

#[cfg(test)]
mod tests {
    use account::domain::event::{AccountDeleted, AccountSuspended, DomainEvent as AccountDomainEvent};
    use uuid::Uuid;

    use super::*;

    /// Serialized with account's own types, read back as the consumer does.
    fn wire(event: AccountDomainEvent) -> AccountEvent {
        serde_json::from_slice(&serde_json::to_vec(&event).unwrap()).unwrap()
    }

    #[test]
    fn account_deleted_erases_the_accounts_media_other_events_skip() {
        let id = account::domain::value_object::AccountId::new();
        let deleted = wire(AccountDomainEvent::AccountDeleted(AccountDeleted {
            account_id: id,
            deleted_by: None,
            occurred_at: Utc::now(),
            correlation_id: Uuid::now_v7(),
        }));
        assert_eq!(outcome(&deleted), Outcome::Erase(OwnerId::try_from(id.to_string().as_str()).unwrap()));
        let suspended = wire(AccountDomainEvent::AccountSuspended(AccountSuspended {
            account_id: id,
            reason: "spam".into(),
            occurred_at: Utc::now(),
            correlation_id: Uuid::now_v7(),
        }));
        assert_eq!(outcome(&suspended), Outcome::Skip);
    }
}
