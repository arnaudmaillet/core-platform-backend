//! `account.v1.events` → wallet: `account_deleted` erases the account's
//! wallet and history. Other account events are skipped.

use std::sync::Arc;

use serde::Deserialize;
use tracing::{error, info};

use transport::kafka::consumer::{run_consumer, KafkaConsumerHandle, ProcessOutcome, RetryPolicy};
use transport::kafka::producer::KafkaProducerHandle;

use crate::application::Wallets;
use crate::domain::AccountId;

/// account's `DomainEvent`, internally tagged on `type`, snake_case; only what
/// the wallet acts on is read.
#[derive(Debug, Deserialize)]
pub struct AccountEvent {
    #[serde(rename = "type")]
    kind:       String,
    #[serde(default)]
    account_id: String,
}

/// What an event asks of the wallet.
#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    Skip,
    Erase(AccountId),
    Poison(String),
}

fn outcome(event: &AccountEvent) -> Outcome {
    if event.kind != "account_deleted" {
        return Outcome::Skip;
    }
    match AccountId::parse(&event.account_id) {
        Ok(account) => Outcome::Erase(account),
        Err(_) => Outcome::Poison(format!("account_deleted with a bad account_id {:?}", event.account_id)),
    }
}

/// Runs until the stream ends or the broker / dead-letter path fails; the
/// supervising task respawns it.
pub async fn run_account_consumer(consumer: KafkaConsumerHandle, wallets: Arc<Wallets>, producer: KafkaProducerHandle) {
    info!("wallet account consumer started");
    let policy = RetryPolicy::default();
    let result = run_consumer::<AccountEvent, _>(&consumer, &producer, &policy, move |event| {
        let wallets = Arc::clone(&wallets);
        Box::pin(async move {
            match outcome(event) {
                Outcome::Skip => ProcessOutcome::Done,
                Outcome::Poison(reason) => ProcessOutcome::Reject(reason),
                Outcome::Erase(account) => ProcessOutcome::from_result(wallets.erase(&account).await.map(|_| ())),
            }
        })
    })
    .await;
    if let Err(e) = result {
        error!(error = %e, "wallet account consumer stopped");
    }
}

#[cfg(test)]
mod tests {
    use account::domain::event::{AccountDeleted, AccountSuspended, DomainEvent as AccountDomainEvent};
    use chrono::Utc;
    use uuid::Uuid;

    use super::*;

    /// Serialized with account's own types, read back as the consumer does.
    fn wire(event: AccountDomainEvent) -> AccountEvent {
        serde_json::from_slice(&serde_json::to_vec(&event).unwrap()).unwrap()
    }

    #[test]
    fn account_deleted_erases_the_wallet_other_events_skip() {
        let id = account::domain::value_object::AccountId::new();
        let deleted = wire(AccountDomainEvent::AccountDeleted(AccountDeleted {
            account_id: id,
            deleted_by: None,
            occurred_at: Utc::now(),
            correlation_id: Uuid::now_v7(),
        }));
        assert_eq!(outcome(&deleted), Outcome::Erase(AccountId::parse(&id.to_string()).unwrap()));
        let suspended = wire(AccountDomainEvent::AccountSuspended(AccountSuspended {
            account_id: id,
            reason: "spam".into(),
            occurred_at: Utc::now(),
            correlation_id: Uuid::now_v7(),
        }));
        assert_eq!(outcome(&suspended), Outcome::Skip);
    }
}
