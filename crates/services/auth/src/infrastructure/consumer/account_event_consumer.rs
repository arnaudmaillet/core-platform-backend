//! `account.v1.events` → auth: `account_deleted` erases what auth holds about
//! the account (GDPR Art. 17, see `AccountErasure`); `gdpr_data_export_completed`
//! emails the holder their export's link (#653, see `ExportReadyNotifier`).
//! Every other kind is a no-op.

use std::sync::Arc;

use serde::Deserialize;
use uuid::Uuid;

use error::AppError;
use transport::kafka::consumer::{run_consumer, KafkaConsumerHandle, ProcessOutcome, RetryPolicy};
use transport::kafka::producer::KafkaProducerHandle;

use crate::application::command::{AccountErasure, ExportReadyNotifier};
use crate::domain::value_object::AccountId;

pub const ACCOUNT_EVENTS_TOPIC: &str = "account.v1.events";
pub const ACCOUNT_EVENTS_GROUP: &str = "auth-account-events";

/// account's `DomainEvent`, internally tagged on `type`, snake_case; only what
/// auth acts on is read.
#[derive(Debug, Deserialize)]
struct AccountEvent {
    #[serde(rename = "type")]
    kind:       String,
    account_id: String,
}

/// Runs until the stream ends or the broker / dead-letter path fails; the
/// supervising task respawns it. The handle must not auto-commit.
pub async fn run_account_event_consumer(
    consumer: KafkaConsumerHandle,
    erasure: Arc<AccountErasure>,
    exports: Arc<ExportReadyNotifier>,
    producer: KafkaProducerHandle,
) {
    tracing::info!("account event consumer started");
    let policy = RetryPolicy::default();
    let result = run_consumer::<AccountEvent, _>(&consumer, &producer, &policy, move |event| {
        let (erasure, exports) = (Arc::clone(&erasure), Arc::clone(&exports));
        Box::pin(async move { process_event(&erasure, &exports, event).await })
    })
    .await;
    if let Err(e) = result {
        tracing::error!(error = %e, "account event consumer stopped");
    }
}

async fn process_event(erasure: &AccountErasure, exports: &ExportReadyNotifier, event: &AccountEvent) -> ProcessOutcome {
    let handled = matches!(event.kind.as_str(), "account_deleted" | "gdpr_data_export_completed");
    if !handled {
        return ProcessOutcome::Done;
    }
    let Ok(uuid) = Uuid::parse_str(&event.account_id) else {
        return ProcessOutcome::Reject(format!("{} with a malformed account_id {:?}", event.kind, event.account_id));
    };
    let account = AccountId::from_uuid(uuid);
    let outcome = match event.kind.as_str() {
        "account_deleted" => erasure.erase(&account).await.map(|_| ()),
        _ => exports.notify(&account).await.map(|outcome| {
            tracing::info!(?outcome, "data export ready notice");
        }),
    };
    match outcome {
        Ok(()) => ProcessOutcome::Done,
        Err(e) if e.is_retryable() => ProcessOutcome::Retry(e.to_string()),
        Err(e) => ProcessOutcome::Reject(e.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use account::domain::event::{AccountDeleted, AccountSuspended, DomainEvent as AccountDomainEvent};
    use chrono::Utc;

    use super::*;

    /// Serialized with account's own types, read back the way the consumer does.
    fn wire(event: AccountDomainEvent) -> AccountEvent {
        serde_json::from_slice(&serde_json::to_vec(&event).unwrap()).unwrap()
    }

    #[test]
    fn reads_account_deleted_as_account_emits_it() {
        let id = account::domain::value_object::AccountId::new();
        let deleted = wire(AccountDomainEvent::AccountDeleted(AccountDeleted {
            account_id: id,
            deleted_by: None,
            occurred_at: Utc::now(),
            correlation_id: Uuid::now_v7(),
        }));
        assert_eq!(deleted.kind, "account_deleted");
        assert_eq!(deleted.account_id, id.to_string());
        assert!(Uuid::parse_str(&deleted.account_id).is_ok());

        let suspended = wire(AccountDomainEvent::AccountSuspended(AccountSuspended {
            account_id: id,
            reason: "spam".into(),
            occurred_at: Utc::now(),
            correlation_id: Uuid::now_v7(),
        }));
        assert_eq!(suspended.kind, "account_suspended");
    }

    /// #653: account's own `GdprDataExportCompleted` reads as the kind the
    /// consumer acts on, with the account id — and no link on the wire.
    #[test]
    fn reads_gdpr_data_export_completed_as_account_emits_it() {
        let id = account::domain::value_object::AccountId::new();
        let event = account::domain::event::GdprDataExportCompleted {
            account_id: id,
            expires_at: Utc::now(),
            occurred_at: Utc::now(),
            correlation_id: Uuid::now_v7(),
        };
        let json = serde_json::to_string(&AccountDomainEvent::GdprDataExportCompleted(event.clone())).unwrap();
        assert!(!json.contains("http") && !json.contains("url"), "no link on the event: {json}");
        let read = wire(AccountDomainEvent::GdprDataExportCompleted(event));
        assert_eq!(read.kind, "gdpr_data_export_completed");
        assert_eq!(read.account_id, id.to_string());
    }
}
