use std::sync::Arc;

use serde::Deserialize;
use tracing::{error, info};
use uuid::Uuid;

use cqrs::{CommandBus, Envelope};
use error::AppError;
use transport::kafka::consumer::{
    run_consumer, KafkaConsumerHandle, ProcessOutcome, RetryPolicy,
};
use transport::kafka::producer::KafkaProducerHandle;

use crate::application::command::{HideAccountProfilesCommand, RestoreAccountProfilesCommand};

/// Kafka event payload published by the account service on `account.v1.events`:
/// account's `DomainEvent`, internally tagged on `type`, snake_case
/// (`{"type":"account_suspended","account_id":…,"reason":…}`).
///
/// Only the fields relevant to profile masking/restoration are deserialized.
/// Event types profile does not act on are committed as no-ops.
#[derive(Debug, Deserialize)]
struct AccountEvent {
    #[serde(rename = "type")]
    kind: String,
    account_id: String,
    #[serde(default)]
    reason: Option<String>,
}

/// Runs the account event consumer on the shared at-least-once runner.
///
/// Subscribes (via the supplied handle) to `account.v1.events` and translates
/// account lifecycle events into profile masking/restoration commands. The runner
/// owns the decode → process → retry → dead-letter → commit loop: transient
/// dispatch failures are retried with backoff then dead-lettered, poison records
/// are dead-lettered immediately, and unknown event kinds are committed as no-ops.
///
/// The function is generic over `CB` because `CommandBus` is not object-safe
/// (its `dispatch` method is generic over `C: Command`). The handle must be built
/// with `enable_auto_commit = false`. Returns when the stream ends or on an
/// unrecoverable broker/dead-letter error; the supervising task should respawn it.
pub async fn run_account_event_consumer<CB: CommandBus + 'static>(
    consumer: KafkaConsumerHandle,
    command_bus: CB,
    producer: KafkaProducerHandle,
) {
    info!("account event consumer started");

    // `Arc<CB>` so the per-message closure captures an owned handle (the returned
    // futures then borrow only the event, satisfying the runner bound).
    let command_bus = Arc::new(command_bus);
    let policy = RetryPolicy::default();

    let result = run_consumer::<AccountEvent, _>(&consumer, &producer, &policy, move |event| {
        let command_bus = Arc::clone(&command_bus);
        Box::pin(async move { process_event(command_bus.as_ref(), event).await })
    })
    .await;

    if let Err(e) = result {
        error!(error = %e, "account event consumer stopped");
    }
}

/// Translates one account event into the matching profile command and classifies
/// the result. Unknown event kinds are intentional no-ops (`Done`, so they commit
/// rather than dead-letter); a transient dispatch failure is retried then
/// dead-lettered, and a permanent one is dead-lettered immediately.
async fn process_event<CB: CommandBus>(command_bus: &CB, event: &AccountEvent) -> ProcessOutcome {
    let correlation_id = Uuid::now_v7();

    // An account owns N profiles: each command walks all of them.
    let dispatch = match event.kind.as_str() {
        kind @ ("account_suspended" | "account_deactivated" | "account_deleted") => {
            // The event kind doubles as the masking reason.
            let cmd = HideAccountProfilesCommand {
                account_id:        event.account_id.clone(),
                masking_reason:    kind.to_owned(),
                suspension_reason: event.reason.clone(),
            };
            command_bus.dispatch(Envelope::new(correlation_id, cmd)).await
        }

        "account_activated" => {
            let cmd = RestoreAccountProfilesCommand { account_id: event.account_id.clone() };
            command_bus.dispatch(Envelope::new(correlation_id, cmd)).await
        }

        other => {
            tracing::trace!(event_kind = other, "ignoring account event kind");
            return ProcessOutcome::Done;
        }
    };

    match dispatch {
        Ok(())                     => ProcessOutcome::Done,
        Err(e) if e.is_retryable() => ProcessOutcome::Retry(e.to_string()),
        Err(e)                     => ProcessOutcome::Reject(e.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use account::domain::event::{
        AccountActivated, AccountDeactivated, AccountDeleted, AccountSuspended,
        DomainEvent as AccountDomainEvent,
    };
    use crate::domain::value_object::MaskingReason;
    use account::domain::value_object::AccountId;
    use chrono::Utc;

    use super::*;

    /// Serializes with account's own types and reads it back the way the
    /// consumer does. The consumer used to expect `{"event_kind":"AccountSuspended"}`,
    /// a shape account never emitted, so every account event dead-lettered.
    fn wire(event: AccountDomainEvent) -> AccountEvent {
        serde_json::from_slice(&serde_json::to_vec(&event).unwrap()).unwrap()
    }

    #[test]
    fn decodes_account_lifecycle_events_as_account_emits_them() {
        let id = AccountId::new();
        let suspended = wire(AccountDomainEvent::AccountSuspended(AccountSuspended {
            account_id: id,
            reason: "spam".into(),
            occurred_at: Utc::now(),
            correlation_id: Uuid::now_v7(),
        }));
        assert_eq!(suspended.kind, "account_suspended");
        assert_eq!(suspended.account_id, id.to_string());
        assert_eq!(suspended.reason.as_deref(), Some("spam"));

        let deleted = wire(AccountDomainEvent::AccountDeleted(AccountDeleted {
            account_id: id,
            deleted_by: None,
            occurred_at: Utc::now(),
            correlation_id: Uuid::now_v7(),
        }));
        assert_eq!(deleted.kind, "account_deleted");

        let deactivated = wire(AccountDomainEvent::AccountDeactivated(AccountDeactivated {
            account_id: id,
            occurred_at: Utc::now(),
            correlation_id: Uuid::now_v7(),
        }));
        assert_eq!(deactivated.kind, "account_deactivated");
        // Its kind is the masking reason the hide command parses.
        assert!(MaskingReason::try_from(deactivated.kind.as_str()).is_ok());

        let activated = wire(AccountDomainEvent::AccountActivated(AccountActivated {
            account_id: id,
            occurred_at: Utc::now(),
            correlation_id: Uuid::now_v7(),
        }));
        assert_eq!(activated.kind, "account_activated");
        assert_eq!(activated.account_id, id.to_string());
    }
}
