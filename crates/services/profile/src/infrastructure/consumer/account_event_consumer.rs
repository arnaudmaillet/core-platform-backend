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

use crate::domain::value_object::SupervisionFloor;
use crate::application::command::{
    ApplySupervisionFloorCommand, EraseAccountVerificationsCommand, HideAccountProfilesCommand, RestoreAccountProfilesCommand,
};

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
    // supervision_limits_set (#670): the floors.
    #[serde(default)]
    private_account: bool,
    #[serde(default)]
    messages: Option<String>,
    #[serde(default)]
    comments: Option<String>,
    #[serde(default)]
    hidden_from_search: bool,
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
            let hidden = command_bus.dispatch(Envelope::new(correlation_id, cmd)).await;
            // GDPR erasure (#777): a deleted account's verification requests go
            // with it (both commands are idempotent, so a retry replays both).
            match (hidden, kind) {
                (Ok(()), "account_deleted") => {
                    let erase = EraseAccountVerificationsCommand { account_id: event.account_id.clone() };
                    command_bus.dispatch(Envelope::new(correlation_id, erase)).await
                }
                (outcome, _) => outcome,
            }
        }

        // Family supervision floors (#670): tighten and lock the teen's
        // profiles; lifted, unlock them.
        "supervision_limits_set" => {
            let audience = |s: &Option<String>| s.as_deref().and_then(crate::infrastructure::persistence::scylla_supervision_floors::audience_from);
            let cmd = ApplySupervisionFloorCommand {
                account_id: event.account_id.clone(),
                floor: Some(SupervisionFloor {
                    private_account:    event.private_account,
                    messages:           audience(&event.messages),
                    comments:           audience(&event.comments),
                    hidden_from_search: event.hidden_from_search,
                }),
            };
            command_bus.dispatch(Envelope::new(correlation_id, cmd)).await
        }
        "supervision_limits_cleared" => {
            let cmd = ApplySupervisionFloorCommand { account_id: event.account_id.clone(), floor: None };
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

    /// #670: the limits as account publishes them (its own types) give the
    /// floors.
    #[test]
    fn supervision_limits_decode_into_floors() {
        use account::domain::event::{SupervisionLimitsCleared, SupervisionLimitsSet};

        let set = wire(AccountDomainEvent::SupervisionLimitsSet(SupervisionLimitsSet {
            account_id: AccountId::new(),
            set_by: AccountId::new(),
            private_account: true,
            messages: Some("mutuals".into()),
            comments: None,
            hidden_from_search: true,
            daily_minutes: Some(60),
            teen_profile_ids: vec![],
            occurred_at: Utc::now(),
            correlation_id: Uuid::now_v7(),
        }));
        assert_eq!(set.kind, "supervision_limits_set");
        assert!(set.private_account && set.hidden_from_search);
        assert_eq!((set.messages.as_deref(), set.comments.as_deref()), (Some("mutuals"), None));

        let cleared = wire(AccountDomainEvent::SupervisionLimitsCleared(SupervisionLimitsCleared {
            account_id: AccountId::new(),
            teen_profile_ids: vec![],
            occurred_at: Utc::now(),
            correlation_id: Uuid::now_v7(),
        }));
        assert_eq!(cleared.kind, "supervision_limits_cleared");
    }
}
