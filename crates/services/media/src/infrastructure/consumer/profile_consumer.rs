//! `profile.v1.events` → media (#777): `ProfileVerificationDecided` moves the
//! decided request's private documents to their purge (the decision + the
//! retention). Other profile events are skipped.

use std::sync::Arc;

use chrono::{TimeZone, Utc};
use serde::Deserialize;
use tracing::{error, info};

use transport::kafka::consumer::{run_consumer, KafkaConsumerHandle, ProcessOutcome, RetryPolicy};
use transport::kafka::producer::KafkaProducerHandle;

use crate::application::command::DocumentRetention;
use crate::domain::value_object::{AssetId, OwnerId};

/// Lenient read of profile's `ProfileEventWire` (tagged `type`).
#[derive(Debug, Deserialize)]
pub struct ProfileEvent {
    #[serde(rename = "type")]
    kind:               String,
    #[serde(default)]
    account_id:         String,
    #[serde(default)]
    document_asset_ids: Vec<String>,
    #[serde(default)]
    decided_at_ms:      i64,
}

#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    Skip,
    Schedule { owner: OwnerId, documents: Vec<AssetId>, decided_at_ms: i64 },
    Poison(String),
}

fn outcome(event: &ProfileEvent) -> Outcome {
    if event.kind != "ProfileVerificationDecided" {
        return Outcome::Skip;
    }
    let Ok(owner) = OwnerId::try_from(event.account_id.as_str()) else {
        return Outcome::Poison(format!("ProfileVerificationDecided with a bad account_id {:?}", event.account_id));
    };
    // A link is not an asset id: only the private documents are listed.
    let documents = event.document_asset_ids.iter().filter_map(|id| AssetId::try_from(id.as_str()).ok()).collect();
    Outcome::Schedule { owner, documents, decided_at_ms: event.decided_at_ms }
}

pub async fn run_profile_consumer(
    consumer: KafkaConsumerHandle,
    retention: Arc<DocumentRetention>,
    producer: KafkaProducerHandle,
) {
    info!("media profile consumer started");
    let policy = RetryPolicy::default();
    let result = run_consumer::<ProfileEvent, _>(&consumer, &producer, &policy, move |event| {
        let retention = Arc::clone(&retention);
        Box::pin(async move {
            match outcome(event) {
                Outcome::Skip => ProcessOutcome::Done,
                Outcome::Poison(reason) => ProcessOutcome::Reject(reason),
                Outcome::Schedule { owner, documents, decided_at_ms } => {
                    let decided_at = Utc.timestamp_millis_opt(decided_at_ms).single().unwrap_or_else(Utc::now);
                    ProcessOutcome::from_result(
                        retention.on_decision(&documents, &owner, decided_at, Utc::now()).await.map(|_| ()),
                    )
                }
            }
        })
    })
    .await;
    if let Err(e) = result {
        error!(error = %e, "media profile consumer stopped");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_decision_schedules_its_documents_and_other_events_skip() {
        let (account, doc) = (uuid::Uuid::now_v7(), AssetId::new());
        let decided: ProfileEvent = serde_json::from_value(serde_json::json!({
            "type": "ProfileVerificationDecided",
            "profile_id": uuid::Uuid::now_v7().to_string(),
            "account_id": account.to_string(),
            "document_asset_ids": [doc.as_str(), "https://example.org/registry"],
            "decided_at_ms": 1_700_000_000_000_i64,
        }))
        .unwrap();
        assert_eq!(
            outcome(&decided),
            Outcome::Schedule { owner: OwnerId::from_uuid(account), documents: vec![doc], decided_at_ms: 1_700_000_000_000 }
        );
        let other: ProfileEvent =
            serde_json::from_value(serde_json::json!({ "type": "ProfileUpdated", "profile_id": "x" })).unwrap();
        assert_eq!(outcome(&other), Outcome::Skip);
    }

    /// The decision as profile serializes it (its own types): a shape drift
    /// fails here.
    #[test]
    fn reads_profiles_own_wire() {
        use profile::domain::event::{DomainEvent, VerificationDecided};
        use profile::domain::value_object::{AccountId, ProfileId};
        use profile::infrastructure::publisher::wire::ProfileEventWire;

        let (account, doc) = (uuid::Uuid::now_v7(), AssetId::new());
        let decided_at = chrono::DateTime::from_timestamp_millis(1_700_000_000_000).unwrap();
        let event = DomainEvent::VerificationDecided(VerificationDecided {
            profile_id:        ProfileId::new(),
            account_id:        AccountId::from(account),
            approved:          true,
            private_documents: vec![doc.as_str()],
            occurred_at:       decided_at,
            correlation_id:    uuid::Uuid::now_v7(),
        });
        let json = serde_json::to_value(ProfileEventWire::from(&event)).unwrap();
        let read: ProfileEvent = serde_json::from_value(json).unwrap();
        assert_eq!(
            outcome(&read),
            Outcome::Schedule { owner: OwnerId::from_uuid(account), documents: vec![doc], decided_at_ms: 1_700_000_000_000 }
        );
    }
}
