//! The async ingest lane's Kafka wiring — the worker's command side.
//!
//! Runs on the shared at-least-once [`run_consumer`] runner (manual commit after a
//! terminal outcome, bounded retry + jitter, DLQ on poison/exhaustion). A decoded
//! [`AuditEventWire`] is distilled into a domain event and chained via the
//! [`IngestHandler`]; `run_consumer` owns deserialization, so poison bytes
//! dead-letter before reaching the mapping.
//!
//! Zero-loss: the offset is committed only after the event is durably persisted and
//! chained. A store fault is retryable (the runner retries without committing); a
//! decode/category/outcome fault is poison and dead-letters. A duplicate replay is
//! a benign success (`IngestHandler::ingest` dedupes), so the offset advances.

use std::sync::Arc;

use transport::kafka::consumer::{ProcessOutcome, RetryPolicy, run_consumer, KafkaConsumerHandle};
use transport::kafka::producer::KafkaProducerHandle;

use crate::application::port::SubjectCipher;
use crate::application::{CryptoShredHandler, IngestHandler};
use crate::domain::{SubjectKeyRef, SubjectPseudonym};
use crate::error::AuditError;
use crate::infrastructure::account_decode::{
    AccountEventWire, map_account_activated, map_account_created, map_account_deactivated,
    map_account_deleted, map_account_suspended, map_consents_updated, map_email_changed,
    map_email_verified, map_gdpr_data_export_requested, map_gdpr_deletion_cancelled,
    map_gdpr_deletion_requested, map_kyc_status_changed,
    map_mfa_enrolled, map_mfa_revoked, map_password_changed, map_phone_changed, map_role_assigned,
    map_role_revoked,
};
use crate::infrastructure::auth_decode::{
    AuthEventWire, map_app_authorization_revoked, map_app_authorized, map_session_issued, map_session_revoked,
};
use crate::infrastructure::decode::{AuditEventWire, map_audit_event};
use crate::infrastructure::moderation_decode::{
    ModerationEventWire, map_decision_recorded, map_enforcement_applied,
};

/// Lets the at-least-once runner classify a failure: delegate to the error's own
/// retry verdict — transient store faults retry; decode / domain faults are poison
/// and dead-letter immediately.
impl transport::kafka::consumer::ClassifyError for AuditError {
    fn is_retryable(&self) -> bool {
        <Self as error::AppError>::is_retryable(self)
    }
}

/// Run the `audit.v1.events` ingest consumer until the stream ends.
pub async fn run_audit_ingest_consumer(
    consumer: KafkaConsumerHandle,
    producer: KafkaProducerHandle,
    handler: Arc<IngestHandler>,
) {
    tracing::info!("audit ingest consumer started");
    let policy = RetryPolicy::default();
    let result = run_consumer::<AuditEventWire, _>(&consumer, &producer, &policy, move |wire| {
        let handler = Arc::clone(&handler);
        let wire = wire.clone();
        Box::pin(async move {
            let outcome = async {
                let event = map_audit_event(wire)?;
                handler.ingest(event).await?;
                Ok::<(), AuditError>(())
            }
            .await;
            ProcessOutcome::from_result(outcome)
        })
    })
    .await;
    if let Err(e) = result {
        tracing::error!(error = %e, "audit ingest consumer stopped");
    }
}

/// Run the `account.v1.events` ingest consumer until the stream ends.
///
/// PII-bearing events (`account_created`, `email_changed`) have their personal
/// data sealed into a crypto-shreddable envelope before chaining. `gdpr_deletion
/// _requested` is chained as a `DataErasure` record AND drives the actual
/// crypto-shred: the subject's DEK is destroyed, so every PII envelope ever sealed
/// for them (here and on the moderation feed) becomes permanently unreadable while
/// the chain still verifies — the GDPR Art. 17 loop, closed end to end. Recording
/// precedes shred, and both are idempotent, so a redelivery is safe.
pub async fn run_account_ingest_consumer(
    consumer: KafkaConsumerHandle,
    producer: KafkaProducerHandle,
    handler: Arc<IngestHandler>,
    cipher: Arc<dyn SubjectCipher>,
    shred: Arc<CryptoShredHandler>,
) {
    tracing::info!("audit account ingest consumer started");
    let policy = RetryPolicy::default();
    let result = run_consumer::<AccountEventWire, _>(&consumer, &producer, &policy, move |wire| {
        let handler = Arc::clone(&handler);
        let cipher = Arc::clone(&cipher);
        let shred = Arc::clone(&shred);
        let wire = wire.clone();
        Box::pin(async move {
            let outcome = async {
                match wire {
                    AccountEventWire::AccountCreated(created) => {
                        let subject = SubjectPseudonym::new(created.account_id.clone())?;
                        let pii = cipher.seal(&subject, &created.pii_plaintext()).await?;
                        handler.ingest(map_account_created(&created, pii)?).await?;
                    }
                    AccountEventWire::EmailChanged(changed) => {
                        let subject = SubjectPseudonym::new(changed.account_id.clone())?;
                        let pii = cipher.seal(&subject, &changed.pii_plaintext()).await?;
                        handler.ingest(map_email_changed(&changed, pii)?).await?;
                    }
                    AccountEventWire::GdprDeletionRequested(deletion) => {
                        // Evidence the request happened. Nothing is shredded yet:
                        // the request has a 30-day grace period and may be
                        // cancelled — the shred follows the erasure itself
                        // (`account_deleted`, emitted when account anonymizes).
                        handler.ingest(map_gdpr_deletion_requested(&deletion)?).await?;
                    }
                    AccountEventWire::GdprDeletionCancelled(cancelled) => {
                        handler.ingest(map_gdpr_deletion_cancelled(&cancelled)?).await?;
                    }
                    AccountEventWire::ConsentsUpdated(consents) => {
                        handler.ingest(map_consents_updated(&consents)?).await?;
                    }
                    AccountEventWire::GdprDataExportRequested(export) => {
                        handler.ingest(map_gdpr_data_export_requested(&export)?).await?;
                    }
                    AccountEventWire::EmailVerified(verified) => {
                        let subject = SubjectPseudonym::new(verified.account_id.clone())?;
                        let pii = cipher.seal(&subject, &verified.pii_plaintext()).await?;
                        handler.ingest(map_email_verified(&verified, pii)?).await?;
                    }
                    AccountEventWire::PhoneChanged(phone) => {
                        // Seal only when a number is present (a removal carries none).
                        let subject = SubjectPseudonym::new(phone.account_id.clone())?;
                        let pii = match phone.pii_plaintext() {
                            Some(plaintext) => Some(cipher.seal(&subject, &plaintext).await?),
                            None => None,
                        };
                        handler.ingest(map_phone_changed(&phone, pii)?).await?;
                    }
                    AccountEventWire::PasswordChanged(e) => {
                        handler.ingest(map_password_changed(&e)?).await?;
                    }
                    AccountEventWire::MfaEnrolled(e) => {
                        handler.ingest(map_mfa_enrolled(&e)?).await?;
                    }
                    AccountEventWire::MfaRevoked(e) => {
                        handler.ingest(map_mfa_revoked(&e)?).await?;
                    }
                    AccountEventWire::RoleAssigned(e) => {
                        handler.ingest(map_role_assigned(&e)?).await?;
                    }
                    AccountEventWire::RoleRevoked(e) => {
                        handler.ingest(map_role_revoked(&e)?).await?;
                    }
                    AccountEventWire::AccountActivated(e) => {
                        handler.ingest(map_account_activated(&e)?).await?;
                    }
                    AccountEventWire::AccountDeactivated(e) => {
                        handler.ingest(map_account_deactivated(&e)?).await?;
                    }
                    AccountEventWire::AccountSuspended(e) => {
                        handler.ingest(map_account_suspended(&e)?).await?;
                    }
                    AccountEventWire::AccountDeleted(e) => {
                        let subject = SubjectPseudonym::new(e.account_id.clone())?;
                        // 1. Chain the deletion record.
                        handler.ingest(map_account_deleted(&e)?).await?;
                        // 2. Crypto-shred (GDPR Art. 17): destroy the subject's DEK →
                        //    all their sealed PII is permanently unreadable; the
                        //    chain stays verifiable. Covers both erasure paths: the
                        //    janitor's anonymization after the grace period and an
                        //    administrative delete.
                        let key_ref = SubjectKeyRef::new(format!("dek:{}", subject.as_str()))?;
                        shred.shred(&subject, &key_ref, &[]).await?;
                    }
                    AccountEventWire::KycStatusChanged(e) => {
                        handler.ingest(map_kyc_status_changed(&e)?).await?;
                    }
                    AccountEventWire::Other => {}
                }
                Ok::<(), AuditError>(())
            }
            .await;
            ProcessOutcome::from_result(outcome)
        })
    })
    .await;
    if let Err(e) = result {
        tracing::error!(error = %e, "audit account ingest consumer stopped");
    }
}

/// Run the `auth.v1.events` ingest consumer until the stream ends. Auth events
/// carry no free-text PII, so there is no sealing — `session_issued` /
/// `session_revoked` map directly and chain; every other auth event is a benign
/// committed skip.
pub async fn run_auth_ingest_consumer(
    consumer: KafkaConsumerHandle,
    producer: KafkaProducerHandle,
    handler: Arc<IngestHandler>,
) {
    tracing::info!("audit auth ingest consumer started");
    let policy = RetryPolicy::default();
    let result = run_consumer::<AuthEventWire, _>(&consumer, &producer, &policy, move |wire| {
        let handler = Arc::clone(&handler);
        let wire = wire.clone();
        Box::pin(async move {
            let outcome = async {
                match wire {
                    AuthEventWire::SessionIssued(issued) => {
                        handler.ingest(map_session_issued(&issued)?).await?;
                    }
                    AuthEventWire::SessionRevoked(revoked) => {
                        handler.ingest(map_session_revoked(&revoked)?).await?;
                    }
                    AuthEventWire::AppAuthorized(granted) => {
                        handler.ingest(map_app_authorized(&granted)?).await?;
                    }
                    AuthEventWire::AppAuthorizationRevoked(revoked) => {
                        handler.ingest(map_app_authorization_revoked(&revoked)?).await?;
                    }
                    AuthEventWire::Other => {}
                }
                Ok::<(), AuditError>(())
            }
            .await;
            ProcessOutcome::from_result(outcome)
        })
    })
    .await;
    if let Err(e) = result {
        tracing::error!(error = %e, "audit auth ingest consumer stopped");
    }
}

/// Run the `moderation.v1.events` ingest consumer until the stream ends.
///
/// For a `decision_recorded` it seals the rationale into a crypto-shreddable
/// envelope (the `cipher`) before mapping + chaining; for an `enforcement_applied`
/// it maps directly (no PII); every other moderation event is a benign skip that
/// still commits the offset. The cipher's key-vault faults are retryable (`run_consumer`
/// retries without committing); a decode/domain fault is poison and dead-letters.
pub async fn run_moderation_ingest_consumer(
    consumer: KafkaConsumerHandle,
    producer: KafkaProducerHandle,
    handler: Arc<IngestHandler>,
    cipher: Arc<dyn SubjectCipher>,
) {
    tracing::info!("audit moderation ingest consumer started");
    let policy = RetryPolicy::default();
    let result = run_consumer::<ModerationEventWire, _>(&consumer, &producer, &policy, move |wire| {
        let handler = Arc::clone(&handler);
        let cipher = Arc::clone(&cipher);
        let wire = wire.clone();
        Box::pin(async move {
            let outcome = async {
                match wire {
                    ModerationEventWire::DecisionRecorded(decision) => {
                        let subject = SubjectPseudonym::new(decision.subject.actor_id.clone())?;
                        // Seal the DSA rationale into a crypto-shreddable envelope.
                        let sealed = cipher.seal(&subject, &decision.rationale).await?;
                        let event = map_decision_recorded(&decision, sealed)?;
                        handler.ingest(event).await?;
                    }
                    ModerationEventWire::EnforcementApplied(enforcement) => {
                        let event = map_enforcement_applied(&enforcement)?;
                        handler.ingest(event).await?;
                    }
                    // Out-of-scope moderation event: a harmless committed skip.
                    ModerationEventWire::Other => {}
                }
                Ok::<(), AuditError>(())
            }
            .await;
            ProcessOutcome::from_result(outcome)
        })
    })
    .await;
    if let Err(e) = result {
        tracing::error!(error = %e, "audit moderation ingest consumer stopped");
    }
}
