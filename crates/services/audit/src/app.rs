//! The audit composition roots.
//!
//! [`Adapters::build`] is the I/O variant that constructs the live storage / object
//! -store / vault / anchor adapters from config; both binaries build from it (the
//! server composes the gRPC read/record handler, the worker additionally builds the
//! ingest + checkpoint handlers). [`compose_server`] is *pure* wiring (ports in,
//! gRPC handler out — no I/O), so the same handler is built over fakes or real
//! adapters.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use postgres_storage::{PgPoolBuilder, PostgresConfig, TransactionManager};
use sqlx::PgPool;

use crate::application::port::{
    CheckpointAnchor, Clock, KeyVault, LedgerStore, SubjectCipher, WormArchive,
};
use crate::application::{
    CheckpointHandler, CryptoShredHandler, ExportHandler, IngestHandler, QueryHandler,
    RecordPrivilegedHandler, VerifyHandler,
};
use crate::config::AuditConfig;
use crate::infrastructure::grpc::{AuditServiceHandler, CallerGate};
use crate::infrastructure::{
    AesGcmSubjectCipher, AwsKms, KmsSigner, KmsSubjectCipher, LocalCheckpointSigner,
    ObjectLockArchive, ObjectLockWitness, PgCheckpointAnchor, PgKeyVault, PgLedger, SystemClock,
    Witness, WitnessCheckpointAnchor,
};

/// The shared adapter set both binaries build from. Retains the concrete [`PgPool`]
/// so the runtime can build a ledger liveness probe.
pub struct Adapters {
    pub ledger: Arc<dyn LedgerStore>,
    pub archive: Arc<dyn WormArchive>,
    pub key_vault: Arc<dyn KeyVault>,
    pub anchor: Arc<dyn CheckpointAnchor>,
    pub clock: Arc<dyn Clock>,
    /// Seals PII (the moderation rationale) into a crypto-shreddable envelope.
    pub cipher: Arc<dyn SubjectCipher>,
    pub pool: PgPool,
}

impl Adapters {
    /// Connect the canonical ledger (Postgres), the WORM archive (object store), the
    /// key vault and the checkpoint anchor, and wrap them as ports. The Postgres
    /// pool is shared across the three Pg-backed adapters.
    pub async fn build(config: &AuditConfig) -> anyhow::Result<Adapters> {
        let pool = build_pool(config.postgres.clone()).await?;

        let ledger: Arc<dyn LedgerStore> =
            Arc::new(PgLedger::new(TransactionManager::new(pool.clone())));
        let key_vault: Arc<dyn KeyVault> =
            Arc::new(PgKeyVault::new(TransactionManager::new(pool.clone())));
        let archive: Arc<dyn WormArchive> = Arc::new(
            ObjectLockArchive::new(config.object_lock.clone())
                .map_err(|e| anyhow::anyhow!("build WORM archive: {e}"))?,
        );
        let clock: Arc<dyn Clock> = Arc::new(SystemClock);

        // The KMS client (issue #482/#483), built once when configured and shared as
        // both the DEK cipher and the checkpoint signer (a single Arc<AwsKms>
        // coerced to each port). Absent KMS → local fallbacks below.
        let kms = match &config.kms {
            Some(kms_cfg) => Some(
                Arc::new(AwsKms::new(kms_cfg).map_err(|e| anyhow::anyhow!("build KMS client: {e}"))?),
            ),
            None => None,
        };

        // #482 — PII sealer: KMS-wrapped DEKs in production, env-KEK AES-GCM locally.
        let cipher: Arc<dyn SubjectCipher> = match (&kms, &config.kms) {
            (Some(kms), Some(kms_cfg)) => {
                tracing::info!("audit subject-cipher: KMS KEK custody (issue #482)");
                // The env KEK stays a reader (#485): DEKs it wrapped before custody
                // moved to KMS are unwrapped with it and re-wrapped under KMS.
                Arc::new(
                    KmsSubjectCipher::new(
                        TransactionManager::new(pool.clone()),
                        Arc::clone(kms) as Arc<dyn crate::infrastructure::KmsCipher>,
                        kms_cfg.dek_key_id.clone(),
                    )
                    .reading_local(config.kek),
                )
            }
            _ => Arc::new(AesGcmSubjectCipher::new(
                TransactionManager::new(pool.clone()),
                config.kek,
            )),
        };

        // #483 — checkpoint anchor: KMS-signed root anchored to an independent WORM
        // witness in production; the Postgres-only anchor for local/dev.
        let anchor: Arc<dyn CheckpointAnchor> = match &config.witness {
            Some(witness_cfg) => {
                config.witness_independence().map_err(|reason| anyhow::anyhow!(reason))?;
                let witness = ObjectLockWitness::new(witness_cfg.clone())
                    .map_err(|e| anyhow::anyhow!("build checkpoint witness: {e}"))?;
                witness
                    .ensure_bucket()
                    .await
                    .map_err(|e| anyhow::anyhow!("ensure witness bucket: {e}"))?;
                let (signer, key_id, algorithm, accepted): (Arc<dyn KmsSigner>, String, String, Vec<String>) =
                    match (&kms, &config.kms) {
                        (Some(kms), Some(kms_cfg)) => (
                            Arc::clone(kms) as Arc<dyn KmsSigner>,
                            kms_cfg.signing_key_id.clone(),
                            kms_cfg.signing_algorithm.clone(),
                            kms_cfg.accepted_signing_algorithms.clone(),
                        ),
                        _ => {
                            tracing::warn!(
                                "audit checkpoint witness WITHOUT KMS: signed with the local HMAC key beside the service — not operator-proof (#483/#485)"
                            );
                            (
                                Arc::new(LocalCheckpointSigner::new(config.checkpoint_signing_key)),
                                "local-hmac".to_owned(),
                                "HMAC_SHA_256".to_owned(),
                                Vec::new(),
                            )
                        }
                    };
                tracing::info!("audit checkpoint anchor: signed + external WORM witness (issue #483)");
                Arc::new(
                    WitnessCheckpointAnchor::new(
                        signer,
                        key_id,
                        algorithm,
                        Arc::new(witness) as Arc<dyn Witness>,
                        Some(PgCheckpointAnchor::new(TransactionManager::new(pool.clone()))),
                    )
                    .accepting(accepted),
                )
            }
            None => Arc::new(PgCheckpointAnchor::new(TransactionManager::new(pool.clone()))),
        };

        Ok(Adapters {
            ledger,
            archive,
            key_vault,
            anchor,
            clock,
            cipher,
            pool,
        })
    }

    pub fn ingest_handler(&self) -> Arc<IngestHandler> {
        Arc::new(IngestHandler::new(
            Arc::clone(&self.ledger),
            Arc::clone(&self.archive),
            Arc::clone(&self.clock),
        ))
    }

    pub fn checkpoint_handler(&self) -> Arc<CheckpointHandler> {
        Arc::new(CheckpointHandler::new(
            Arc::clone(&self.ledger),
            Arc::clone(&self.anchor),
            Arc::clone(&self.clock),
        ))
    }

    pub fn shred_handler(&self) -> Arc<CryptoShredHandler> {
        Arc::new(CryptoShredHandler::new(
            Arc::clone(&self.key_vault),
            Arc::clone(&self.clock),
        ))
    }
}

/// Pure read/record composition: the four use-case handlers wrapped in the gRPC
/// handler, behind the caller `gate`. The `key_vault` is retained for the
/// crypto-shred path (wired when an erasure-request source lands — see the
/// README deferral).
pub fn compose_server(
    gate: Arc<dyn CallerGate>,
    ledger: Arc<dyn LedgerStore>,
    archive: Arc<dyn WormArchive>,
    anchor: Arc<dyn CheckpointAnchor>,
    clock: Arc<dyn Clock>,
    record_timeout: Duration,
) -> AuditServiceHandler {
    let record = Arc::new(RecordPrivilegedHandler::new(
        Arc::clone(&ledger),
        Arc::clone(&archive),
        Arc::clone(&clock),
    ));
    let query = Arc::new(QueryHandler::new(Arc::clone(&ledger)));
    let export = Arc::new(ExportHandler::new(
        Arc::clone(&ledger),
        Arc::clone(&archive),
        Arc::clone(&clock),
    ));
    let verify = Arc::new(VerifyHandler::new(Arc::clone(&ledger), Arc::clone(&anchor)));
    AuditServiceHandler::new(gate, record, query, export, verify, record_timeout)
}

async fn build_pool(postgres: PostgresConfig) -> anyhow::Result<PgPool> {
    PgPoolBuilder::build(postgres)
        .await
        .context("build Postgres pool")
}

#[cfg(test)]
mod tests {
    use tonic::{Code, Request};

    use super::*;
    use crate::application::fakes::Fixture;
    use crate::domain::event::fixtures;
    use crate::domain::{AuditEvent, EventCategory};
    use crate::infrastructure::codec;
    use crate::infrastructure::grpc::proto;

    use crate::infrastructure::grpc::access::{perm, StaticCallerGate};

    /// Compose the gRPC handler over the in-memory fakes — the same wiring the
    /// binary builds over the live adapters, exercised end-to-end through proto.
    /// The gate authenticates a fully-permissioned operator; the denial paths
    /// have their own tests below.
    fn handler(fx: &Fixture) -> AuditServiceHandler {
        handler_with_gate(
            fx,
            StaticCallerGate::allowing(
                "ops-test",
                &[perm::RECORD, perm::READ, perm::EXPORT, perm::VERIFY],
            ),
        )
    }

    fn handler_with_gate(fx: &Fixture, gate: Arc<StaticCallerGate>) -> AuditServiceHandler {
        compose_server(
            gate,
            fx.ledger.clone(),
            fx.archive.clone(),
            fx.anchor.clone(),
            fx.clock.clone(),
            Duration::from_secs(5),
        )
    }

    fn pb_event(id: &str, category: EventCategory) -> proto::AuditEvent {
        codec::event_to_pb(&AuditEvent::try_new(fixtures::draft(id, category)).unwrap())
    }

    #[tokio::test]
    async fn record_privileged_returns_a_durable_proof() {
        let fx = Fixture::new();
        let req = Request::new(proto::RecordPrivilegedRequest {
            event: Some(pb_event("bg-1", EventCategory::PrivilegedAction)),
            privileged_action: proto::PrivilegedActionType::BreakGlassAccess as i32,
        });
        let resp = handler(&fx).record_privileged(req).await.unwrap().into_inner();
        assert_eq!(resp.sequence_no, 1);
        assert!(!resp.record_hash.is_empty());
        assert_eq!(fx.ledger.record_count(), 1);
    }

    #[tokio::test]
    async fn record_privileged_without_event_is_invalid_argument() {
        let fx = Fixture::new();
        let req = Request::new(proto::RecordPrivilegedRequest {
            event: None,
            privileged_action: proto::PrivilegedActionType::BreakGlassAccess as i32,
        });
        let status = handler(&fx).record_privileged(req).await.unwrap_err();
        assert_eq!(status.code(), Code::InvalidArgument);
    }

    #[tokio::test]
    async fn query_returns_recorded_events_for_a_subject() {
        let fx = Fixture::new();
        // Seed one event through the record path (fixtures::draft uses subject 7f3a).
        handler(&fx)
            .record_privileged(Request::new(proto::RecordPrivilegedRequest {
                event: Some(pb_event("e1", EventCategory::Moderation)),
                privileged_action: proto::PrivilegedActionType::LegalHoldPlace as i32,
            }))
            .await
            .unwrap();

        let resp = handler(&fx)
            .query(Request::new(proto::QueryRequest {
                subject_pseudonym: "7f3a".to_owned(),
                page_size: 50,
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.records.len(), 1);
        assert_eq!(resp.records[0].sequence_no, 1);
    }

    #[tokio::test]
    async fn verify_integrity_global_with_nothing_anchored_is_verified() {
        let fx = Fixture::new();
        let resp = handler(&fx)
            .verify_integrity(Request::new(proto::VerifyIntegrityRequest::default()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.status, proto::IntegrityStatus::Verified as i32);
    }

    /// Failure simulation (Phase 7): the audit store is wedged when a break-glass
    /// action is attempted. The synchronous lane must FAIL CLOSED — the durable
    /// -commit deadline elapses, the RPC returns DeadlineExceeded (AUD-4004), and
    /// nothing is recorded, so the caller denies the privileged action.
    #[tokio::test]
    async fn break_glass_is_denied_when_audit_cannot_confirm_durability() {
        let fx = Fixture::new();
        fx.ledger.set_hang(true); // the ledger is wedged

        let svc = compose_server(
            StaticCallerGate::allowing("ops-test", &[perm::RECORD]),
            fx.ledger.clone(),
            fx.archive.clone(),
            fx.anchor.clone(),
            fx.clock.clone(),
            Duration::from_millis(50), // tight durable-commit deadline
        );

        let status = svc
            .record_privileged(Request::new(proto::RecordPrivilegedRequest {
                event: Some(pb_event("bg-1", EventCategory::PrivilegedAction)),
                privileged_action: proto::PrivilegedActionType::BreakGlassAccess as i32,
            }))
            .await
            .unwrap_err();

        assert_eq!(status.code(), Code::DeadlineExceeded);
        assert_eq!(fx.ledger.record_count(), 0, "nothing may be recorded on a denied break-glass");
    }

    // ── The caller gate (finding 4): the privileged surface fails CLOSED ──────

    /// An unauthenticated caller is rejected on every RPC with UNAUTHENTICATED
    /// (AUD-3004) — including the deny-all posture when no verifier is configured.
    #[tokio::test]
    async fn an_unauthenticated_caller_is_rejected_on_every_rpc() {
        let fx = Fixture::new();
        let svc = handler_with_gate(&fx, StaticCallerGate::denying());

        let record = svc
            .record_privileged(Request::new(proto::RecordPrivilegedRequest {
                event: Some(pb_event("bg-1", EventCategory::PrivilegedAction)),
                privileged_action: proto::PrivilegedActionType::BreakGlassAccess as i32,
            }))
            .await
            .unwrap_err();
        assert_eq!(record.code(), Code::Unauthenticated);
        assert_eq!(fx.ledger.record_count(), 0, "a denied record must not reach the ledger");

        let query = svc
            .query(Request::new(proto::QueryRequest::default()))
            .await
            .unwrap_err();
        assert_eq!(query.code(), Code::Unauthenticated);

        let export = svc
            .export(Request::new(proto::ExportRequest::default()))
            .await
            .unwrap_err();
        assert_eq!(export.code(), Code::Unauthenticated);

        let verify = svc
            .verify_integrity(Request::new(proto::VerifyIntegrityRequest::default()))
            .await
            .unwrap_err();
        assert_eq!(verify.code(), Code::Unauthenticated);
    }

    /// An authenticated caller without the RPC's `audit:*` permission gets
    /// PERMISSION_DENIED — a read grant does not confer export or record.
    #[tokio::test]
    async fn a_read_only_caller_cannot_export_or_record() {
        let fx = Fixture::new();
        let svc = handler_with_gate(&fx, StaticCallerGate::allowing("ops-read", &[perm::READ]));

        // Read is allowed…
        let query = svc.query(Request::new(proto::QueryRequest {
            subject_pseudonym: "7f3a".to_owned(),
            page_size: 10,
            ..Default::default()
        }));
        assert!(query.await.is_ok());

        // …but export and the privileged record lane are not.
        let export = svc
            .export(Request::new(proto::ExportRequest::default()))
            .await
            .unwrap_err();
        assert_eq!(export.code(), Code::PermissionDenied);

        let record = svc
            .record_privileged(Request::new(proto::RecordPrivilegedRequest {
                event: Some(pb_event("bg-2", EventCategory::PrivilegedAction)),
                privileged_action: proto::PrivilegedActionType::BreakGlassAccess as i32,
            }))
            .await
            .unwrap_err();
        assert_eq!(record.code(), Code::PermissionDenied);
        assert_eq!(fx.ledger.record_count(), 0);
    }
}
