//! The infrastructure tier — the concrete adapters behind the application ports,
//! plus the pure proto/wire mapping layers. This is the only tier that knows about
//! `audit-api`, Postgres, the object store, or Kafka.
//!
//! * [`codec`] — the pure `audit.v1` proto ⇄ domain mapping (gRPC surface). Tested.
//! * [`decode`] — the audit-owned JSON wire schema for `audit.v1.events` ⇄ domain
//!   (the async ingest lane). Tested.
//! * [`pg_ledger`] — the append-only, hash-chained Postgres [`LedgerStore`] with
//!   compare-and-append; `UPDATE`/`DELETE` revoked at the role level.
//! * [`object_lock_archive`] — the S3/MinIO Object-Lock (compliance-mode)
//!   [`WormArchive`].
//! * [`pg_key_vault`] — the per-subject DEK [`KeyVault`] (Postgres-backed row
//!   lifecycle; crypto-shred = delete the wrapped-DEK row).
//! * [`subject_cipher`] — the [`SubjectCipher`] (PII sealer) over a swappable
//!   [`KekCustodian`](subject_cipher::KekCustodian): env-KEK AES-GCM for local/dev,
//!   KMS for production (issue #482) — no raw KEK ever in the service env.
//! * [`kms`] — KMS as a separate trust domain: DEK wrap/unwrap (#482) + checkpoint
//!   sign/verify (#483) over SigV4 ([`sigv4`]); plus the local HMAC signer fallback.
//! * [`pg_anchor`] — the local/dev [`CheckpointAnchor`] (unsigned, over Postgres).
//! * [`witness_anchor`] — the production [`CheckpointAnchor`] (issue #483): a
//!   KMS-signed Merkle root anchored to an independent WORM witness, so a DB
//!   operator who rewrites the Postgres pointer is still caught by `verify_global`.
//! * [`consumer`] — the `run_consumer` ingest-lane wiring + the `ClassifyError`
//!   impl that drives retry/DLQ.
//!
//! [`LedgerStore`]: crate::application::port::LedgerStore
//! [`WormArchive`]: crate::application::port::WormArchive
//! [`KeyVault`]: crate::application::port::KeyVault
//! [`SubjectCipher`]: crate::application::port::SubjectCipher
//! [`CheckpointAnchor`]: crate::application::port::CheckpointAnchor

pub mod account_decode;
pub mod auth_decode;
pub mod codec;
pub mod consumer;
pub mod decode;
pub mod envelope;
pub mod grpc;
pub mod kms;
pub mod loops;
pub mod moderation_decode;
pub mod object_lock_archive;
pub mod pg_anchor;
pub mod pg_key_vault;
pub mod pg_ledger;
pub mod sigv4;
pub mod subject_cipher;
pub mod witness_anchor;
pub mod system_clock;

pub use account_decode::{
    AccountEventWire, TOPIC_ACCOUNT_EVENTS, map_account_activated, map_account_created,
    map_account_deactivated, map_account_deleted, map_account_suspended, map_email_changed,
    map_consents_updated, map_email_verified, map_gdpr_data_export_requested,
    map_gdpr_deletion_cancelled, map_gdpr_deletion_requested, map_kyc_status_changed, map_mfa_enrolled, map_mfa_revoked, map_password_changed,
    map_phone_changed, map_role_assigned, map_role_revoked,
};
pub use auth_decode::{
    AuthEventWire, TOPIC_AUTH_EVENTS, map_session_issued, map_session_revoked,
};
pub use kms::{AwsKms, KmsCipher, KmsConfig, KmsSigner, LocalCheckpointSigner};
pub use subject_cipher::{AesGcmSubjectCipher, KmsSubjectCipher};
pub use witness_anchor::{ObjectLockWitness, SignedCheckpoint, Witness, WitnessCheckpointAnchor};
pub use consumer::{
    run_account_ingest_consumer, run_audit_ingest_consumer, run_auth_ingest_consumer,
    run_moderation_ingest_consumer,
};
pub use decode::{AuditEventWire, map_audit_event};
pub use grpc::{AuditServiceHandler, AuditServiceServer, FILE_DESCRIPTOR_SET};
pub use loops::run_checkpoint_loop;
pub use moderation_decode::{
    ModerationEventWire, TOPIC_MODERATION_EVENTS, map_decision_recorded, map_enforcement_applied,
};
pub use object_lock_archive::{ObjectLockArchive, ObjectLockConfig};
pub use pg_anchor::PgCheckpointAnchor;
pub use pg_key_vault::PgKeyVault;
pub use pg_ledger::PgLedger;
pub use system_clock::SystemClock;
