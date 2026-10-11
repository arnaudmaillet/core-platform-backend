//! The one-off staff bootstrap (#880): `auth-server bootstrap-staff <account_id>`.
//!
//! Staff permissions need a staff passkey, a staff passkey needs a grant, and a
//! grant needs an admin in a staff passkey session — so the first one comes
//! from here. Run it **inside an auth pod** (`kubectl exec`): there is no network
//! surface, and the grant is printed to that terminal only (never logged).
//!
//! Refused unless `AUTH_STAFF_BOOTSTRAP=true`, and refused for good once any
//! staff passkey exists anywhere. The grant is published like any other
//! (`auth.staff_enrollment_granted`, no `granted_by`) and recorded by audit
//! once the running server's outbox relay drains it.

use std::sync::Arc;

use chrono::Utc;
use postgres_storage::{PgPoolBuilder, PostgresConfig, TransactionManager};
use tonic::transport::Channel;
use uuid::Uuid;

use crate::application::command::{IssuedStaffGrant, StaffEnrollment};
use crate::config::AuthConfig;
use crate::infrastructure::directory::GrpcAccountDirectory;
use crate::infrastructure::event::pg_outbox_publisher::PgOutboxPublisher;
use crate::infrastructure::persistence::{PgPasskeyRepository, PgStaffGrants};

/// Opens the bootstrap: `true` only.
pub const STAFF_BOOTSTRAP_ENV: &str = "AUTH_STAFF_BOOTSTRAP";

/// A staff enrollment grant for `account_id`, if the bootstrap is open.
pub async fn bootstrap_staff(account_id: &str) -> anyhow::Result<IssuedStaffGrant> {
    let enabled = std::env::var(STAFF_BOOTSTRAP_ENV).is_ok_and(|v| v == "true");
    let config = AuthConfig::from_env()?;
    let pool = PgPoolBuilder::build(PostgresConfig::from_env()).await.map_err(|e| anyhow::anyhow!("postgres: {e}"))?;
    let tx = TransactionManager::new(pool.clone());
    let channel = Channel::from_shared(config.account_endpoint)?
        .timeout(config.account_rpc_timeout)
        .connect_timeout(config.account_connect_timeout)
        .connect_lazy();
    let staff = StaffEnrollment::new(
        Arc::new(PgStaffGrants::new(tx.clone())),
        Arc::new(PgPasskeyRepository::new(tx)),
        Arc::new(GrpcAccountDirectory::new(channel)),
        Arc::new(PgOutboxPublisher::new(pool)),
    )
    .with_bootstrap(enabled);
    staff.bootstrap(account_id, Utc::now(), Uuid::now_v7()).await.map_err(|e| anyhow::anyhow!("{e}"))
}
