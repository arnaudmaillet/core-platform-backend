//! Integration harness: boots an ephemeral Postgres container, applies the `.sql`
//! migrations, wires a real account graph against it through the production
//! composition root, and exposes the buses for assertions.
//!
//! This is the only harness in the workspace backed by Postgres rather than
//! ScyllaDB/Redis: it uses `test_support::containers::postgres_ready`, which boots
//! the container and runs the migrations once per binary, then builds a `sqlx`
//! pool against the container URL.
#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use uuid::Uuid;

use cqrs::command::InMemoryCommandBus;
use cqrs::query::InMemoryQueryBus;
use cqrs::{CommandBus, CqrsError, Envelope, QueryBus};
use postgres_storage::config::StatementLogLevel;
use postgres_storage::{PgPoolBuilder, PostgresConfig};

use account::app::App;
use account::application::command::{CreateAccountCommand, RecordLoginCommand, VerifyEmailCommand};
use account::application::query::{AccountView, GetAccountByIdentityIdQuery};

pub use test_support::await_until;

/// Generous default patience for a cross-component assertion.
pub const DEADLINE: Duration = Duration::from_secs(10);

/// On-disk migration assets, resolved against *this* crate's manifest.
const MIGRATIONS_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/migrations");

/// A fully-wired account service bound to ephemeral Postgres, plus the buses.
pub struct TestHarness {
    pub command_bus: Arc<InMemoryCommandBus>,
    pub query_bus:   Arc<InMemoryQueryBus>,
    /// Direct reads for assertions on tables no query exposes.
    pub pool:        sqlx::PgPool,
    pub repository:  Arc<dyn account::application::port::AccountRepository>,
}

impl TestHarness {
    /// Boots/reuses the shared Postgres container, applies the `.sql` migrations,
    /// and assembles the service graph against a pool to that container.
    pub async fn start() -> Self {
        let url = test_support::containers::postgres_ready(MIGRATIONS_DIR).await;

        let config = PostgresConfig {
            database_url:             url,
            max_connections:          8,
            min_connections:          1,
            acquire_timeout:          Duration::from_secs(5),
            idle_timeout:             None,
            max_lifetime:             None,
            statement_log_level:      StatementLogLevel::Debug,
            slow_statement_threshold: Duration::from_millis(500),
        };
        let pool = PgPoolBuilder::build(config).await.expect("integration: Postgres pool");

        // Log publisher — this suite tests account behaviour, not event emission.
        let publisher = std::sync::Arc::new(account::infrastructure::event::LogEventPublisher);
        let app = App::build(pool.clone(), publisher)
            .await
            .expect("integration: build account app");

        Self { command_bus: app.command_bus, query_bus: app.query_bus, pool, repository: app.repository }
    }

    /// Creates an account, expecting success.
    pub async fn create(&self, identity_id: &str, email: &str) {
        dispatch_create(Arc::clone(&self.command_bus), identity_id.to_owned(), email.to_owned())
            .await
            .expect("create_account");
    }

    /// Dispatches VerifyEmail — the canonical first MUTATION of an account's
    /// life (PendingVerification → Active). Exists because the optimistic-CAS
    /// regression made every mutation abort while create+read stayed green.
    pub async fn verify_email(&self, account_id: &str) -> Result<(), CqrsError> {
        self.command_bus
            .dispatch(Envelope::new(
                Uuid::now_v7(),
                VerifyEmailCommand { account_id: account_id.to_owned() },
            ))
            .await
    }

    /// Dispatches RecordLogin — a second, distinct mutation to prove the CAS
    /// survives reload cycles (not just the first bump).
    pub async fn record_login(&self, account_id: &str) -> Result<(), CqrsError> {
        self.command_bus
            .dispatch(Envelope::new(
                Uuid::now_v7(),
                RecordLoginCommand { account_id: account_id.to_owned() },
            ))
            .await
    }

    /// Resolves an account by its IdP identity id (`Err` when absent).
    pub async fn get_by_identity(&self, identity_id: &str) -> Result<AccountView, CqrsError> {
        self.query_bus
            .dispatch(Envelope::new(
                Uuid::now_v7(),
                GetAccountByIdentityIdQuery { identity_id: identity_id.to_owned() },
            ))
            .await
    }
}

/// Dispatches a create on a shared bus — a free function so scenarios can fire
/// many concurrently from spawned tasks.
pub async fn dispatch_create(
    command_bus: Arc<InMemoryCommandBus>,
    identity_id: String,
    email:       String,
) -> Result<(), CqrsError> {
    let cmd = CreateAccountCommand {
        identity_id,
        email,
        phone:                None,
        password_hash:        None,
        country_of_residence: None,
        role:                 None,
        created_by:           None,
    };
    command_bus.dispatch(Envelope::new(Uuid::now_v7(), cmd)).await
}

/// A fresh random IdP identity id.
pub fn random_identity() -> String {
    format!("idp-{}", Uuid::now_v7())
}

/// A fresh random, unique email address.
pub fn random_email() -> String {
    format!("u{}@example.com", Uuid::now_v7().simple())
}
