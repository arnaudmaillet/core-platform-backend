//! Integration harness: boots the shared infra, wires a real profile graph
//! against it through the production composition root, and exposes the buses plus
//! the repository and Redis cache handles for assertions.
#![allow(dead_code)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use uuid::Uuid;

use cqrs::command::InMemoryCommandBus;
use cqrs::query::InMemoryQueryBus;
use cqrs::{CommandBus, CqrsError, Envelope, QueryBus};
use redis_storage::RedisConfig;
use infra_config::{CacheRegistry, InfrastructureConfig};
use scylla_storage::{ScyllaClient, ScyllaConfig, ScyllaSessionBuilder};

use profile::app::{App, Backends};
use profile::application::command::{CreateProfileCommand, DeleteProfileCommand, UpdateProfileCommand};
use profile::application::port::{EventPublisher, PrivateDocuments, ProfileCache, ProfileRepository};
use profile::application::query::{GetProfileByHandleQuery, GetProfileByIdQuery};
use profile::domain::event::{DomainEvent, VerificationDecided};
use profile::domain::value_object::AccountId;
use profile::error::ProfileError;
use profile::infrastructure::publisher::wire::ProfileEventWire;

pub use profile::application::port::profile_cache::ProfileView;
pub use profile::domain::value_object::{ProfileId, Viewer};
pub use test_support::await_until;

/// Generous default patience for a cross-component assertion (ScyllaDB LWT,
/// Redis read-through warm / invalidation round-trip).
pub const DEADLINE: Duration = Duration::from_secs(10);

/// ScyllaDB keyspace the migrations provision.
const KEYSPACE: &str = "profile";
/// On-disk migration assets, resolved against *this* crate's manifest.
const MIGRATIONS_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/migrations");

/// Minimal externalized cache config the harness feeds the composition root —
/// the same `[cache]` shape production ships, exercising the real resolution path.
const CACHE_TOML: &str = r#"
[resilience]
default_profile = "standard"
[resilience.profiles.standard]
timeout = { duration_ms = 10000 }
circuit_breaker = { failure_threshold = 5, success_threshold = 2, open_duration_ms = 30000, half_open_max_calls = 1 }
retry = { max_attempts = 3, backoff = { kind = "exponential", base_ms = 50, max_ms = 10000, jitter = "full" } }

[cache]
default_profile = "standard"
[cache.profiles.standard]
ttl_secs = 300
[cache.profiles.handles]
ttl_secs = 600
[cache.bindings]
"profile-view" = "standard"
"handle-lookup" = "handles"
"#;

/// Captures the `type` tag of every published profile event, so a scenario can
/// assert the durable write emitted the right `profile.v1.events` notification.
#[derive(Default)]
pub struct CapturingEventPublisher {
    events:  Mutex<Vec<String>>,
    decided: Mutex<Vec<VerificationDecided>>,
}

#[async_trait::async_trait]
impl EventPublisher for CapturingEventPublisher {
    async fn publish(&self, event: &DomainEvent) -> Result<(), ProfileError> {
        let kind = ProfileEventWire::from(event).event_type();
        self.events.lock().unwrap().push(kind.to_owned());
        if let DomainEvent::VerificationDecided(decided) = event {
            self.decided.lock().unwrap().push(decided.clone());
        }
        Ok(())
    }
}

impl CapturingEventPublisher {
    pub fn published(&self) -> Vec<String> {
        self.events.lock().unwrap().clone()
    }

    /// The verification decisions announced (#777), oldest first.
    pub fn decided(&self) -> Vec<VerificationDecided> {
        self.decided.lock().unwrap().clone()
    }
}

/// Media, as far as verification evidence goes (#777): the private documents
/// each account owns, ready.
#[derive(Default)]
pub struct FakePrivateDocuments {
    owned: Mutex<Vec<(String, String)>>,
}

impl FakePrivateDocuments {
    /// `account` uploaded a private document; returns its asset id.
    pub fn upload(&self, account: &str) -> String {
        let asset = Uuid::now_v7().to_string();
        self.owned.lock().unwrap().push((asset.clone(), account.to_owned()));
        asset
    }
}

#[async_trait::async_trait]
impl PrivateDocuments for FakePrivateDocuments {
    async fn check_owned(&self, asset_id: &str, account: &AccountId) -> Result<(), ProfileError> {
        let owned = self.owned.lock().unwrap().iter().any(|(a, o)| a == asset_id && *o == account.to_string());
        if owned { Ok(()) } else { Err(ProfileError::VerificationDocumentInvalid { id: asset_id.to_owned() }) }
    }
}

/// A fully-wired profile service bound to ephemeral infra, plus assertion handles.
pub struct TestHarness {
    pub command_bus: Arc<InMemoryCommandBus>,
    pub query_bus:   Arc<InMemoryQueryBus>,
    pub repository:  Arc<dyn ProfileRepository>,
    pub cache:       Arc<dyn ProfileCache>,
    pub publisher:   Arc<CapturingEventPublisher>,
    pub documents:   Arc<FakePrivateDocuments>,
    /// A raw session, for scenarios that must shape rows directly (e.g. age a
    /// handle tombstone past its reservation).
    pub scylla:      Arc<ScyllaClient>,
}

impl TestHarness {
    /// Boots/reuses the shared containers, applies migrations, and assembles the
    /// service graph.
    pub async fn start() -> Self {
        let scylla_cp = test_support::containers::scylla_ready(KEYSPACE, MIGRATIONS_DIR).await;
        let redis_endpoint = test_support::containers::redis_endpoint().await;

        let scylla = Arc::new(
            ScyllaSessionBuilder::new(ScyllaConfig {
                contact_points: vec![scylla_cp.clone()],
                keyspace:       None,
                ..ScyllaConfig::default()
            })
            .build()
            .await
            .expect("integration: raw scylla session"),
        );
        let backends = Backends {
            scylla: ScyllaConfig {
                contact_points: vec![scylla_cp],
                keyspace:       None,
                ..ScyllaConfig::default()
            },
            redis: RedisConfig { hosts: vec![redis_endpoint], ..RedisConfig::default() },
        };

        let cache_section = InfrastructureConfig::from_toml(CACHE_TOML)
            .expect("integration: parse cache config")
            .cache
            .expect("integration: [cache] section present");
        let cache_registry = Arc::new(
            CacheRegistry::from_section(cache_section).expect("integration: resolve cache registry"),
        );

        let publisher = Arc::new(CapturingEventPublisher::default());
        let documents = Arc::new(FakePrivateDocuments::default());
        let app = App::build(
            backends,
            cache_registry,
            Arc::clone(&publisher) as Arc<dyn EventPublisher>,
            Some(Arc::clone(&documents) as Arc<dyn PrivateDocuments>),
        )
        .await
        .expect("integration: build profile app");

        Self {
            command_bus: app.command_bus,
            query_bus:   app.query_bus,
            repository:  app.repository,
            cache:       app.cache,
            publisher,
            documents,
            scylla,
        }
    }

    /// Creates a profile, expecting success.
    pub async fn create(&self, account_id: &str, handle: &str, display_name: &str) {
        dispatch_create(Arc::clone(&self.command_bus), account_id, handle, display_name)
            .await
            .expect("create_profile");
    }

    /// Updates a profile's display name.
    pub async fn update_display(&self, profile_id: &str, display_name: &str) {
        let cmd = UpdateProfileCommand {
            profile_id:   profile_id.to_owned(),
            display_name: Some(display_name.to_owned()),
            bio:          None,
            website_url:  None,
            locale:       None,
            custom_links: Vec::new(),
        };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .expect("update_profile");
    }

    /// Resolves a profile by handle, as a trusted internal caller.
    pub async fn get_by_handle(&self, handle: &str) -> Option<ProfileView> {
        self.get_by_handle_as(handle, Viewer::Internal).await
    }

    /// Resolves a profile by handle, as `viewer`.
    pub async fn get_by_handle_as(&self, handle: &str, viewer: Viewer) -> Option<ProfileView> {
        self.query_bus
            .dispatch(Envelope::new(
                Uuid::now_v7(),
                GetProfileByHandleQuery { handle: handle.to_owned(), viewer },
            ))
            .await
            .expect("get_profile_by_handle")
    }

    /// Resolves a profile by id (read-through: warms the cache on a miss).
    pub async fn get_by_id(&self, profile_id: &str) -> Option<ProfileView> {
        self.query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), GetProfileByIdQuery { profile_id: profile_id.to_owned(), viewer: Viewer::Internal }))
            .await
            .expect("get_profile_by_id")
    }
}

/// Dispatches a create on a shared bus — a free function so scenarios can fire
/// many concurrently from spawned tasks.
pub async fn dispatch_create(
    command_bus:  Arc<InMemoryCommandBus>,
    account_id:   &str,
    handle:       &str,
    display_name: &str,
) -> Result<(), CqrsError> {
    let cmd = CreateProfileCommand {
        account_id:   account_id.to_owned(),
        handle:       handle.to_owned(),
        display_name: display_name.to_owned(),
        bio:          None,
        avatar_url:   None,
        banner_url:   None,
        profile_kind: "personal".to_owned(),
        locale:       "en-US".to_owned(),
        minor:        false,
    };
    command_bus.dispatch(Envelope::new(Uuid::now_v7(), cmd)).await
}

/// Deletes a profile (its handle enters the reservation window).
pub async fn dispatch_delete(command_bus: Arc<InMemoryCommandBus>, profile_id: &str) -> Result<(), CqrsError> {
    command_bus
        .dispatch(Envelope::new(Uuid::now_v7(), DeleteProfileCommand { profile_id: profile_id.to_owned() }))
        .await
}

/// A fresh random account id (UUID string).
pub fn random_account_id() -> String {
    Uuid::now_v7().to_string()
}

/// A fresh random handle: `u` + 12 hex chars — valid (2–30 chars, starts/ends
/// alphanumeric, no disallowed characters).
pub fn random_handle() -> String {
    // The first 12 hex of a UUID v7 are just the unix-millis timestamp — two
    // scenarios starting in the same millisecond generated IDENTICAL handles,
    // and the resulting cross-test LWT conflicts produced "fresh handle
    // already taken" and zero-winner claim races under parallel execution
    // (deterministic-looking flakes on the suites' first CI runs). Use the
    // RANDOM tail of a v4 instead.
    format!("u{}", &Uuid::new_v4().simple().to_string()[..12])
}
