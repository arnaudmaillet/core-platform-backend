//! Integration harness: boots the shared infra, wires a real post graph against
//! it through the production composition root, and exposes the buses plus the
//! capturing publisher for assertions.
//!
//! Reads go through the query bus: `GetPost` reads the `posts` table and
//! `ListPostsByProfile` reads `posts_by_profile`, so querying both is how a
//! scenario proves the dual-table write stayed consistent.
#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use uuid::Uuid;

use cqrs::command::InMemoryCommandBus;
use cqrs::query::InMemoryQueryBus;
use cqrs::{CommandBus, CqrsError, Envelope, QueryBus};
use scylla_storage::ScyllaConfig;

use post::app::{App, Backends};
use post::application::command::create_post::CreatePostCommand;
use post::application::command::apply_moderation::ApplyModerationCommand;
use post::application::command::delete_post::DeletePostCommand;
use post::application::command::publish_post::PublishPostCommand;
use post::application::query::get_post::GetPostQuery;
use post::application::query::list_posts_by_profile::ListPostsByProfileQuery;

pub use post::application::port::PostSummary;
pub use post::domain::aggregate::Post;
pub use post::application::port::{AuthorLocationStore, AuthorWindowStore, PostRepository};
use post::infrastructure::persistence::ScyllaPostRepository;
pub use post::domain::value_object::{
    ContentAccess, LocationSharing, ModerationRestriction, PostStatus, ProfileId, Viewer,
};
pub use test_support::await_until;

use crate::post_it::fakes::{CapturingPublisher, ScriptedGate};

/// Generous default patience for a cross-component assertion (ScyllaDB
/// dual-table write visibility).
pub const DEADLINE: Duration = Duration::from_secs(10);

/// ScyllaDB keyspace the migrations provision.
const KEYSPACE: &str = "post";
/// On-disk migration assets, resolved against *this* crate's manifest.
const MIGRATIONS_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/migrations");

/// `PostKind::TextOnly` — the simplest valid post (no media attachments).
pub const KIND_TEXT_ONLY: i32 = 1;

/// A fully-wired post service bound to ephemeral infra, plus assertion handles.
pub struct TestHarness {
    pub command_bus: Arc<InMemoryCommandBus>,
    pub query_bus:   Arc<InMemoryQueryBus>,
    pub publisher:   Arc<CapturingPublisher>,
    /// The audience check: authors are visible unless a scenario scripts it.
    pub gate:        Arc<ScriptedGate>,
    /// The authors' location sharing (the `profile.v1.events` projection).
    pub locations:   Arc<dyn AuthorLocationStore>,
    /// The authors' post window (the `profile.v1.events` projection).
    pub windows:     Arc<dyn AuthorWindowStore>,
    /// Direct store access, to seed posts dated in the past.
    pub repository:  Arc<ScyllaPostRepository>,
}

impl TestHarness {
    /// Boots/reuses the shared ScyllaDB container, applies migrations, and
    /// assembles the service graph with a capturing event publisher.
    pub async fn start() -> Self {
        let scylla_cp = test_support::containers::scylla_ready(KEYSPACE, MIGRATIONS_DIR).await;

        let backends = Backends {
            scylla: ScyllaConfig {
                contact_points: vec![scylla_cp],
                keyspace:       None,
                ..ScyllaConfig::default()
            },
        };

        let publisher = Arc::new(CapturingPublisher::new());
        let gate = Arc::new(ScriptedGate::default());
        let app = App::build(backends, Arc::clone(&publisher), Arc::clone(&gate) as _)
            .await
            .expect("integration: build post app");

        Self {
            command_bus: app.command_bus,
            query_bus:   app.query_bus,
            publisher,
            gate,
            locations:   app.author_location_store,
            windows:     app.author_window_store,
            repository:  Arc::new(ScyllaPostRepository::new(Arc::clone(&app.scylla))),
        }
    }

    /// Creates a `TextOnly` post, expecting success.
    pub async fn create(&self, post_id: &str, profile_id: &str) {
        dispatch_create(Arc::clone(&self.command_bus), post_id.to_owned(), profile_id.to_owned())
            .await
            .expect("create_post");
    }

    /// Creates a `TextOnly` post made at `(lat, lng)`, expecting success.
    pub async fn create_at(&self, post_id: &str, profile_id: &str, lat: f64, lng: f64) {
        let mut cmd = create_command(post_id.to_owned(), profile_id.to_owned());
        cmd.location = Some((lat, lng));
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .expect("create_post");
    }

    /// Stores a published `TextOnly` post by `profile_id` created `days_ago`.
    pub async fn seed_published(&self, post_id: &str, profile_id: &str, days_ago: i64) {
        use post::domain::value_object::{Caption, ModerationState, PostId, PostKind};
        let at = chrono::Utc::now() - chrono::Duration::days(days_ago);
        let post = Post::reconstitute(
            PostId::try_from(post_id).expect("post id"),
            ProfileId::try_from(profile_id).expect("profile id"),
            PostKind::TextOnly,
            PostStatus::Published,
            Caption::new("old".to_owned()).expect("caption"),
            Vec::new(),
            None,
            None,
            None,
            None,
            at,
            at,
            Some(at),
            None,
            ModerationState::default(),
        );
        self.repository.insert(&post).await.expect("seed post");
    }

    /// Publishes a draft post.
    pub async fn publish(&self, post_id: &str, profile_id: &str) {
        let cmd = PublishPostCommand {
            post_id:    post_id.to_owned(),
            profile_id: profile_id.to_owned(),
        };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .expect("publish_post");
    }

    /// Deletes a post.
    pub async fn delete(&self, post_id: &str, profile_id: &str) {
        let cmd = DeletePostCommand {
            post_id:    post_id.to_owned(),
            profile_id: profile_id.to_owned(),
        };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .expect("delete_post");
    }

    /// Records a moderation outcome, as the `moderation.v1.events` consumer does.
    pub async fn moderate(&self, post_id: &str, restriction: ModerationRestriction, version: i64) {
        let cmd = ApplyModerationCommand { post_id: post_id.to_owned(), restriction, version };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .expect("apply_moderation");
    }

    /// Reads a single post from the `posts` table (`Err` when absent), as a
    /// trusted internal caller.
    pub async fn get(&self, post_id: &str) -> Result<Post, CqrsError> {
        self.get_as(post_id, Viewer::Internal).await
    }

    /// Reads a single post as `viewer`, an adult reader (`Err` when absent or
    /// not visible).
    pub async fn get_as(&self, post_id: &str, viewer: Viewer) -> Result<Post, CqrsError> {
        self.get_rated(post_id, viewer, true).await
    }

    /// Reads a single post as `viewer`, cleared for mature content or not.
    pub async fn get_rated(&self, post_id: &str, viewer: Viewer, mature: bool) -> Result<Post, CqrsError> {
        self.query_bus
            .dispatch(Envelope::new(
                Uuid::now_v7(),
                GetPostQuery { post_id: post_id.to_owned(), viewer, mature },
            ))
            .await
    }

    /// Lists a profile's posts from the `posts_by_profile` table, as a trusted
    /// internal caller.
    pub async fn list(&self, profile_id: &str) -> Vec<PostSummary> {
        self.list_as(profile_id, Viewer::Internal).await
    }

    /// Lists a profile's posts as `viewer`, an adult reader.
    pub async fn list_as(&self, profile_id: &str, viewer: Viewer) -> Vec<PostSummary> {
        self.list_rated(profile_id, viewer, true).await
    }

    /// Lists a profile's posts as `viewer`, cleared for mature content or not.
    pub async fn list_rated(&self, profile_id: &str, viewer: Viewer, mature: bool) -> Vec<PostSummary> {
        let (summaries, _next) = self
            .query_bus
            .dispatch(Envelope::new(
                Uuid::now_v7(),
                ListPostsByProfileQuery {
                    profile_id: profile_id.to_owned(),
                    limit:      100,
                    page_token: None,
                    viewer,
                    mature,
                },
            ))
            .await
            .expect("list_posts_by_profile");
        summaries
    }
}

/// Dispatches a create on a shared bus — a free function so scenarios can fire
/// many concurrently from spawned tasks.
pub async fn dispatch_create(
    command_bus: Arc<InMemoryCommandBus>,
    post_id:     String,
    profile_id:  String,
) -> Result<(), CqrsError> {
    command_bus.dispatch(Envelope::new(Uuid::now_v7(), create_command(post_id, profile_id))).await
}

fn create_command(post_id: String, profile_id: String) -> CreatePostCommand {
    CreatePostCommand {
        post_id,
        profile_id,
        kind:        KIND_TEXT_ONLY,
        caption:     "hello".to_owned(),
        attachments: Vec::new(),
        parent_id:   None,
        root_id:     None,
        audio_ref:   None,
        location:    None,
    }
}

/// A fresh random id (UUID string) usable as a post_id or profile_id.
pub fn random_id() -> String {
    Uuid::now_v7().to_string()
}
