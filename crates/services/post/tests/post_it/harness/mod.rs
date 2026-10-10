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
use post::application::command::create_post::{CreatePostCommand, CreatePosts, CreatedPost};
use post::application::command::apply_moderation::ApplyModerationCommand;
use post::application::command::delete_post::DeletePostCommand;
use post::application::command::publish_post::PublishPostCommand;
use post::application::query::get_post::GetPostQuery;
use post::application::query::list_posts_by_profile::{ListPostsByProfileQuery, ProfileTab};

pub use post::application::port::PostSummary;
pub use post::domain::aggregate::Post;
pub use post::application::port::{AuthorLocationStore, AuthorWindowStore, PostRepository, ReuseDefaults, ReuseRegistry};
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
    /// CreatePost as the gRPC layer calls it (its answer, #876).
    pub creator:     Arc<dyn CreatePosts>,
    pub publisher:   Arc<CapturingPublisher>,
    /// The audience check: authors are visible unless a scenario scripts it.
    pub gate:        Arc<ScriptedGate>,
    /// The authors' location sharing (the `profile.v1.events` projection).
    pub locations:   Arc<dyn AuthorLocationStore>,
    /// The authors' post window (the `profile.v1.events` projection).
    pub windows:     Arc<dyn AuthorWindowStore>,
    /// The authors' reuse defaults and sound origins (#669).
    pub reuse:       Arc<dyn ReuseRegistry>,
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
            creator:     app.creator,
            publisher,
            gate,
            locations:   app.author_location_store,
            windows:     app.author_window_store,
            reuse:       app.reuse_registry,
            repository:  Arc::new(ScyllaPostRepository::new(Arc::clone(&app.scylla))),
        }
    }

    /// Creates a `TextOnly` post, expecting success.
    pub async fn create(&self, post_id: &str, profile_id: &str) {
        dispatch_create(Arc::clone(&self.command_bus), post_id.to_owned(), profile_id.to_owned())
            .await
            .expect("create_post");
    }

    /// Creates a `TextOnly` post under a fresh post id with `key` (#876), as
    /// a retried CreatePost does: the post it answers.
    pub async fn create_keyed(&self, profile_id: &str, key: &str) -> Result<CreatedPost, post::error::PostError> {
        let mut cmd = create_command(random_id(), profile_id.to_owned());
        cmd.idempotency_key = Some(key.to_owned());
        self.creator.create(&cmd).await
    }

    /// [`Self::create_keyed`] with `caption`.
    pub async fn try_create_keyed_captioned(
        &self,
        profile_id: &str,
        key: &str,
        caption: &str,
    ) -> Result<CreatedPost, post::error::PostError> {
        let mut cmd = create_command(random_id(), profile_id.to_owned());
        cmd.idempotency_key = Some(key.to_owned());
        cmd.caption = caption.to_owned();
        self.creator.create(&cmd).await
    }

    /// Tries to create a `TextOnly` post with `caption`; the error when refused.
    pub async fn try_create_captioned(&self, post_id: &str, profile_id: &str, caption: &str) -> Result<(), CqrsError> {
        let mut cmd = create_command(post_id.to_owned(), profile_id.to_owned());
        cmd.caption = caption.to_owned();
        self.command_bus.dispatch(Envelope::new(Uuid::now_v7(), cmd)).await
    }

    /// Tries to change a post's caption; the error when refused.
    pub async fn try_recaption(&self, post_id: &str, profile_id: &str, caption: &str) -> Result<(), CqrsError> {
        let cmd = post::application::command::update_post::UpdatePostCommand {
            post_id:     post_id.to_owned(),
            profile_id:  profile_id.to_owned(),
            caption:     caption.to_owned(),
            attachments: Vec::new(),
        };
        self.command_bus.dispatch(Envelope::new(Uuid::now_v7(), cmd)).await
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

    /// Creates a repost of `parent_id` (#829), expecting success.
    pub async fn create_repost(&self, post_id: &str, profile_id: &str, parent_id: &str) {
        let mut cmd = create_command(post_id.to_owned(), profile_id.to_owned());
        cmd.parent_id = Some(parent_id.to_owned());
        cmd.root_id = Some(parent_id.to_owned());
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

    /// Tries to create a post with a sound (and, optionally, the post's own
    /// reuse permission); the error when it is refused.
    pub async fn try_create_with_sound(
        &self,
        post_id: &str,
        profile_id: &str,
        audio_id: &str,
        original: bool,
        allow_sound_reuse: Option<bool>,
    ) -> Result<(), CqrsError> {
        use post::domain::aggregate::ReuseOverrides;
        use post::domain::value_object::{AudioId, AudioKind, AudioReference};
        let mut cmd = create_command(post_id.to_owned(), profile_id.to_owned());
        cmd.audio_ref = Some(AudioReference {
            audio_id:   AudioId::try_from(audio_id).expect("audio id"),
            audio_kind: if original { AudioKind::OriginalSound } else { AudioKind::Reused },
        });
        cmd.reuse = ReuseOverrides { allow_remix: None, allow_sound_reuse };
        self.command_bus.dispatch(Envelope::new(Uuid::now_v7(), cmd)).await
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
                GetPostQuery { post_id: post_id.to_owned(), viewer, mature, as_author: None },
            ))
            .await
    }

    /// Reads a single post as `viewer`, on `as_author`'s behalf (the export).
    pub async fn get_on_behalf(&self, post_id: &str, viewer: Viewer, as_author: &str) -> Result<Post, CqrsError> {
        self.query_bus
            .dispatch(Envelope::new(
                Uuid::now_v7(),
                GetPostQuery { post_id: post_id.to_owned(), viewer, mature: true, as_author: Some(as_author.to_owned()) },
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
        self.list_tab(profile_id, viewer, mature, ProfileTab::All).await
    }

    /// Lists one of a profile's post tabs (#829) as `viewer`.
    pub async fn list_tab(&self, profile_id: &str, viewer: Viewer, mature: bool, tab: ProfileTab) -> Vec<PostSummary> {
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
                    tab,
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
        reuse:       Default::default(),
        idempotency_key: None,
    }
}

/// A fresh random id (UUID string) usable as a post_id or profile_id.
pub fn random_id() -> String {
    Uuid::now_v7().to_string()
}
