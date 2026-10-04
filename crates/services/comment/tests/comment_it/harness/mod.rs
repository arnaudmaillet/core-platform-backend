//! Integration harness: boots an ephemeral ScyllaDB container, wires a real
//! comment graph against it through the production composition root, and exposes
//! the buses for assertions. The event publisher is an in-process no-op.
#![allow(dead_code)]

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use uuid::Uuid;

use cqrs::command::InMemoryCommandBus;
use cqrs::query::InMemoryQueryBus;
use cqrs::{CommandBus, CqrsError, Envelope, QueryBus};
use scylla_storage::ScyllaConfig;

use comment::app::{App, Backends};
use comment::application::command::create_comment::CreateCommentCommand;
use comment::application::command::delete_comment::DeleteCommentCommand;
use comment::application::port::{CommentEventPublisher, CommentSummary, ReadGate};
use comment::application::query::get_comment::GetCommentQuery;
use comment::application::query::list_replies::ListRepliesQuery;
use comment::application::query::list_top_level::ListTopLevelQuery;
use comment::domain::event::DomainEvent;
use comment::error::CommentError;

pub use comment::domain::aggregate::Comment;
pub use comment::domain::value_object::{CommentStatus, PostId, ProfileId, Viewer};
pub use test_support::await_until;

/// Generous default patience for a cross-component assertion (ScyllaDB dual-table
/// write visibility).
pub const DEADLINE: Duration = Duration::from_secs(10);

/// ScyllaDB keyspace the migrations provision.
const KEYSPACE: &str = "comment";
/// On-disk migration assets, resolved against *this* crate's manifest.
const MIGRATIONS_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/migrations");

/// A no-op event publisher: these scenarios assert on ScyllaDB, not on the Kafka
/// contract.
struct NoopPublisher;

#[async_trait]
impl CommentEventPublisher for NoopPublisher {
    async fn publish(&self, _event: &DomainEvent) -> Result<(), CommentError> {
        Ok(())
    }
}

/// A scriptable read gate: every post is readable and no author hidden unless
/// a scenario says otherwise; it can also be made to fail.
#[derive(Default)]
pub struct ScriptedGate {
    unreadable_posts: Mutex<HashSet<String>>,
    hidden_authors:   Mutex<HashSet<String>>,
    down:             Mutex<bool>,
}

impl ScriptedGate {
    pub fn post_unreadable(&self, post_id: &str) {
        self.unreadable_posts.lock().unwrap().insert(post_id.to_owned());
    }
    pub fn hide_author(&self, author_id: &str) {
        self.hidden_authors.lock().unwrap().insert(author_id.to_owned());
    }
    pub fn set_down(&self, down: bool) {
        *self.down.lock().unwrap() = down;
    }
}

#[async_trait]
impl ReadGate for ScriptedGate {
    async fn check(
        &self,
        _viewer: &Viewer,
        post_id: &PostId,
        comment_authors: &[ProfileId],
    ) -> Result<Option<HashSet<ProfileId>>, CommentError> {
        if *self.down.lock().unwrap() {
            return Err(CommentError::AccessCheckUnavailable { reason: "scripted outage".into() });
        }
        if self.unreadable_posts.lock().unwrap().contains(&post_id.as_str()) {
            return Ok(None);
        }
        let hidden = self.hidden_authors.lock().unwrap();
        Ok(Some(comment_authors.iter().filter(|a| hidden.contains(&a.as_str())).cloned().collect()))
    }
}

/// A fully-wired comment service bound to ephemeral infra, plus the buses.
pub struct TestHarness {
    pub command_bus: Arc<InMemoryCommandBus>,
    pub query_bus:   Arc<InMemoryQueryBus>,
    pub gate:        Arc<ScriptedGate>,
}

impl TestHarness {
    /// Boots/reuses the shared ScyllaDB container, applies migrations, and
    /// assembles the service graph with a no-op publisher.
    pub async fn start() -> Self {
        let scylla_cp = test_support::containers::scylla_ready(KEYSPACE, MIGRATIONS_DIR).await;

        let backends = Backends {
            scylla: ScyllaConfig {
                contact_points: vec![scylla_cp],
                keyspace:       None,
                ..ScyllaConfig::default()
            },
        };

        let gate = Arc::new(ScriptedGate::default());
        let app = App::build(backends, Arc::new(NoopPublisher), Arc::clone(&gate) as _)
            .await
            .expect("integration: build comment app");

        Self { command_bus: app.command_bus, query_bus: app.query_bus, gate }
    }

    /// Creates a comment (top-level when `parent` is `None`, else a reply) authored
    /// by `author_id`, returning its id.
    pub async fn create(&self, post_id: &str, parent: Option<&str>, author_id: &str) -> String {
        let comment_id = Uuid::now_v7().to_string();
        let cmd = CreateCommentCommand {
            comment_id: comment_id.clone(),
            post_id:    post_id.to_owned(),
            author_id:  author_id.to_owned(),
            parent_id:  parent.map(str::to_owned),
            body:       Some("a comment".to_owned()),
            gif_id:     None,
            gif_url:    None,
            gif_width:  None,
            gif_height: None,
        };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .expect("create_comment");
        comment_id
    }

    /// Deletes a comment as `author`.
    pub async fn delete(&self, comment_id: &str, author_id: &str) {
        let cmd = DeleteCommentCommand {
            comment_id: comment_id.to_owned(),
            author_id:  author_id.to_owned(),
        };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .expect("delete_comment");
    }

    /// Reads a single comment from the `comments` table (`Err` when absent).
    pub async fn get(&self, comment_id: &str) -> Result<Comment, CqrsError> {
        self.get_as(comment_id, Viewer::Internal).await
    }

    /// Reads a single comment as `viewer`.
    pub async fn get_as(&self, comment_id: &str, viewer: Viewer) -> Result<Comment, CqrsError> {
        self.query_bus
            .dispatch(Envelope::new(
                Uuid::now_v7(),
                GetCommentQuery { comment_id: comment_id.to_owned(), viewer },
            ))
            .await
    }

    /// Lists top-level comments of a post as `viewer` (`Err` on a gate outage).
    pub async fn try_list_top_level_as(
        &self,
        post_id: &str,
        viewer: Viewer,
    ) -> Result<Vec<CommentSummary>, CqrsError> {
        let (summaries, _next): (Vec<CommentSummary>, Option<String>) = self
            .query_bus
            .dispatch(Envelope::new(
                Uuid::now_v7(),
                ListTopLevelQuery { post_id: post_id.to_owned(), limit: 100, page_token: None, viewer },
            ))
            .await?;
        Ok(summaries)
    }

    /// Lists top-level comments of a post from the `comments_by_post` index.
    pub async fn list_top_level(&self, post_id: &str) -> Vec<CommentSummary> {
        let (summaries, _next) = self
            .query_bus
            .dispatch(Envelope::new(
                Uuid::now_v7(),
                ListTopLevelQuery {
                    post_id:    post_id.to_owned(),
                    limit:      100,
                    page_token: None,
                    viewer:     Viewer::Internal,
                },
            ))
            .await
            .expect("list_top_level");
        summaries
    }

    /// Lists the replies to `parent` under `post` from the thread index.
    pub async fn list_replies(&self, post_id: &str, parent: &str) -> Vec<CommentSummary> {
        let (summaries, _next) = self
            .query_bus
            .dispatch(Envelope::new(
                Uuid::now_v7(),
                ListRepliesQuery {
                    post_id:    post_id.to_owned(),
                    comment_id: parent.to_owned(),
                    limit:      100,
                    page_token: None,
                    viewer:     Viewer::Internal,
                },
            ))
            .await
            .expect("list_replies");
        summaries
    }
}

/// A fresh random post id (UUID string).
pub fn random_post() -> String {
    Uuid::now_v7().to_string()
}

/// A fresh random author/profile id (UUID string).
pub fn random_author() -> String {
    Uuid::now_v7().to_string()
}

/// Whether `summaries` contains a comment with the given id.
pub fn summaries_contain(summaries: &[CommentSummary], comment_id: &str) -> bool {
    summaries.iter().any(|s| s.comment_id.as_str() == comment_id)
}
