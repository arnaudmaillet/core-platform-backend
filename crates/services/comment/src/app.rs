//! The comment service's composition root.
//!
//! [`App::build`] is *pure composition*: a ScyllaDB config and a
//! [`CommentEventPublisher`] in, a fully-wired CQRS graph out. It binds no socket
//! and reads no environment, so the production entrypoint
//! ([`crate::infrastructure::grpc::server::serve`]) and the live integration
//! harness assemble the exact same graph.
//!
//! The event publisher is a generic parameter (the chat/post pattern): production
//! passes the Kafka publisher; the integration harness passes an in-process no-op,
//! so the dual-table and tombstone-vs-purge scenarios run without a broker.

use std::sync::Arc;

use cqrs::command::{CommandBusBuilder, InMemoryCommandBus};
use cqrs::query::{InMemoryQueryBus, QueryBusBuilder};
use scylla_storage::{ScyllaClient, ScyllaConfig, ScyllaSessionBuilder};

use crate::application::command::create_comment::{CreateCommentCommand, CreateCommentHandler};
use crate::application::command::delete_comment::{DeleteCommentCommand, DeleteCommentHandler};
use crate::application::port::{CommentEventPublisher, ReadGate};
use crate::application::query::get_comment::{GetCommentHandler, GetCommentQuery};
use crate::application::query::list_replies::{ListRepliesHandler, ListRepliesQuery};
use crate::application::query::list_top_level::{ListTopLevelHandler, ListTopLevelQuery};
use crate::infrastructure::persistence::ScyllaCommentRepository;

/// Storage endpoints the graph is wired against. Comment is ScyllaDB-only; its
/// events are emitted through the injected publisher.
pub struct Backends {
    pub scylla: ScyllaConfig,
}

/// A fully-wired comment service bound to its backends. The buses exposed here
/// are the *same* instances the handlers are registered into; `GetComment` reads
/// the canonical `comments` table while `ListTopLevel`/`ListReplies` read the
/// `comments_by_post` thread index, so the query bus proves their consistency.
pub struct App {
    pub command_bus: Arc<InMemoryCommandBus>,
    pub query_bus:   Arc<InMemoryQueryBus>,
    /// Live storage client, retained so the runtime's readiness loop can probe
    /// its liveness (see [`crate::service`]).
    pub scylla:      Arc<ScyllaClient>,
}

impl App {
    /// Builds the ScyllaDB client and repository, then the CQRS buses with every
    /// comment command and query registered against the supplied `publisher`.
    pub async fn build<P: CommentEventPublisher>(
        backends:  Backends,
        publisher: Arc<P>,
        gate:      Arc<dyn ReadGate>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let scylla_client = Arc::new(ScyllaSessionBuilder::new(backends.scylla).build().await?);
        let repository = Arc::new(ScyllaCommentRepository::new(Arc::clone(&scylla_client)));

        let command_bus = Arc::new(
            CommandBusBuilder::new()
                .register::<CreateCommentCommand, _>(CreateCommentHandler {
                    repository: Arc::clone(&repository),
                    publisher:  Arc::clone(&publisher),
                    gate:       Arc::clone(&gate),
                })?
                .register::<DeleteCommentCommand, _>(DeleteCommentHandler {
                    repository: Arc::clone(&repository),
                    publisher:  Arc::clone(&publisher),
                })?
                .build(),
        );

        let query_bus = Arc::new(
            QueryBusBuilder::new()
                .register::<GetCommentQuery, _>(GetCommentHandler {
                    repository: Arc::clone(&repository),
                    gate:       Arc::clone(&gate),
                })?
                .register::<ListTopLevelQuery, _>(ListTopLevelHandler {
                    repository: Arc::clone(&repository),
                    gate:       Arc::clone(&gate),
                })?
                .register::<ListRepliesQuery, _>(ListRepliesHandler {
                    repository: Arc::clone(&repository),
                    gate,
                })?
                .build(),
        );

        Ok(Self { command_bus, query_bus, scylla: scylla_client })
    }
}
