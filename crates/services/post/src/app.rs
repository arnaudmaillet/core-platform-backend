//! The post service's composition root.
//!
//! [`App::build`] is *pure composition*: a ScyllaDB config and an
//! [`EventPublisher`] in, a fully-wired CQRS graph out. It binds no socket and
//! reads no environment, so a binary entrypoint and the live integration harness
//! assemble the exact same graph.
//!
//! The event publisher is a generic parameter, mirroring the chat pattern:
//! production passes the Kafka publisher; the integration harness passes an
//! in-process capturing fake, so the create→publish→delete lifecycle and its
//! emitted events are asserted without a broker.

use std::sync::Arc;

use cqrs::command::{CommandBusBuilder, InMemoryCommandBus};
use cqrs::query::{InMemoryQueryBus, QueryBusBuilder};
use scylla_storage::{ScyllaClient, ScyllaConfig, ScyllaSessionBuilder};

use crate::application::command::apply_moderation::{ApplyModerationCommand, ApplyModerationHandler};
use crate::application::command::create_post::{CreatePostCommand, CreatePostHandler};
use crate::application::command::delete_post::{DeletePostCommand, DeletePostHandler};
use crate::application::command::publish_post::{PublishPostCommand, PublishPostHandler};
use crate::application::command::restore_post::{RestorePostCommand, RestorePostHandler};
use crate::application::query::list_recently_deleted::{ListRecentlyDeletedHandler, ListRecentlyDeletedQuery};
use crate::application::command::update_post::{UpdatePostCommand, UpdatePostHandler};
use crate::application::port::{
    AudienceGate, AuthorLocationStore, AuthorTierStore, AuthorWindowStore, EventPublisher, RecentlyDeleted,
};
use crate::application::query::get_post::{GetPostHandler, GetPostQuery};
use crate::application::query::list_posts_by_profile::{
    ListPostsByProfileHandler, ListPostsByProfileQuery,
};
use crate::infrastructure::persistence::{
    ScyllaAuthorLocationStore, ScyllaAuthorTierStore, ScyllaAuthorWindowStore, ScyllaPostRepository,
    ScyllaRecentlyDeleted,
};

/// Storage endpoints the graph is wired against. Post has no Redis and emits its
/// events through the injected [`EventPublisher`], so only ScyllaDB is needed.
pub struct Backends {
    pub scylla: ScyllaConfig,
}

/// A fully-wired post service bound to its backends. The buses exposed here are
/// the *same* instances the handlers are registered into; the two read paths
/// (`GetPost` over `posts`, `ListPostsByProfile` over `posts_by_profile`) let a
/// scenario assert dual-table consistency through the query bus alone.
pub struct App {
    pub command_bus: Arc<InMemoryCommandBus>,
    pub query_bus:   Arc<InMemoryQueryBus>,
    /// Live storage client, retained so the runtime's readiness loop can probe
    /// its liveness (see [`crate::service`]).
    pub scylla:      Arc<ScyllaClient>,
    /// The author-tier projection, exposed so the serving binary can wire its
    /// `profile.v1.events` consumer against the same instance.
    pub author_tier_store: Arc<dyn AuthorTierStore>,
    /// The authors' location sharing, exposed likewise for its
    /// `profile.v1.events` consumer.
    pub author_location_store: Arc<dyn AuthorLocationStore>,
    /// The authors' post window, exposed likewise for the same consumer.
    pub author_window_store: Arc<dyn AuthorWindowStore>,
}

impl App {
    /// Builds the ScyllaDB client and repository, then the CQRS buses with every
    /// post command and query registered against the supplied `publisher`.
    pub async fn build<P: EventPublisher>(
        backends:  Backends,
        publisher: Arc<P>,
        audience:  Arc<dyn AudienceGate>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let scylla_client = Arc::new(ScyllaSessionBuilder::new(backends.scylla).build().await?);
        let repository = Arc::new(ScyllaPostRepository::new(Arc::clone(&scylla_client)));
        let author_tier_store: Arc<dyn AuthorTierStore> =
            Arc::new(ScyllaAuthorTierStore::new(Arc::clone(&scylla_client)));
        let author_location_store: Arc<dyn AuthorLocationStore> =
            Arc::new(ScyllaAuthorLocationStore::new(Arc::clone(&scylla_client)));
        let recently_deleted: Arc<dyn RecentlyDeleted> =
            Arc::new(ScyllaRecentlyDeleted::new(Arc::clone(&scylla_client)));
        let author_window_store: Arc<dyn AuthorWindowStore> =
            Arc::new(ScyllaAuthorWindowStore::new(Arc::clone(&scylla_client)));

        let command_bus = Arc::new(
            CommandBusBuilder::new()
                .register::<CreatePostCommand, _>(CreatePostHandler {
                    repository: Arc::clone(&repository),
                    publisher:  Arc::clone(&publisher),
                })?
                .register::<PublishPostCommand, _>(PublishPostHandler {
                    repository:        Arc::clone(&repository),
                    publisher:         Arc::clone(&publisher),
                    author_tier_store: Arc::clone(&author_tier_store),
                })?
                .register::<UpdatePostCommand, _>(UpdatePostHandler {
                    repository: Arc::clone(&repository),
                    publisher:  Arc::clone(&publisher),
                })?
                .register::<DeletePostCommand, _>(DeletePostHandler {
                    repository:       Arc::clone(&repository),
                    publisher:        Arc::clone(&publisher),
                    recently_deleted: Arc::clone(&recently_deleted),
                })?
                .register::<RestorePostCommand, _>(RestorePostHandler {
                    repository:        Arc::clone(&repository),
                    publisher:         Arc::clone(&publisher),
                    author_tier_store: Arc::clone(&author_tier_store),
                    recently_deleted:  Arc::clone(&recently_deleted),
                })?
                .register::<ApplyModerationCommand, _>(ApplyModerationHandler {
                    repository: Arc::clone(&repository),
                })?
                .build(),
        );

        let query_bus = Arc::new(
            QueryBusBuilder::new()
                .register::<GetPostQuery, _>(GetPostHandler {
                    repository: Arc::clone(&repository),
                    audience:   Arc::clone(&audience),
                    locations:  Arc::clone(&author_location_store),
                    windows:    Arc::clone(&author_window_store),
                })?
                .register::<ListRecentlyDeletedQuery, _>(ListRecentlyDeletedHandler {
                    repository:       Arc::clone(&repository),
                    recently_deleted: Arc::clone(&recently_deleted),
                })?
                .register::<ListPostsByProfileQuery, _>(ListPostsByProfileHandler {
                    repository: Arc::clone(&repository),
                    audience,
                    windows:    Arc::clone(&author_window_store),
                })?
                .build(),
        );

        Ok(Self {
            command_bus,
            query_bus,
            scylla: scylla_client,
            author_tier_store,
            author_location_store,
            author_window_store,
        })
    }
}
