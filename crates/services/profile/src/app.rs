//! The profile service's composition root.
//!
//! [`App::build`] is *pure composition*: storage configs in, a fully-wired CQRS
//! graph out. It binds no socket and reads no environment, so a binary entrypoint
//! and the live integration harness assemble the exact same graph.
//!
//! Profile publishes its lifecycle events to `profile.v1.events` via an injected
//! [`EventPublisher`]: each command handler drains the aggregate's pending events
//! after the durable write and publishes them. The publisher is injected (Kafka in
//! the binary, a no-op in broker-free composition) so the graph itself needs no
//! broker. Its one inbound Kafka touchpoint (the account-event consumer) is wired
//! separately by the serving binary against [`App::command_bus`].

use std::sync::Arc;

use cqrs::command::{CommandBusBuilder, InMemoryCommandBus};
use cqrs::query::{InMemoryQueryBus, QueryBusBuilder};
use redis_storage::{RedisClient, RedisClientBuilder, RedisConfig};
use infra_config::CacheRegistry;
use scylla_storage::{ScyllaClient, ScyllaConfig, ScyllaSessionBuilder};

use crate::application::command::{
    ChangeHandleCommand, ChangeHandleHandler, CreateProfileCommand, CreateProfileHandler,
    DeleteProfileCommand, DeleteProfileHandler, HideAccountProfilesCommand, HideProfileCommand,
    HideProfileHandler, MaskAccountProfilesHandler, RestoreAccountProfilesCommand,
    RestoreProfileCommand, RestoreProfileHandler, SetProfileTierCommand, SetProfileTierHandler,
    SetCommentFiltersCommand, SetCommentFiltersHandler, SetDiscoverySettingsCommand, SetFeedSettingsCommand,
    SetFeedSettingsHandler, SetTabSettingsCommand,
    SetTabSettingsHandler,
    SetDiscoverySettingsHandler,
    SetInteractionLimitCommand, SetInteractionLimitHandler,
    SetInteractionSettingsCommand, SetInteractionSettingsHandler, SetLocationSettingsCommand,
    SetLocationSettingsHandler, SetVisibilityCommand, SetVisibilityHandler, UpdateAvatarCommand, UpdateAvatarHandler,
    UpdateBannerCommand, UpdateBannerHandler, UpdateProfileCommand, UpdateProfileHandler,
    VerifyProfileCommand, VerifyProfileHandler,
    DecideVerificationCommand, DecideVerificationHandler, RequestVerificationCommand,
    RequestVerificationHandler, SetAccountTypeCommand, SetAccountTypeHandler,
};
use crate::application::port::{EventPublisher, ProfileCache, ProfileRepository, VerificationStore};
use crate::application::query::{
    CheckHandleAvailabilityHandler, CheckHandleAvailabilityQuery, GetProfileByHandleHandler,
    GetProfileByHandleQuery, GetProfileByIdHandler, GetProfileByIdQuery,
    ListProfilesByAccountHandler, ListProfilesByAccountQuery, GetVerificationRequestHandler,
    GetVerificationRequestQuery, ListPendingVerificationsHandler, ListPendingVerificationsQuery,
};
use crate::infrastructure::cache::{
    RedisProfileCache, HANDLE_CACHE_NAMESPACE, PROFILE_CACHE_NAMESPACE,
};
use crate::infrastructure::persistence::ScyllaProfileRepository;

/// Storage endpoints the graph is wired against.
pub struct Backends {
    pub scylla: ScyllaConfig,
    pub redis:  RedisConfig,
}

/// A fully-wired profile service bound to its backends, plus the shared `Arc`
/// handles a scenario asserts against. The repository and cache exposed here are
/// the *same* instances the command/query handlers hold.
pub struct App {
    pub command_bus: Arc<InMemoryCommandBus>,
    pub query_bus:   Arc<InMemoryQueryBus>,
    pub repository:  Arc<dyn ProfileRepository>,
    pub cache:       Arc<dyn ProfileCache>,
    /// Live storage clients, retained so the runtime's readiness loop can probe
    /// their liveness (see [`crate::service`]).
    pub scylla:      Arc<ScyllaClient>,
    pub redis:       Arc<RedisClient>,
}

impl App {
    /// Builds storage clients from `backends`, assembles the repository, cache,
    /// and CQRS buses with every profile command and query registered.
    ///
    /// `cache_registry` carries this service's externalized cache-TTL profiles
    /// (resolved from `infrastructure.toml`); the caller owns the registry and its
    /// hot-reload watcher, so a TTL change reaches the live cache with no rebuild.
    pub async fn build(
        backends: Backends,
        cache_registry: Arc<CacheRegistry>,
        publisher: Arc<dyn EventPublisher>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let Backends { scylla, redis } = backends;

        // ── Storage clients ──────────────────────────────────────────────────
        let scylla_client = Arc::new(ScyllaSessionBuilder::new(scylla).build().await?);
        let redis_client = Arc::new(RedisClientBuilder::new(redis).build().await?);

        // ── Adapters (exposed behind their ports) ────────────────────────────
        // `Arc::clone` rather than move: the readiness loop probes the same live
        // clients the repository and cache use.
        let repository: Arc<dyn ProfileRepository> =
            Arc::new(ScyllaProfileRepository::new(Arc::clone(&scylla_client)));
        let cache: Arc<dyn ProfileCache> = Arc::new(RedisProfileCache::new(
            Arc::clone(&redis_client),
            cache_registry.profile_for(PROFILE_CACHE_NAMESPACE),
            cache_registry.profile_for(HANDLE_CACHE_NAMESPACE),
        ));
        let verifications: Arc<dyn VerificationStore> = Arc::new(
            crate::infrastructure::persistence::ScyllaVerificationStore::new(Arc::clone(&scylla_client)),
        );
        let share_tokens: Arc<dyn crate::application::port::ShareTokenStore> = Arc::new(
            crate::infrastructure::persistence::ScyllaShareTokenStore::new(Arc::clone(&scylla_client)),
        );
        let share_token_handler = || {
            crate::application::query::ShareTokenHandler::new(
                Arc::clone(&repository),
                Arc::clone(&cache),
                Arc::clone(&share_tokens),
            )
        };

        // ── Command bus ──────────────────────────────────────────────────────
        let command_bus = Arc::new(
            CommandBusBuilder::new()
                .register::<SetAccountTypeCommand, _>(SetAccountTypeHandler {
                    repo:      Arc::clone(&repository),
                    cache:     Arc::clone(&cache),
                    publisher: Arc::clone(&publisher),
                })?
                .register::<RequestVerificationCommand, _>(RequestVerificationHandler {
                    repo:          Arc::clone(&repository),
                    verifications: Arc::clone(&verifications),
                })?
                .register::<DecideVerificationCommand, _>(DecideVerificationHandler {
                    repo:          Arc::clone(&repository),
                    cache:         Arc::clone(&cache),
                    publisher:     Arc::clone(&publisher),
                    verifications: Arc::clone(&verifications),
                })?
                .register::<CreateProfileCommand, _>(CreateProfileHandler::new(
                    Arc::clone(&repository),
                    Arc::clone(&cache),
                    Arc::clone(&publisher),
                ))?
                .register::<UpdateProfileCommand, _>(UpdateProfileHandler::new(
                    Arc::clone(&repository),
                    Arc::clone(&cache),
                    Arc::clone(&publisher),
                ))?
                .register::<ChangeHandleCommand, _>(ChangeHandleHandler::new(
                    Arc::clone(&repository),
                    Arc::clone(&cache),
                    Arc::clone(&publisher),
                ))?
                .register::<UpdateAvatarCommand, _>(UpdateAvatarHandler::new(
                    Arc::clone(&repository),
                    Arc::clone(&cache),
                    Arc::clone(&publisher),
                ))?
                .register::<UpdateBannerCommand, _>(UpdateBannerHandler::new(
                    Arc::clone(&repository),
                    Arc::clone(&cache),
                    Arc::clone(&publisher),
                ))?
                .register::<SetVisibilityCommand, _>(SetVisibilityHandler::new(
                    Arc::clone(&repository),
                    Arc::clone(&cache),
                    Arc::clone(&publisher),
                ))?
                .register::<SetInteractionSettingsCommand, _>(SetInteractionSettingsHandler::new(
                    Arc::clone(&repository),
                    Arc::clone(&cache),
                    Arc::clone(&publisher),
                ))?
                .register::<SetInteractionLimitCommand, _>(SetInteractionLimitHandler {
                    repo:      Arc::clone(&repository),
                    cache:     Arc::clone(&cache),
                    publisher: Arc::clone(&publisher),
                })?
                .register::<SetLocationSettingsCommand, _>(SetLocationSettingsHandler::new(
                    Arc::clone(&repository),
                    Arc::clone(&cache),
                    Arc::clone(&publisher),
                ))?
                .register::<SetFeedSettingsCommand, _>(SetFeedSettingsHandler::new(
                    Arc::clone(&repository),
                    Arc::clone(&cache),
                ))?
                .register::<SetTabSettingsCommand, _>(SetTabSettingsHandler::new(
                    Arc::clone(&repository),
                    Arc::clone(&cache),
                    Arc::clone(&publisher),
                ))?
                .register::<SetCommentFiltersCommand, _>(SetCommentFiltersHandler::new(
                    Arc::clone(&repository),
                    Arc::clone(&cache),
                    Arc::clone(&publisher),
                ))?
                .register::<crate::application::query::RotateShareTokenCommand, _>(share_token_handler())?
                .register::<SetDiscoverySettingsCommand, _>(SetDiscoverySettingsHandler::new(
                    Arc::clone(&repository),
                    Arc::clone(&cache),
                    Arc::clone(&publisher),
                ))?
                .register::<VerifyProfileCommand, _>(VerifyProfileHandler::new(
                    Arc::clone(&repository),
                    Arc::clone(&cache),
                    Arc::clone(&publisher),
                ))?
                .register::<HideProfileCommand, _>(HideProfileHandler::new(
                    Arc::clone(&repository),
                    Arc::clone(&cache),
                    Arc::clone(&publisher),
                ))?
                .register::<RestoreProfileCommand, _>(RestoreProfileHandler::new(
                    Arc::clone(&repository),
                    Arc::clone(&cache),
                    Arc::clone(&publisher),
                ))?
                .register::<HideAccountProfilesCommand, _>(MaskAccountProfilesHandler::new(
                    Arc::clone(&repository),
                    Arc::clone(&cache),
                    Arc::clone(&publisher),
                ))?
                .register::<RestoreAccountProfilesCommand, _>(MaskAccountProfilesHandler::new(
                    Arc::clone(&repository),
                    Arc::clone(&cache),
                    Arc::clone(&publisher),
                ))?
                .register::<DeleteProfileCommand, _>(DeleteProfileHandler::new(
                    Arc::clone(&repository),
                    Arc::clone(&cache),
                    Arc::clone(&publisher),
                ))?
                .register::<SetProfileTierCommand, _>(SetProfileTierHandler::new(
                    Arc::clone(&repository),
                    Arc::clone(&cache),
                    Arc::clone(&publisher),
                ))?
                .build(),
        );

        // ── Query bus ────────────────────────────────────────────────────────
        let query_bus = Arc::new(
            QueryBusBuilder::new()
                .register::<crate::application::query::GetShareTokenQuery, _>(share_token_handler())?
                .register::<crate::application::query::ResolveShareTokenQuery, _>(share_token_handler())?
                .register::<GetProfileByIdQuery, _>(GetProfileByIdHandler::new(
                    Arc::clone(&repository),
                    Arc::clone(&cache),
                ))?
                .register::<GetProfileByHandleQuery, _>(GetProfileByHandleHandler::new(
                    Arc::clone(&repository),
                    Arc::clone(&cache),
                ))?
                .register::<CheckHandleAvailabilityQuery, _>(CheckHandleAvailabilityHandler::new(
                    Arc::clone(&repository),
                ))?
                .register::<ListProfilesByAccountQuery, _>(ListProfilesByAccountHandler::new(
                    Arc::clone(&repository),
                ))?
                .register::<GetVerificationRequestQuery, _>(GetVerificationRequestHandler {
                    verifications: Arc::clone(&verifications),
                })?
                .register::<ListPendingVerificationsQuery, _>(ListPendingVerificationsHandler {
                    verifications: Arc::clone(&verifications),
                })?
                .build(),
        );

        Ok(Self {
            command_bus,
            query_bus,
            repository,
            cache,
            scylla: scylla_client,
            redis: redis_client,
        })
    }
}
