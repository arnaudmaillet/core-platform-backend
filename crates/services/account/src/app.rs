//! The account service's composition root.
//!
//! [`App::build`] is *pure composition*: a Postgres connection pool in, a
//! fully-wired CQRS graph out. It binds no socket and reads no environment, so a
//! binary entrypoint and the live integration harness assemble the exact same
//! graph.
//!
//! Account is the platform's only relational service — its repository is backed
//! by a [`TransactionManager`] over a `sqlx` pool rather than ScyllaDB/Redis, so
//! the live suite exercises a real Postgres container (no replication rewrite)
//! instead of the single-node CQL adaptation the other services use.

use std::sync::Arc;

use cqrs::command::{CommandBusBuilder, InMemoryCommandBus};
use cqrs::query::{InMemoryQueryBus, QueryBusBuilder};
use postgres_storage::TransactionManager;
use sqlx::PgPool;

use crate::application::command::{
    AnonymizeAccountCommand, AnonymizeAccountHandler, AssignRoleCommand, AssignRoleHandler,
    CancelGdprDeletionCommand, CancelGdprDeletionHandler, SetDateOfBirthCommand,
    SetDateOfBirthHandler,
    ChangePasswordCommand, ChangePasswordHandler, CreateAccountCommand, CreateAccountHandler,
    DeactivateAccountCommand, DeactivateAccountHandler, EnrollMfaCommand, EnrollMfaHandler,
    ReactivateAccountCommand, ReactivateAccountHandler, RecordFailedLoginCommand,
    RecordFailedLoginHandler, RecordLoginCommand, RecordLoginHandler, RequestDataExportCommand,
    RequestDataExportHandler, RequestGdprDeletionCommand, RequestGdprDeletionHandler,
    ResumeDeactivatedAccountCommand, ResumeDeactivatedAccountHandler,
    RevokeMfaCommand, RevokeMfaHandler, RevokeRoleCommand, RevokeRoleHandler, SuspendAccountCommand,
    SuspendAccountHandler, UpdateConsentsCommand, UpdateConsentsHandler, UpdateKycStatusCommand, UpdateKycStatusHandler, VerifyEmailCommand,
    VerifyEmailHandler, VerifyPhoneCommand, VerifyPhoneHandler,
};
use crate::application::port::{AccountRepository, EventPublisher};
use crate::application::query::{
    GetAccountByIdHandler, GetAccountByIdQuery, GetAccountByIdentityIdHandler,
    GetAccountByIdentityIdQuery, GetAccountStatusHandler, GetAccountStatusQuery,
    GetGdprRecordHandler, GetGdprRecordQuery, ListAccountsByStatusHandler,
    ListAccountsByStatusQuery,
};
use crate::infrastructure::persistence::PgAccountRepository;

/// A fully-wired account service bound to its Postgres pool. The buses exposed
/// here are the *same* instances the handlers are registered into; `repository`
/// is the shared port handle for direct assertions.
pub struct App {
    pub command_bus: Arc<InMemoryCommandBus>,
    pub query_bus:   Arc<InMemoryQueryBus>,
    pub repository:  Arc<dyn AccountRepository>,
}

impl App {
    /// Wraps `pool` in a [`TransactionManager`], builds the Postgres-backed
    /// repository, and registers every account command and query.
    pub async fn build(
        pool: PgPool,
        publisher: Arc<dyn EventPublisher>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let repository: Arc<dyn AccountRepository> = Arc::new(PgAccountRepository::new(
            TransactionManager::new(pool),
            publisher,
        ));

        let command_bus = Arc::new(
            CommandBusBuilder::new()
                .register::<CreateAccountCommand, _>(CreateAccountHandler::new(Arc::clone(&repository)))?
                .register::<VerifyEmailCommand, _>(VerifyEmailHandler::new(Arc::clone(&repository)))?
                .register::<VerifyPhoneCommand, _>(VerifyPhoneHandler::new(Arc::clone(&repository)))?
                .register::<ChangePasswordCommand, _>(ChangePasswordHandler::new(Arc::clone(&repository)))?
                .register::<EnrollMfaCommand, _>(EnrollMfaHandler::new(Arc::clone(&repository)))?
                .register::<RevokeMfaCommand, _>(RevokeMfaHandler::new(Arc::clone(&repository)))?
                .register::<UpdateKycStatusCommand, _>(UpdateKycStatusHandler::new(Arc::clone(&repository)))?
                .register::<SuspendAccountCommand, _>(SuspendAccountHandler::new(Arc::clone(&repository)))?
                .register::<ReactivateAccountCommand, _>(ReactivateAccountHandler::new(Arc::clone(&repository)))?
                .register::<DeactivateAccountCommand, _>(DeactivateAccountHandler::new(Arc::clone(&repository)))?
                .register::<ResumeDeactivatedAccountCommand, _>(ResumeDeactivatedAccountHandler::new(Arc::clone(&repository)))?
                .register::<RecordLoginCommand, _>(RecordLoginHandler::new(Arc::clone(&repository)))?
                .register::<RecordFailedLoginCommand, _>(RecordFailedLoginHandler::new(Arc::clone(&repository)))?
                .register::<RequestGdprDeletionCommand, _>(RequestGdprDeletionHandler::new(Arc::clone(&repository)))?
                .register::<AnonymizeAccountCommand, _>(AnonymizeAccountHandler::new(Arc::clone(&repository)))?
                .register::<RequestDataExportCommand, _>(RequestDataExportHandler::new(Arc::clone(&repository)))?
                .register::<UpdateConsentsCommand, _>(UpdateConsentsHandler::new(Arc::clone(&repository)))?
                .register::<CancelGdprDeletionCommand, _>(CancelGdprDeletionHandler::new(Arc::clone(&repository)))?
                .register::<SetDateOfBirthCommand, _>(SetDateOfBirthHandler::new(Arc::clone(&repository)))?
                .register::<AssignRoleCommand, _>(AssignRoleHandler::new(Arc::clone(&repository)))?
                .register::<RevokeRoleCommand, _>(RevokeRoleHandler::new(Arc::clone(&repository)))?
                .build(),
        );

        let query_bus = Arc::new(
            QueryBusBuilder::new()
                .register::<GetAccountByIdQuery, _>(GetAccountByIdHandler::new(Arc::clone(&repository)))?
                .register::<GetAccountByIdentityIdQuery, _>(GetAccountByIdentityIdHandler::new(Arc::clone(&repository)))?
                .register::<GetAccountStatusQuery, _>(GetAccountStatusHandler::new(Arc::clone(&repository)))?
                .register::<GetGdprRecordQuery, _>(GetGdprRecordHandler::new(Arc::clone(&repository)))?
                .register::<ListAccountsByStatusQuery, _>(ListAccountsByStatusHandler::new(Arc::clone(&repository)))?
                .build(),
        );

        Ok(Self { command_bus, query_bus, repository })
    }
}
