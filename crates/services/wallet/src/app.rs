//! The composition root: the ledger store, the use cases and the gRPC handler.

use std::sync::Arc;

use postgres_storage::TransactionManager;
use sqlx::PgPool;

use crate::application::port::{AudienceCheck, EventPublisher, TargetDirectory};
use crate::application::Wallets;
use crate::config::WalletConfig;
use crate::infrastructure::grpc::WalletServiceHandler;
use crate::infrastructure::persistence::PgWalletStore;

/// What likes need (#665): their targets, who may see them, and where the
/// outbox is published.
pub struct StakeDeps {
    pub targets:   Arc<dyn TargetDirectory>,
    pub audience:  Arc<dyn AudienceCheck>,
    pub publisher: Arc<dyn EventPublisher>,
}

pub struct App {
    pub wallets: Arc<Wallets>,
    pub handler: WalletServiceHandler,
}

impl App {
    /// `stakes`: what likes need (#665); without them, a stake answers
    /// `WAL-6001`.
    pub fn build(pool: PgPool, config: WalletConfig, stakes: Option<StakeDeps>) -> Self {
        let store = Arc::new(PgWalletStore::new(TransactionManager::new(pool)));
        let wallets = Wallets::new(store, config);
        let wallets = Arc::new(match stakes {
            Some(deps) => wallets.with_stakes(deps.targets, deps.audience, deps.publisher),
            None => wallets,
        });
        let handler = WalletServiceHandler::new(Arc::clone(&wallets));
        Self { wallets, handler }
    }
}
