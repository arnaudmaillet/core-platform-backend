//! The composition root: the ledger store, the use cases and the gRPC handler.

use std::sync::Arc;

use postgres_storage::TransactionManager;
use sqlx::PgPool;

use crate::application::port::{EventPublisher, TargetDirectory};
use crate::application::Wallets;
use crate::config::WalletConfig;
use crate::infrastructure::grpc::WalletServiceHandler;
use crate::infrastructure::persistence::PgWalletStore;

pub struct App {
    pub wallets: Arc<Wallets>,
    pub handler: WalletServiceHandler,
}

impl App {
    /// `stakes`: what likes land on and where they are announced (#665);
    /// without them, a stake answers `WAL-6001`.
    pub fn build(
        pool: PgPool,
        config: WalletConfig,
        stakes: Option<(Arc<dyn TargetDirectory>, Arc<dyn EventPublisher>)>,
    ) -> Self {
        let store = Arc::new(PgWalletStore::new(TransactionManager::new(pool)));
        let wallets = Wallets::new(store, config);
        let wallets = Arc::new(match stakes {
            Some((targets, publisher)) => wallets.with_stakes(targets, publisher),
            None => wallets,
        });
        let handler = WalletServiceHandler::new(Arc::clone(&wallets));
        Self { wallets, handler }
    }
}
