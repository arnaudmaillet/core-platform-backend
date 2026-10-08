//! The composition root: the ledger store, the use cases and the gRPC handler.

use std::sync::Arc;

use postgres_storage::TransactionManager;
use sqlx::PgPool;

use crate::application::Wallets;
use crate::config::WalletConfig;
use crate::infrastructure::grpc::WalletServiceHandler;
use crate::infrastructure::persistence::PgWalletStore;

pub struct App {
    pub wallets: Arc<Wallets>,
    pub handler: WalletServiceHandler,
}

impl App {
    pub fn build(pool: PgPool, config: WalletConfig) -> Self {
        let store = Arc::new(PgWalletStore::new(TransactionManager::new(pool)));
        let wallets = Arc::new(Wallets::new(store, config));
        let handler = WalletServiceHandler::new(Arc::clone(&wallets));
        Self { wallets, handler }
    }
}
