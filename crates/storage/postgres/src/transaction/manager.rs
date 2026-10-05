use crate::error::StorageError;
use crate::routing::{ShardCluster, ShardKey};
use futures::future::BoxFuture;
use sqlx::PgPool;
use std::sync::Arc;

/// Owned, pool-scoped PostgreSQL transaction handle.
///
/// `'static` reflects that `pool.begin()` moves an owned connection out of the
/// pool rather than borrowing one; the transaction owns that connection for its
/// entire lifetime.
pub type PgTransaction = sqlx::Transaction<'static, sqlx::Postgres>;

/// Internal topology discriminant.
///
/// Wrapped in `Arc` so [`TransactionManager::clone`] is always O(1),
/// regardless of how many shards the cluster contains.
#[derive(Debug)]
enum Topology {
    SingleNode { pool: PgPool },
    ApplicationSharded { cluster: ShardCluster },
}

/// Thin, clone-friendly, topology-aware entry point for ACID-compliant
/// PostgreSQL operations.
///
/// [`TransactionManager`] abstracts over two execution topologies:
///
/// - **SingleNode** — a single global [`PgPool`], as used with CockroachDB,
///   Aurora, or traditional single-instance PostgreSQL.
/// - **ApplicationSharded** — a registry of per-shard pools.  Write operations
///   are routed to the correct shard via a deterministic hash of the caller's
///   shard key.
///
/// Service code written against [`run_on_shard`] is **topology-agnostic**: it
/// compiles and behaves correctly under both modes without any conditional
/// branching at the call site.
///
/// # Clone semantics
///
/// The internal topology is wrapped in `Arc`, so every `clone()` is O(1) and
/// all clones share the same underlying pool(s).
///
/// # Cross-shard atomicity
///
/// [`run_on_shard`] provides full ACID guarantees **within a single shard**.
/// Cross-shard atomicity is intentionally out of scope — it must be handled by
/// the outbox pattern or distributed saga orchestration at the service layer.
///
/// [`run_on_shard`]: TransactionManager::run_on_shard
#[derive(Clone)]
pub struct TransactionManager {
    topology: Arc<Topology>,
}

impl std::fmt::Debug for TransactionManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.topology.as_ref() {
            Topology::SingleNode { .. } => f
                .debug_struct("TransactionManager")
                .field("topology", &"SingleNode")
                .finish(),
            Topology::ApplicationSharded { cluster } => f
                .debug_struct("TransactionManager")
                .field("topology", &"ApplicationSharded")
                .field("shard_count", &cluster.shard_count())
                .finish(),
        }
    }
}

impl TransactionManager {
    /// Constructs a manager for the **SingleNode** topology.
    ///
    /// Backward-compatible with the existing API: existing call sites
    /// `TransactionManager::new(pool)` continue to compile unchanged.
    pub fn new(pool: PgPool) -> Self {
        Self {
            topology: Arc::new(Topology::SingleNode { pool }),
        }
    }

    /// Constructs a manager for the **ApplicationSharded** topology.
    pub fn from_cluster(cluster: ShardCluster) -> Self {
        Self {
            topology: Arc::new(Topology::ApplicationSharded { cluster }),
        }
    }

    /// Returns the raw pool for non-transactional reads (e.g. `SELECT`, `COPY`).
    ///
    /// # Panics
    ///
    /// Panics in `ApplicationSharded` mode. Use [`pool_for`] to obtain the
    /// correct pool when the topology is unknown at the call site.
    ///
    /// [`pool_for`]: TransactionManager::pool_for
    pub fn pool(&self) -> &PgPool {
        match self.topology.as_ref() {
            Topology::SingleNode { pool } => pool,
            Topology::ApplicationSharded { .. } => panic!(
                "TransactionManager::pool() called in ApplicationSharded mode; \
                 use pool_for(key) to route by shard key"
            ),
        }
    }

    /// Every pool: the single node's, or each shard's. For maintenance that
    /// must visit all data (retention purges), never for routed reads/writes.
    pub fn all_pools(&self) -> Vec<&PgPool> {
        match self.topology.as_ref() {
            Topology::SingleNode { pool } => vec![pool],
            Topology::ApplicationSharded { cluster } => cluster.pools().map(|(_, pool)| pool).collect(),
        }
    }

    /// Returns the pool that owns `key` — topology-agnostic.
    ///
    /// - **SingleNode**: `key` is accepted but routing is skipped; the single
    ///   pool is returned.
    /// - **ApplicationSharded**: routes deterministically to the shard that
    ///   owns `key`.
    pub fn pool_for<K: ShardKey + ?Sized>(&self, key: &K) -> Result<&PgPool, StorageError> {
        match self.topology.as_ref() {
            Topology::SingleNode { pool } => Ok(pool),
            Topology::ApplicationSharded { cluster } => cluster.resolve(key),
        }
    }

    /// Opens an ACID transaction on the **single pool**.
    ///
    /// **This method is backward-compatible** with the pre-sharding API.
    /// All existing call sites continue to compile and behave correctly in
    /// `SingleNode` mode.
    ///
    /// **Commit path:** if `f` returns `Ok(T)`, the transaction is committed.
    /// **Rollback path:** if `f` returns `Err(E)`, the transaction is rolled
    /// back (sqlx's `Drop` impl provides a safety-net rollback if the explicit
    /// call panics).
    ///
    /// # Errors in ApplicationSharded mode
    ///
    /// Returns `Err(StorageError::ShardRoutingFailed)` immediately — no shard
    /// key was provided so the correct pool cannot be determined. This is the
    /// intentional migration signal: callers must move to [`run_on_shard`].
    ///
    /// [`run_on_shard`]: TransactionManager::run_on_shard
    #[tracing::instrument(name = "db.transaction", skip(self, f))]
    pub async fn run<F, T, E>(&self, f: F) -> Result<T, E>
    where
        F: for<'tx> FnOnce(&'tx mut PgTransaction) -> BoxFuture<'tx, Result<T, E>>,
        E: From<StorageError> + Send,
        T: Send,
    {
        let pool = match self.topology.as_ref() {
            Topology::SingleNode { pool } => pool,
            Topology::ApplicationSharded { .. } => {
                return Err(E::from(StorageError::ShardRoutingFailed {
                    reason: "run() cannot be used with ApplicationSharded topology; \
                             provide a shard key and call run_on_shard() instead"
                        .into(),
                }));
            }
        };

        Self::exec(pool, f).await
    }

    /// Opens an ACID transaction on the pool that owns `key`.
    ///
    /// **This is the preferred entry point for all new service code.**
    ///
    /// The call is **topology-agnostic**:
    /// - **SingleNode**: `key` is accepted but routing is skipped; the single
    ///   pool receives the transaction.  No code changes are needed if the
    ///   deployment later migrates to ApplicationSharded.
    /// - **ApplicationSharded**: the shard that owns `key` is selected
    ///   deterministically; the `shard_id` field is recorded on the active
    ///   tracing span.
    ///
    /// Cross-shard atomicity is **not** provided. If an operation touches data
    /// on multiple shards, coordinate via the outbox pattern or a distributed
    /// saga at the service layer.
    #[tracing::instrument(
        name = "db.transaction.sharded",
        skip(self, key, f),
        fields(shard_id = tracing::field::Empty),
    )]
    pub async fn run_on_shard<K, F, T, E>(&self, key: &K, f: F) -> Result<T, E>
    where
        K: ShardKey + ?Sized,
        F: for<'tx> FnOnce(&'tx mut PgTransaction) -> BoxFuture<'tx, Result<T, E>>,
        E: From<StorageError> + Send,
        T: Send,
    {
        let pool = match self.topology.as_ref() {
            Topology::SingleNode { pool } => pool,
            Topology::ApplicationSharded { cluster } => {
                let shard_id = cluster.shard_id_for(key);
                tracing::Span::current().record("shard_id", shard_id.as_u16());
                cluster.resolve(key).map_err(E::from)?
            }
        };

        Self::exec(pool, f).await
    }

    /// Shared transaction execution kernel — acquires, runs, commits or rolls back.
    async fn exec<F, T, E>(pool: &PgPool, f: F) -> Result<T, E>
    where
        F: for<'tx> FnOnce(&'tx mut PgTransaction) -> BoxFuture<'tx, Result<T, E>>,
        E: From<StorageError> + Send,
        T: Send,
    {
        let mut tx = pool
            .begin()
            .await
            .map_err(|e| E::from(StorageError::from(e)))?;

        match f(&mut tx).await {
            Ok(value) => {
                tx.commit()
                    .await
                    .map_err(|e| E::from(StorageError::from(e)))?;
                Ok(value)
            }
            Err(e) => {
                // Best-effort; `Drop` on `Transaction` issues a rollback if
                // this explicit call fails or is skipped.
                let _ = tx.rollback().await;
                Err(e)
            }
        }
    }
}
