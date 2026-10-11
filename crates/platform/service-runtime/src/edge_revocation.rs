//! The edge-token revocation check over `auth`'s Redis (#884).
//!
//! `auth` keeps an account's revocation generation and each revoked session's
//! blacklist marker in its Redis (`auth_context::edge::revocation` names the
//! keys). With [`EDGE_REVOCATION_HOSTS_ENV`] set, the runtime reads them for
//! every verified token on the client edge and in the
//! [`StaffGate`](transport::grpc::edge::StaffGate): two reads, cached in process
//! for [`EDGE_REVOCATION_CACHE_MS_ENV`] (default 5 s), so a revocation reaches
//! every pod within that window instead of at `exp`.
//!
//! The Redis is configured like the service's own, under the
//! `EDGE_REVOCATION_` prefix (`EDGE_REVOCATION_REDIS_HOSTS`, `…_TOPOLOGY`,
//! `…_TLS`, `…_PASSWORD`, see `redis_storage::RedisConfig::from_env_prefixed`).
//! Unset ⇒ no check (a warning at boot): tokens die at `exp`, as before.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use auth_context::edge::revocation::{blacklist_key, generation_key, INITIAL_GENERATION};
use fred::interfaces::KeysInterface;
use futures::future::BoxFuture;
use redis_storage::{RedisClient, RedisClientBuilder, RedisConfig};
use transport::grpc::revocation::{RevocationCheck, RevocationLookup, RevocationUnavailable};

/// The env-var prefix of the revocation Redis (`EDGE_REVOCATION_REDIS_HOSTS`, …).
pub const EDGE_REVOCATION_PREFIX: &str = "EDGE_REVOCATION_";
/// Presence switches the check on.
pub const EDGE_REVOCATION_HOSTS_ENV: &str = "EDGE_REVOCATION_REDIS_HOSTS";
/// How long a read revocation state is reused (milliseconds; default 5 000).
pub const EDGE_REVOCATION_CACHE_MS_ENV: &str = "EDGE_REVOCATION_CACHE_MS";

const DEFAULT_CACHE: Duration = Duration::from_secs(5);
/// Entries kept at most (sessions seen within the cache window).
const CACHE_CAP: usize = 100_000;

/// `auth`'s revocation state for a token: the account's current generation and
/// whether the session is blacklisted.
pub type State = (i64, bool);

/// Reads the state (one implementation per store; the cache sits on top).
pub trait RevocationState: Send + Sync + 'static {
    fn read<'a>(&'a self, owner: &'a str, session: &'a str) -> BoxFuture<'a, Result<State, RevocationUnavailable>>;
}

/// The state in `auth`'s Redis. The two keys hash to different slots, so they are
/// two reads (concurrent), not one `MGET`.
pub struct RedisRevocationState {
    client: RedisClient,
}

impl RedisRevocationState {
    pub fn new(client: RedisClient) -> Self {
        Self { client }
    }
}

impl RevocationState for RedisRevocationState {
    fn read<'a>(&'a self, owner: &'a str, session: &'a str) -> BoxFuture<'a, Result<State, RevocationUnavailable>> {
        Box::pin(async move {
            let generation = self.client.get::<Option<i64>, _>(generation_key(owner));
            let blacklisted = self.client.exists::<i64, _>(blacklist_key(session));
            let (generation, blacklisted) = tokio::try_join!(generation, blacklisted).map_err(|error| {
                tracing::warn!(%error, "edge revocation state unreadable");
                RevocationUnavailable
            })?;
            Ok((generation.unwrap_or(INITIAL_GENERATION), blacklisted > 0))
        })
    }
}

/// A [`RevocationCheck`] over a [`RevocationState`], each read reused for `ttl`.
/// A failed read is never cached.
pub struct CachedRevocationCheck<S> {
    state:   S,
    ttl:     Duration,
    entries: Mutex<HashMap<String, (State, Instant)>>,
}

impl<S: RevocationState> CachedRevocationCheck<S> {
    pub fn new(state: S, ttl: Duration) -> Self {
        Self { state, ttl, entries: Mutex::new(HashMap::new()) }
    }

    /// A poisoned lock still holds a consistent map; it is used as is.
    fn entries(&self) -> MutexGuard<'_, HashMap<String, (State, Instant)>> {
        self.entries.lock().unwrap_or_else(PoisonError::into_inner)
    }

    async fn state(&self, owner: &str, session: &str) -> Result<State, RevocationUnavailable> {
        let key = format!("{owner}/{session}");
        if let Some((state, at)) = self.entries().get(&key)
            && at.elapsed() < self.ttl
        {
            return Ok(*state);
        }
        let state = self.state.read(owner, session).await?;
        let mut entries = self.entries();
        if entries.len() >= CACHE_CAP {
            let ttl = self.ttl;
            entries.retain(|_, (_, at)| at.elapsed() < ttl);
            if entries.len() >= CACHE_CAP {
                entries.clear();
            }
        }
        entries.insert(key, (state, Instant::now()));
        Ok(state)
    }
}

impl<S: RevocationState> RevocationCheck for CachedRevocationCheck<S> {
    fn is_revoked<'a>(&'a self, owner: &'a str, session: &'a str, generation: i64) -> RevocationLookup<'a> {
        Box::pin(async move {
            let (current, blacklisted) = self.state(owner, session).await?;
            Ok(generation < current || blacklisted)
        })
    }
}

/// The check from the environment: `Ok(None)` when [`EDGE_REVOCATION_HOSTS_ENV`]
/// is unset, `Err` when the Redis cannot be built.
pub async fn from_env() -> anyhow::Result<Option<Arc<dyn RevocationCheck>>> {
    if std::env::var(EDGE_REVOCATION_HOSTS_ENV).ok().filter(|v| !v.trim().is_empty()).is_none() {
        return Ok(None);
    }
    let ttl = std::env::var(EDGE_REVOCATION_CACHE_MS_ENV)
        .ok()
        .and_then(|v| v.parse().ok())
        .map(Duration::from_millis)
        .unwrap_or(DEFAULT_CACHE);
    let client = RedisClientBuilder::new(RedisConfig::from_env_prefixed(EDGE_REVOCATION_PREFIX))
        .build()
        .await
        .map_err(|e| anyhow::anyhow!("edge revocation Redis: {e}"))?;
    tracing::info!(cache_ms = ttl.as_millis() as u64, "edge revocation check on (#884)");
    Ok(Some(Arc::new(CachedRevocationCheck::new(RedisRevocationState::new(client), ttl))))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    /// A state that counts its reads; `None` = unreadable.
    struct Counted {
        state: Mutex<Option<State>>,
        reads: AtomicUsize,
    }

    impl RevocationState for Arc<Counted> {
        fn read<'a>(&'a self, _: &'a str, _: &'a str) -> BoxFuture<'a, Result<State, RevocationUnavailable>> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            let state = *self.state.lock().unwrap();
            Box::pin(async move { state.ok_or(RevocationUnavailable) })
        }
    }

    fn counted(state: Option<State>) -> Arc<Counted> {
        Arc::new(Counted { state: Mutex::new(state), reads: AtomicUsize::new(0) })
    }

    #[tokio::test]
    async fn a_token_below_the_generation_or_of_a_blacklisted_session_is_revoked() {
        let check = CachedRevocationCheck::new(counted(Some((2, false))), Duration::ZERO);
        assert!(!check.is_revoked("a", "s", 2).await.unwrap());
        assert!(check.is_revoked("a", "s", 1).await.unwrap());
        let blacklisted = CachedRevocationCheck::new(counted(Some((0, true))), Duration::ZERO);
        assert!(blacklisted.is_revoked("a", "s", 0).await.unwrap());
    }

    /// The state is read once per window, and a revocation is seen once the
    /// window has passed; a failed read is not cached.
    #[tokio::test]
    async fn reads_are_reused_within_the_window_and_failures_are_not_cached() {
        let state = counted(Some((1, false)));
        let check = CachedRevocationCheck::new(Arc::clone(&state), Duration::from_millis(50));
        for _ in 0..3 {
            assert!(!check.is_revoked("a", "s", 1).await.unwrap());
        }
        assert_eq!(state.reads.load(Ordering::SeqCst), 1);

        *state.state.lock().unwrap() = Some((2, false)); // auth bumped the generation
        assert!(!check.is_revoked("a", "s", 1).await.unwrap(), "still the cached read");
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert!(check.is_revoked("a", "s", 1).await.unwrap(), "seen after the window");

        let down = counted(None);
        let check = CachedRevocationCheck::new(Arc::clone(&down), Duration::from_secs(60));
        assert_eq!(check.is_revoked("a", "s", 1).await, Err(RevocationUnavailable));
        assert_eq!(check.is_revoked("a", "s", 1).await, Err(RevocationUnavailable));
        assert_eq!(down.reads.load(Ordering::SeqCst), 2);
    }
}
