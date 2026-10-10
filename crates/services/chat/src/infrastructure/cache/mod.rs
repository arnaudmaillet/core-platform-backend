pub mod keys;
pub mod redis_hot_tail_cache;
pub mod redis_presence_store;
pub mod redis_receipt_store;
pub mod redis_routing_registry;
pub mod redis_send_keys;
pub mod script;

pub use redis_hot_tail_cache::RedisHotTailCache;
pub use redis_presence_store::RedisPresenceStore;
pub use redis_receipt_store::RedisReceiptStore;
pub use redis_routing_registry::RedisRoutingRegistry;
pub use redis_send_keys::RedisSendKeys;

/// Maps a fred error into the service error contract, preserving the underlying
/// Redis storage error code and retryability.
pub(crate) fn redis_err(e: fred::error::Error) -> crate::error::ChatError {
    crate::error::ChatError::Redis(redis_storage::RedisStorageError::from(e))
}
