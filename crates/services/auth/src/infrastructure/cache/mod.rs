//! Redis Cluster adapter for the hot-path [`SessionCache`](crate::application::port::SessionCache).
//!
//! Keys are hash-tagged so each is slot-pinned and every operation is a
//! single-key, single-round-trip command (no `CROSSSLOT`):
//! * `auth:{acct:<id>}:gen`     — the account's revocation generation (`INCR`/`GET`).
//! * `auth:{sess:<id>}:revoked` — per-session blacklist marker with TTL.
//! * `auth:{otp:<challenge>}` / `auth:{otpd:<hash>}:*` — one-time codes and their
//!   per-address send budget (see [`redis_verification_store`]).

pub mod keys;
pub mod redis_session_cache;
pub mod redis_verification_store;

pub use redis_session_cache::RedisSessionCache;
pub use redis_verification_store::RedisVerificationStore;
