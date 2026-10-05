//! Two-step sign-in state in Redis (#649):
//!
//! - `auth:{mfal:<challenge hash>}` — a sign-in waiting for its second factor
//!   (JSON), with the challenge's TTL; completing it is a `GETDEL`, so exactly
//!   one caller gets it.
//! - `auth:{mfa:<account>}:step:<n>` — TOTP step `n` was used (`SET NX` with a
//!   TTL past the code's acceptance window): a replayed code is refused.
//! - `auth:{mfa:<account>}:fail` — code attempts in the current window,
//!   reserved before each code is checked (one script: no parallel guess
//!   slips past the limit).

use async_trait::async_trait;
use fred::interfaces::{KeysInterface, LuaInterface};
use fred::types::{Expiration, SetOptions};
use redis_storage::{RedisClient, RedisStorageError};

use crate::application::port::{MfaStore, PendingLogin};
use crate::domain::value_object::AccountId;
use crate::error::AuthError;

fn pending_key(token_hash: &str) -> String {
    format!("auth:{{mfal:{token_hash}}}")
}

fn step_key(account: &AccountId, step: i64) -> String {
    format!("auth:{{mfa:{}}}:step:{step}", account.as_str())
}

fn failure_key(account: &AccountId) -> String {
    format!("auth:{{mfa:{}}}:fail", account.as_str())
}

fn cache_err(e: fred::error::Error) -> AuthError {
    AuthError::Cache(RedisStorageError::from(e))
}

/// KEYS = attempt counter · ARGV = window secs. Counts this attempt (the
/// window starts at the first) and returns {count, ttl}.
const RESERVE_ATTEMPT: &str = r#"
local n = redis.call('INCR', KEYS[1])
if n == 1 then redis.call('EXPIRE', KEYS[1], tonumber(ARGV[1])) end
return {n, redis.call('TTL', KEYS[1])}
"#;

#[derive(Clone)]
pub struct RedisMfaStore {
    client: RedisClient,
}

impl RedisMfaStore {
    pub fn new(client: RedisClient) -> Self {
        Self { client }
    }
}

fn decode(raw: Option<String>) -> Result<Option<PendingLogin>, AuthError> {
    raw.map(|json| {
        serde_json::from_str(&json).map_err(|e| AuthError::DomainViolation {
            field: "pending_login".into(),
            message: format!("unreadable pending sign-in: {e}"),
        })
    })
    .transpose()
}

#[async_trait]
impl MfaStore for RedisMfaStore {
    async fn save_pending_login(&self, token_hash: &str, login: &PendingLogin, ttl_secs: u64) -> Result<(), AuthError> {
        let json = serde_json::to_string(login).map_err(|e| AuthError::DomainViolation {
            field: "pending_login".into(),
            message: e.to_string(),
        })?;
        let _: Option<String> = self
            .client
            .set(pending_key(token_hash), json, Some(Expiration::EX(ttl_secs.max(1) as i64)), None, false)
            .await
            .map_err(cache_err)?;
        Ok(())
    }

    async fn pending_login(&self, token_hash: &str) -> Result<Option<PendingLogin>, AuthError> {
        decode(self.client.get(pending_key(token_hash)).await.map_err(cache_err)?)
    }

    async fn take_pending_login(&self, token_hash: &str) -> Result<Option<PendingLogin>, AuthError> {
        decode(self.client.getdel(pending_key(token_hash)).await.map_err(cache_err)?)
    }

    async fn claim_step(&self, account: &AccountId, step: i64, ttl_secs: u64) -> Result<bool, AuthError> {
        let set: Option<String> = self
            .client
            .set(step_key(account, step), "1", Some(Expiration::EX(ttl_secs.max(1) as i64)), Some(SetOptions::NX), false)
            .await
            .map_err(cache_err)?;
        Ok(set.is_some())
    }

    async fn reserve_attempt(&self, account: &AccountId, window_secs: u64) -> Result<(u32, i64), AuthError> {
        let found: Vec<i64> = self
            .client
            .eval(RESERVE_ATTEMPT, vec![failure_key(account)], vec![window_secs.max(1).to_string()])
            .await
            .map_err(cache_err)?;
        match found.as_slice() {
            [n, ttl] => Ok(((*n).max(0) as u32, *ttl)),
            _ => Err(AuthError::Cache(RedisStorageError::from(fred::error::Error::new(
                fred::error::ErrorKind::Unknown,
                "unexpected attempt-counter reply",
            )))),
        }
    }

    async fn clear_failures(&self, account: &AccountId) -> Result<(), AuthError> {
        let _: i64 = self.client.del(failure_key(account)).await.map_err(cache_err)?;
        Ok(())
    }
}
