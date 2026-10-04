//! One-time codes in Redis.
//!
//! - `auth:{otp:<challenge_id>}` — a HASH (`channel`, `destination`, `code_hash`,
//!   `attempts_left`) with the code's TTL. Checking a code is one script:
//!   match → delete and return the address (single use); miss → spend an
//!   attempt, delete when none are left.
//! - `auth:{otpd:<destination hash>}:hour` / `:day` / `:last` / `:fail` — the
//!   per-address send budget (hourly and daily counters, a resend cooldown
//!   marker) and its wrong-code count across challenges. The address only
//!   appears hashed in key names; all four share one slot.

use async_trait::async_trait;
use chrono::Duration;
use fred::interfaces::LuaInterface;
use redis_storage::{RedisClient, RedisStorageError};

use crate::application::port::{
    ConsumeOutcome, PendingChallenge, SendAdmission, SendLimits, VerificationChannel, VerificationStore,
    VerifiedDestination,
};
use crate::error::AuthError;

fn challenge_key(challenge_id: &str) -> String {
    format!("auth:{{otp:{challenge_id}}}")
}

fn budget_keys(destination_key: &str) -> Vec<String> {
    vec![
        format!("auth:{{otpd:{destination_key}}}:hour"),
        format!("auth:{{otpd:{destination_key}}}:day"),
        format!("auth:{{otpd:{destination_key}}}:last"),
    ]
}

fn failure_key(destination_key: &str) -> String {
    format!("auth:{{otpd:{destination_key}}}:fail")
}

fn cache_err(e: fred::error::Error) -> AuthError {
    AuthError::Cache(RedisStorageError::from(e))
}

/// KEYS = hour counter, day counter, last-send marker · ARGV = per_hour,
/// per_day, resend secs. Returns 0 when admitted, else the seconds to wait.
const ADMIT_SEND: &str = r#"
local last_ttl = redis.call('TTL', KEYS[3])
if last_ttl > 0 then return last_ttl end
local function over(key, limit, window)
    local n = redis.call('INCR', key)
    if n == 1 then redis.call('EXPIRE', key, window) end
    if n > limit then
        local ttl = redis.call('TTL', key)
        if ttl < 1 then ttl = window end
        return ttl
    end
    return 0
end
local wait = over(KEYS[2], tonumber(ARGV[2]), 86400)
if wait > 0 then return wait end
wait = over(KEYS[1], tonumber(ARGV[1]), 3600)
if wait > 0 then return wait end
redis.call('SET', KEYS[3], '1', 'EX', tonumber(ARGV[3]))
return 0
"#;

/// KEYS = failure counter · ARGV = window secs. Returns the count.
const RECORD_FAILURE: &str = r#"
local n = redis.call('INCR', KEYS[1])
if n == 1 then redis.call('EXPIRE', KEYS[1], tonumber(ARGV[1])) end
return n
"#;

/// KEYS = failure counter. Returns {count, ttl}.
const FAILURES: &str = r#"
local n = tonumber(redis.call('GET', KEYS[1]) or '0')
return {n, redis.call('TTL', KEYS[1])}
"#;

/// KEYS = challenge · ARGV = code hash. Returns {'ok', channel, destination,
/// destination key} on a match (and deletes), {'miss', destination key} on a
/// wrong code, {} for no such challenge.
const CONSUME: &str = r#"
local stored = redis.call('HMGET', KEYS[1], 'code_hash', 'channel', 'destination', 'destination_key')
if not stored[1] then return {} end
if stored[1] == ARGV[1] then
    redis.call('DEL', KEYS[1])
    return {'ok', stored[2], stored[3], stored[4]}
end
local left = redis.call('HINCRBY', KEYS[1], 'attempts_left', -1)
if left <= 0 then redis.call('DEL', KEYS[1]) end
return {'miss', stored[4]}
"#;

/// KEYS = challenge · ARGV = channel, destination, destination key, code hash,
/// attempts, ttl secs.
const SAVE: &str = r#"
redis.call('HSET', KEYS[1], 'channel', ARGV[1], 'destination', ARGV[2], 'destination_key', ARGV[3], 'code_hash', ARGV[4], 'attempts_left', ARGV[5])
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[6]))
return 1
"#;

#[derive(Clone)]
pub struct RedisVerificationStore {
    client: RedisClient,
}

impl RedisVerificationStore {
    pub fn new(client: RedisClient) -> Self {
        Self { client }
    }
}

fn channel_from(s: &str) -> Option<VerificationChannel> {
    match s {
        "email" => Some(VerificationChannel::Email),
        "sms" => Some(VerificationChannel::Sms),
        _ => None,
    }
}

#[async_trait]
impl VerificationStore for RedisVerificationStore {
    async fn admit_send(&self, destination_key: &str, limits: SendLimits) -> Result<SendAdmission, AuthError> {
        let wait: i64 = self
            .client
            .eval(
                ADMIT_SEND,
                budget_keys(destination_key),
                vec![
                    limits.per_hour.max(1).to_string(),
                    limits.per_day.max(1).to_string(),
                    limits.resend.num_seconds().max(1).to_string(),
                ],
            )
            .await
            .map_err(cache_err)?;
        Ok(if wait <= 0 { SendAdmission::Allowed } else { SendAdmission::Refused { retry_after_secs: wait } })
    }

    async fn save(&self, challenge: &PendingChallenge, ttl: Duration, max_attempts: u32) -> Result<(), AuthError> {
        let _: i64 = self
            .client
            .eval(
                SAVE,
                vec![challenge_key(&challenge.challenge_id)],
                vec![
                    challenge.channel.as_str().to_owned(),
                    challenge.destination.clone(),
                    challenge.destination_key.clone(),
                    challenge.code_hash.clone(),
                    max_attempts.max(1).to_string(),
                    ttl.num_seconds().max(1).to_string(),
                ],
            )
            .await
            .map_err(cache_err)?;
        Ok(())
    }

    async fn consume(&self, challenge_id: &str, code_hash: &str) -> Result<ConsumeOutcome, AuthError> {
        let found: Vec<String> = self
            .client
            .eval(CONSUME, vec![challenge_key(challenge_id)], vec![code_hash.to_owned()])
            .await
            .map_err(cache_err)?;
        Ok(match found.as_slice() {
            [tag, channel, destination, key] if tag == "ok" => match channel_from(channel) {
                Some(channel) => ConsumeOutcome::Verified {
                    destination: VerifiedDestination { channel, destination: destination.clone() },
                    destination_key: key.clone(),
                },
                None => ConsumeOutcome::Unknown,
            },
            [tag, key] if tag == "miss" => ConsumeOutcome::Miss { destination_key: key.clone() },
            _ => ConsumeOutcome::Unknown,
        })
    }

    async fn record_failure(&self, destination_key: &str, window: Duration) -> Result<u32, AuthError> {
        let n: i64 = self
            .client
            .eval(RECORD_FAILURE, vec![failure_key(destination_key)], vec![window.num_seconds().max(1).to_string()])
            .await
            .map_err(cache_err)?;
        Ok(n.max(0) as u32)
    }

    async fn failures(&self, destination_key: &str) -> Result<(u32, i64), AuthError> {
        let found: Vec<i64> = self
            .client
            .eval(FAILURES, vec![failure_key(destination_key)], Vec::<String>::new())
            .await
            .map_err(cache_err)?;
        Ok(match found.as_slice() {
            [n, ttl] => ((*n).max(0) as u32, *ttl),
            _ => (0, 0),
        })
    }

    async fn discard(&self, challenge_id: &str) -> Result<(), AuthError> {
        use fred::interfaces::KeysInterface;
        let _: i64 = self.client.del(challenge_key(challenge_id)).await.map_err(cache_err)?;
        Ok(())
    }
}
