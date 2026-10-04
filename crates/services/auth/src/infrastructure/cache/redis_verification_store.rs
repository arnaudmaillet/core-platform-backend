//! One-time codes in Redis.
//!
//! - `auth:{otp:<challenge_id>}` — a HASH (`channel`, `destination`, `code_hash`,
//!   `attempts_left`) with the code's TTL. Checking a code is one script:
//!   match → delete and return the address (single use); miss → spend an
//!   attempt, delete when none are left.
//! - `auth:{otpd:<destination hash>}:hour` / `:last` — the per-address send
//!   budget (a counter with a one-hour window, and a resend cooldown marker).
//!   The address only appears hashed in key names.

use async_trait::async_trait;
use chrono::Duration;
use fred::interfaces::LuaInterface;
use redis_storage::{RedisClient, RedisStorageError};

use crate::application::port::{
    PendingChallenge, SendAdmission, VerificationChannel, VerificationStore, VerifiedDestination,
};
use crate::error::AuthError;

fn challenge_key(challenge_id: &str) -> String {
    format!("auth:{{otp:{challenge_id}}}")
}

fn budget_keys(destination_key: &str) -> Vec<String> {
    vec![format!("auth:{{otpd:{destination_key}}}:hour"), format!("auth:{{otpd:{destination_key}}}:last")]
}

fn cache_err(e: fred::error::Error) -> AuthError {
    AuthError::Cache(RedisStorageError::from(e))
}

/// KEYS = hour counter, last-send marker · ARGV = per_hour, resend secs.
/// Returns 0 when admitted, else the seconds to wait.
const ADMIT_SEND: &str = r#"
local last_ttl = redis.call('TTL', KEYS[2])
if last_ttl > 0 then return last_ttl end
local n = redis.call('INCR', KEYS[1])
if n == 1 then redis.call('EXPIRE', KEYS[1], 3600) end
if n > tonumber(ARGV[1]) then
    local ttl = redis.call('TTL', KEYS[1])
    if ttl < 1 then ttl = 3600 end
    return ttl
end
redis.call('SET', KEYS[2], '1', 'EX', tonumber(ARGV[2]))
return 0
"#;

/// KEYS = challenge · ARGV = code hash. Returns {channel, destination} on a
/// match (and deletes), {} otherwise.
const CONSUME: &str = r#"
local stored = redis.call('HMGET', KEYS[1], 'code_hash', 'channel', 'destination', 'attempts_left')
if not stored[1] then return {} end
if stored[1] == ARGV[1] then
    redis.call('DEL', KEYS[1])
    return {stored[2], stored[3]}
end
local left = redis.call('HINCRBY', KEYS[1], 'attempts_left', -1)
if left <= 0 then redis.call('DEL', KEYS[1]) end
return {}
"#;

/// KEYS = challenge · ARGV = channel, destination, code hash, attempts, ttl secs.
const SAVE: &str = r#"
redis.call('HSET', KEYS[1], 'channel', ARGV[1], 'destination', ARGV[2], 'code_hash', ARGV[3], 'attempts_left', ARGV[4])
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[5]))
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
    async fn admit_send(
        &self,
        destination_key: &str,
        per_hour: u32,
        resend: Duration,
    ) -> Result<SendAdmission, AuthError> {
        let wait: i64 = self
            .client
            .eval(
                ADMIT_SEND,
                budget_keys(destination_key),
                vec![per_hour.max(1).to_string(), resend.num_seconds().max(1).to_string()],
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
                    challenge.code_hash.clone(),
                    max_attempts.max(1).to_string(),
                    ttl.num_seconds().max(1).to_string(),
                ],
            )
            .await
            .map_err(cache_err)?;
        Ok(())
    }

    async fn consume(&self, challenge_id: &str, code_hash: &str) -> Result<Option<VerifiedDestination>, AuthError> {
        let found: Vec<String> = self
            .client
            .eval(CONSUME, vec![challenge_key(challenge_id)], vec![code_hash.to_owned()])
            .await
            .map_err(cache_err)?;
        Ok(match found.as_slice() {
            [channel, destination] => channel_from(channel)
                .map(|channel| VerifiedDestination { channel, destination: destination.clone() }),
            _ => None,
        })
    }

    async fn discard(&self, challenge_id: &str) -> Result<(), AuthError> {
        use fred::interfaces::KeysInterface;
        let _: i64 = self.client.del(challenge_key(challenge_id)).await.map_err(cache_err)?;
        Ok(())
    }
}
