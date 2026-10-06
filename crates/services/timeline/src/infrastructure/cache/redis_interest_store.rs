//! A profile's interest tags (#662) in Redis, three keys on the profile's slot:
//!
//! - `timeline:int:{<profile>}` — a ZSET tag → inflated weight (see
//!   [`crate::domain::value_object::interest`]), capped to the heaviest tags;
//! - `timeline:int:{<profile>}:seen` — a ZSET post → reaction time, so a post
//!   counts once per dedup window;
//! - `timeline:int:{<profile>}:muted` — a SET of the tags the profile removed;
//! - `timeline:int:{<profile>}:off` — set while the holder opted out of
//!   personalisation (no TTL: it lasts until they opt back in or are erased).
//!
//! Every key but `:off` expires after the TTL without a write: an idle
//! profile's interests fade out on their own.

use async_trait::async_trait;
use fred::interfaces::LuaInterface;
use redis_storage::RedisClient;

use crate::application::port::InterestStore;
use crate::domain::value_object::interest::{
    decayed, inflated_increment, Interest, INTEREST_TTL_SECS, MAX_INTERESTS, MIN_WEIGHT, REACTION_DEDUP_MS,
};
use crate::domain::value_object::{PostId, ProfileId};
use crate::error::TimelineError;

/// Most posts remembered per profile for the dedup.
const MAX_SEEN: usize = 5_000;

fn keys(profile: &ProfileId) -> Vec<String> {
    let base = format!("timeline:int:{{{profile}}}");
    vec![base.clone(), format!("{base}:seen"), format!("{base}:muted"), format!("{base}:off")]
}

fn fred_err(e: fred::error::Error) -> TimelineError {
    TimelineError::Redis(redis_storage::RedisStorageError::from(e))
}

/// Counts a reaction once: returns 0 when the post was already counted or
/// the holder opted out.
///
/// KEYS = weights, seen, muted, off
/// ARGV = post, at_ms, seen cutoff_ms, increment, cap, max seen, ttl_secs, tags…
const REINFORCE: &str = r#"
if redis.call('EXISTS', KEYS[4]) == 1 then return 0 end
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', '(' .. ARGV[3])
if redis.call('ZSCORE', KEYS[2], ARGV[1]) then return 0 end
redis.call('ZADD', KEYS[2], ARGV[2], ARGV[1])
redis.call('ZREMRANGEBYRANK', KEYS[2], 0, -(tonumber(ARGV[6]) + 1))
redis.call('EXPIRE', KEYS[2], ARGV[7])
local added = 0
for i = 8, #ARGV do
    if redis.call('SISMEMBER', KEYS[3], ARGV[i]) == 0 then
        redis.call('ZINCRBY', KEYS[1], ARGV[4], ARGV[i])
        added = added + 1
    end
end
if added > 0 then
    redis.call('ZREMRANGEBYRANK', KEYS[1], 0, -(tonumber(ARGV[5]) + 1))
    redis.call('EXPIRE', KEYS[1], ARGV[7])
end
return 1
"#;

/// KEYS = weights, seen, muted, off · ARGV = tag, ttl_secs
const REMOVE: &str = r#"
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('SADD', KEYS[3], ARGV[1])
redis.call('EXPIRE', KEYS[3], ARGV[2])
return 1
"#;

/// KEYS = weights, seen, muted, off
const RESET: &str = r#"
return redis.call('DEL', KEYS[1], KEYS[2], KEYS[3])
"#;

/// Opts out (ARGV[1] = '0': erases and marks) or back in ('1': unmarks).
///
/// KEYS = weights, seen, muted, off · ARGV = on
const PERSONALIZED: &str = r#"
if ARGV[1] == '1' then return redis.call('DEL', KEYS[4]) end
redis.call('DEL', KEYS[1], KEYS[2], KEYS[3])
redis.call('SET', KEYS[4], '1')
return 1
"#;

/// KEYS = weights, seen, muted, off
const ERASE: &str = r#"
return redis.call('DEL', KEYS[1], KEYS[2], KEYS[3], KEYS[4])
"#;

/// KEYS = weights · ARGV = count
const TOP: &str = r#"
return redis.call('ZREVRANGE', KEYS[1], 0, tonumber(ARGV[1]) - 1, 'WITHSCORES')
"#;

pub struct RedisInterestStore {
    client: RedisClient,
}

impl RedisInterestStore {
    pub fn new(client: RedisClient) -> Self {
        Self { client }
    }
}

#[async_trait]
impl InterestStore for RedisInterestStore {
    async fn reinforce(
        &self,
        profile: &ProfileId,
        post:    &PostId,
        tags:    &[String],
        at_ms:   i64,
    ) -> Result<(), TimelineError> {
        if tags.is_empty() {
            return Ok(());
        }
        let mut args = vec![
            post.to_string(),
            at_ms.to_string(),
            (at_ms - REACTION_DEDUP_MS).to_string(),
            inflated_increment(at_ms).to_string(),
            MAX_INTERESTS.to_string(),
            MAX_SEEN.to_string(),
            INTEREST_TTL_SECS.to_string(),
        ];
        args.extend(tags.iter().cloned());
        let _: i64 = self.client.inner.eval(REINFORCE, keys(profile), args).await.map_err(fred_err)?;
        Ok(())
    }

    async fn top(&self, profile: &ProfileId, now_ms: i64, limit: usize) -> Result<Vec<Interest>, TimelineError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let raw: Vec<String> = self
            .client
            .inner
            .eval(TOP, vec![keys(profile).swap_remove(0)], vec![limit.to_string()])
            .await
            .map_err(fred_err)?;
        if !raw.len().is_multiple_of(2) {
            return Err(TimelineError::ScriptReturnInvalid { context: "interest top" });
        }
        Ok(raw
            .chunks_exact(2)
            .filter_map(|pair| {
                let weight = decayed(pair[1].parse::<f64>().ok()?, now_ms);
                (weight >= MIN_WEIGHT).then(|| Interest { tag: pair[0].clone(), weight })
            })
            .collect())
    }

    async fn remove(&self, profile: &ProfileId, tag: &str) -> Result<(), TimelineError> {
        let _: i64 = self
            .client
            .inner
            .eval(REMOVE, keys(profile), vec![tag.to_owned(), INTEREST_TTL_SECS.to_string()])
            .await
            .map_err(fred_err)?;
        Ok(())
    }

    async fn reset(&self, profile: &ProfileId) -> Result<(), TimelineError> {
        let _: i64 = self.client.inner.eval(RESET, keys(profile), Vec::<String>::new()).await.map_err(fred_err)?;
        Ok(())
    }

    async fn set_personalized(&self, profile: &ProfileId, on: bool) -> Result<(), TimelineError> {
        let on = if on { "1" } else { "0" };
        let _: i64 = self.client.inner.eval(PERSONALIZED, keys(profile), vec![on.to_owned()]).await.map_err(fred_err)?;
        Ok(())
    }

    async fn erase(&self, profile: &ProfileId) -> Result<(), TimelineError> {
        let _: i64 = self.client.inner.eval(ERASE, keys(profile), Vec::<String>::new()).await.map_err(fred_err)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_three_keys_share_the_profiles_slot() {
        let profile = ProfileId::from_uuid(uuid::Uuid::nil());
        let tag = format!("{{{profile}}}");
        for key in keys(&profile) {
            assert!(key.contains(&tag), "{key}");
        }
    }
}
