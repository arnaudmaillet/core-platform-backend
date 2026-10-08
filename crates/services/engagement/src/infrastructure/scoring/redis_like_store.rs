//! [`LikeStore`] on Redis (#665). Per target, a hash of each liking account's
//! total and the target's sum, under the target's hash tag
//! (`engagement:{post:<id>}:…`) so one script updates both.
//!
//! The likers hash expires [`LIKERS_TTL_SECS`] after the last like; the count
//! never does. A hash that holds every liker carries `_complete`: it is set
//! when the hash starts with the target's first like (count 0) or when a
//! rehydration from the durable copy finished. In a hash without it — one
//! started again after it expired — a missing account is unknown, not zero.
//!
//! Each liker's value is `total|arrival`: its points and the target's count
//! just before its first like (its position, for the settlement).

use async_trait::async_trait;
use fred::interfaces::{HashesInterface, KeysInterface, LuaInterface};
use fred::types::{Expiration, SetOptions};
use redis_storage::RedisClient;

use crate::application::port::{Applied, LikeStore, Position};
use crate::domain::value_object::LikeTarget;
use crate::error::EngagementError;

/// A target's likers are kept 30 days after its last like.
pub const LIKERS_TTL_SECS: i64 = 30 * 24 * 3600;

/// Shared by the scripts. A liker's value is `total|arrival` (`total` alone
/// for one recorded before arrivals were kept). `known` answers whether
/// `account` is known on the target: its total, arrival and presence; a zero
/// total when the hash is whole or the target never had a like; `false` when
/// unknown.
const KNOWN: &str = r#"
local function parse(held)
  local bar = string.find(held, '|', 1, true)
  if bar then
    return tonumber(string.sub(held, 1, bar - 1)), tonumber(string.sub(held, bar + 1))
  end
  return tonumber(held), nil
end
local function known(count_key, likers_key, account)
  local held = redis.call('HGET', likers_key, account)
  if held then
    local total, arrival = parse(held)
    return total, arrival, true
  end
  if redis.call('HEXISTS', likers_key, '_complete') == 1 then return 0, nil, false end
  if tonumber(redis.call('GET', count_key) or '0') == 0 then return 0, nil, false end
  return false
end
"#;

/// KEYS[1] the target's like count, KEYS[2] its likers; ARGV account, total,
/// ttl. A total no larger than the one held changes nothing: redeliveries and
/// late events are absorbed. The account's first like keeps the count just
/// before it as its arrival. Returns {likes added, arrival or -1}, or
/// {-1, -1} when the account is unknown (rehydrate first).
const APPLY_TOTAL_SCRIPT: &str = r#"
local old, arrival, present = known(KEYS[1], KEYS[2], ARGV[1])
if old == false then return {-1, -1} end
local count = tonumber(redis.call('GET', KEYS[1]) or '0')
if redis.call('EXISTS', KEYS[2]) == 0 and count == 0 then
  redis.call('HSET', KEYS[2], '_complete', '1')
end
if not present then arrival = count end
local new = tonumber(ARGV[2])
local added = 0
if new > old then
  local value = ARGV[2]
  if arrival then value = value .. '|' .. arrival end
  redis.call('HSET', KEYS[2], ARGV[1], value)
  redis.call('INCRBY', KEYS[1], new - old)
  added = new - old
end
redis.call('EXPIRE', KEYS[2], ARGV[3])
return {added, arrival or -1}
"#;

/// KEYS as above; ARGV account. {total, arrival or -1}, or {-1, -1} when
/// unknown.
const POSITION_SCRIPT: &str = r#"
local total, arrival = known(KEYS[1], KEYS[2], ARGV[1])
if total == false then return {-1, -1} end
return {total, arrival or -1}
"#;

/// KEYS[1] the likers; ARGV ttl, complete ('1'/'0'), then account, value
/// pairs. Keeps any value already held (newer than the durable copy's).
const REHYDRATE_SCRIPT: &str = r#"
for i = 3, #ARGV, 2 do
  redis.call('HSETNX', KEYS[1], ARGV[i], ARGV[i + 1])
end
if ARGV[2] == '1' then
  redis.call('HSET', KEYS[1], '_complete', '1')
end
redis.call('EXPIRE', KEYS[1], ARGV[1])
return 0
"#;

/// `-1` is the scripts' "none".
fn known_or_none(n: i64) -> Option<i64> {
    (n >= 0).then_some(n)
}

/// A liker's value as the scripts read it.
fn liker_value(position: &Position) -> String {
    match position.arrival {
        Some(arrival) => format!("{}|{arrival}", position.total),
        None => position.total.to_string(),
    }
}

fn count_key(target: &LikeTarget) -> String {
    format!("engagement:{{{target}}}:likes")
}

fn likers_key(target: &LikeTarget) -> String {
    format!("engagement:{{{target}}}:likers")
}

fn rehydrating_key(target: &LikeTarget) -> String {
    format!("engagement:{{{target}}}:rehydrating")
}

fn script(body: &str) -> String {
    format!("{KNOWN}{body}")
}

fn fred_err(e: fred::error::Error) -> EngagementError {
    EngagementError::Redis(redis_storage::RedisStorageError::from(e))
}

pub struct RedisLikeStore {
    client: RedisClient,
}

impl RedisLikeStore {
    pub fn new(client: RedisClient) -> Self {
        Self { client }
    }
}

#[async_trait]
impl LikeStore for RedisLikeStore {
    async fn apply_total(&self, target: &LikeTarget, account: &str, total: i64) -> Result<Option<Applied>, EngagementError> {
        let (added, arrival): (i64, i64) = self
            .client
            .inner
            .eval(
                script(APPLY_TOTAL_SCRIPT),
                vec![count_key(target), likers_key(target)],
                vec![account.to_owned(), total.to_string(), LIKERS_TTL_SECS.to_string()],
            )
            .await
            .map_err(fred_err)?;
        Ok(known_or_none(added).map(|added| Applied { added, arrival: known_or_none(arrival) }))
    }

    async fn counts(&self, targets: &[LikeTarget]) -> Result<Vec<i64>, EngagementError> {
        // One GET per target: keys sit on different cluster slots.
        let reads = targets.iter().map(|t| {
            let client = self.client.clone();
            let key = count_key(t);
            async move { client.inner.get::<Option<i64>, _>(key).await }
        });
        let counts = futures::future::try_join_all(reads).await.map_err(fred_err)?;
        Ok(counts.into_iter().map(|c| c.unwrap_or(0)).collect())
    }

    async fn positions(&self, account: &str, targets: &[LikeTarget]) -> Result<Vec<Option<Position>>, EngagementError> {
        let reads = targets.iter().map(|t| {
            let client = self.client.clone();
            let keys = vec![count_key(t), likers_key(t)];
            let account = account.to_owned();
            async move { client.inner.eval::<(i64, i64), _, _, _>(script(POSITION_SCRIPT), keys, vec![account]).await }
        });
        let positions = futures::future::try_join_all(reads).await.map_err(fred_err)?;
        Ok(positions
            .into_iter()
            .map(|(total, arrival)| known_or_none(total).map(|total| Position { total, arrival: known_or_none(arrival) }))
            .collect())
    }

    async fn forget(&self, account: &str, targets: &[LikeTarget]) -> Result<(), EngagementError> {
        let deletes = targets.iter().map(|t| {
            let client = self.client.clone();
            let key = likers_key(t);
            let account = account.to_owned();
            async move { client.inner.hdel::<i64, _, _>(key, account).await }
        });
        futures::future::try_join_all(deletes).await.map_err(fred_err)?;
        Ok(())
    }

    async fn rehydrate(&self, target: &LikeTarget, likers: &[(String, Position)], complete: bool) -> Result<(), EngagementError> {
        let mut args = vec![LIKERS_TTL_SECS.to_string(), if complete { "1" } else { "0" }.to_owned()];
        for (account, position) in likers {
            args.push(account.clone());
            args.push(liker_value(position));
        }
        self.client.inner.eval::<i64, _, _, _>(REHYDRATE_SCRIPT, vec![likers_key(target)], args).await.map_err(fred_err)?;
        Ok(())
    }

    async fn claim_rehydration(&self, target: &LikeTarget) -> Result<bool, EngagementError> {
        let claimed: Option<String> = self
            .client
            .inner
            .set(rehydrating_key(target), "1", Some(Expiration::EX(60)), Some(SetOptions::NX), false)
            .await
            .map_err(fred_err)?;
        Ok(claimed.is_some())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_targets_keys_share_its_hash_tag() {
        let t = LikeTarget::Comment("c1".into());
        assert_eq!(count_key(&t), "engagement:{comment:c1}:likes");
        assert_eq!(likers_key(&t), "engagement:{comment:c1}:likers");
        assert_eq!(rehydrating_key(&t), "engagement:{comment:c1}:rehydrating");
        // The field marking a whole hash, never an account id (UUIDs).
        assert!(KNOWN.contains("'_complete'") && REHYDRATE_SCRIPT.contains("'_complete'"));
    }

    #[test]
    fn a_likers_value_carries_its_arrival_when_known() {
        assert_eq!(liker_value(&Position { total: 35, arrival: Some(120) }), "35|120");
        assert_eq!(liker_value(&Position { total: 35, arrival: None }), "35");
        assert_eq!(known_or_none(-1), None);
        assert_eq!(known_or_none(0), Some(0));
    }
}
