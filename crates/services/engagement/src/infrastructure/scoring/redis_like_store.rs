//! [`LikeStore`] on Redis (#665). Per target, a hash of each liking account's
//! total and the target's sum, under the target's hash tag
//! (`engagement:{post:<id>}:…`) so one script updates both.

use async_trait::async_trait;
use fred::interfaces::{HashesInterface, KeysInterface, LuaInterface};
use redis_storage::RedisClient;

use crate::application::port::LikeStore;
use crate::domain::value_object::LikeTarget;
use crate::error::EngagementError;

/// KEYS[1] the target's like count, KEYS[2] its likers' totals; ARGV account,
/// total. A total no larger than the one held changes nothing: redeliveries
/// and late events are absorbed. Returns the likes added.
const APPLY_TOTAL_SCRIPT: &str = r#"
local old = tonumber(redis.call('HGET', KEYS[2], ARGV[1]) or '0')
local new = tonumber(ARGV[2])
if new <= old then
  return 0
end
redis.call('HSET', KEYS[2], ARGV[1], ARGV[2])
redis.call('INCRBY', KEYS[1], new - old)
return new - old
"#;

fn count_key(target: &LikeTarget) -> String {
    format!("engagement:{{{target}}}:likes")
}

fn likers_key(target: &LikeTarget) -> String {
    format!("engagement:{{{target}}}:likers")
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
    async fn apply_total(&self, target: &LikeTarget, account: &str, total: i64) -> Result<i64, EngagementError> {
        self.client
            .inner
            .eval(APPLY_TOTAL_SCRIPT, vec![count_key(target), likers_key(target)], vec![account.to_owned(), total.to_string()])
            .await
            .map_err(fred_err)
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

    async fn mine(&self, account: &str, targets: &[LikeTarget]) -> Result<Vec<i64>, EngagementError> {
        let reads = targets.iter().map(|t| {
            let client = self.client.clone();
            let key = likers_key(t);
            let account = account.to_owned();
            async move { client.inner.hget::<Option<i64>, _, _>(key, account).await }
        });
        let mine = futures::future::try_join_all(reads).await.map_err(fred_err)?;
        Ok(mine.into_iter().map(|m| m.unwrap_or(0)).collect())
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_targets_keys_share_its_hash_tag() {
        let t = LikeTarget::Comment("c1".into());
        assert_eq!(count_key(&t), "engagement:{comment:c1}:likes");
        assert_eq!(likers_key(&t), "engagement:{comment:c1}:likers");
    }
}
