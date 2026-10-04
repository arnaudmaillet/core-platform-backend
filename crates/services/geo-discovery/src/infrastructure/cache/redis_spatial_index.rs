use async_trait::async_trait;
use fred::interfaces::{LuaInterface, SortedSetsInterface};
use fred::types::Value as FredValue;
use redis_storage::RedisClient;
use uuid::Uuid;

use crate::application::port::SpatialIndex;
use crate::domain::value_object::{H3Index, H3Resolution, PostId, ViralityScore};
use crate::error::GeoDiscoveryError;

// ── Redis key builders ────────────────────────────────────────────────────────

/// `sg:geo:tile:{h3_index}:{resolution}`
pub fn tile_key(h3: u64, res: i8) -> String {
    format!("sg:geo:tile:{}:{}", h3, res)
}

/// `sg:geo:hot_tiles` — ZSET tracking last-access epoch per tile.
pub const HOT_TILES_KEY: &str = "sg:geo:hot_tiles";

// ── Lua scripts ───────────────────────────────────────────────────────────────

/// Atomically ADDs a member to a tile ZSET and enforces the Top-K cap.
///
/// KEYS[1] = tile ZSET key
/// ARGV[1] = score (float as string)
/// ARGV[2] = member (post_id hyphenated string)
/// ARGV[3] = top_k cap (integer as string)
///
/// Returns: remaining ZSET cardinality after the cap (integer).
///
/// Design: single atomic round-trip. The ZREMRANGEBYRANK after ZADD evicts the
/// lowest-score member(s) when the cap is exceeded, bounding RAM usage per tile
/// regardless of urban post density.
const ZADD_TOPK_SCRIPT: &str = r#"
local key   = KEYS[1]
local score = ARGV[1]
local member = ARGV[2]
local top_k = tonumber(ARGV[3])

redis.call('ZADD', key, score, member)

local size = redis.call('ZCARD', key)
if size > top_k then
    -- Remove lowest-score entries that exceed the cap.
    redis.call('ZREMRANGEBYRANK', key, 0, size - top_k - 1)
end

return redis.call('ZCARD', key)
"#;

/// Updates the score for an existing ZSET member (XX semantics).
///
/// KEYS[1] = tile ZSET key
/// ARGV[1] = new_score (float as string)
/// ARGV[2] = member (post_id hyphenated string)
///
/// Returns: 1 if the member existed and was updated, 0 otherwise.
///
/// Design: ZSCORE check before ZADD avoids a spurious insertion when the post
/// has been evicted by Top-K or cold-tile pruning. This preserves the invariant
/// that the ZSET only contains posts that were explicitly indexed.
const ZADD_XX_SCRIPT: &str = r#"
local key    = KEYS[1]
local score  = ARGV[1]
local member = ARGV[2]

local exists = redis.call('ZSCORE', key, member)
if exists ~= false then
    redis.call('ZADD', key, score, member)
    return 1
end
return 0
"#;

fn fred_err(e: fred::error::Error) -> GeoDiscoveryError {
    GeoDiscoveryError::Redis(redis_storage::RedisStorageError::from(e))
}

// ── RedisGeoSpatialIndex ──────────────────────────────────────────────────────

pub struct RedisGeoSpatialIndex {
    client: RedisClient,
}

impl RedisGeoSpatialIndex {
    pub fn new(client: RedisClient) -> Self {
        Self { client }
    }
}

#[async_trait]
impl SpatialIndex for RedisGeoSpatialIndex {
    async fn upsert(
        &self,
        tile:    H3Index,
        res:     H3Resolution,
        post_id: &PostId,
        score:   ViralityScore,
    ) -> Result<(), GeoDiscoveryError> {
        let key    = tile_key(tile.as_u64(), res.as_i8());
        let member = post_id.as_uuid().to_string();
        let cap    = res.top_k_cap();

        let _: i64 = self.client.inner
            .eval(
                ZADD_TOPK_SCRIPT,
                vec![key],
                vec![
                    score.as_f64().to_string(),
                    member,
                    cap.to_string(),
                ],
            )
            .await
            .map_err(fred_err)?;

        Ok(())
    }

    async fn update_score(
        &self,
        tile:    H3Index,
        res:     H3Resolution,
        post_id: &PostId,
        score:   ViralityScore,
    ) -> Result<bool, GeoDiscoveryError> {
        let key    = tile_key(tile.as_u64(), res.as_i8());
        let member = post_id.as_uuid().to_string();

        let updated: i64 = self.client.inner
            .eval(
                ZADD_XX_SCRIPT,
                vec![key],
                vec![score.as_f64().to_string(), member],
            )
            .await
            .map_err(fred_err)?;

        Ok(updated == 1)
    }

    async fn query(
        &self,
        tile:      H3Index,
        res:       H3Resolution,
        min_score: f64,
    ) -> Result<Vec<Uuid>, GeoDiscoveryError> {
        let key = tile_key(tile.as_u64(), res.as_i8());
        // Score bounds go to fred as f64: a string other than "±inf" becomes a
        // LEX bound ("[500"), which Redis rejects for ZRANGEBYSCORE ("min or
        // max is not a float") — every Radar query below zoom 13 (floor > 0)
        // failed that way.
        let min = if min_score <= 0.0 { f64::NEG_INFINITY } else { min_score };

        // ZRANGEBYSCORE with score ascending → all members with score ≥ min_score.
        let members: Vec<String> = self.client.inner
            .zrangebyscore(&key, min, f64::INFINITY, false, None)
            .await
            .map_err(fred_err)?;

        let mut post_ids = Vec::with_capacity(members.len());
        for m in members {
            if let Ok(id) = Uuid::parse_str(&m) {
                post_ids.push(id);
            } else {
                tracing::warn!(member = %m, tile = tile.as_u64(), "non-UUID member in tile ZSET — skipping");
            }
        }
        Ok(post_ids)
    }

    async fn touch_hot_tiles(
        &self,
        tiles: &[(H3Index, H3Resolution)],
    ) -> Result<(), GeoDiscoveryError> {
        if tiles.is_empty() {
            return Ok(());
        }

        let now = chrono::Utc::now().timestamp() as f64;

        // Each ZADD updates the last-access epoch for a tile suffix.
        // Issued concurrently through fred's lock-free command queue.
        let futs: Vec<_> = tiles
            .iter()
            .map(|(tile, res)| {
                let client = self.client.clone();
                let suffix = format!("{}:{}", tile.as_u64(), res.as_i8());
                async move {
                    let _: FredValue = client.inner
                        .zadd(HOT_TILES_KEY, None, None, false, false, (now, suffix))
                        .await
                        .unwrap_or(FredValue::Null);
                }
            })
            .collect();

        futures::future::join_all(futs).await;
        Ok(())
    }
}
