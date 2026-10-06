//! The discovery pool in Redis.
//!
//! - `timeline:{disc}:recent` / `:fresh` / `:hot` — the three index ZSETs, on one
//!   cluster slot (hash tag `{disc}`) so a script moves a post between them
//!   atomically. Member `"{post_id}:{author_id}"`; score = `published_at_ms`
//!   (recent, fresh) or the hot score.
//! - `timeline:disc:post:<post_id>` — a HASH per post: `a` author, `t`
//!   published_at_ms, `g` hashtags (space-separated), `p` popularity, `r`
//!   restriction code, `v` moderation version, `x` deleted. It expires with the window (a decision recorded
//!   before the publication — a tombstone — lives a full window).
//!
//! The pool is a cache-like read model: if Redis loses it, it refills from new
//! events within one window. The index writes follow the meta write (two slots,
//! not atomic together); reads re-check the meta, so an index entry left behind
//! by a race never shows a post.

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use fred::interfaces::{HashesInterface, LuaInterface};
use redis_storage::RedisClient;

use crate::application::port::{DiscoveryPool, PoolEntry};
use crate::domain::value_object::{
    hot_score, AuthorId, DiscoveryMeta, DiscoveryStream, PostId, Restriction, StreamPosition,
};
use crate::error::TimelineError;

const RECENT_KEY: &str = "timeline:{disc}:recent";
const FRESH_KEY: &str = "timeline:{disc}:fresh";
const HOT_KEY: &str = "timeline:{disc}:hot";

/// Most aged-out or over-cap entries one write evicts (bounds the script).
const TRIM_BATCH: usize = 200;

/// Entries read past the limit, to step over ties at the cursor's score.
const TIE_SLACK: usize = 16;

fn meta_key(post_id: &PostId) -> String {
    format!("timeline:disc:post:{post_id}")
}

fn member(post_id: &PostId, author_id: &AuthorId) -> String {
    format!("{post_id}:{author_id}")
}

fn stream_key(stream: DiscoveryStream) -> Option<&'static str> {
    match stream {
        DiscoveryStream::Recent => Some(RECENT_KEY),
        DiscoveryStream::Fresh  => Some(FRESH_KEY),
        DiscoveryStream::Hot    => Some(HOT_KEY),
        DiscoveryStream::Nearby => None,
    }
}

fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or_default()
}

fn fred_err(e: fred::error::Error) -> TimelineError {
    TimelineError::Redis(redis_storage::RedisStorageError::from(e))
}

// ── Lua ───────────────────────────────────────────────────────────────────────

/// Records a publication in the meta; returns `[p, r, x]` ('' when unset).
///
/// KEYS[1] = meta · ARGV = author, published_at_ms, ttl_secs, hashtags
const META_PUBLISHED: &str = r#"
redis.call('HSET', KEYS[1], 'a', ARGV[1], 't', ARGV[2], 'g', ARGV[4])
redis.call('EXPIRE', KEYS[1], ARGV[3])
local r = redis.call('HMGET', KEYS[1], 'p', 'r', 'x')
for i = 1, 3 do if not r[i] then r[i] = '' end end
return r
"#;

/// Records a popularity on a known post; returns `[a, t, r, x]`, or `[]` when
/// the post is unknown.
///
/// KEYS[1] = meta · ARGV = popularity
const META_POPULARITY: &str = r#"
if redis.call('EXISTS', KEYS[1]) == 0 then return {} end
redis.call('HSET', KEYS[1], 'p', ARGV[1])
local r = redis.call('HMGET', KEYS[1], 'a', 't', 'r', 'x')
for i = 1, 4 do if not r[i] then r[i] = '' end end
return r
"#;

/// Records a moderation restriction unless an equal-or-newer one is recorded
/// (an equal version re-applies); returns 1 when applied, 0 when stale. A key
/// that did not exist (no publication seen) becomes a tombstone for `ttl`.
///
/// KEYS[1] = meta · ARGV = restriction code, version, ttl_secs
const META_RESTRICTION: &str = r#"
local v = redis.call('HGET', KEYS[1], 'v')
if v and tonumber(v) > tonumber(ARGV[2]) then return 0 end
redis.call('HSET', KEYS[1], 'r', ARGV[1], 'v', ARGV[2])
if redis.call('TTL', KEYS[1]) < 0 then redis.call('EXPIRE', KEYS[1], ARGV[3]) end
return 1
"#;

/// Records a deletion; returns the author ('' when unknown).
///
/// KEYS[1] = meta · ARGV = ttl_secs
const META_DELETED: &str = r#"
redis.call('HSET', KEYS[1], 'x', '1')
if redis.call('TTL', KEYS[1]) < 0 then redis.call('EXPIRE', KEYS[1], ARGV[1]) end
return redis.call('HGET', KEYS[1], 'a') or ''
"#;

/// Indexes a post, then evicts what aged out of the window or overflows the cap.
///
/// KEYS = recent, fresh, hot
/// ARGV = member, published_at_ms, hot score ('' = no popularity yet),
///        cutoff_ms, cap, trim batch
const POOL_ADD: &str = r#"
local recent, fresh, hot = KEYS[1], KEYS[2], KEYS[3]
local m = ARGV[1]
redis.call('ZADD', recent, ARGV[2], m)
if ARGV[3] ~= '' then
    redis.call('ZREM', fresh, m)
    redis.call('ZADD', hot, ARGV[3], m)
elseif not redis.call('ZSCORE', hot, m) then
    redis.call('ZADD', fresh, ARGV[2], m)
end
local batch = tonumber(ARGV[6])
local evict = redis.call('ZRANGEBYSCORE', recent, '-inf', '(' .. ARGV[4], 'LIMIT', 0, batch)
local over = redis.call('ZCARD', recent) - #evict - tonumber(ARGV[5])
if over > 0 then
    local extra = redis.call('ZRANGE', recent, #evict, #evict + math.min(over, batch) - 1)
    for _, e in ipairs(extra) do evict[#evict + 1] = e end
end
for _, e in ipairs(evict) do
    redis.call('ZREM', recent, e)
    redis.call('ZREM', fresh, e)
    redis.call('ZREM', hot, e)
end
return #evict
"#;

/// Moves a pooled post to `hot` at a new score (popularity > 0), or only
/// re-scores it there (popularity 0). Posts no longer in `recent` are left out.
///
/// KEYS = recent, fresh, hot · ARGV = member, hot score, promote ('1'/'0')
const POOL_SCORE: &str = r#"
if not redis.call('ZSCORE', KEYS[1], ARGV[1]) then return 0 end
if ARGV[3] == '1' then
    redis.call('ZREM', KEYS[2], ARGV[1])
    redis.call('ZADD', KEYS[3], ARGV[2], ARGV[1])
else
    redis.call('ZADD', KEYS[3], 'XX', ARGV[2], ARGV[1])
end
return 1
"#;

/// KEYS = recent, fresh, hot · ARGV = member
const POOL_REMOVE: &str = r#"
for i = 1, 3 do redis.call('ZREM', KEYS[i], ARGV[1]) end
return 1
"#;

/// Entries at or below a score, newest/highest first, with scores.
///
/// KEYS[1] = stream · ARGV = max score (inclusive, or '+inf'), count
const RANGE: &str = r#"
return redis.call('ZREVRANGEBYSCORE', KEYS[1], ARGV[1], '-inf', 'WITHSCORES', 'LIMIT', 0, tonumber(ARGV[2]))
"#;

// ── Adapter ───────────────────────────────────────────────────────────────────

pub struct RedisDiscoveryPool {
    client:       RedisClient,
    window_secs:  u64,
    cap:          usize,
    gravity_secs: f64,
}

impl RedisDiscoveryPool {
    pub fn new(client: RedisClient, window_secs: u64, cap: usize, gravity_secs: f64) -> Self {
        Self { client, window_secs: window_secs.max(1), cap: cap.max(1), gravity_secs }
    }

    fn pool_keys() -> Vec<String> {
        vec![RECENT_KEY.to_owned(), FRESH_KEY.to_owned(), HOT_KEY.to_owned()]
    }

    /// Seconds left before a post published at `published_at_ms` leaves the
    /// window, or `None` once it has.
    fn remaining_secs(&self, published_at_ms: i64) -> Option<u64> {
        let age_secs = (now_ms() - published_at_ms).max(0) as u64 / 1000;
        self.window_secs.checked_sub(age_secs).filter(|s| *s > 0)
    }

    async fn index(
        &self,
        post_id:         &PostId,
        author_id:       &AuthorId,
        published_at_ms: i64,
        popularity:      f64,
    ) -> Result<(), TimelineError> {
        let hot = if popularity > 0.0 {
            hot_score(popularity, published_at_ms, self.gravity_secs).to_string()
        } else {
            String::new()
        };
        let cutoff = now_ms() - (self.window_secs as i64) * 1000;
        let _: i64 = self
            .client
            .inner
            .eval(
                POOL_ADD,
                Self::pool_keys(),
                vec![
                    member(post_id, author_id),
                    published_at_ms.to_string(),
                    hot,
                    cutoff.to_string(),
                    self.cap.to_string(),
                    TRIM_BATCH.to_string(),
                ],
            )
            .await
            .map_err(fred_err)?;
        Ok(())
    }

    async fn unindex(&self, post_id: &PostId, author_id: &AuthorId) -> Result<(), TimelineError> {
        let _: i64 = self
            .client
            .inner
            .eval(POOL_REMOVE, Self::pool_keys(), vec![member(post_id, author_id)])
            .await
            .map_err(fred_err)?;
        Ok(())
    }
}

fn parse_meta(post_id: PostId, fields: &[Option<String>]) -> DiscoveryMeta {
    let field = |i: usize| fields.get(i).and_then(|f| f.as_deref()).filter(|f| !f.is_empty());
    DiscoveryMeta {
        post_id,
        author_id:       field(0).and_then(|a| AuthorId::try_from(a).ok()),
        published_at_ms: field(1).and_then(|t| t.parse().ok()),
        popularity:      field(2).and_then(|p| p.parse().ok()).unwrap_or(0.0),
        restriction:     field(3).map_or(Restriction::None, |r| Restriction::from_code(r.parse().unwrap_or(u8::MAX))),
        deleted:         field(4).is_some(),
        tags:            field(5).map(|g| g.split(' ').map(str::to_owned).collect()).unwrap_or_default(),
    }
}

#[async_trait]
impl DiscoveryPool for RedisDiscoveryPool {
    async fn record_published(
        &self,
        post_id:         &PostId,
        author_id:       &AuthorId,
        published_at_ms: i64,
        tags:            &[String],
    ) -> Result<(), TimelineError> {
        let Some(ttl) = self.remaining_secs(published_at_ms) else {
            return Ok(()); // already out of the window
        };
        let state: Vec<String> = self
            .client
            .inner
            .eval(
                META_PUBLISHED,
                vec![meta_key(post_id)],
                vec![author_id.to_string(), published_at_ms.to_string(), ttl.to_string(), tags.join(" ")],
            )
            .await
            .map_err(fred_err)?;
        let [p, r, x] = state.as_slice() else {
            return Err(TimelineError::ScriptReturnInvalid { context: "discovery meta_published" });
        };
        let restriction = if r.is_empty() { Restriction::None } else { Restriction::from_code(r.parse().unwrap_or(u8::MAX)) };
        if !x.is_empty() || restriction.hides() {
            return Ok(());
        }
        self.index(post_id, author_id, published_at_ms, p.parse().unwrap_or(0.0)).await
    }

    async fn record_popularity(&self, post_id: &PostId, popularity: f64) -> Result<(), TimelineError> {
        if !popularity.is_finite() {
            return Ok(());
        }
        let state: Vec<String> = self
            .client
            .inner
            .eval(META_POPULARITY, vec![meta_key(post_id)], vec![popularity.to_string()])
            .await
            .map_err(fred_err)?;
        let [a, t, r, x] = state.as_slice() else {
            return Ok(()); // unknown post
        };
        let (Ok(author_id), Ok(published_at_ms)) = (AuthorId::try_from(a.as_str()), t.parse::<i64>()) else {
            return Ok(()); // a tombstone: the publication was not seen
        };
        let restriction = if r.is_empty() { Restriction::None } else { Restriction::from_code(r.parse().unwrap_or(u8::MAX)) };
        if !x.is_empty() || restriction.hides() {
            return Ok(());
        }
        let score = hot_score(popularity, published_at_ms, self.gravity_secs);
        let promote = if popularity > 0.0 { "1" } else { "0" };
        let _: i64 = self
            .client
            .inner
            .eval(
                POOL_SCORE,
                Self::pool_keys(),
                vec![member(post_id, &author_id), score.to_string(), promote.to_owned()],
            )
            .await
            .map_err(fred_err)?;
        Ok(())
    }

    async fn record_restriction(
        &self,
        post_id:     &PostId,
        restriction: Restriction,
        version:     i64,
    ) -> Result<(), TimelineError> {
        let applied: i64 = self
            .client
            .inner
            .eval(
                META_RESTRICTION,
                vec![meta_key(post_id)],
                vec![restriction.code().to_string(), version.to_string(), self.window_secs.to_string()],
            )
            .await
            .map_err(fred_err)?;
        if applied == 0 {
            return Ok(()); // a newer decision is recorded
        }
        let meta = self.meta(std::slice::from_ref(post_id)).await?.remove(post_id);
        let Some(DiscoveryMeta { author_id: Some(author_id), published_at_ms: Some(published_at_ms), popularity, deleted, .. }) = meta else {
            return Ok(()); // tombstone only: the publication will read it
        };
        if restriction.hides() {
            return self.unindex(post_id, &author_id).await;
        }
        if deleted || self.remaining_secs(published_at_ms).is_none() {
            return Ok(());
        }
        // Lifted (or age-gated, which stays pooled and is filtered at read):
        // back in the indices.
        self.index(post_id, &author_id, published_at_ms, popularity).await
    }

    async fn record_deleted(&self, post_id: &PostId) -> Result<(), TimelineError> {
        let author: String = self
            .client
            .inner
            .eval(META_DELETED, vec![meta_key(post_id)], vec![self.window_secs.to_string()])
            .await
            .map_err(fred_err)?;
        match AuthorId::try_from(author.as_str()) {
            Ok(author_id) => self.unindex(post_id, &author_id).await,
            Err(_) => Ok(()), // never indexed
        }
    }

    async fn range(
        &self,
        stream: DiscoveryStream,
        after:  Option<&StreamPosition>,
        count:  usize,
    ) -> Result<Vec<PoolEntry>, TimelineError> {
        let Some(key) = stream_key(stream) else {
            return Ok(Vec::new());
        };
        let max = after.map_or_else(|| "+inf".to_owned(), |p| p.score.to_string());
        let raw: Vec<String> = self
            .client
            .inner
            .eval(RANGE, vec![key.to_owned()], vec![max, (count + TIE_SLACK).to_string()])
            .await
            .map_err(fred_err)?;
        if !raw.len().is_multiple_of(2) {
            return Err(TimelineError::ScriptReturnInvalid { context: "discovery range" });
        }
        let mut entries = Vec::with_capacity(count);
        for pair in raw.chunks_exact(2) {
            let (m, score) = (&pair[0], &pair[1]);
            let score: f64 = score
                .parse()
                .map_err(|_| TimelineError::ScriptReturnInvalid { context: "discovery range score" })?;
            if after.is_some_and(|p| !p.is_before(score, m)) {
                continue; // at or before the cursor (a tie already served)
            }
            let Some((post, author)) = m.split_once(':') else { continue };
            let (Ok(post_id), Ok(author_id)) = (PostId::try_from(post), AuthorId::try_from(author)) else {
                continue;
            };
            entries.push(PoolEntry { post_id, author_id, position: StreamPosition { score, member: m.clone() } });
            if entries.len() == count {
                break;
            }
        }
        Ok(entries)
    }

    async fn meta(&self, posts: &[PostId]) -> Result<HashMap<PostId, DiscoveryMeta>, TimelineError> {
        let reads = posts.iter().map(|post_id| async move {
            let fields: Vec<Option<String>> = self
                .client
                .inner
                .hmget(meta_key(post_id), vec!["a", "t", "p", "r", "x", "g"])
                .await
                .map_err(fred_err)?;
            Ok::<_, TimelineError>((*post_id, fields))
        });
        let mut out = HashMap::with_capacity(posts.len());
        for (post_id, fields) in futures::future::try_join_all(reads).await? {
            if fields.iter().all(Option::is_none) {
                continue; // never heard of it
            }
            out.insert(post_id, parse_meta(post_id, &fields));
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meta_fields_parse_leniently_and_fail_closed() {
        let post = PostId::from_uuid(uuid::Uuid::now_v7());
        let author = AuthorId::from_uuid(uuid::Uuid::now_v7());
        let full = parse_meta(post, &[
            Some(author.to_string()), Some("1700".into()), Some("4.5".into()), Some("3".into()), None,
            Some("rust go".into()),
        ]);
        assert_eq!(full.tags, vec!["rust", "go"]);
        assert_eq!(full.author_id, Some(author));
        assert_eq!(full.published_at_ms, Some(1700));
        assert_eq!(full.popularity, 4.5);
        assert_eq!(full.restriction, Restriction::AgeGated);
        assert!(!full.deleted);

        let tombstone = parse_meta(post, &[None, None, None, Some("garbage".into()), Some("1".into())]);
        assert_eq!(tombstone.author_id, None);
        assert_eq!(tombstone.restriction, Restriction::Removed);
        assert!(tombstone.deleted);
        assert!(tombstone.tags.is_empty());
    }
}
