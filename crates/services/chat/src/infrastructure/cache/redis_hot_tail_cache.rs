use async_trait::async_trait;
use chrono::DateTime;
use fred::interfaces::{KeysInterface, LuaInterface};
use redis_storage::RedisClient;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::application::port::{HotTailCache, MessageSummary};
use crate::domain::value_object::{ContentType, ConversationId};
use crate::error::ChatError;
use crate::infrastructure::cache::keys::tail_key;
use crate::infrastructure::cache::redis_err;

/// Atomically adds a message to the tail and trims to `cap` (newest kept).
/// KEYS[1]=tail  ARGV[1]=score(created_at_ms)  ARGV[2]=member(json)  ARGV[3]=cap
const ZADD_CAP: &str = r#"
redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2])
local card = redis.call('ZCARD', KEYS[1])
local cap = tonumber(ARGV[3])
if card > cap then
    redis.call('ZREMRANGEBYRANK', KEYS[1], 0, card - cap - 1)
end
return redis.call('ZCARD', KEYS[1])
"#;

/// Newest `limit` members. KEYS[1]=tail  ARGV[1]=limit
const RECENT: &str = r#"
return redis.call('ZREVRANGE', KEYS[1], 0, tonumber(ARGV[1]) - 1)
"#;

/// Members with score <= max, newest-first, capped. KEYS[1]=tail ARGV[1]=max ARGV[2]=limit
const RANGE_DESC: &str = r#"
return redis.call('ZREVRANGEBYSCORE', KEYS[1], ARGV[1], '-inf', 'LIMIT', 0, tonumber(ARGV[2]))
"#;

/// Wire form of a cached message — scalar-only so serialization never depends on
/// the domain types and the JSON member is delimiter-safe.
#[derive(Serialize, Deserialize)]
struct CachedMessage {
    message_id:    String,
    sender_id:     String,
    content_type:  i8,
    body:          String,
    media_ref:     Option<String>,
    reply_to:      Option<String>,
    created_at_ms: i64,
}

pub struct RedisHotTailCache {
    client: RedisClient,
}

impl RedisHotTailCache {
    pub fn new(client: RedisClient) -> Self {
        Self { client }
    }
}

#[async_trait]
impl HotTailCache for RedisHotTailCache {
    async fn push(
        &self,
        conversation_id: &ConversationId,
        message:         &MessageSummary,
        cap:             u16,
    ) -> Result<(), ChatError> {
        let member = encode(message)?;
        let score  = message.created_at.timestamp_millis().to_string();

        let _: i64 = self
            .client
            .inner
            .eval(ZADD_CAP, vec![tail_key(conversation_id)], vec![score, member, cap.to_string()])
            .await
            .map_err(redis_err)?;
        Ok(())
    }

    async fn recent(
        &self,
        conversation_id: &ConversationId,
        limit:           usize,
    ) -> Result<Vec<MessageSummary>, ChatError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let raw: Vec<String> = self
            .client
            .inner
            .eval(RECENT, vec![tail_key(conversation_id)], vec![limit.to_string()])
            .await
            .map_err(redis_err)?;
        raw.iter().map(|s| decode(s)).collect()
    }

    async fn range_desc(
        &self,
        conversation_id:     &ConversationId,
        max_score_inclusive: i64,
        limit:               usize,
    ) -> Result<Vec<MessageSummary>, ChatError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let raw: Vec<String> = self
            .client
            .inner
            .eval(
                RANGE_DESC,
                vec![tail_key(conversation_id)],
                vec![max_score_inclusive.to_string(), limit.to_string()],
            )
            .await
            .map_err(redis_err)?;
        raw.iter().map(|s| decode(s)).collect()
    }

    async fn exists(&self, conversation_id: &ConversationId) -> Result<bool, ChatError> {
        self.client
            .inner
            .exists(tail_key(conversation_id))
            .await
            .map_err(redis_err)
    }
}

fn encode(m: &MessageSummary) -> Result<String, ChatError> {
    let cm = CachedMessage {
        message_id:    m.message_id.to_string(),
        sender_id:     m.sender_id.to_string(),
        content_type:  m.content_type.as_tinyint(),
        body:          m.body.clone(),
        media_ref:     m.media_ref.clone(),
        reply_to:      m.reply_to.map(|u| u.to_string()),
        created_at_ms: m.created_at.timestamp_millis(),
    };
    serde_json::to_string(&cm).map_err(|e| ChatError::DomainViolation {
        field:   "hot_tail.encode".to_owned(),
        message: e.to_string(),
    })
}

fn decode(raw: &str) -> Result<MessageSummary, ChatError> {
    let cm: CachedMessage = serde_json::from_str(raw).map_err(|e| ChatError::DomainViolation {
        field:   "hot_tail.decode".to_owned(),
        message: e.to_string(),
    })?;

    let reply_to = match cm.reply_to {
        Some(s) => Some(Uuid::parse_str(&s).map_err(|_| ChatError::InvalidMessageId(s))?),
        None => None,
    };

    Ok(MessageSummary {
        message_id:   Uuid::parse_str(&cm.message_id)
            .map_err(|_| ChatError::InvalidMessageId(cm.message_id.clone()))?,
        sender_id:    Uuid::parse_str(&cm.sender_id)
            .map_err(|_| ChatError::InvalidProfileId(cm.sender_id.clone()))?,
        content_type: ContentType::try_from(cm.content_type)?,
        body:         cm.body,
        media_ref:    cm.media_ref,
        reply_to,
        created_at:   DateTime::from_timestamp_millis(cm.created_at_ms)
            .unwrap_or(DateTime::UNIX_EPOCH),
        // A withheld message is never cached (see `MessageFanout`).
        withheld:     false,
    })
}
