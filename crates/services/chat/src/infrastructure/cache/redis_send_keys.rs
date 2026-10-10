use async_trait::async_trait;
use fred::interfaces::LuaInterface;
use redis_storage::RedisClient;

use crate::application::port::send_keys::{PENDING_KEY_TTL_SECS, SENT_KEY_TTL_SECS};
use crate::application::port::{SendClaim, SendKeys};
use crate::domain::value_object::{ConversationId, IdempotencyKey, MessageId, ProfileId};
use crate::error::ChatError;
use crate::infrastructure::cache::keys::send_key;
use crate::infrastructure::cache::redis_err;

/// A key's value is `p:<message_id>` while its send is pending and
/// `s:<message_id>` once the message is stored.
const PENDING: &str = "p:";
const SENT: &str = "s:";

/// Answers the key's current value, or claims it pending when it has none.
/// KEYS[1]=key ARGV[1]=pending value ARGV[2]=pending ttl secs
const CLAIM: &str = r#"
local current = redis.call('GET', KEYS[1])
if current then return current end
redis.call('SET', KEYS[1], ARGV[1], 'EX', tonumber(ARGV[2]))
return ''
"#;

/// Replaces the key's value only while it still holds the expected one.
/// KEYS[1]=key ARGV[1]=expected ARGV[2]=new value ARGV[3]=ttl secs
const COMPLETE: &str = r#"
if redis.call('GET', KEYS[1]) == ARGV[1] then
  redis.call('SET', KEYS[1], ARGV[2], 'EX', tonumber(ARGV[3]))
end
return 1
"#;

/// Deletes the key only while it still holds the expected value.
/// KEYS[1]=key ARGV[1]=expected
const RELEASE: &str = r#"
if redis.call('GET', KEYS[1]) == ARGV[1] then
  redis.call('DEL', KEYS[1])
end
return 1
"#;

pub struct RedisSendKeys {
    client: RedisClient,
}

impl RedisSendKeys {
    pub fn new(client: RedisClient) -> Self {
        Self { client }
    }
}

#[async_trait]
impl SendKeys for RedisSendKeys {
    async fn claim(
        &self,
        conversation_id: &ConversationId,
        sender_id:       &ProfileId,
        key:             &IdempotencyKey,
        message_id:      MessageId,
    ) -> Result<SendClaim, ChatError> {
        let current: String = self
            .client
            .inner
            .eval(
                CLAIM,
                vec![send_key(conversation_id, sender_id, key)],
                vec![format!("{PENDING}{message_id}"), PENDING_KEY_TTL_SECS.to_string()],
            )
            .await
            .map_err(redis_err)?;
        parse_claim(&current)
    }

    async fn complete(
        &self,
        conversation_id: &ConversationId,
        sender_id:       &ProfileId,
        key:             &IdempotencyKey,
        message_id:      MessageId,
    ) -> Result<(), ChatError> {
        let _: i64 = self
            .client
            .inner
            .eval(
                COMPLETE,
                vec![send_key(conversation_id, sender_id, key)],
                vec![
                    format!("{PENDING}{message_id}"),
                    format!("{SENT}{message_id}"),
                    SENT_KEY_TTL_SECS.to_string(),
                ],
            )
            .await
            .map_err(redis_err)?;
        Ok(())
    }

    async fn release(
        &self,
        conversation_id: &ConversationId,
        sender_id:       &ProfileId,
        key:             &IdempotencyKey,
        message_id:      MessageId,
    ) -> Result<(), ChatError> {
        let _: i64 = self
            .client
            .inner
            .eval(
                RELEASE,
                vec![send_key(conversation_id, sender_id, key)],
                vec![format!("{PENDING}{message_id}")],
            )
            .await
            .map_err(redis_err)?;
        Ok(())
    }
}

/// Reads what [`CLAIM`] answered: empty when it claimed the key.
fn parse_claim(current: &str) -> Result<SendClaim, ChatError> {
    if current.is_empty() {
        Ok(SendClaim::Fresh)
    } else if current.starts_with(PENDING) {
        Ok(SendClaim::InFlight)
    } else if let Some(id) = current.strip_prefix(SENT) {
        Ok(SendClaim::Sent(MessageId::try_from(id)?))
    } else {
        Err(ChatError::DomainViolation {
            field:   "send_key".to_owned(),
            message: format!("unexpected send key value '{current}'"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn claim_values_round_trip() {
        let id = MessageId::new();
        assert_eq!(parse_claim("").unwrap(), SendClaim::Fresh);
        assert_eq!(parse_claim(&format!("{PENDING}{id}")).unwrap(), SendClaim::InFlight);
        assert_eq!(parse_claim(&format!("{SENT}{id}")).unwrap(), SendClaim::Sent(id));
        assert!(parse_claim("garbage").is_err());
    }
}
