use uuid::Uuid;

use crate::domain::value_object::{ConversationId, IdempotencyKey, ProfileId};

// ── Member-Plane keys ───────────────────────────────────────────────────────
//
// Every per-conversation key carries the `{conv:<id>}` Redis Cluster hash tag,
// pinning the tail cache, presence, typing, receipts, and the routing
// bookkeeping for one conversation onto a single slot. This is what lets the
// bounded, high-frequency Member-Plane state be read/written in one round-trip
// without CROSSSLOT — and keeps that churn off the Audience-Plane nodes.

/// Capped sorted set of the conversation's most recent messages (the hot tail).
pub fn tail_key(conversation_id: &ConversationId) -> String {
    format!("chat:{{conv:{conversation_id}}}:tail")
}

/// Expiring sorted set of online members (score = last-seen epoch ms).
pub fn presence_key(conversation_id: &ConversationId) -> String {
    format!("chat:{{conv:{conversation_id}}}:presence")
}

/// Expiring sorted set of currently-typing members (score = last-typed epoch ms).
pub fn typing_key(conversation_id: &ConversationId) -> String {
    format!("chat:{{conv:{conversation_id}}}:typing")
}

/// Hash of per-member read-receipt horizons (field = member_id, value = MessageId).
pub fn receipts_key(conversation_id: &ConversationId) -> String {
    format!("chat:{{conv:{conversation_id}}}:receipts")
}

/// One sender's `SendMessage` idempotency key (#875): `p:<message_id>` while
/// its send is pending, `s:<message_id>` once stored. Expires (60 s pending,
/// 24 h sent).
pub fn send_key(conversation_id: &ConversationId, sender_id: &ProfileId, key: &IdempotencyKey) -> String {
    format!("chat:{{conv:{conversation_id}}}:send:{sender_id}:{key}")
}

/// Expiring sorted set of active Audience-Plane shards (score = last heartbeat
/// epoch ms). Small per-conversation bookkeeping — intentionally on the home
/// slot; the actual broadcast channels (Phase 5) use spreading tags.
pub fn audience_shards_key(conversation_id: &ConversationId) -> String {
    format!("chat:{{conv:{conversation_id}}}:aud:shards")
}

/// Deterministically assigns a subscriber to one of `shard_count` Audience-Plane
/// shards, hashing the subscriber id so a given guest sticks to a stable shard
/// (stable stream) while the population spreads evenly across the cluster.
pub fn audience_shard_for(subscriber_id: Uuid, shard_count: u16) -> u16 {
    let count = shard_count.max(1);
    let bytes = subscriber_id.as_bytes();
    let hi = u64::from_be_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ]);
    (hi % count as u64) as u16
}
