use async_trait::async_trait;

use crate::domain::value_object::{ConversationId, IdempotencyKey, MessageId, ProfileId};
use crate::error::ChatError;

/// How long a sent key answers its message: long enough for any client retry.
pub const SENT_KEY_TTL_SECS: u64 = 24 * 60 * 60;

/// How long a claimed key waits for its send to finish. A send that dies
/// between the claim and the write (a crashed pod) frees its key after this,
/// so a retry is never refused for longer.
pub const PENDING_KEY_TTL_SECS: u64 = 60;

/// What a claim found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendClaim {
    /// Unused: the caller now holds it, for the message id it claimed with.
    Fresh,
    /// Already sent: this is the message the first send stored.
    Sent(MessageId),
    /// Another send holds it and has not finished.
    InFlight,
}

/// The idempotency keys of `SendMessage` (#875), scoped to one sender in one
/// conversation.
///
/// A key is claimed for a message id before the message is written, completed
/// once it is, and released if the write fails, so the client's retry can
/// send again. Best-effort like the rest of chat's Redis state: the send path
/// treats a store error as "no key" rather than failing the message.
#[async_trait]
pub trait SendKeys: Send + Sync + 'static {
    /// Claims `key` for `message_id` (pending, [`PENDING_KEY_TTL_SECS`]) unless
    /// it is already claimed or sent.
    async fn claim(
        &self,
        conversation_id: &ConversationId,
        sender_id:       &ProfileId,
        key:             &IdempotencyKey,
        message_id:      MessageId,
    ) -> Result<SendClaim, ChatError>;

    /// Marks the claim of `message_id` sent ([`SENT_KEY_TTL_SECS`]). A no-op
    /// when the key no longer holds that claim.
    async fn complete(
        &self,
        conversation_id: &ConversationId,
        sender_id:       &ProfileId,
        key:             &IdempotencyKey,
        message_id:      MessageId,
    ) -> Result<(), ChatError>;

    /// Drops the pending claim of `message_id`. A no-op when the key no longer
    /// holds that claim.
    async fn release(
        &self,
        conversation_id: &ConversationId,
        sender_id:       &ProfileId,
        key:             &IdempotencyKey,
        message_id:      MessageId,
    ) -> Result<(), ChatError>;
}
