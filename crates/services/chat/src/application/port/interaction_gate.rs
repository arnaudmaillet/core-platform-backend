use async_trait::async_trait;

use crate::domain::value_object::ProfileId;
use crate::error::ChatError;

/// What the recipient's settings say about a message from the actor (#656).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageVerdict {
    /// Their audience takes the actor.
    Allowed,
    /// Their audience (followers / mutuals) does not, or a temporary limit
    /// holds the actor back (#669): a message request.
    Request,
    /// They take messages from no one.
    Refused,
    /// A block, either way: the actor's messages are withheld, and the actor
    /// must never learn it — they see a request that is never answered.
    Silenced,
}

/// Who may message whom: social-graph's mesh-only `CheckInteraction(MESSAGE)`.
/// Unreachable ⇒ [`ChatError::InteractionCheckUnavailable`]: direct messages
/// and invitations fail closed.
#[async_trait]
pub trait InteractionGate: Send + Sync + 'static {
    async fn may_message(&self, actor: &ProfileId, recipient: &ProfileId) -> Result<MessageVerdict, ChatError>;
}
