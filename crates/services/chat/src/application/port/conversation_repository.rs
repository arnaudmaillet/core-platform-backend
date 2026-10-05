use async_trait::async_trait;

use crate::domain::aggregate::Conversation;
use crate::domain::value_object::{ConversationId, MessageRequest, ProfileId};
use crate::error::ChatError;

/// Persistence port for the [`Conversation`] aggregate (`chat.conversations`).
///
/// Backed by a single point-read/point-write per call — the aggregate is small
/// and bounded by design, and the audience is never loaded through it.
#[async_trait]
pub trait ConversationRepository: Send + Sync + 'static {
    /// Inserts a freshly created conversation.
    async fn insert(&self, conversation: &Conversation) -> Result<(), ChatError>;

    /// Persists the mutable aggregate state after a transition: `visibility`,
    /// `public_since`, `member_count`, and `updated_at`. The immutable columns
    /// (`kind`, `owner_id`, `created_at`) are not rewritten.
    async fn update(&self, conversation: &Conversation) -> Result<(), ChatError>;

    /// Reconstitutes the aggregate by id, or `None` if it does not exist.
    async fn find(&self, id: &ConversationId) -> Result<Option<Conversation>, ChatError>;

    /// The direct conversation between `a` and `b` (#656): the existing one's
    /// id, or `proposed` once it is claimed for the pair — whichever of two
    /// concurrent openers wins, both get the same id. Order-insensitive.
    async fn claim_direct(&self, a: &ProfileId, b: &ProfileId, proposed: ConversationId) -> Result<ConversationId, ChatError>;

    /// Inserts a new direct conversation unless its row exists; `false` when
    /// another opener's insert won (re-read it).
    async fn insert_direct(&self, conversation: &Conversation) -> Result<bool, ChatError>;

    /// Moves a direct conversation's request from `from` to `to`, only if it
    /// is still in `from`'s state (compare-and-set); `false` when it moved on.
    /// Leaving a decline starts a fresh request: its one message unused.
    async fn transition_request(&self, id: &ConversationId, from: MessageRequest, to: MessageRequest) -> Result<bool, ChatError>;

    /// Spends a pending request's one message; `false` when it is spent.
    async fn claim_request_message(&self, id: &ConversationId) -> Result<bool, ChatError>;
}
