use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::domain::value_object::{ContentType, ConversationId, ConversationKind, MessageId, ProfileId};
use crate::error::ChatError;

/// Where a conversation sits in a member's inbox (#656).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Folder {
    /// Accepted direct conversations, groups, the channels one writes in.
    Inbox,
    /// Message requests to this member, unanswered.
    Requests,
    /// The requests the member's hidden words or offensive filter catch
    /// (#810). Read-only: stored as [`Folder::Requests`], sorted at read time
    /// with the member's current filter.
    HiddenRequests,
}

impl Folder {
    /// Where it is stored (hidden requests are requests).
    pub fn as_tinyint(self) -> i8 {
        match self {
            Self::Inbox => 0,
            Self::Requests | Self::HiddenRequests => 1,
        }
    }

    /// Where its entries are stored.
    pub fn stored(self) -> Self {
        match self {
            Self::HiddenRequests => Self::Requests,
            other => other,
        }
    }

    pub fn from_tinyint(v: i8) -> Self {
        if v == 1 { Self::Requests } else { Self::Inbox }
    }
}

/// The last delivered message, as the inbox previews it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LastMessage {
    pub message_id:   MessageId,
    pub sender_id:    ProfileId,
    pub content_type: ContentType,
    /// The body's first [`PREVIEW_CHARS`] characters.
    pub preview:      String,
}

/// Characters of a message the inbox previews.
pub const PREVIEW_CHARS: usize = 100;

/// One conversation in a member's inbox.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboxEntry {
    pub conversation_id: ConversationId,
    pub kind:            ConversationKind,
    /// The other member of a direct conversation.
    pub peer:            Option<ProfileId>,
    pub folder:          Folder,
    /// Its last activity: the last message, else when the member joined.
    pub activity:        DateTime<Utc>,
    pub last:            Option<LastMessage>,
}

/// Each member's inbox, newest activity first, per folder.
#[async_trait]
pub trait InboxStore: Send + Sync + 'static {
    /// Writes `member`'s entry, moving it out of wherever it was.
    async fn put(&self, member: &ProfileId, entry: &InboxEntry) -> Result<(), ChatError>;

    /// `member`'s entry for `conversation_id`, if any.
    async fn find(&self, member: &ProfileId, conversation_id: &ConversationId) -> Result<Option<InboxEntry>, ChatError>;

    /// Takes `conversation_id` out of `member`'s inbox.
    async fn remove(&self, member: &ProfileId, conversation_id: &ConversationId) -> Result<(), ChatError>;

    /// Up to `limit` entries of `folder`, newest first, after the cursor
    /// `(activity_ms, conversation_id)` of the previous page.
    async fn list(
        &self,
        member: &ProfileId,
        folder: Folder,
        limit:  i32,
        after:  Option<(i64, ConversationId)>,
    ) -> Result<Vec<InboxEntry>, ChatError>;
}
