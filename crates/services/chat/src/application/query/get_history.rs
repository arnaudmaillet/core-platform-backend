use std::sync::Arc;

use cqrs::{Envelope, Query, QueryHandler};
use uuid::Uuid;

use crate::application::port::{
    ConversationRepository, MemberRepository, MessageRepository, MessageSummary,
};
use crate::domain::value_object::{ConversationId, ProfileId};
use crate::error::ChatError;

pub struct MessagePage {
    pub messages:        Vec<MessageSummary>,
    pub next_page_token: Option<String>,
}

/// Reads a page of conversation history for `requester_id`.
///
/// Visibility resolution is the crux:
/// - a **member** reads the full history (`floor = None`);
/// - a **non-member** of a **public** conversation reads only from the
///   public-since watermark (`floor = public_since`), pushed into ScyllaDB as a
///   server-side `created_at >= ?` predicate;
/// - a non-member of a **private** conversation gets
///   [`ChatError::ConversationConcealed`], indistinguishable from a missing
///   conversation.
pub struct GetHistoryQuery {
    pub conversation_id: String,
    pub requester_id:    String,
    pub limit:           i32,
    /// Opaque cursor from the previous `next_page_token`.
    /// Encoding: `"{created_at_ms}_{message_id}"`.
    pub page_token:      Option<String>,
}

impl Query for GetHistoryQuery {
    type Response = MessagePage;
}

pub struct GetHistoryHandler<CR, MR, MSG> {
    pub conversation_repo: Arc<CR>,
    pub member_repo:       Arc<MR>,
    pub message_repo:      Arc<MSG>,
    pub max_page_size:     i32,
}

impl<CR, MR, MSG> QueryHandler<GetHistoryQuery> for GetHistoryHandler<CR, MR, MSG>
where
    CR:  ConversationRepository,
    MR:  MemberRepository,
    MSG: MessageRepository,
{
    type Error = ChatError;

    async fn handle(&self, envelope: Envelope<GetHistoryQuery>) -> Result<MessagePage, ChatError> {
        let q = &envelope.payload;

        let conversation_id = ConversationId::try_from(q.conversation_id.as_str())?;
        let requester_id    = ProfileId::try_from(q.requester_id.as_str())?;
        let limit           = q.limit.min(self.max_page_size).max(1);
        let cursor          = q.page_token.as_deref().map(decode_cursor).transpose()?;

        let conversation = self
            .conversation_repo
            .find(&conversation_id)
            .await?
            .ok_or_else(|| ChatError::ConversationNotFound {
                conversation_id: conversation_id.as_str(),
            })?;

        let is_member = self
            .member_repo
            .find(&conversation_id, &requester_id)
            .await?
            .is_some();

        // Members: full history. Non-members: audience read gated by the watermark.
        let floor_created_at_ms = if is_member {
            None
        } else if conversation.visibility().is_public() {
            // Public conversations always carry a watermark; absence is treated as
            // "no visible history" rather than a leak.
            match conversation.public_since().and_then(|w| w.timestamp_ms()) {
                Some(ms) => Some(ms),
                None => {
                    return Ok(MessagePage { messages: Vec::new(), next_page_token: None });
                }
            }
        } else {
            return Err(conversation.concealed());
        };

        let (messages, next_cursor) = self
            .message_repo
            .list_history(&conversation_id, limit, cursor, floor_created_at_ms)
            .await?;

        let next_page_token = next_cursor.map(|(ts, id)| encode_cursor(ts, id));

        Ok(MessagePage { messages, next_page_token })
    }
}

/// Decodes a page cursor. Format: `"{created_at_ms}_{message_id}"`.
fn decode_cursor(token: &str) -> Result<(i64, Uuid), ChatError> {
    let (ts_str, id_str) = token
        .split_once('_')
        .ok_or_else(|| ChatError::InvalidPageToken { token: token.to_owned() })?;

    let ts = ts_str
        .parse::<i64>()
        .map_err(|_| ChatError::InvalidPageToken { token: token.to_owned() })?;

    let id = Uuid::parse_str(id_str)
        .map_err(|_| ChatError::InvalidPageToken { token: token.to_owned() })?;

    Ok((ts, id))
}

/// Encodes a cursor from the last row of a page.
pub fn encode_cursor(created_at_ms: i64, message_id: Uuid) -> String {
    format!("{created_at_ms}_{message_id}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::command::fakes::{FakeConversations, FakeMembers, FakeMessages, Fixture};

    async fn history(
        f: &Fixture,
        conversation_id: ConversationId,
        requester: ProfileId,
        messages: &Arc<FakeMessages>,
    ) -> Result<MessagePage, ChatError> {
        let handler: GetHistoryHandler<FakeConversations, FakeMembers, FakeMessages> = GetHistoryHandler {
            conversation_repo: Arc::clone(&f.conversations),
            member_repo:       Arc::clone(&f.members),
            message_repo:      Arc::clone(messages),
            max_page_size:     50,
        };
        handler
            .handle(Envelope::new(Uuid::now_v7(), GetHistoryQuery {
                conversation_id: conversation_id.as_str(),
                requester_id:    requester.as_str(),
                limit:           20,
                page_token:      None,
            }))
            .await
    }

    #[tokio::test]
    async fn outsider_of_a_private_conversation_answers_like_a_missing_one() {
        let f = Fixture::private_group();
        let messages = Arc::default();
        let outsider = Fixture::profile();

        let err = history(&f, f.conversation_id, outsider, &messages).await.err().unwrap();
        assert!(matches!(err, ChatError::ConversationConcealed { .. }), "{err:?}");
        let err = history(&f, ConversationId::new(), outsider, &messages).await.err().unwrap();
        assert!(matches!(err, ChatError::ConversationNotFound { .. }), "{err:?}");
        assert!(messages.floors().is_empty(), "no history read for a refused caller");
    }

    #[tokio::test]
    async fn member_reads_full_history_and_outsider_of_public_reads_from_the_watermark() {
        let f = Fixture::public_group();
        let messages = Arc::default();

        history(&f, f.conversation_id, f.owner, &messages).await.unwrap();
        history(&f, f.conversation_id, Fixture::profile(), &messages).await.unwrap();

        let floors = messages.floors();
        assert_eq!(floors[0], None, "member: full history");
        assert!(floors[1].is_some(), "audience: gated by public_since");
    }
}
