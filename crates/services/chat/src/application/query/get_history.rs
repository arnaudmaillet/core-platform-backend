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

        let (mut messages, next_cursor) = self
            .message_repo
            .list_history(&conversation_id, limit, cursor, floor_created_at_ms)
            .await?;
        // A withheld message is its sender's alone (#656). The cursor stays
        // the store's, so a page may come back short, never skip.
        messages.retain(|m| m.visible_to(requester_id.as_uuid()));

        let next_page_token = next_cursor.map(|(ts, id)| encode_cursor(ts, id));

        Ok(MessagePage { messages, next_page_token })
    }
}

/// A profile's view of a conversation it is or was a member of, for the GDPR
/// data export (#653 / #656) — **mesh only**. Its own messages in full;
/// everyone else's reduced to their time (`sender_id` nil, no content), as the
/// export shows them; withheld ones from others not at all. A former member
/// reads up to its departure (`left_at`), nothing after.
pub struct FormerMemberHistoryQuery {
    pub conversation_id: String,
    pub member_id:       String,
    pub limit:           i32,
    pub page_token:      Option<String>,
}

impl Query for FormerMemberHistoryQuery {
    type Response = MessagePage;
}

impl<CR, MR, MSG> QueryHandler<FormerMemberHistoryQuery> for GetHistoryHandler<CR, MR, MSG>
where
    CR:  ConversationRepository,
    MR:  MemberRepository,
    MSG: MessageRepository,
{
    type Error = ChatError;

    async fn handle(&self, envelope: Envelope<FormerMemberHistoryQuery>) -> Result<MessagePage, ChatError> {
        let q = &envelope.payload;
        let conversation_id = ConversationId::try_from(q.conversation_id.as_str())?;
        let member = ProfileId::try_from(q.member_id.as_str())?;
        let limit = q.limit.min(self.max_page_size).max(1);
        let membership = self
            .member_repo
            .find_membership(&member, &conversation_id)
            .await?
            .ok_or_else(|| ChatError::ConversationNotFound { conversation_id: conversation_id.as_str() })?;
        // The first page starts at the departure; later pages are older still.
        let cursor = match q.page_token.as_deref() {
            Some(token) => Some(decode_cursor(token)?),
            None => membership.left_at.map(|at| (at.timestamp_millis() + 1, Uuid::nil())),
        };
        let (messages, next_cursor) = self.message_repo.list_history(&conversation_id, limit, cursor, None).await?;
        let me = member.as_uuid();
        let messages = messages
            .into_iter()
            .filter(|m| m.visible_to(me))
            .map(|m| if m.sender_id == me { m } else { m.reduced_to_its_time() })
            .collect();
        Ok(MessagePage { messages, next_page_token: next_cursor.map(|(ts, id)| encode_cursor(ts, id)) })
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

    /// The export's read of a left group: its own messages in full, others'
    /// reduced to their time, withheld ones from others absent, nothing after
    /// the departure.
    #[tokio::test]
    async fn a_former_member_reads_up_to_its_departure_its_own_in_full() {
        use crate::application::port::{MemberRepository, MessageSummary};
        use crate::domain::aggregate::Participant;
        use crate::domain::value_object::{ContentType, Role};

        let f = Fixture::private_group();
        let me = f.add_member(Role::Member);
        let other = f.owner;
        let at = |ms| chrono::DateTime::from_timestamp_millis(ms).unwrap();
        let message = |sender: ProfileId, ms, body: &str, withheld| MessageSummary {
            message_id: Uuid::now_v7(),
            sender_id: sender.as_uuid(),
            content_type: ContentType::Text,
            body: body.into(),
            media_ref: None,
            reply_to: None,
            created_at: at(ms),
            withheld,
        };
        let messages = Arc::<FakeMessages>::default();
        messages.seed(vec![
            message(me, 1_000, "mine", false),
            message(other, 2_000, "theirs", false),
            message(other, 2_500, "withheld", true),
            message(other, 4_000, "after I left", false),
        ]);
        let participant = Participant::reconstitute(me, Role::Member, at(0), None);
        f.members.leave(&f.conversation_id, &participant, at(3_000)).await.unwrap();

        let handler: GetHistoryHandler<FakeConversations, FakeMembers, FakeMessages> = GetHistoryHandler {
            conversation_repo: Arc::clone(&f.conversations),
            member_repo:       Arc::clone(&f.members),
            message_repo:      Arc::clone(&messages),
            max_page_size:     50,
        };
        let query = FormerMemberHistoryQuery {
            conversation_id: f.conversation_id.as_str(),
            member_id:       me.as_str(),
            limit:           20,
            page_token:      None,
        };
        let page = handler.handle(Envelope::new(Uuid::now_v7(), query)).await.unwrap();
        let seen: Vec<_> = page.messages.iter().map(|m| (m.sender_id, m.body.as_str())).collect();
        assert_eq!(seen, vec![(Uuid::nil(), ""), (me.as_uuid(), "mine")]);

        let stranger = FormerMemberHistoryQuery {
            conversation_id: f.conversation_id.as_str(),
            member_id:       Fixture::profile().as_str(),
            limit:           20,
            page_token:      None,
        };
        assert!(handler.handle(Envelope::new(Uuid::now_v7(), stranger)).await.is_err(), "never a member");
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
