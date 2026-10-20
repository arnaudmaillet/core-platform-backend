use std::sync::Arc;

use cqrs::{Envelope, Query, QueryHandler};
use uuid::Uuid;

use crate::application::port::{
    ConversationRepository, MemberRepository, MessageRepository, MessageSummary,
};
use crate::application::sync::{SyncKind, Watermark};
use crate::domain::value_object::{ConversationId, ConversationKind, ProfileId};
use crate::error::ChatError;

pub struct MessagePage {
    pub messages:        Vec<MessageSummary>,
    pub next_page_token: Option<String>,
    /// `GetHistory`'s first page only (#930): the watermark to sync the thread
    /// from (taken before reading).
    pub sync_watermark:  Option<String>,
}

/// What is new in a thread since a watermark (#930): messages are
/// append-only, so new messages and the members' read positions.
pub struct SyncConversationQuery {
    pub conversation_id: String,
    pub requester_id:    String,
    /// From `GetHistory`'s first page, or a previous sync.
    pub watermark:       String,
    pub limit:           i32,
}

pub struct ThreadSync {
    /// Oldest first.
    pub messages:      Vec<MessageSummary>,
    /// Each member's read position now — a direct or group conversation, to
    /// a member (a channel keeps no receipts for its readers).
    pub read_receipts: Vec<(ProfileId, Option<crate::domain::value_object::MessageId>)>,
    pub watermark:     String,
    /// More messages wait: sync again at once from `watermark`.
    pub has_more:      bool,
}

impl Query for SyncConversationQuery {
    type Response = ThreadSync;
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
        let sync_watermark  = cursor.is_none().then(|| Watermark::upper(chrono::Utc::now()).encode(SyncKind::Thread));

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
                    return Ok(MessagePage { messages: Vec::new(), next_page_token: None, sync_watermark });
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

        Ok(MessagePage { messages, next_page_token, sync_watermark })
    }
}

impl<CR, MR, MSG> QueryHandler<SyncConversationQuery> for GetHistoryHandler<CR, MR, MSG>
where
    CR:  ConversationRepository,
    MR:  MemberRepository,
    MSG: MessageRepository,
{
    type Error = ChatError;

    async fn handle(&self, envelope: Envelope<SyncConversationQuery>) -> Result<ThreadSync, ChatError> {
        let q = &envelope.payload;
        let conversation_id = ConversationId::try_from(q.conversation_id.as_str())?;
        let requester_id = ProfileId::try_from(q.requester_id.as_str())?;
        let now = chrono::Utc::now();
        let after = Watermark::decode(&q.watermark, SyncKind::Thread, now)?;
        let upper = Watermark::upper(now).max_with(after);
        let limit = q.limit.min(self.max_page_size).max(1);
        let conversation = self
            .conversation_repo
            .find(&conversation_id)
            .await?
            .ok_or_else(|| ChatError::ConversationNotFound { conversation_id: conversation_id.as_str() })?;
        let is_member = self.member_repo.find(&conversation_id, &requester_id).await?.is_some();
        // The same audience rules as GetHistory.
        let floor = if is_member {
            None
        } else if conversation.visibility().is_public() {
            match conversation.public_since().and_then(|w| w.timestamp_ms()) {
                Some(ms) => Some(ms),
                None => return Ok(ThreadSync { messages: Vec::new(), read_receipts: Vec::new(), watermark: upper.encode(SyncKind::Thread), has_more: false }),
            }
        } else {
            return Err(conversation.concealed());
        };
        let (mut messages, has_more) = self
            .message_repo
            .list_since(&conversation_id, (after.ms, after.id), (upper.ms, upper.id), floor, limit)
            .await?;
        let next = match messages.last() {
            Some(last) if has_more => Watermark { ms: last.created_at.timestamp_millis(), id: last.message_id },
            _ => upper,
        };
        // A withheld message is its sender's alone (#656).
        messages.retain(|m| m.visible_to(requester_id.as_uuid()));
        let read_receipts = if is_member && conversation.kind() != ConversationKind::Channel {
            self.member_repo
                .list(&conversation_id)
                .await?
                .into_iter()
                .map(|p| (p.profile_id(), p.last_read()))
                .collect()
        } else {
            Vec::new()
        };
        Ok(ThreadSync { messages, read_receipts, watermark: next.encode(SyncKind::Thread), has_more })
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
        Ok(MessagePage { messages, next_page_token: next_cursor.map(|(ts, id)| encode_cursor(ts, id)), sync_watermark: None })
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
            media_asset_id: None,
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

    /// #930: what is new since a watermark, oldest first and inside the
    /// grace window, withheld ones only to their sender; the members' read
    /// positions with them.
    #[tokio::test]
    async fn a_member_syncs_the_new_messages_and_the_read_positions() {
        use crate::application::port::MessageSummary;
        use crate::domain::value_object::{ContentType, Role};

        let f = Fixture::private_group();
        let me = f.add_member(Role::Member);
        let now = chrono::Utc::now().timestamp_millis();
        let at = |ms| chrono::DateTime::from_timestamp_millis(ms).unwrap();
        let message = |sender: ProfileId, ms, body: &str, withheld| MessageSummary {
            message_id: Uuid::now_v7(),
            sender_id: sender.as_uuid(),
            content_type: ContentType::Text,
            body: body.into(),
            media_ref: None,
            media_asset_id: None,
            reply_to: None,
            created_at: at(ms),
            withheld,
        };
        let messages = Arc::<FakeMessages>::default();
        messages.seed(vec![
            message(f.owner, now - 600_000, "already known", false),
            message(f.owner, now - 60_000, "new", false),
            message(f.owner, now - 50_000, "withheld", true),
            message(me, now - 40_000, "mine", false),
            message(f.owner, now - 1_000, "too fresh", false),
        ]);
        let handler: GetHistoryHandler<FakeConversations, FakeMembers, FakeMessages> = GetHistoryHandler {
            conversation_repo: Arc::clone(&f.conversations),
            member_repo:       Arc::clone(&f.members),
            message_repo:      Arc::clone(&messages),
            max_page_size:     50,
        };
        let sync = |requester: ProfileId, watermark: String, limit| {
            handler.handle(Envelope::new(Uuid::now_v7(), SyncConversationQuery {
                conversation_id: f.conversation_id.as_str(),
                requester_id: requester.as_str(),
                watermark,
                limit,
            }))
        };
        let since = Watermark { ms: now - 120_000, id: Uuid::nil() }.encode(SyncKind::Thread);

        let page = sync(me, since.clone(), 20).await.unwrap();
        let bodies: Vec<_> = page.messages.iter().map(|m| m.body.as_str()).collect();
        assert_eq!(bodies, vec!["new", "mine"]);
        assert!(!page.has_more);
        let mut readers: Vec<_> = page.read_receipts.iter().map(|(p, _)| *p).collect();
        readers.sort_by_key(ProfileId::as_uuid);
        let mut members = vec![f.owner, me];
        members.sort_by_key(ProfileId::as_uuid);
        assert_eq!(readers, members);
        assert!(sync(me, page.watermark, 20).await.unwrap().messages.is_empty(), "nothing since");

        // A full page resumes after its last message.
        let first = sync(me, since, 1).await.unwrap();
        assert!(first.has_more);
        assert_eq!(first.messages[0].body, "new");
        let second = sync(me, first.watermark, 1).await.unwrap();
        assert!(second.messages.is_empty(), "the withheld one is filtered out of its page");
        assert!(second.has_more);
        assert_eq!(sync(me, second.watermark, 1).await.unwrap().messages[0].body, "mine");

        // An outsider of a private conversation is told nothing.
        let fresh = Watermark { ms: now, id: Uuid::nil() }.encode(SyncKind::Thread);
        let err = sync(Fixture::profile(), fresh, 20).await.err().unwrap();
        assert!(matches!(err, ChatError::ConversationConcealed { .. }), "{err:?}");
    }
}
