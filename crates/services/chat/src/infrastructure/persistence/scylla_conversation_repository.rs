use std::sync::Arc;

use async_trait::async_trait;
use scylla_storage::ScyllaClient;

use crate::application::port::ConversationRepository;
use crate::domain::aggregate::{Conversation, Direct};
use crate::domain::value_object::{
    ConversationId, ConversationKind, MessageId, MessageRequest, ProfileId, Visibility,
};
use crate::error::ChatError;
use crate::infrastructure::persistence::model::ConversationRow;
use crate::infrastructure::persistence::statement::{fast, row_err, scylla_err, strict, lwt_applied};
use crate::infrastructure::persistence::time::{to_cql, to_utc};

/// ScyllaDB adapter for the [`Conversation`] aggregate (`chat.conversations`).
pub struct ScyllaConversationRepository {
    client: Arc<ScyllaClient>,
}

impl ScyllaConversationRepository {
    pub fn new(client: Arc<ScyllaClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl ConversationRepository for ScyllaConversationRepository {
    async fn insert(&self, c: &Conversation) -> Result<(), ChatError> {
        let stmt = strict(
            &self.client,
            "INSERT INTO chat.conversations \
             (conversation_id, kind, visibility, owner_id, member_count, public_since, \
              created_at, updated_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        );
        self.client
            .session
            .execute_unpaged(
                stmt,
                (
                    c.id().as_uuid(),
                    c.kind().as_tinyint(),
                    c.visibility().as_tinyint(),
                    c.owner_id().as_uuid(),
                    c.member_count() as i32,
                    c.public_since().map(|m| m.as_uuid()),
                    to_cql(c.created_at()),
                    to_cql(c.updated_at()),
                ),
            )
            .await
            .map_err(scylla_err)?;
        Ok(())
    }

    async fn update(&self, c: &Conversation) -> Result<(), ChatError> {
        // Only mutable columns are rewritten; kind/owner_id/created_at are immutable.
        let stmt = strict(
            &self.client,
            "UPDATE chat.conversations \
             SET visibility = ?, public_since = ?, member_count = ?, updated_at = ? \
             WHERE conversation_id = ?",
        );
        self.client
            .session
            .execute_unpaged(
                stmt,
                (
                    c.visibility().as_tinyint(),
                    c.public_since().map(|m| m.as_uuid()),
                    c.member_count() as i32,
                    to_cql(c.updated_at()),
                    c.id().as_uuid(),
                ),
            )
            .await
            .map_err(scylla_err)?;
        Ok(())
    }

    async fn find(&self, id: &ConversationId) -> Result<Option<Conversation>, ChatError> {
        let stmt = fast(
            &self.client,
            "SELECT conversation_id, kind, visibility, owner_id, member_count, public_since, \
                    created_at, updated_at, peer_id, request_state, requester_id, declined_at \
             FROM chat.conversations WHERE conversation_id = ?",
        );

        let row = self
            .client
            .session
            .execute_unpaged(stmt, (id.as_uuid(),))
            .await
            .map_err(scylla_err)?
            .into_rows_result()
            .map_err(|e| row_err("conversation.find:rows", e))?
            .rows::<ConversationRow>()
            .map_err(|e| row_err("conversation.find:iter", e))?
            .next();

        let Some(row) = row else { return Ok(None) };
        let row = row.map_err(|e| row_err("conversation.find:deser", e))?;
        let direct = row.peer_id.map(|peer| Direct {
            peer:    ProfileId::from_uuid(peer),
            request: MessageRequest::from_columns(
                row.request_state,
                row.requester_id.map(ProfileId::from_uuid),
                row.declined_at.map(to_utc),
            ),
        });

        Ok(Some(Conversation::reconstitute(
            ConversationId::from_uuid(row.conversation_id),
            ConversationKind::try_from(row.kind)?,
            Visibility::try_from(row.visibility)?,
            ProfileId::from_uuid(row.owner_id),
            row.member_count.max(0) as u16,
            row.public_since.map(MessageId::from_uuid),
            direct,
            to_utc(row.created_at),
            to_utc(row.updated_at),
        )))
    }

    async fn claim_direct(&self, a: &ProfileId, b: &ProfileId, proposed: ConversationId) -> Result<ConversationId, ChatError> {
        let (low, high) = if a.as_uuid() < b.as_uuid() { (a.as_uuid(), b.as_uuid()) } else { (b.as_uuid(), a.as_uuid()) };
        let claim = strict(
            &self.client,
            "INSERT INTO chat.direct_conversations (low_id, high_id, conversation_id) VALUES (?, ?, ?) IF NOT EXISTS",
        );
        let result = self
            .client
            .session
            .execute_unpaged(claim, (low, high, proposed.as_uuid()))
            .await
            .map_err(scylla_err)?;
        if lwt_applied(result.into_rows_result().map_err(|e| row_err("direct.claim:rows", e))?, "direct.claim:deser")? {
            return Ok(proposed);
        }
        // Taken: the winner's id (its LWT committed at a serial quorum).
        let read = strict(
            &self.client,
            "SELECT conversation_id FROM chat.direct_conversations WHERE low_id = ? AND high_id = ?",
        );
        let row = self
            .client
            .session
            .execute_unpaged(read, (low, high))
            .await
            .map_err(scylla_err)?
            .into_rows_result()
            .map_err(|e| row_err("direct.read:rows", e))?
            .maybe_first_row::<(uuid::Uuid,)>()
            .map_err(|e| row_err("direct.read:deser", e))?;
        row.map(|(id,)| ConversationId::from_uuid(id))
            .ok_or_else(|| row_err("direct.read", "the pair's claim vanished"))
    }

    async fn insert_direct(&self, c: &Conversation) -> Result<bool, ChatError> {
        let Some(direct) = c.direct() else {
            return Err(row_err("conversation.insert_direct", "not a direct conversation"));
        };
        let request = direct.request;
        let stmt = strict(
            &self.client,
            "INSERT INTO chat.conversations \
             (conversation_id, kind, visibility, owner_id, member_count, public_since, created_at, updated_at, \
              peer_id, request_state, requester_id, request_sent) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, false) IF NOT EXISTS",
        );
        let result = self
            .client
            .session
            .execute_unpaged(
                stmt,
                (
                    c.id().as_uuid(),
                    c.kind().as_tinyint(),
                    c.visibility().as_tinyint(),
                    c.owner_id().as_uuid(),
                    c.member_count() as i32,
                    c.public_since().map(|m| m.as_uuid()),
                    to_cql(c.created_at()),
                    to_cql(c.updated_at()),
                    direct.peer.as_uuid(),
                    request.as_tinyint(),
                    request.requester().map(|r| r.as_uuid()),
                ),
            )
            .await
            .map_err(scylla_err)?;
        lwt_applied(result.into_rows_result().map_err(|e| row_err("direct.insert:rows", e))?, "direct.insert:deser")
    }

    async fn transition_request(&self, id: &ConversationId, from: MessageRequest, to: MessageRequest) -> Result<bool, ChatError> {
        let declined_at = match to {
            MessageRequest::Declined { at, .. } => Some(to_cql(at)),
            _ => None,
        };
        // Leaving a decline is a fresh request: its one message unused.
        let cql = if matches!(from, MessageRequest::Declined { .. }) {
            "UPDATE chat.conversations SET request_state = ?, requester_id = ?, declined_at = ?, request_sent = false, \
             updated_at = ? WHERE conversation_id = ? IF request_state = ?"
        } else {
            "UPDATE chat.conversations SET request_state = ?, requester_id = ?, declined_at = ?, \
             updated_at = ? WHERE conversation_id = ? IF request_state = ?"
        };
        let result = self
            .client
            .session
            .execute_unpaged(
                strict(&self.client, cql),
                (
                    to.as_tinyint(),
                    to.requester().map(|r| r.as_uuid()),
                    declined_at,
                    to_cql(chrono::Utc::now()),
                    id.as_uuid(),
                    from.as_tinyint(),
                ),
            )
            .await
            .map_err(scylla_err)?;
        lwt_applied(result.into_rows_result().map_err(|e| row_err("request.transition:rows", e))?, "request.transition:deser")
    }

    async fn claim_request_message(&self, id: &ConversationId) -> Result<bool, ChatError> {
        let stmt = strict(
            &self.client,
            "UPDATE chat.conversations SET request_sent = true WHERE conversation_id = ? IF request_sent = false",
        );
        let result = self.client.session.execute_unpaged(stmt, (id.as_uuid(),)).await.map_err(scylla_err)?;
        lwt_applied(result.into_rows_result().map_err(|e| row_err("request.claim:rows", e))?, "request.claim:deser")
    }
}

