use std::sync::Arc;

use async_trait::async_trait;
use scylla::value::CqlTimestamp;
use scylla::DeserializeRow;
use scylla_storage::ScyllaClient;
use uuid::Uuid;

use crate::application::port::InvitationRepository;
use crate::domain::aggregate::Invitation;
use crate::domain::value_object::{ConversationId, ProfileId};
use crate::error::ChatError;
use crate::infrastructure::persistence::statement::{row_err, scylla_err, strict};
use crate::infrastructure::persistence::time::{to_cql, to_utc};

/// Column order MUST match the SELECT list (`enforce_order`).
#[derive(Debug, DeserializeRow)]
#[scylla(flavor = "enforce_order")]
struct InvitationRow {
    invitee_id: Uuid,
    inviter_id: Uuid,
    invited_at: CqlTimestamp,
}

/// ScyllaDB adapter for pending invitations (`chat.invitations_by_conversation`).
///
/// Every statement runs on the **Strict** profile: the invitation is written by
/// one caller (the admin) and read by another (the invitee), so the join-path
/// read must observe the admin's quorum write.
pub struct ScyllaInvitationRepository {
    client: Arc<ScyllaClient>,
}

impl ScyllaInvitationRepository {
    pub fn new(client: Arc<ScyllaClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl InvitationRepository for ScyllaInvitationRepository {
    async fn upsert(
        &self,
        conversation_id: &ConversationId,
        invitation:      &Invitation,
    ) -> Result<(), ChatError> {
        // The table's default_time_to_live (7 days) applies to every write, so a
        // re-invite restarts the expiry.
        let stmt = strict(
            &self.client,
            "INSERT INTO chat.invitations_by_conversation \
             (conversation_id, invitee_id, inviter_id, invited_at) VALUES (?, ?, ?, ?)",
        );
        self.client
            .session
            .execute_unpaged(
                stmt,
                (
                    conversation_id.as_uuid(),
                    invitation.invitee_id().as_uuid(),
                    invitation.inviter_id().as_uuid(),
                    to_cql(invitation.invited_at()),
                ),
            )
            .await
            .map_err(scylla_err)?;
        Ok(())
    }

    async fn find(
        &self,
        conversation_id: &ConversationId,
        invitee_id:      &ProfileId,
    ) -> Result<Option<Invitation>, ChatError> {
        let stmt = strict(
            &self.client,
            "SELECT invitee_id, inviter_id, invited_at FROM chat.invitations_by_conversation \
             WHERE conversation_id = ? AND invitee_id = ?",
        );

        let row = self
            .client
            .session
            .execute_unpaged(stmt, (conversation_id.as_uuid(), invitee_id.as_uuid()))
            .await
            .map_err(scylla_err)?
            .into_rows_result()
            .map_err(|e| row_err("invitation.find:rows", e))?
            .rows::<InvitationRow>()
            .map_err(|e| row_err("invitation.find:iter", e))?
            .next();

        let Some(row) = row else { return Ok(None) };
        let row = row.map_err(|e| row_err("invitation.find:deser", e))?;

        Ok(Some(Invitation::reconstitute(
            ProfileId::from_uuid(row.invitee_id),
            ProfileId::from_uuid(row.inviter_id),
            to_utc(row.invited_at),
        )))
    }

    async fn delete(
        &self,
        conversation_id: &ConversationId,
        invitee_id:      &ProfileId,
    ) -> Result<(), ChatError> {
        let stmt = strict(
            &self.client,
            "DELETE FROM chat.invitations_by_conversation \
             WHERE conversation_id = ? AND invitee_id = ?",
        );
        self.client
            .session
            .execute_unpaged(stmt, (conversation_id.as_uuid(), invitee_id.as_uuid()))
            .await
            .map_err(scylla_err)?;
        Ok(())
    }
}
