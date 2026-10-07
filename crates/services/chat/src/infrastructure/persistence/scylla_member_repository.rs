use std::sync::Arc;

use async_trait::async_trait;
use scylla_storage::ScyllaClient;

use crate::application::port::{MemberRepository, Membership};
use crate::domain::aggregate::Participant;
use crate::domain::value_object::{ConversationId, MessageId, ProfileId, Role};
use crate::error::ChatError;
use crate::infrastructure::persistence::model::MemberRow;
use crate::infrastructure::persistence::statement::{fast, lwt_applied, row_err, scylla_err, strict, strict_batch};
use crate::infrastructure::persistence::time::{to_cql, to_utc};

const MEMBER_COLS: &str = "member_id, role, joined_at, last_read, muted_until";

/// ScyllaDB adapter for the bounded Member Plane roster
/// (`chat.members_by_conversation`).
pub struct ScyllaMemberRepository {
    client: Arc<ScyllaClient>,
}

impl ScyllaMemberRepository {
    pub fn new(client: Arc<ScyllaClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl MemberRepository for ScyllaMemberRepository {
    async fn insert(
        &self,
        conversation_id: &ConversationId,
        p:               &Participant,
    ) -> Result<(), ChatError> {
        // The roster and its reverse index together (a logged batch, #653).
        let mut batch = strict_batch(&self.client);
        batch.append_statement(
            "INSERT INTO chat.members_by_conversation \
             (conversation_id, member_id, role, joined_at, last_read) \
             VALUES (?, ?, ?, ?, ?)",
        );
        batch.append_statement(
            "INSERT INTO chat.conversations_by_member (member_id, conversation_id, role, joined_at, left_at) \
             VALUES (?, ?, ?, ?, null)",
        );
        let values = (
            (
                conversation_id.as_uuid(),
                p.profile_id().as_uuid(),
                p.role().as_tinyint(),
                to_cql(p.joined_at()),
                p.last_read().map(|m| m.as_uuid()),
            ),
            (p.profile_id().as_uuid(), conversation_id.as_uuid(), p.role().as_tinyint(), to_cql(p.joined_at())),
        );
        self.client.session.batch(&batch, values).await.map_err(scylla_err)?;
        Ok(())
    }

    async fn find(
        &self,
        conversation_id: &ConversationId,
        member_id:       &ProfileId,
    ) -> Result<Option<Participant>, ChatError> {
        let stmt = fast(
            &self.client,
            &format!(
                "SELECT {MEMBER_COLS} FROM chat.members_by_conversation \
                 WHERE conversation_id = ? AND member_id = ?"
            ),
        );

        let row = self
            .client
            .session
            .execute_unpaged(stmt, (conversation_id.as_uuid(), member_id.as_uuid()))
            .await
            .map_err(scylla_err)?
            .into_rows_result()
            .map_err(|e| row_err("member.find:rows", e))?
            .rows::<MemberRow>()
            .map_err(|e| row_err("member.find:iter", e))?
            .next();

        let Some(row) = row else { return Ok(None) };
        let row = row.map_err(|e| row_err("member.find:deser", e))?;

        Ok(Some(participant_from_row(row)?))
    }

    async fn update_last_read(
        &self,
        conversation_id: &ConversationId,
        member_id:       &ProfileId,
        last_read:       MessageId,
    ) -> Result<(), ChatError> {
        let stmt = strict(
            &self.client,
            "UPDATE chat.members_by_conversation SET last_read = ? \
             WHERE conversation_id = ? AND member_id = ?",
        );
        self.client
            .session
            .execute_unpaged(
                stmt,
                (last_read.as_uuid(), conversation_id.as_uuid(), member_id.as_uuid()),
            )
            .await
            .map_err(scylla_err)?;
        Ok(())
    }

    async fn set_muted_until(
        &self,
        conversation_id: &ConversationId,
        member_id:       &ProfileId,
        muted_until:     Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<bool, ChatError> {
        // `IF EXISTS`: a plain UPDATE racing a departure would leave a roster
        // row with no role, which every roster read would then fail on.
        let stmt = strict(
            &self.client,
            "UPDATE chat.members_by_conversation SET muted_until = ? \
             WHERE conversation_id = ? AND member_id = ? IF EXISTS",
        );
        let result = self
            .client
            .session
            .execute_unpaged(stmt, (muted_until.map(to_cql), conversation_id.as_uuid(), member_id.as_uuid()))
            .await
            .map_err(scylla_err)?;
        lwt_applied(result.into_rows_result().map_err(|e| row_err("member.mute:rows", e))?, "member.mute:deser")
    }

    async fn list(
        &self,
        conversation_id: &ConversationId,
    ) -> Result<Vec<Participant>, ChatError> {
        // Single bounded partition (<= 500 rows) — a full clustering scan is safe.
        let stmt = fast(
            &self.client,
            &format!(
                "SELECT {MEMBER_COLS} FROM chat.members_by_conversation \
                 WHERE conversation_id = ?"
            ),
        );

        let rows = self
            .client
            .session
            .execute_unpaged(stmt, (conversation_id.as_uuid(),))
            .await
            .map_err(scylla_err)?
            .into_rows_result()
            .map_err(|e| row_err("member.list:rows", e))?
            .rows::<MemberRow>()
            .map_err(|e| row_err("member.list:iter", e))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| row_err("member.list:deser", e))?;

        rows.into_iter().map(participant_from_row).collect()
    }

    async fn leave(
        &self,
        conversation_id: &ConversationId,
        p:               &Participant,
        at:              chrono::DateTime<chrono::Utc>,
    ) -> Result<(), ChatError> {
        let mut batch = strict_batch(&self.client);
        batch.append_statement("DELETE FROM chat.members_by_conversation WHERE conversation_id = ? AND member_id = ?");
        batch.append_statement(
            "INSERT INTO chat.conversations_by_member (member_id, conversation_id, role, joined_at, left_at) \
             VALUES (?, ?, ?, ?, ?)",
        );
        let values = (
            (conversation_id.as_uuid(), p.profile_id().as_uuid()),
            (p.profile_id().as_uuid(), conversation_id.as_uuid(), p.role().as_tinyint(), to_cql(p.joined_at()), to_cql(at)),
        );
        self.client.session.batch(&batch, values).await.map_err(scylla_err)?;
        Ok(())
    }

    async fn find_membership(
        &self,
        member_id:       &ProfileId,
        conversation_id: &ConversationId,
    ) -> Result<Option<Membership>, ChatError> {
        let stmt = strict(
            &self.client,
            "SELECT conversation_id, role, joined_at, left_at FROM chat.conversations_by_member \
             WHERE member_id = ? AND conversation_id = ?",
        );
        let row = self
            .client
            .session
            .execute_unpaged(stmt, (member_id.as_uuid(), conversation_id.as_uuid()))
            .await
            .map_err(scylla_err)?
            .into_rows_result()
            .map_err(|e| row_err("member.find_membership:rows", e))?
            .maybe_first_row::<MembershipRow>()
            .map_err(|e| row_err("member.find_membership:deser", e))?;
        row.map(membership_from_row).transpose()
    }

    async fn delete(
        &self,
        conversation_id: &ConversationId,
        member_id:       &ProfileId,
    ) -> Result<(), ChatError> {
        let mut batch = strict_batch(&self.client);
        batch.append_statement("DELETE FROM chat.members_by_conversation WHERE conversation_id = ? AND member_id = ?");
        batch.append_statement("DELETE FROM chat.conversations_by_member WHERE member_id = ? AND conversation_id = ?");
        let values =
            ((conversation_id.as_uuid(), member_id.as_uuid()), (member_id.as_uuid(), conversation_id.as_uuid()));
        self.client.session.batch(&batch, values).await.map_err(scylla_err)?;
        Ok(())
    }

    async fn list_by_member(
        &self,
        member_id: &ProfileId,
        limit:     i32,
        after:     Option<&ConversationId>,
    ) -> Result<Vec<Membership>, ChatError> {
        let limit = limit.clamp(1, 500);
        let result = match after {
            Some(after) => {
                let stmt = fast(
                    &self.client,
                    "SELECT conversation_id, role, joined_at, left_at FROM chat.conversations_by_member \
                     WHERE member_id = ? AND conversation_id > ? LIMIT ?",
                );
                self.client.session.execute_unpaged(stmt, (member_id.as_uuid(), after.as_uuid(), limit)).await
            }
            None => {
                let stmt = fast(
                    &self.client,
                    "SELECT conversation_id, role, joined_at, left_at FROM chat.conversations_by_member WHERE member_id = ? LIMIT ?",
                );
                self.client.session.execute_unpaged(stmt, (member_id.as_uuid(), limit)).await
            }
        }
        .map_err(scylla_err)?;
        result
            .into_rows_result()
            .map_err(|e| row_err("member.list_by_member:rows", e))?
            .rows::<MembershipRow>()
            .map_err(|e| row_err("member.list_by_member:iter", e))?
            .map(|row| membership_from_row(row.map_err(|e| row_err("member.list_by_member:deser", e))?))
            .collect()
    }

    async fn backfill_member_index(&self) -> Result<u64, ChatError> {
        let mut scan = fast(
            &self.client,
            "SELECT conversation_id, member_id, role, joined_at FROM chat.members_by_conversation",
        );
        scan.set_page_size(BACKFILL_PAGE_SIZE);
        let mut paging = scylla::response::PagingState::start();
        let mut written = 0u64;
        loop {
            let (result, next) =
                self.client.session.execute_single_page(scan.clone(), (), paging).await.map_err(scylla_err)?;
            let rows = result
                .into_rows_result()
                .map_err(|e| row_err("member.backfill:rows", e))?
                .rows::<(uuid::Uuid, uuid::Uuid, i8, scylla::value::CqlTimestamp)>()
                .map_err(|e| row_err("member.backfill:iter", e))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| row_err("member.backfill:deser", e))?;
            for (conversation_id, member_id, role, joined_at) in rows {
                let stmt = strict(
                    &self.client,
                    "INSERT INTO chat.conversations_by_member (member_id, conversation_id, role, joined_at, left_at) \
                     VALUES (?, ?, ?, ?, null)",
                );
                self.client
                    .session
                    .execute_unpaged(stmt, (member_id, conversation_id, role, joined_at))
                    .await
                    .map_err(scylla_err)?;
                written += 1;
            }
            match next.into_paging_control_flow() {
                std::ops::ControlFlow::Continue(state) => paging = state,
                std::ops::ControlFlow::Break(()) => return Ok(written),
            }
        }
    }
}

/// Rows per page when scanning the rosters to backfill the member index.
const BACKFILL_PAGE_SIZE: i32 = 500;

/// A `conversations_by_member` row: `(conversation_id, role, joined_at, left_at)`.
type MembershipRow = (uuid::Uuid, i8, scylla::value::CqlTimestamp, Option<scylla::value::CqlTimestamp>);

fn membership_from_row((conversation_id, role, joined_at, left_at): MembershipRow) -> Result<Membership, ChatError> {
    Ok(Membership {
        conversation_id: ConversationId::from_uuid(conversation_id),
        role:            Role::try_from(role)?,
        joined_at:       to_utc(joined_at),
        left_at:         left_at.map(to_utc),
    })
}

fn participant_from_row(row: MemberRow) -> Result<Participant, ChatError> {
    Ok(Participant::reconstitute(
        ProfileId::from_uuid(row.member_id),
        Role::try_from(row.role)?,
        to_utc(row.joined_at),
        row.last_read.map(MessageId::from_uuid),
    )
    .with_muted_until(row.muted_until.map(to_utc)))
}
