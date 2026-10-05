use std::sync::Arc;

use async_trait::async_trait;
use scylla::DeserializeRow;
use scylla::value::CqlTimestamp;
use scylla_storage::ScyllaClient;
use uuid::Uuid;

use crate::application::port::{Folder, InboxEntry, InboxStore, LastMessage};
use crate::domain::value_object::{ContentType, ConversationId, ConversationKind, MessageId, ProfileId};
use crate::error::ChatError;
use crate::infrastructure::persistence::statement::{fast, row_err, scylla_err, strict, strict_batch};
use crate::infrastructure::persistence::time::{to_cql, to_utc};

const COLS: &str =
    "conversation_id, folder, activity, kind, peer_id, last_message_id, last_sender_id, last_content, last_preview";

/// Either inbox table's row (same columns, same order).
#[derive(Debug, DeserializeRow)]
#[scylla(flavor = "enforce_order")]
struct InboxRow {
    conversation_id: Uuid,
    folder:          i8,
    activity:        CqlTimestamp,
    kind:            i8,
    peer_id:         Option<Uuid>,
    last_message_id: Option<Uuid>,
    last_sender_id:  Option<Uuid>,
    last_content:    Option<i8>,
    last_preview:    Option<String>,
}

impl InboxRow {
    fn into_entry(self) -> Result<InboxEntry, ChatError> {
        let last = match (self.last_message_id, self.last_sender_id) {
            (Some(message_id), Some(sender_id)) => Some(LastMessage {
                message_id:   MessageId::from_uuid(message_id),
                sender_id:    ProfileId::from_uuid(sender_id),
                content_type: ContentType::try_from(self.last_content.unwrap_or(0))?,
                preview:      self.last_preview.unwrap_or_default(),
            }),
            _ => None,
        };
        Ok(InboxEntry {
            conversation_id: ConversationId::from_uuid(self.conversation_id),
            kind:            ConversationKind::try_from(self.kind)?,
            peer:            self.peer_id.map(ProfileId::from_uuid),
            folder:          Folder::from_tinyint(self.folder),
            activity:        to_utc(self.activity),
            last,
        })
    }
}

/// Extra rows read past a cursor, for entries sharing its exact millisecond.
const TIE_SLACK: i32 = 16;

/// ScyllaDB adapter for the inbox (`chat.inbox_entries` +
/// `chat.inbox_by_activity`, migration 0011).
pub struct ScyllaInboxStore {
    client: Arc<ScyllaClient>,
}

impl ScyllaInboxStore {
    pub fn new(client: Arc<ScyllaClient>) -> Self {
        Self { client }
    }
}

type RowValues = (
    Uuid,
    i8,
    CqlTimestamp,
    Uuid,
    i8,
    Option<Uuid>,
    Option<Uuid>,
    Option<Uuid>,
    Option<i8>,
    Option<String>,
);

fn values(member: &ProfileId, e: &InboxEntry) -> RowValues {
    (
        member.as_uuid(),
        e.folder.as_tinyint(),
        to_cql(e.activity),
        e.conversation_id.as_uuid(),
        e.kind.as_tinyint(),
        e.peer.map(|p| p.as_uuid()),
        e.last.as_ref().map(|l| l.message_id.as_uuid()),
        e.last.as_ref().map(|l| l.sender_id.as_uuid()),
        e.last.as_ref().map(|l| l.content_type.as_tinyint()),
        e.last.as_ref().map(|l| l.preview.clone()),
    )
}

const INSERT_LISTING: &str = "INSERT INTO chat.inbox_by_activity \
     (member_id, folder, activity, conversation_id, kind, peer_id, last_message_id, last_sender_id, last_content, last_preview) \
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
const INSERT_ENTRY: &str = "INSERT INTO chat.inbox_entries \
     (member_id, folder, activity, conversation_id, kind, peer_id, last_message_id, last_sender_id, last_content, last_preview) \
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
const DELETE_LISTING: &str = "DELETE FROM chat.inbox_by_activity \
     WHERE member_id = ? AND folder = ? AND activity = ? AND conversation_id = ?";

#[async_trait]
impl InboxStore for ScyllaInboxStore {
    async fn put(&self, member: &ProfileId, entry: &InboxEntry) -> Result<(), ChatError> {
        let old = self.find(member, &entry.conversation_id).await?;
        // Never move back: an older fact replayed late changes nothing.
        if old.as_ref().is_some_and(|o| o.activity > entry.activity) {
            return Ok(());
        }
        let row = values(member, entry);
        let mut batch = strict_batch(&self.client);
        match old.filter(|o| o.folder != entry.folder || o.activity != entry.activity) {
            Some(old) => {
                batch.append_statement(DELETE_LISTING);
                batch.append_statement(INSERT_LISTING);
                batch.append_statement(INSERT_ENTRY);
                let delete =
                    (member.as_uuid(), old.folder.as_tinyint(), to_cql(old.activity), old.conversation_id.as_uuid());
                self.client.session.batch(&batch, (delete, row.clone(), row)).await.map_err(scylla_err)?;
            }
            None => {
                batch.append_statement(INSERT_LISTING);
                batch.append_statement(INSERT_ENTRY);
                self.client.session.batch(&batch, (row.clone(), row)).await.map_err(scylla_err)?;
            }
        }
        Ok(())
    }

    async fn find(&self, member: &ProfileId, conversation_id: &ConversationId) -> Result<Option<InboxEntry>, ChatError> {
        let stmt = strict(
            &self.client,
            &format!("SELECT {COLS} FROM chat.inbox_entries WHERE member_id = ? AND conversation_id = ?"),
        );
        let row = self
            .client
            .session
            .execute_unpaged(stmt, (member.as_uuid(), conversation_id.as_uuid()))
            .await
            .map_err(scylla_err)?
            .into_rows_result()
            .map_err(|e| row_err("inbox.find:rows", e))?
            .maybe_first_row::<InboxRow>()
            .map_err(|e| row_err("inbox.find:deser", e))?;
        row.map(InboxRow::into_entry).transpose()
    }

    async fn remove(&self, member: &ProfileId, conversation_id: &ConversationId) -> Result<(), ChatError> {
        let Some(old) = self.find(member, conversation_id).await? else { return Ok(()) };
        let mut batch = strict_batch(&self.client);
        batch.append_statement(DELETE_LISTING);
        batch.append_statement("DELETE FROM chat.inbox_entries WHERE member_id = ? AND conversation_id = ?");
        let values = (
            (member.as_uuid(), old.folder.as_tinyint(), to_cql(old.activity), conversation_id.as_uuid()),
            (member.as_uuid(), conversation_id.as_uuid()),
        );
        self.client.session.batch(&batch, values).await.map_err(scylla_err)?;
        Ok(())
    }

    async fn list(
        &self,
        member: &ProfileId,
        folder: Folder,
        limit:  i32,
        after:  Option<(i64, ConversationId)>,
    ) -> Result<Vec<InboxEntry>, ChatError> {
        // Clustering is (activity DESC, conversation_id ASC): a CQL tuple
        // comparison cannot say "after", so the page starts at the cursor's
        // activity and the ties already served are dropped here.
        let result = match after {
            Some((ms, _)) => {
                let stmt = fast(
                    &self.client,
                    &format!(
                        "SELECT {COLS} FROM chat.inbox_by_activity WHERE member_id = ? AND folder = ? \
                         AND activity <= ? LIMIT ?"
                    ),
                );
                self.client
                    .session
                    .execute_unpaged(stmt, (member.as_uuid(), folder.as_tinyint(), CqlTimestamp(ms), limit + TIE_SLACK))
                    .await
            }
            None => {
                let stmt = fast(
                    &self.client,
                    &format!("SELECT {COLS} FROM chat.inbox_by_activity WHERE member_id = ? AND folder = ? LIMIT ?"),
                );
                self.client.session.execute_unpaged(stmt, (member.as_uuid(), folder.as_tinyint(), limit)).await
            }
        };
        let mut entries = result
            .map_err(scylla_err)?
            .into_rows_result()
            .map_err(|e| row_err("inbox.list:rows", e))?
            .rows::<InboxRow>()
            .map_err(|e| row_err("inbox.list:iter", e))?
            .map(|row| row.map_err(|e| row_err("inbox.list:deser", e)).and_then(InboxRow::into_entry))
            .collect::<Result<Vec<_>, _>>()?;
        if let Some((ms, id)) = after {
            entries.retain(|e| e.activity.timestamp_millis() < ms || e.conversation_id.as_uuid() > id.as_uuid());
        }
        entries.truncate(limit.max(0) as usize);
        Ok(entries)
    }
}
