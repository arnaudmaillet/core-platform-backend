use serde::{Deserialize, Serialize};

/// Events emitted on the message write path.
///
/// A message is not part of the `Conversation` aggregate's consistency boundary
/// (the message log is a separate, high-volume, append-only stream), so these
/// events are produced by the `SendMessage` handler rather than buffered on the
/// aggregate. The infrastructure layer forks each `Sent` event into the full
/// Member-Plane broadcast and the stripped Audience-Plane shadow.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event_type", rename_all = "snake_case")]
pub enum MessageEvent {
    Sent(MessageSentEvent),
}

/// Emitted after a message is durably written to the ScyllaDB message log.
///
/// `body` carries the textual content for `Text`/`System` messages; `media_ref`
/// carries the out-of-band pointer for `Media` messages. The Audience-Plane
/// shadow reuses these fields verbatim — only the Member-Plane signals
/// (presence/typing/receipts) are stripped, never the message payload itself.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageSentEvent {
    pub conversation_id: String,
    pub message_id:      String,
    pub sender_id:       String,
    pub content_type:    String,
    pub body:            String,
    pub media_ref:       Option<String>,
    pub reply_to:        Option<String>,
    pub created_at_ms:   i64,
    /// Shown to its sender only (#656): deliver it to nobody else.
    #[serde(default)]
    pub withheld:        bool,
    /// A message request (#656): it waits in the recipient's requests — no
    /// push until they accept.
    #[serde(default)]
    pub request:         bool,
}
