use serde::{Deserialize, Serialize};

use crate::application::port::MessageSummary;
use crate::error::ChatError;

/// Wire form of a message as it travels over a pub/sub plane. Scalar-only and
/// self-contained, so the same frame serves the full Member-Plane delivery and
/// the stripped Audience-Plane shadow without re-encoding.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageFrame {
    pub message_id:    String,
    pub sender_id:     String,
    pub content_type:  i8,
    pub body:          String,
    pub media_ref:     Option<String>,
    pub reply_to:      Option<String>,
    pub created_at_ms: i64,
    /// Its sender's alone (#656): every other member's stream drops it.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub withheld:      bool,
}

impl MessageFrame {
    /// May the member `reader` see this frame on their stream?
    pub fn visible_to(&self, reader: &str) -> bool {
        !self.withheld || self.sender_id == reader
    }

    pub fn from_summary(s: &MessageSummary) -> Self {
        Self {
            message_id:    s.message_id.to_string(),
            sender_id:     s.sender_id.to_string(),
            content_type:  s.content_type.as_tinyint(),
            body:          s.body.clone(),
            media_ref:     s.media_ref.clone(),
            reply_to:      s.reply_to.map(|u| u.to_string()),
            created_at_ms: s.created_at.timestamp_millis(),
            withheld:      s.withheld,
        }
    }
}

/// An event delivered over a plane.
///
/// The Shadowing Pattern is enforced structurally by *which* variants reach
/// *which* plane, not by stripping fields: the Audience Plane only ever carries
/// [`PlaneEvent::Message`], while the Member Plane carries all of them. Presence,
/// typing, and receipt variants are published exclusively to the member channel,
/// so a guest stream is incapable of receiving them.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "t", rename_all = "snake_case")]
pub enum PlaneEvent {
    Message(MessageFrame),
    Presence { member_id: String, online: bool },
    Typing { member_id: String },
    Receipt { member_id: String, last_read: String },
}

impl PlaneEvent {
    pub fn to_json(&self) -> Result<String, ChatError> {
        serde_json::to_string(self).map_err(|e| ChatError::DomainViolation {
            field:   "plane_event.encode".to_owned(),
            message: e.to_string(),
        })
    }

    pub fn from_json(raw: &str) -> Result<Self, ChatError> {
        serde_json::from_str(raw).map_err(|e| ChatError::DomainViolation {
            field:   "plane_event.decode".to_owned(),
            message: e.to_string(),
        })
    }
}
