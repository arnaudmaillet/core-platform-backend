//! Incremental sync (#930): opaque watermarks for "what changed since".
//!
//! A watermark is `"{kind}1_{ms}_{uuid}"`: `i` for an inbox (the inbox change
//! journal's `(changed_at, conversation_id)`), `t` for a thread (the message
//! log's `(created_at, message_id)`). A read returns what lies strictly after
//! it and at most [`GRACE`] before the server's now, so a write that commits a
//! little late is still picked up next time (the client dedups by id). A
//! watermark older than [`MAX_AGE`] answers `SyncWatermarkExpired`: resync in
//! full.

use chrono::{DateTime, TimeDelta, Utc};
use uuid::Uuid;

use crate::error::ChatError;

/// How long a watermark stays usable: past the client's 30-day retention (the
/// inbox journal keeps its rows this long).
pub const MAX_AGE: TimeDelta = TimeDelta::days(35);

/// How far behind the server's now a sync reads up to.
pub const GRACE: TimeDelta = TimeDelta::seconds(5);

/// What a watermark marks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncKind {
    Inbox,
    Thread,
}

impl SyncKind {
    fn prefix(self) -> &'static str {
        match self {
            Self::Inbox => "i1",
            Self::Thread => "t1",
        }
    }
}

/// A position: everything at or before `(ms, id)` is known to the client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Watermark {
    pub ms: i64,
    pub id: Uuid,
}

impl Watermark {
    /// The upper bound of a read at `now`: `now - GRACE`, past every id.
    pub fn upper(now: DateTime<Utc>) -> Self {
        Self { ms: (now - GRACE).timestamp_millis(), id: Uuid::max() }
    }

    pub fn encode(&self, kind: SyncKind) -> String {
        format!("{}_{}_{}", kind.prefix(), self.ms, self.id)
    }

    /// Reads a watermark of `kind`; `SyncWatermarkExpired` when older than
    /// [`MAX_AGE`] at `now`.
    pub fn decode(token: &str, kind: SyncKind, now: DateTime<Utc>) -> Result<Self, ChatError> {
        let invalid = || ChatError::InvalidPageToken { token: token.to_owned() };
        let mut parts = token.splitn(3, '_');
        let (prefix, ms, id) = (parts.next(), parts.next(), parts.next());
        if prefix != Some(kind.prefix()) {
            return Err(invalid());
        }
        let ms: i64 = ms.and_then(|m| m.parse().ok()).ok_or_else(invalid)?;
        let id = id.and_then(|i| Uuid::parse_str(i).ok()).ok_or_else(invalid)?;
        if ms < (now - MAX_AGE).timestamp_millis() {
            return Err(ChatError::SyncWatermarkExpired);
        }
        Ok(Self { ms, id })
    }

    /// This upper bound, or `floor` when the client's watermark is ahead of
    /// it (its clock, or a sync within [`GRACE`]): a read never goes back.
    pub fn max_with(self, floor: Self) -> Self {
        if (self.ms, self.id) < (floor.ms, floor.id) { floor } else { self }
    }

    /// Whether `(ms, id)` lies strictly after this watermark.
    pub fn before(&self, ms: i64, id: Uuid) -> bool {
        (ms, id) > (self.ms, self.id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_watermark_round_trips_and_ages_out() {
        let now = Utc::now();
        let w = Watermark { ms: now.timestamp_millis() - 1_000, id: Uuid::now_v7() };
        assert_eq!(Watermark::decode(&w.encode(SyncKind::Inbox), SyncKind::Inbox, now).unwrap(), w);
        assert!(matches!(
            Watermark::decode(&w.encode(SyncKind::Inbox), SyncKind::Thread, now),
            Err(ChatError::InvalidPageToken { .. })
        ), "a thread watermark is not an inbox one");
        let old = Watermark { ms: (now - TimeDelta::days(36)).timestamp_millis(), id: Uuid::nil() };
        assert!(matches!(Watermark::decode(&old.encode(SyncKind::Thread), SyncKind::Thread, now), Err(ChatError::SyncWatermarkExpired)));
        assert!(Watermark::decode("nope", SyncKind::Thread, now).is_err());
        assert!(w.before(w.ms, Uuid::max()) && !w.before(w.ms, w.id) && w.before(w.ms + 1, Uuid::nil()));
        assert_eq!(Watermark::upper(now).ms, (now - GRACE).timestamp_millis());
    }
}
