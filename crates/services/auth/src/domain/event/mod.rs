pub mod app_authorization;
pub mod session_issued;
pub mod session_revoked;
pub mod subject_linked;

pub use app_authorization::{AppAuthorizationRevoked, AppAuthorized};
pub use session_issued::SessionIssued;
pub use session_revoked::SessionRevoked;
pub use subject_linked::SubjectLinked;

use serde::{Deserialize, Serialize};

/// Sealed sum type of every domain event the auth context publishes to the
/// `auth.v1.events` Kafka topic.
///
/// Events are serde structs (JSON on the wire), matching the fleet convention —
/// they are deliberately **not** proto messages. The infrastructure event
/// adapter (Phase 4) pattern-matches on this enum for routing keys and payload
/// serialization. Refresh-token rotation is intentionally absent: it is an
/// internal mechanic, not a published fact.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DomainEvent {
    SessionIssued(SessionIssued),
    SessionRevoked(SessionRevoked),
    SubjectLinked(SubjectLinked),
    AppAuthorized(AppAuthorized),
    AppAuthorizationRevoked(AppAuthorizationRevoked),
}

impl DomainEvent {
    /// Dotted routing key used as the Kafka message type.
    pub fn event_type(&self) -> &'static str {
        match self {
            Self::SessionIssued(_) => "auth.session_issued",
            Self::SessionRevoked(_) => "auth.session_revoked",
            Self::SubjectLinked(_) => "auth.subject_linked",
            Self::AppAuthorized(_) => "auth.app_authorized",
            Self::AppAuthorizationRevoked(_) => "auth.app_authorization_revoked",
        }
    }
}
