use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::domain::aggregate::Session;
use crate::domain::value_object::{
    AccountId, DeviceFingerprint, Generation, IdpSubject, SessionId, SessionKind, SessionMethod, SessionSignIn,
    SessionStatus,
};
use crate::error::AuthError;

/// Flat projection of the `sessions` table. Domain reconstruction (validation,
/// value-object construction) happens in [`TryFrom`], keeping persistence free of
/// domain logic.
#[derive(Debug, sqlx::FromRow)]
pub struct SessionRow {
    pub id: Uuid,
    pub account_id: Uuid,
    /// `member` | `guest` (migration 0003; existing rows default to `member`).
    pub kind: String,
    pub issuer: String,
    pub subject: String,
    pub generation: i64,
    pub status: String,
    pub device_user_agent: Option<String>,
    pub device_ip: Option<String>,
    pub device_id: Option<String>,
    pub issued_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub absolute_expiry: DateTime<Utc>,
    pub revoked_at: Option<DateTime<Utc>>,
    pub version: i64,
    /// How it was opened (migration 0009, #880); null on older rows.
    pub sign_in_method: Option<String>,
    pub passkey_credential_id: Option<Vec<u8>>,
}

impl TryFrom<SessionRow> for Session {
    type Error = AuthError;

    fn try_from(row: SessionRow) -> Result<Self, Self::Error> {
        let subject = IdpSubject::new(row.issuer, row.subject)?;
        let device = DeviceFingerprint::new(row.device_user_agent, row.device_ip, row.device_id);
        let status = SessionStatus::try_from(row.status.as_str())?;
        let kind = SessionKind::try_from(row.kind.as_str())?;

        Ok(Session::reconstitute(
            SessionId::from_uuid(row.id),
            AccountId::from_uuid(row.account_id),
            kind,
            subject,
            Generation::from_i64(row.generation),
            status,
            device,
            SessionSignIn::from_parts(row.sign_in_method.as_deref().and_then(SessionMethod::parse), row.passkey_credential_id),
            row.issued_at,
            row.expires_at,
            row.absolute_expiry,
            row.revoked_at,
            row.version,
        ))
    }
}
