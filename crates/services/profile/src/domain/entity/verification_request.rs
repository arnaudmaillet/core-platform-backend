use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::domain::value_object::VerificationKind;
use crate::error::ProfileError;

/// At most this many supporting documents (media keys) per request.
pub const MAX_VERIFICATION_DOCUMENTS: usize = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerificationStatus {
    Pending,
    Approved,
    Rejected,
}

impl VerificationStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Approved => "approved",
            Self::Rejected => "rejected",
        }
    }
}

/// A profile's request for a verification badge (#668): reviewed by staff,
/// whose approval verifies the profile (the admin `VerifyProfile` path).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerificationRequest {
    pub request_id:   Uuid,
    pub category:     VerificationKind,
    /// Media keys of the supporting documents.
    pub documents:    Vec<String>,
    pub status:       VerificationStatus,
    /// Why it was rejected (shown to the owner).
    pub reason:       Option<String>,
    pub submitted_at: DateTime<Utc>,
    pub decided_at:   Option<DateTime<Utc>>,
}

impl VerificationRequest {
    pub fn submit(category: VerificationKind, documents: Vec<String>, now: DateTime<Utc>) -> Result<Self, ProfileError> {
        let documents: Vec<String> = documents.into_iter().map(|d| d.trim().to_owned()).filter(|d| !d.is_empty()).collect();
        if documents.is_empty() || documents.len() > MAX_VERIFICATION_DOCUMENTS {
            return Err(ProfileError::DomainViolation {
                field:   "documents".into(),
                message: format!("1–{MAX_VERIFICATION_DOCUMENTS} supporting documents"),
            });
        }
        if documents.iter().any(|d| d.len() > 512) {
            return Err(ProfileError::DomainViolation { field: "documents".into(), message: "a document key is too long".into() });
        }
        Ok(Self {
            request_id: Uuid::now_v7(),
            category,
            documents,
            status: VerificationStatus::Pending,
            reason: None,
            submitted_at: now,
            decided_at: None,
        })
    }

    /// Approves or rejects a pending request; a rejection needs a reason.
    pub fn decide(&mut self, approve: bool, reason: Option<String>, now: DateTime<Utc>) -> Result<(), ProfileError> {
        if self.status != VerificationStatus::Pending {
            return Err(ProfileError::NoPendingVerification);
        }
        let reason = reason.map(|r| r.trim().to_owned()).filter(|r| !r.is_empty());
        if !approve && reason.is_none() {
            return Err(ProfileError::DomainViolation { field: "reason".into(), message: "a rejection needs a reason".into() });
        }
        self.status = if approve { VerificationStatus::Approved } else { VerificationStatus::Rejected };
        self.reason = reason;
        self.decided_at = Some(now);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_request_needs_documents_and_is_decided_once() {
        let now = Utc::now();
        assert!(VerificationRequest::submit(VerificationKind::Notable, vec![], now).is_err());
        assert!(VerificationRequest::submit(VerificationKind::Notable, vec!["d".into(); 6], now).is_err());
        let mut r = VerificationRequest::submit(VerificationKind::Notable, vec!["media/id.jpg".into()], now).unwrap();
        assert_eq!(r.status, VerificationStatus::Pending);
        assert!(r.decide(false, None, now).is_err(), "a rejection needs a reason");
        r.decide(false, Some("Not enough press coverage".into()), now).unwrap();
        assert_eq!(r.status, VerificationStatus::Rejected);
        assert!(matches!(r.decide(true, None, now), Err(ProfileError::NoPendingVerification)));
    }
}
