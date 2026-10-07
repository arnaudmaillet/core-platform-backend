use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::domain::value_object::VerificationKind;
use crate::error::ProfileError;

/// At most this many pieces of evidence (links and private documents together)
/// per request.
pub const MAX_VERIFICATION_DOCUMENTS: usize = 5;
/// The longest link kept.
pub const MAX_LINK_CHARS: usize = 512;

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
    /// Supporting links (web addresses: an official site, press coverage, a
    /// business registry).
    pub documents:    Vec<String>,
    /// The requester's private documents (#777): media asset ids of
    /// `PRIVATE_DOCUMENT`s, checked at submission (theirs, ready). Purged by
    /// media 30 days after the decision.
    #[serde(default)]
    pub private_documents: Vec<String>,
    pub status:       VerificationStatus,
    /// Why it was rejected (shown to the owner).
    pub reason:       Option<String>,
    pub submitted_at: DateTime<Utc>,
    pub decided_at:   Option<DateTime<Utc>>,
}

impl VerificationRequest {
    /// `documents`: links; `private_documents`: asset ids (their ownership is
    /// checked by the caller, against media).
    pub fn submit(
        category: VerificationKind,
        documents: Vec<String>,
        private_documents: Vec<String>,
        now: DateTime<Utc>,
    ) -> Result<Self, ProfileError> {
        let tidy = |v: Vec<String>| -> Vec<String> {
            let mut out: Vec<String> = Vec::new();
            for d in v.into_iter().map(|d| d.trim().to_owned()).filter(|d| !d.is_empty()) {
                if !out.contains(&d) {
                    out.push(d);
                }
            }
            out
        };
        let (documents, private_documents) = (tidy(documents), tidy(private_documents));
        let total = documents.len() + private_documents.len();
        if total == 0 || total > MAX_VERIFICATION_DOCUMENTS {
            return Err(ProfileError::DomainViolation {
                field:   "documents".into(),
                message: format!("1–{MAX_VERIFICATION_DOCUMENTS} links and private documents"),
            });
        }
        if documents.iter().any(|d| !is_web_link(d)) {
            return Err(ProfileError::DomainViolation {
                field:   "documents".into(),
                message: format!("a link is an http(s) URL of at most {MAX_LINK_CHARS} characters"),
            });
        }
        if private_documents.iter().any(|d| Uuid::parse_str(d).is_err()) {
            return Err(ProfileError::DomainViolation {
                field:   "private_document_ids".into(),
                message: "a private document is a media asset id".into(),
            });
        }
        Ok(Self {
            request_id: Uuid::now_v7(),
            category,
            documents,
            private_documents,
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

/// A web address as the client sends it: `http://` or `https://` (any case),
/// no whitespace, at most [`MAX_LINK_CHARS`].
fn is_web_link(link: &str) -> bool {
    let scheme = link.split_once("://").map(|(s, _)| s.to_ascii_lowercase());
    matches!(scheme.as_deref(), Some("http" | "https"))
        && link.len() <= MAX_LINK_CHARS
        && !link.contains(char::is_whitespace)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_request_needs_documents_and_is_decided_once() {
        let now = Utc::now();
        let link = |n: usize| format!("https://example.org/{n}");
        let doc = || Uuid::now_v7().to_string();
        assert!(VerificationRequest::submit(VerificationKind::Notable, vec![], vec![], now).is_err());
        assert!(VerificationRequest::submit(VerificationKind::Notable, (0..3).map(link).collect(), vec![doc(), doc(), doc()], now).is_err(), "6 in all");
        assert!(VerificationRequest::submit(VerificationKind::Notable, vec!["media/id.jpg".into()], vec![], now).is_err(), "a web link");
        assert!(VerificationRequest::submit(VerificationKind::Notable, vec!["ftp://files.org/a".into()], vec![], now).is_err(), "http(s) only");
        for link in ["http://plain.org", "HTTPS://Example.org/About"] {
            assert!(VerificationRequest::submit(VerificationKind::Notable, vec![link.into()], vec![], now).is_ok(), "{link}");
        }
        assert!(VerificationRequest::submit(VerificationKind::Notable, vec![], vec!["media/id.jpg".into()], now).is_err(), "an asset id");
        let both = VerificationRequest::submit(VerificationKind::Notable, vec![link(1), link(1)], vec![doc()], now).unwrap();
        assert_eq!((both.documents.len(), both.private_documents.len()), (1, 1), "deduplicated");
        let mut r = VerificationRequest::submit(VerificationKind::Notable, vec![link(1)], vec![], now).unwrap();
        assert_eq!(r.status, VerificationStatus::Pending);
        assert!(r.decide(false, None, now).is_err(), "a rejection needs a reason");
        r.decide(false, Some("Not enough press coverage".into()), now).unwrap();
        assert_eq!(r.status, VerificationStatus::Rejected);
        assert!(matches!(r.decide(true, None, now), Err(ProfileError::NoPendingVerification)));
    }
}
