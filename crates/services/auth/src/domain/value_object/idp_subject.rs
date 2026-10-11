use std::fmt;

use serde::{Deserialize, Serialize};

use crate::error::AuthError;

/// A normalized identity-provider subject: the `(issuer, subject)` pair that
/// uniquely identifies a principal at the IdP, independent of which IdP it is.
///
/// `issuer` is the identity's issuer (Apple's, Google's, or the code issuers); `subject` is its `sub`
/// claim. Keying the [`SubjectLink`](crate::domain::aggregate::SubjectLink) on the
/// pair — not on `sub` alone — is what makes IdP migration safe: links minted
/// under the old issuer stay valid and unambiguous after a new issuer is added.
/// No IdP-specific structure is assumed; both fields are opaque strings.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IdpSubject {
    issuer: String,
    subject: String,
}

impl IdpSubject {
    /// Builds a subject, rejecting empty components.
    pub fn new(
        issuer: impl Into<String>,
        subject: impl Into<String>,
    ) -> Result<Self, AuthError> {
        let issuer = issuer.into();
        let subject = subject.into();
        if issuer.trim().is_empty() {
            return Err(AuthError::DomainViolation {
                field: "idp_subject.issuer".into(),
                message: "issuer must not be empty".into(),
            });
        }
        if subject.trim().is_empty() {
            return Err(AuthError::DomainViolation {
                field: "idp_subject.subject".into(),
                message: "subject must not be empty".into(),
            });
        }
        Ok(Self { issuer, subject })
    }

    pub fn issuer(&self) -> &str {
        &self.issuer
    }

    pub fn subject(&self) -> &str {
        &self.subject
    }
}

impl fmt::Display for IdpSubject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Stable, log-safe rendering; the issuer disambiguates the subject.
        write!(f, "{}#{}", self.issuer, self.subject)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_with_both_components() {
        let s = IdpSubject::new("https://idp.example/realms/app", "abc-123").unwrap();
        assert_eq!(s.issuer(), "https://idp.example/realms/app");
        assert_eq!(s.subject(), "abc-123");
    }

    #[test]
    fn rejects_empty_components() {
        assert!(matches!(
            IdpSubject::new("", "sub").unwrap_err(),
            AuthError::DomainViolation { .. }
        ));
        assert!(matches!(
            IdpSubject::new("iss", "   ").unwrap_err(),
            AuthError::DomainViolation { .. }
        ));
    }

    #[test]
    fn equality_is_pairwise() {
        let a = IdpSubject::new("iss-1", "sub").unwrap();
        let b = IdpSubject::new("iss-2", "sub").unwrap();
        // same subject, different issuer ⇒ distinct principals
        assert_ne!(a, b);
    }
}
