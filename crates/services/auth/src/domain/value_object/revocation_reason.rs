use std::fmt;

use serde::{Deserialize, Serialize};

use crate::error::AuthError;

/// Why a session was revoked — carried on the `SessionRevoked` event for audit
/// and anomaly analysis.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RevocationReason {
    /// User-initiated single-session sign-out.
    Logout,
    /// Account-wide sign-out (generation bump).
    GlobalLogout,
    /// A rotated refresh token was re-presented — treated as compromise.
    RefreshReuse,
    /// Operator / security action.
    Administrative,
    /// The holder changed their password and signed their other devices out.
    PasswordChanged,
    /// A guest session ended because its device signed up or signed in: the
    /// guest became the member.
    GuestUpgraded,
    /// The holder turned two-step sign-in on or regenerated its backup codes:
    /// their other sessions end (#649).
    MfaChanged,
}

impl RevocationReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Logout => "logout",
            Self::GlobalLogout => "global_logout",
            Self::RefreshReuse => "refresh_reuse",
            Self::Administrative => "administrative",
            Self::PasswordChanged => "password_changed",
            Self::GuestUpgraded => "guest_upgraded",
            Self::MfaChanged => "mfa_changed",
        }
    }
}

impl fmt::Display for RevocationReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl TryFrom<&str> for RevocationReason {
    type Error = AuthError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s {
            "logout" => Ok(Self::Logout),
            "global_logout" => Ok(Self::GlobalLogout),
            "refresh_reuse" => Ok(Self::RefreshReuse),
            "administrative" => Ok(Self::Administrative),
            "password_changed" => Ok(Self::PasswordChanged),
            "guest_upgraded" => Ok(Self::GuestUpgraded),
            "mfa_changed" => Ok(Self::MfaChanged),
            other => Err(AuthError::DomainViolation {
                field: "revocation_reason".into(),
                message: format!("unknown revocation reason: '{other}'"),
            }),
        }
    }
}
