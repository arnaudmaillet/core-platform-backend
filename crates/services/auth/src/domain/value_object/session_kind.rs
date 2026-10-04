use std::fmt;

use serde::{Deserialize, Serialize};

use crate::error::AuthError;

/// Who a session belongs to.
///
/// - `Member`: an account that signed in through the IdP.
/// - `Guest`: an anonymous installation browsing before sign-up. Its "account
///   id" is a fresh guest id that no account owns; its token carries
///   `sub = "guest:<id>"`, `kind = "guest"`, the `read:public` permission and
///   no profiles, and the client edge refuses it on every `authenticated`
///   method.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionKind {
    #[default]
    Member,
    Guest,
}

impl SessionKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Member => "member",
            Self::Guest => "guest",
        }
    }
}

impl TryFrom<&str> for SessionKind {
    type Error = AuthError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "member" => Ok(Self::Member),
            "guest" => Ok(Self::Guest),
            other => Err(AuthError::DomainViolation {
                field: "session.kind".into(),
                message: format!("unknown session kind '{other}'"),
            }),
        }
    }
}

impl fmt::Display for SessionKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_and_defaults_to_member() {
        for kind in [SessionKind::Member, SessionKind::Guest] {
            assert_eq!(SessionKind::try_from(kind.as_str()).unwrap(), kind);
        }
        assert_eq!(SessionKind::default(), SessionKind::Member);
        assert!(SessionKind::try_from("admin").is_err());
    }
}
