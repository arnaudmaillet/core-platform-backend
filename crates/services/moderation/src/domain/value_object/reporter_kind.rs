use std::fmt;

use serde::{Deserialize, Serialize};

use crate::error::ModerationError;

/// Who filed a report: a signed-in account or a guest installation. Member and
/// guest ids share one UUID space, so a reporter is always the pair (kind, id) —
/// a guest never sees a member's reports, nor the reverse.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReporterKind {
    Member,
    Guest,
}

impl ReporterKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Member => "member",
            Self::Guest => "guest",
        }
    }
}

impl fmt::Display for ReporterKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl TryFrom<&str> for ReporterKind {
    type Error = ModerationError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s {
            "member" => Ok(Self::Member),
            "guest" => Ok(Self::Guest),
            other => Err(ModerationError::DomainViolation {
                field: "reporter_kind".into(),
                message: format!("unknown reporter kind: '{other}'"),
            }),
        }
    }
}
