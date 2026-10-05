use std::fmt;

use serde::{Deserialize, Serialize};

use crate::error::ModerationError;

/// Lifecycle state of a [`Case`](crate::domain::aggregate::Case).
///
/// `Dismissed` is terminal. `Actioned` is terminal *except* that an appeal can
/// move it to `Appealed`, from which it returns to `Actioned` (upheld) or
/// `Dismissed` (overturned).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CaseStatus {
    Open,
    Triaged,
    Actioned,
    Dismissed,
    Appealed,
}

impl CaseStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Triaged => "triaged",
            Self::Actioned => "actioned",
            Self::Dismissed => "dismissed",
            Self::Appealed => "appealed",
        }
    }

    /// Returns `true` if `self → next` is a permitted transition.
    pub fn can_transition_to(&self, next: CaseStatus) -> bool {
        use CaseStatus::*;
        matches!(
            (self, next),
            (Open, Triaged)
                | (Open, Actioned)
                | (Open, Dismissed)
                | (Triaged, Actioned)
                | (Triaged, Dismissed)
                | (Actioned, Appealed)
                | (Appealed, Actioned)
                | (Appealed, Dismissed)
                // A reporter's appeal overturned: back to review for a new
                // decision (DSA Art. 20).
                | (Actioned, Triaged)
                | (Dismissed, Triaged)
                | (Appealed, Triaged)
        )
    }

    /// A case that holds no further open work (an appeal may still reopen an
    /// `Actioned` case, so it is *not* considered fully terminal here).
    pub fn is_resolved(&self) -> bool {
        matches!(self, Self::Actioned | Self::Dismissed)
    }

    /// Decided, or decided and under the sanctioned account's appeal: what a
    /// reporter's overturned appeal sends back to review.
    pub fn is_resolved_or_appealed(&self) -> bool {
        self.is_resolved() || matches!(self, Self::Appealed)
    }
}

impl fmt::Display for CaseStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl TryFrom<&str> for CaseStatus {
    type Error = ModerationError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s {
            "open" => Ok(Self::Open),
            "triaged" => Ok(Self::Triaged),
            "actioned" => Ok(Self::Actioned),
            "dismissed" => Ok(Self::Dismissed),
            "appealed" => Ok(Self::Appealed),
            other => Err(ModerationError::DomainViolation {
                field: "case_status".into(),
                message: format!("unknown case status: '{other}'"),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legal_transitions() {
        assert!(CaseStatus::Open.can_transition_to(CaseStatus::Triaged));
        assert!(CaseStatus::Open.can_transition_to(CaseStatus::Dismissed));
        assert!(CaseStatus::Triaged.can_transition_to(CaseStatus::Actioned));
        assert!(CaseStatus::Actioned.can_transition_to(CaseStatus::Appealed));
        assert!(CaseStatus::Appealed.can_transition_to(CaseStatus::Dismissed));
    }

    #[test]
    fn illegal_transitions() {
        assert!(!CaseStatus::Dismissed.can_transition_to(CaseStatus::Actioned));
        assert!(!CaseStatus::Open.can_transition_to(CaseStatus::Appealed));
        assert!(!CaseStatus::Actioned.can_transition_to(CaseStatus::Dismissed));
    }

    #[test]
    fn string_round_trip() {
        for s in [
            CaseStatus::Open,
            CaseStatus::Triaged,
            CaseStatus::Actioned,
            CaseStatus::Dismissed,
            CaseStatus::Appealed,
        ] {
            assert_eq!(CaseStatus::try_from(s.as_str()).unwrap(), s);
        }
        assert!(CaseStatus::try_from("bogus").is_err());
    }
}
