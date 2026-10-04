use serde::{Deserialize, Serialize};

/// The holder's age bracket, as `account` computes it from the date of birth.
/// Minted into the edge token's `age` claim (`auth_context::edge::EDGE_AGE_CLAIM`)
/// so client-facing services apply the teen protections without a lookup.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AgeBracket {
    Teen13To15,
    Teen16To17,
    Adult,
}

impl AgeBracket {
    /// The wire value of the `age` claim.
    pub fn as_claim(&self) -> &'static str {
        match self {
            Self::Teen13To15 => "13-15",
            Self::Teen16To17 => "16-17",
            Self::Adult => "18+",
        }
    }

    pub fn from_claim(claim: &str) -> Option<Self> {
        match claim {
            "13-15" => Some(Self::Teen13To15),
            "16-17" => Some(Self::Teen16To17),
            "18+" => Some(Self::Adult),
            _ => None,
        }
    }
}
