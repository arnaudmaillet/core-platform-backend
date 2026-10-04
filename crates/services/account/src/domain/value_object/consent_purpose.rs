use std::fmt;

use serde::{Deserialize, Serialize};

use crate::error::AccountError;

/// A processing purpose the account holder consents to — or withdraws from —
/// separately (GDPR Art. 7: specific, and as easy to withdraw as to give).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsentPurpose {
    DataProcessing,
    Marketing,
    Analytics,
}

impl ConsentPurpose {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::DataProcessing => "data_processing",
            Self::Marketing => "marketing",
            Self::Analytics => "analytics",
        }
    }
}

impl fmt::Display for ConsentPurpose {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl TryFrom<&str> for ConsentPurpose {
    type Error = AccountError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s {
            "data_processing" => Ok(Self::DataProcessing),
            "marketing" => Ok(Self::Marketing),
            "analytics" => Ok(Self::Analytics),
            other => Err(AccountError::DomainViolation {
                field: "consent_purpose".into(),
                message: format!("unknown consent purpose: '{other}'"),
            }),
        }
    }
}
