use std::fmt;

use serde::{Deserialize, Serialize};

use crate::error::AccountError;

/// The identity that uniquely identifies the authenticated holder behind
/// this account: the `issuer:subject` of an Apple, Google or one-time-code
/// identity, as auth forms it.
///
/// The value is kept opaque (the subject layout is the issuer's), so any
/// issuer works without coercion. It is immutable after account creation.
#[derive(Clone, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub struct IdentityId(String);

impl IdentityId {
    const MAX_LEN: usize = 255;

    pub fn new(value: impl Into<String>) -> Result<Self, AccountError> {
        let s = value.into();
        if s.is_empty() {
            return Err(AccountError::InvalidIdentityId(
                "identity_id must not be empty".into(),
            ));
        }
        if s.len() > Self::MAX_LEN {
            return Err(AccountError::InvalidIdentityId(format!(
                "identity_id exceeds maximum length of {} characters",
                Self::MAX_LEN
            )));
        }
        Ok(Self(s))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

impl fmt::Display for IdentityId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
