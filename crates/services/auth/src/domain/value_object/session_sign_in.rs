//! How a session was opened (#880): the sign-in method, and the passkey when
//! it was one. A staff account's privileged permissions are minted only in a
//! session opened with a **staff-enrolled** passkey, re-checked at every
//! refresh against that passkey.

use serde::{Deserialize, Serialize};

use super::{IdpSubject, SignInMethod};

/// The credential a session was opened with.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionMethod {
    Passkey,
    Apple,
    Google,
    Password,
    EmailCode,
    PhoneCode,
}

impl SessionMethod {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Passkey => "passkey",
            Self::Apple => "apple",
            Self::Google => "google",
            Self::Password => "password",
            Self::EmailCode => "email_code",
            Self::PhoneCode => "phone_code",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "passkey" => Self::Passkey,
            "apple" => Self::Apple,
            "google" => Self::Google,
            "password" => Self::Password,
            "email_code" => Self::EmailCode,
            "phone_code" => Self::PhoneCode,
            _ => return None,
        })
    }

    /// The method behind a proven identity (its issuer).
    pub fn of_subject(subject: &IdpSubject) -> Self {
        match SignInMethod::from_issuer(subject.issuer()) {
            SignInMethod::Apple => Self::Apple,
            SignInMethod::Google => Self::Google,
            SignInMethod::Password => Self::Password,
            SignInMethod::EmailCode => Self::EmailCode,
            SignInMethod::PhoneCode => Self::PhoneCode,
        }
    }
}

/// How a session was opened. `method` is `None` for a guest session and for
/// sessions from before #880.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionSignIn {
    method:                Option<SessionMethod>,
    passkey_credential_id: Option<Vec<u8>>,
}

impl SessionSignIn {
    /// No recorded method (a guest).
    pub fn none() -> Self {
        Self::default()
    }

    /// Opened with `method` (not a passkey).
    pub fn by(method: SessionMethod) -> Self {
        Self { method: Some(method), passkey_credential_id: None }
    }

    /// Opened with this passkey (as the first factor or the second step).
    pub fn by_passkey(credential_id: Vec<u8>) -> Self {
        Self { method: Some(SessionMethod::Passkey), passkey_credential_id: Some(credential_id) }
    }

    /// As stored; a passkey without its credential id is no passkey session.
    pub fn from_parts(method: Option<SessionMethod>, passkey_credential_id: Option<Vec<u8>>) -> Self {
        match (method, passkey_credential_id) {
            (Some(SessionMethod::Passkey), Some(id)) => Self::by_passkey(id),
            (Some(SessionMethod::Passkey), None) | (None, _) => Self::none(),
            (Some(method), _) => Self::by(method),
        }
    }

    pub fn method(&self) -> Option<SessionMethod> {
        self.method
    }

    /// The passkey the session was opened with, if any.
    pub fn passkey_credential_id(&self) -> Option<&[u8]> {
        self.passkey_credential_id.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn methods_round_trip_and_a_passkey_needs_its_credential() {
        for method in [
            SessionMethod::Passkey,
            SessionMethod::Apple,
            SessionMethod::Google,
            SessionMethod::Password,
            SessionMethod::EmailCode,
            SessionMethod::PhoneCode,
        ] {
            assert_eq!(SessionMethod::parse(method.as_str()), Some(method));
        }
        assert_eq!(SessionSignIn::from_parts(Some(SessionMethod::Passkey), None), SessionSignIn::none());
        let passkey = SessionSignIn::from_parts(Some(SessionMethod::Passkey), Some(vec![1, 2]));
        assert_eq!(passkey.passkey_credential_id(), Some(&[1u8, 2][..]));
        assert_eq!(SessionSignIn::from_parts(Some(SessionMethod::Apple), Some(vec![1])).passkey_credential_id(), None);
    }
}
