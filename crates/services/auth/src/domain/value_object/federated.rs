//! Third-party identity providers whose id_tokens auth verifies itself (native
//! Sign in with Apple / Google), and how an existing account signs in.

/// Apple's id_token issuer.
pub const APPLE_ISSUER: &str = "https://appleid.apple.com";
/// The issuer of identities proven by a one-time code sent to an email address
/// (passwordless accounts): the subject is the normalized address.
pub const EMAIL_CODE_ISSUER: &str = "urn:core-platform:email";

/// The issuer of identities proven by a one-time code sent by SMS (phone-only
/// accounts): the subject is the number in E.164.
pub const PHONE_CODE_ISSUER: &str = "urn:core-platform:phone";

/// The issuer of a passkey session's subject when its account has no
/// identity link (#808); the subject is the credential id.
pub const PASSKEY_ISSUER: &str = "urn:core-platform:passkey";

/// Google's id_token issuers (both forms are in use).
pub const GOOGLE_ISSUERS: [&str; 2] = ["https://accounts.google.com", "accounts.google.com"];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FederatedProvider {
    Apple,
    Google,
}

impl FederatedProvider {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Apple => "apple",
            Self::Google => "google",
        }
    }
}

/// How an existing account signs in, as SignUp tells a person who already has one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignInMethod {
    Apple,
    Google,
    /// The IdP's own credential (email / username + password).
    Password,
    /// A one-time code sent to the email address.
    EmailCode,
    /// A one-time code sent by SMS to the phone number.
    PhoneCode,
}

impl SignInMethod {
    /// The method behind an IdP subject's issuer: Apple's, Google's, or else the
    /// fleet's own IdP (password).
    pub fn from_issuer(issuer: &str) -> Self {
        if issuer == APPLE_ISSUER {
            Self::Apple
        } else if GOOGLE_ISSUERS.contains(&issuer) {
            Self::Google
        } else if issuer == EMAIL_CODE_ISSUER {
            Self::EmailCode
        } else if issuer == PHONE_CODE_ISSUER {
            Self::PhoneCode
        } else {
            Self::Password
        }
    }
}

impl From<FederatedProvider> for SignInMethod {
    fn from(provider: FederatedProvider) -> Self {
        match provider {
            FederatedProvider::Apple => Self::Apple,
            FederatedProvider::Google => Self::Google,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn issuers_map_to_methods() {
        assert_eq!(SignInMethod::from_issuer(APPLE_ISSUER), SignInMethod::Apple);
        assert_eq!(SignInMethod::from_issuer("accounts.google.com"), SignInMethod::Google);
        assert_eq!(SignInMethod::from_issuer("https://accounts.google.com"), SignInMethod::Google);
        assert_eq!(SignInMethod::from_issuer("https://sso.example/realms/core"), SignInMethod::Password);
        assert_eq!(SignInMethod::from_issuer(EMAIL_CODE_ISSUER), SignInMethod::EmailCode);
    }
}
