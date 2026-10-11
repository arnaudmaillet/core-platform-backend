use std::fmt;

use serde::{Deserialize, Serialize};

/// A normalized permission string, e.g. `"posts:write"` or `"ROLE_ADMIN"`.
///
/// This mirrors the `Permission` shape the `auth-context` library produces on the
/// inbound path, so the claims this service mints and the claims downstream
/// services read are the same vocabulary. Normalization from IdP-specific shapes
/// (Keycloak `realm_access.roles`, Okta `groups`, …) happens in the
/// infrastructure adapter; the domain only ever sees the normalized form.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Permission(String);

/// Reading public content (posts, profiles, the map, search…) on the client
/// edge: the only permission a guest holds, and one every member holds too, so
/// read RPCs can require it instead of an account.
pub const READ_PUBLIC: &str = "read:public";

/// The member role `account` grants every account: the one grant that is not
/// staff (#880).
pub const MEMBER_ROLE: &str = "user";

impl Permission {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// The `read:public` permission.
    pub fn read_public() -> Self {
        Self::new(READ_PUBLIC)
    }

    /// `permissions` plus `read:public` when it is missing — what a member's
    /// token carries.
    pub fn with_read_public(mut permissions: Vec<Permission>) -> Vec<Permission> {
        if !permissions.iter().any(|p| p.as_str() == READ_PUBLIC) {
            permissions.push(Self::read_public());
        }
        permissions
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// A staff grant (#880): anything but the member role and `read:public` —
    /// a staff role (`admin`, `support_agent`…), a fine-grained permission
    /// (`audit:read`…) or an override. Unknown grants count as staff (fail
    /// closed).
    pub fn is_staff(&self) -> bool {
        !matches!(self.0.as_str(), MEMBER_ROLE | READ_PUBLIC)
    }

    /// What a session's token carries (#880): every grant in a session opened
    /// with a staff-enrolled passkey, else the member grants only — plus
    /// `read:public` either way.
    pub fn for_session(grants: Vec<Permission>, staff_passkey: bool) -> Vec<Permission> {
        let grants = if staff_passkey { grants } else { grants.into_iter().filter(|g| !g.is_staff()).collect() };
        Self::with_read_public(grants)
    }
}

impl fmt::Display for Permission {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn members_get_read_public_once() {
        let perms = Permission::with_read_public(vec![Permission::new("posts:write")]);
        assert_eq!(perms, vec![Permission::new("posts:write"), Permission::read_public()]);
        assert_eq!(Permission::with_read_public(perms.clone()), perms, "not duplicated");
    }

    /// #880: outside a staff passkey session, a staff account keeps only its
    /// member grants.
    #[test]
    fn staff_grants_ride_only_in_staff_passkey_sessions() {
        let grants = || vec![Permission::new("user"), Permission::new("admin"), Permission::new("audit:read")];
        assert_eq!(Permission::for_session(grants(), false), vec![Permission::new("user"), Permission::read_public()]);
        assert_eq!(Permission::for_session(grants(), true).len(), 4);
        assert!(Permission::new("something:new").is_staff(), "unknown grants are staff");
    }

    #[test]
    fn wraps_value() {
        assert_eq!(Permission::new("posts:write").as_str(), "posts:write");
    }
}
