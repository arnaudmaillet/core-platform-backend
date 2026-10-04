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

    #[test]
    fn wraps_value() {
        assert_eq!(Permission::new("posts:write").as_str(), "posts:write");
    }
}
