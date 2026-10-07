use std::fmt;

use serde::{Deserialize, Serialize};

use crate::error::AccountError;

/// Internal platform role assigned to an account.
///
/// Roles are cumulative and coarse-grained. Fine-grained authorisation (e.g.
/// per-resource write access) is handled by `permission_overrides` on the
/// aggregate. `privilege_level` provides a simple ordering for UI rendering
/// and audit trail prioritisation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccountRole {
    /// Standard platform user.
    User,
    /// Can review and remove user-generated content.
    ContentModerator,
    /// Customer support access — can view account details.
    SupportAgent,
    /// Access to financial operations and ledger data.
    FinanceOperator,
    /// Full administrative access excluding super-admin operations.
    Admin,
    /// Unrestricted access including role assignment for all other roles.
    SuperAdmin,
}

impl AccountRole {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::User             => "user",
            Self::ContentModerator => "content_moderator",
            Self::SupportAgent     => "support_agent",
            Self::FinanceOperator  => "finance_operator",
            Self::Admin            => "admin",
            Self::SuperAdmin       => "super_admin",
        }
    }

    /// Fine-grained permission grants this role carries into the edge token.
    ///
    /// `account` is authoritative for RBAC: `auth` re-reads these on every
    /// login/refresh and mints them verbatim (opaque strings by contract —
    /// see `auth-context::Permission`). Today the only fine-grained consumers
    /// are the audit ledger's read-side gates (`audit-service
    /// infrastructure/grpc/access.rs`), which fail closed on absence:
    ///
    /// * `audit:read`   — query ledger records (need-to-know read)
    /// * `audit:verify` — integrity verification (chain reports, no payloads)
    /// * `audit:export` — evidence-bundle egress (stricter than read)
    /// * `audit:record` — the synchronous break-glass record lane
    /// * `verification:review` — review verification requests and open their
    ///   private documents (#837: profile's staff queue and decision, media's
    ///   `GetPrivateDocumentUrl`), held by content moderators and admins
    ///
    /// Admin gets read+verify (operate and prove the ledger, no bulk egress);
    /// SuperAdmin adds export and the break-glass lane. Per-account exceptions
    /// (e.g. a compliance officer needing export without SuperAdmin) belong in
    /// `permission_overrides` on the aggregate, not here.
    pub fn granted_permissions(&self) -> &'static [&'static str] {
        match self {
            Self::User             => &[],
            Self::ContentModerator => &["verification:review"],
            Self::SupportAgent     => &[],
            Self::FinanceOperator  => &[],
            Self::Admin            => &["audit:read", "audit:verify", "verification:review"],
            Self::SuperAdmin       => &["audit:read", "audit:verify", "audit:export", "audit:record", "verification:review"],
        }
    }

    /// Numeric privilege level for ordering and audit comparisons.
    ///
    /// Higher values indicate broader access. Roles at the same level have
    /// different scope rather than different privilege depth.
    pub fn privilege_level(&self) -> u8 {
        match self {
            Self::User             => 1,
            Self::ContentModerator => 2,
            Self::SupportAgent     => 2,
            Self::FinanceOperator  => 3,
            Self::Admin            => 4,
            Self::SuperAdmin       => 5,
        }
    }
}

impl fmt::Display for AccountRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl TryFrom<&str> for AccountRole {
    type Error = AccountError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s {
            "user"              => Ok(Self::User),
            "content_moderator" => Ok(Self::ContentModerator),
            "support_agent"     => Ok(Self::SupportAgent),
            "finance_operator"  => Ok(Self::FinanceOperator),
            "admin"             => Ok(Self::Admin),
            "super_admin"       => Ok(Self::SuperAdmin),
            other => Err(AccountError::InvalidAccountRole(other.to_owned())),
        }
    }
}

impl TryFrom<String> for AccountRole {
    type Error = AccountError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::try_from(s.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The audit gate contract (audit access.rs `perm` module) — baseline roles
    /// carry no audit grants; Admin can operate/prove the ledger but not bulk-
    /// export; only SuperAdmin holds the egress + break-glass grants.
    #[test]
    fn audit_grants_follow_privilege_boundaries() {
        for role in [AccountRole::User, AccountRole::SupportAgent, AccountRole::FinanceOperator] {
            assert!(role.granted_permissions().is_empty(), "{role} must grant nothing");
        }
        assert!(
            AccountRole::ContentModerator.granted_permissions().iter().all(|p| !p.starts_with("audit:")),
            "a moderator holds no audit grant"
        );

        let admin = AccountRole::Admin.granted_permissions();
        assert!(admin.contains(&"audit:read") && admin.contains(&"audit:verify"));
        assert!(!admin.contains(&"audit:export") && !admin.contains(&"audit:record"));

        let sa = AccountRole::SuperAdmin.granted_permissions();
        for p in ["audit:read", "audit:verify", "audit:export", "audit:record"] {
            assert!(sa.contains(&p), "SuperAdmin must grant {p}");
        }
    }

    /// #837: verification review (identity documents) is moderators' and
    /// admins' — not support, not finance, not users.
    #[test]
    fn verification_review_is_moderators_and_admins() {
        for role in [AccountRole::ContentModerator, AccountRole::Admin, AccountRole::SuperAdmin] {
            assert!(role.granted_permissions().contains(&"verification:review"), "{role}");
        }
        for role in [AccountRole::User, AccountRole::SupportAgent, AccountRole::FinanceOperator] {
            assert!(!role.granted_permissions().contains(&"verification:review"), "{role}");
        }
    }
}
