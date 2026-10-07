use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::domain::value_object::AccountId;

/// An account authorised a third-party app (#667): the proof of consent
/// (GDPR Art. 7(1)), recorded by audit as a `consent` record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppAuthorized {
    pub account_id:     AccountId,
    pub app_id:         String,
    pub scopes:         Vec<String>,
    /// The consent's instant (`granted_at`): a retried grant repeats it.
    pub occurred_at:    DateTime<Utc>,
    pub correlation_id: Uuid,
}

/// The account withdrew an app's authorisation (#667, GDPR Art. 7(3)).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppAuthorizationRevoked {
    pub account_id:     AccountId,
    pub app_id:         String,
    /// The withdrawal's instant (`revoked_at`): a retried revoke repeats it.
    pub occurred_at:    DateTime<Utc>,
    pub correlation_id: Uuid,
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use uuid::Uuid;

    use super::*;
    use crate::domain::event::DomainEvent;

    /// audit's consent records decode the events as auth publishes them.
    #[test]
    fn audit_reads_the_consent_events() {
        use audit::infrastructure::auth_decode::{map_app_authorization_revoked, map_app_authorized, AuthEventWire};

        let account_id = AccountId::try_from(Uuid::now_v7().to_string().as_str()).unwrap();
        let granted = DomainEvent::AppAuthorized(AppAuthorized {
            account_id,
            app_id: "partner.app".into(),
            scopes: vec!["email".into()],
            occurred_at: Utc::now(),
            correlation_id: Uuid::now_v7(),
        });
        let AuthEventWire::AppAuthorized(wire) = serde_json::from_value(serde_json::to_value(&granted).unwrap()).unwrap()
        else {
            panic!("app_authorized decodes as such");
        };
        assert_eq!(map_app_authorized(&wire).unwrap().action(), "auth.app_authorized");

        let revoked = DomainEvent::AppAuthorizationRevoked(AppAuthorizationRevoked {
            account_id,
            app_id: "partner.app".into(),
            occurred_at: Utc::now(),
            correlation_id: Uuid::now_v7(),
        });
        let AuthEventWire::AppAuthorizationRevoked(wire) =
            serde_json::from_value(serde_json::to_value(&revoked).unwrap()).unwrap()
        else {
            panic!("app_authorization_revoked decodes as such");
        };
        assert_eq!(map_app_authorization_revoked(&wire).unwrap().action(), "auth.app_authorization_revoked");
    }
}
