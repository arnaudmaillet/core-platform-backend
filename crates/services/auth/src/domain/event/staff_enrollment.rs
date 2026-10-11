use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::domain::value_object::AccountId;

/// A staff passkey enrollment grant was issued for `account_id` (#880): by an
/// admin in a staff passkey session, or by the one-off bootstrap
/// (`granted_by` absent). Recorded by audit as a privileged action. The grant
/// itself (a secret) never leaves auth; `grant_id` names it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StaffEnrollmentGranted {
    pub account_id:     AccountId,
    pub grant_id:       Uuid,
    pub granted_by:     Option<AccountId>,
    pub expires_at:     DateTime<Utc>,
    pub occurred_at:    DateTime<Utc>,
    pub correlation_id: Uuid,
}

/// A passkey was registered as a staff passkey with grant `grant_id` (#880):
/// sessions opened with it carry the account's staff permissions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StaffPasskeyEnrolled {
    pub account_id:     AccountId,
    pub grant_id:       Uuid,
    pub occurred_at:    DateTime<Utc>,
    pub correlation_id: Uuid,
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use uuid::Uuid;

    use super::*;
    use crate::domain::event::DomainEvent;

    /// audit's privileged-action records decode the events as auth publishes
    /// them — a grant by an admin, the bootstrap's (no `granted_by`), and the
    /// enrollment.
    #[test]
    fn audit_reads_the_staff_enrollment_events() {
        use audit::infrastructure::auth_decode::{map_staff_enrollment_granted, map_staff_passkey_enrolled, AuthEventWire};

        let account_id = AccountId::from_uuid(Uuid::now_v7());
        let grant_id = Uuid::now_v7();
        for granted_by in [Some(AccountId::from_uuid(Uuid::now_v7())), None] {
            let granted = DomainEvent::StaffEnrollmentGranted(StaffEnrollmentGranted {
                account_id,
                grant_id,
                granted_by,
                expires_at: Utc::now(),
                occurred_at: Utc::now(),
                correlation_id: Uuid::now_v7(),
            });
            let AuthEventWire::StaffEnrollmentGranted(wire) = serde_json::from_value(serde_json::to_value(&granted).unwrap()).unwrap()
            else {
                panic!("staff_enrollment_granted decodes as such");
            };
            let record = map_staff_enrollment_granted(&wire).unwrap();
            assert_eq!(record.action(), "auth.staff_enrollment_granted");
        }

        let enrolled = DomainEvent::StaffPasskeyEnrolled(StaffPasskeyEnrolled {
            account_id,
            grant_id,
            occurred_at: Utc::now(),
            correlation_id: Uuid::now_v7(),
        });
        let AuthEventWire::StaffPasskeyEnrolled(wire) = serde_json::from_value(serde_json::to_value(&enrolled).unwrap()).unwrap()
        else {
            panic!("staff_passkey_enrolled decodes as such");
        };
        assert_eq!(map_staff_passkey_enrolled(&wire).unwrap().action(), "auth.staff_passkey_enrolled");
    }
}
