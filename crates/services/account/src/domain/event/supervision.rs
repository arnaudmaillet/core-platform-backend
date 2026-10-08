use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::domain::value_object::AccountId;

/// A parent and a teen paired (#670). `account_id` is the teen's (the
/// partition key; every `account.v1.events` reader expects one).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SupervisionStarted {
    pub account_id:             AccountId,
    pub supervisor_id:          AccountId,
    /// Both sides' profiles, so each can be told (notification).
    pub teen_profile_ids:       Vec<String>,
    pub supervisor_profile_ids: Vec<String>,
    pub occurred_at:            DateTime<Utc>,
    pub correlation_id:         Uuid,
}

/// A supervision ended (#670). `ended_by`: `by_teen` (the supervisor is
/// told), `by_supervisor` (the teen is told), `came_of_age`, `account_deleted`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SupervisionEnded {
    pub account_id:             AccountId,
    pub supervisor_id:          AccountId,
    pub ended_by:               String,
    pub teen_profile_ids:       Vec<String>,
    pub supervisor_profile_ids: Vec<String>,
    pub occurred_at:            DateTime<Utc>,
    pub correlation_id:         Uuid,
}

/// A teen's supervision limits were set (#670 part 2): floors the teen's
/// profiles must meet (profile tightens and locks them). `account_id` = the
/// teen.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SupervisionLimitsSet {
    pub account_id:         AccountId,
    pub set_by:             AccountId,
    pub private_account:    bool,
    /// `followers`, `mutuals`, `no_one`; absent: no floor.
    pub messages:           Option<String>,
    pub comments:           Option<String>,
    pub hidden_from_search: bool,
    pub daily_minutes:      Option<u16>,
    pub teen_profile_ids:   Vec<String>,
    pub occurred_at:        DateTime<Utc>,
    pub correlation_id:     Uuid,
}

/// The teen's last supervision ended: their limits are lifted (the settings
/// keep their values, unlocked).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SupervisionLimitsCleared {
    pub account_id:       AccountId,
    pub teen_profile_ids: Vec<String>,
    pub occurred_at:      DateTime<Utc>,
    pub correlation_id:   Uuid,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::event::DomainEvent;

    /// Every account.v1.events reader expects `type` and `account_id`: the
    /// supervision events carry the teen's as `account_id`.
    #[test]
    fn the_wire_names_the_teen_as_account_id() {
        let (teen, parent) = (AccountId::new(), AccountId::new());
        let ended = DomainEvent::SupervisionEnded(SupervisionEnded {
            account_id: teen,
            supervisor_id: parent,
            ended_by: "by_teen".into(),
            teen_profile_ids: vec!["p-1".into()],
            supervisor_profile_ids: vec![],
            occurred_at: Utc::now(),
            correlation_id: Uuid::now_v7(),
        });
        let json = serde_json::to_value(&ended).unwrap();
        assert_eq!(json["type"], "supervision_ended");
        assert_eq!(json["account_id"], teen.to_string());
        assert_eq!(json["supervisor_id"], parent.to_string());
        assert_eq!(json["ended_by"], "by_teen");
    }

    #[test]
    fn limits_events_name_the_teen_and_carry_the_floors() {
        let teen = AccountId::new();
        let set = DomainEvent::SupervisionLimitsSet(SupervisionLimitsSet {
            account_id: teen,
            set_by: AccountId::new(),
            private_account: true,
            messages: Some("mutuals".into()),
            comments: None,
            hidden_from_search: true,
            daily_minutes: Some(60),
            teen_profile_ids: vec!["p-1".into()],
            occurred_at: Utc::now(),
            correlation_id: Uuid::now_v7(),
        });
        let json = serde_json::to_value(&set).unwrap();
        assert_eq!((json["type"].as_str(), json["account_id"].as_str()), (Some("supervision_limits_set"), Some(teen.to_string().as_str())));
        assert_eq!((json["messages"].as_str(), json["comments"].is_null()), (Some("mutuals"), true));
        let cleared = DomainEvent::SupervisionLimitsCleared(SupervisionLimitsCleared {
            account_id: teen,
            teen_profile_ids: vec![],
            occurred_at: Utc::now(),
            correlation_id: Uuid::now_v7(),
        });
        assert_eq!(serde_json::to_value(&cleared).unwrap()["type"], "supervision_limits_cleared");
    }
}
