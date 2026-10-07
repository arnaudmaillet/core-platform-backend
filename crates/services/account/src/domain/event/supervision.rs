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
}
