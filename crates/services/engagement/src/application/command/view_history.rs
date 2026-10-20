//! A profile's viewing history (#944): a member's view recorded, one post
//! taken out, all of it cleared. The handler binds nothing: the gRPC layer
//! has bound the account and the profile to the edge token. All three are
//! idempotent.

use std::sync::Arc;

use chrono::Utc;
use cqrs::{Command, CommandHandler, Envelope};
use validate_core::{FieldViolation, Validate};

use crate::application::port::ViewHistory;
use crate::domain::value_object::PostId;
use crate::error::EngagementError;

/// What happens to a profile's history.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HistoryChange {
    /// `post_id` was viewed (it moves to the top).
    Viewed { post_id: String },
    /// `post_id` leaves the history.
    Removed { post_id: String },
    /// Everything leaves it.
    Cleared,
}

pub struct ChangeViewHistoryCommand {
    pub account_id: String,
    pub profile_id: String,
    pub change:     HistoryChange,
}

impl Command for ChangeViewHistoryCommand {}

impl Validate for ChangeViewHistoryCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        let mut v = Vec::new();
        if uuid::Uuid::parse_str(&self.account_id).is_err() {
            v.push(FieldViolation::new("account_id", "ENG-VAL-002", "account_id must be a UUID"));
        }
        if self.profile_id.is_empty() || self.profile_id.len() > 64 {
            v.push(FieldViolation::new("profile_id", "ENG-VAL-003", "profile_id must be 1 to 64 characters"));
        }
        if let HistoryChange::Viewed { post_id } | HistoryChange::Removed { post_id } = &self.change
            && post_id.trim().is_empty()
        {
            v.push(FieldViolation::new("post_id", "ENG-VAL-001", "post_id must not be empty"));
        }
        if v.is_empty() { Ok(()) } else { Err(v) }
    }
}

pub struct ChangeViewHistoryHandler {
    /// `None`: this instance runs without the durable store.
    pub history: Option<Arc<dyn ViewHistory>>,
}

impl CommandHandler<ChangeViewHistoryCommand> for ChangeViewHistoryHandler {
    type Error = EngagementError;

    async fn handle(&self, envelope: Envelope<ChangeViewHistoryCommand>) -> Result<(), EngagementError> {
        let cmd = &envelope.payload;
        let history = self.history.as_ref().ok_or(EngagementError::LedgerUnavailable)?;
        let now = Utc::now().timestamp_micros();
        match &cmd.change {
            HistoryChange::Viewed { post_id } => {
                let post = PostId::try_from(post_id.as_str())?.to_string();
                history.record(&cmd.account_id, &cmd.profile_id, &post, now).await
            }
            HistoryChange::Removed { post_id } => {
                let post = PostId::try_from(post_id.as_str())?.to_string();
                history.remove(&cmd.account_id, &cmd.profile_id, &post, now).await
            }
            HistoryChange::Cleared => history.clear(&cmd.account_id, &cmd.profile_id, now).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;

    use super::*;
    use crate::application::port::{ViewCursor, ViewedPost};

    /// What reached the store, in order.
    #[derive(Default)]
    struct Calls(Mutex<Vec<String>>);

    #[async_trait]
    impl ViewHistory for Calls {
        async fn record(&self, _: &str, profile: &str, post: &str, _: i64) -> Result<(), EngagementError> {
            self.0.lock().unwrap().push(format!("record {profile} {post}"));
            Ok(())
        }
        async fn list(&self, _: &str, _: i32, _: Option<&ViewCursor>) -> Result<(Vec<ViewedPost>, Option<ViewCursor>), EngagementError> {
            Ok((Vec::new(), None))
        }
        async fn remove(&self, _: &str, profile: &str, post: &str, _: i64) -> Result<(), EngagementError> {
            self.0.lock().unwrap().push(format!("remove {profile} {post}"));
            Ok(())
        }
        async fn clear(&self, _: &str, profile: &str, _: i64) -> Result<(), EngagementError> {
            self.0.lock().unwrap().push(format!("clear {profile}"));
            Ok(())
        }
        async fn list_by_account(&self, _: &str, _: i32, _: Option<(&str, &str)>) -> Result<Vec<ViewedPost>, EngagementError> {
            Ok(Vec::new())
        }
        async fn forget_profile(&self, _: &str, _: i64) -> Result<(), EngagementError> {
            Ok(())
        }
        async fn forget_account(&self, _: &str, _: i64) -> Result<(), EngagementError> {
            Ok(())
        }
    }

    fn command(change: HistoryChange) -> ChangeViewHistoryCommand {
        ChangeViewHistoryCommand { account_id: uuid::Uuid::now_v7().to_string(), profile_id: "me".into(), change }
    }

    #[tokio::test]
    async fn each_change_reaches_the_store_and_a_bad_post_does_not() {
        let calls = Arc::new(Calls::default());
        let handler = ChangeViewHistoryHandler { history: Some(Arc::clone(&calls) as Arc<dyn ViewHistory>) };
        let post = uuid::Uuid::now_v7().to_string();
        for change in [
            HistoryChange::Viewed { post_id: post.clone() },
            HistoryChange::Removed { post_id: post.clone() },
            HistoryChange::Cleared,
        ] {
            handler.handle(Envelope::new(uuid::Uuid::now_v7(), command(change))).await.unwrap();
        }
        assert_eq!(*calls.0.lock().unwrap(), vec![format!("record me {post}"), format!("remove me {post}"), "clear me".into()]);

        let bad = command(HistoryChange::Viewed { post_id: "not a post".into() });
        assert!(handler.handle(Envelope::new(uuid::Uuid::now_v7(), bad)).await.is_err());
        let none = ChangeViewHistoryHandler { history: None };
        let err = none.handle(Envelope::new(uuid::Uuid::now_v7(), command(HistoryChange::Cleared))).await.unwrap_err();
        assert!(matches!(err, EngagementError::LedgerUnavailable), "{err:?}");
    }

    #[test]
    fn a_change_needs_an_account_a_profile_and_a_post() {
        let mut cmd = command(HistoryChange::Removed { post_id: " ".into() });
        assert!(cmd.validate().is_err());
        cmd.change = HistoryChange::Cleared;
        assert!(cmd.validate().is_ok(), "a clear names no post");
        cmd.account_id = "nope".into();
        assert!(cmd.validate().is_err());
    }
}
