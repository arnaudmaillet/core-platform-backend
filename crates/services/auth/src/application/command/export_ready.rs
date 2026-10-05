//! A delivered GDPR data export reaches the holder (#653): on account's
//! `gdpr_data_export_completed`, the link — signed by account when its GDPR
//! record is read, never carried on the event — is emailed to the account's
//! address. Without an email on file (a phone-only account), the app shows
//! the link in the holder's GDPR status instead.

use std::sync::Arc;

use crate::application::command::verification::VerificationCodes;
use crate::application::port::AccountDirectory;
use crate::domain::value_object::AccountId;
use crate::error::AuthError;

/// What telling the holder came to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExportReadyOutcome {
    Emailed,
    /// No link to hand out any more (superseded by a newer request, expired).
    NoLink,
    /// No email on file.
    NoEmail,
}

pub struct ExportReadyNotifier {
    directory: Arc<dyn AccountDirectory>,
    codes: Arc<VerificationCodes>,
}

impl ExportReadyNotifier {
    pub fn new(directory: Arc<dyn AccountDirectory>, codes: Arc<VerificationCodes>) -> Self {
        Self { directory, codes }
    }

    pub async fn notify(&self, account_id: &AccountId) -> Result<ExportReadyOutcome, AuthError> {
        let Some((link, expires_at)) = self.directory.export_link(account_id).await? else {
            return Ok(ExportReadyOutcome::NoLink);
        };
        let Some(email) = self.directory.contact(account_id).await?.email else {
            return Ok(ExportReadyOutcome::NoEmail);
        };
        self.codes.notify_export_ready(&email, &link, expires_at, None).await?;
        Ok(ExportReadyOutcome::Emailed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::command::verification::VerificationPolicy;
    use crate::application::fakes::{InMemoryVerificationStore, RecordingCodeSender, StubAccountDirectory};
    use crate::application::port::ContactDetails;
    use uuid::Uuid;

    #[tokio::test]
    async fn the_link_is_emailed_when_there_is_one_and_an_address() {
        let directory = Arc::new(StubAccountDirectory::new());
        let sender = Arc::new(RecordingCodeSender::default());
        let codes = Arc::new(VerificationCodes::new(
            Arc::new(InMemoryVerificationStore::default()),
            Arc::clone(&sender) as _,
            VerificationPolicy::default(),
        ));
        let notifier = ExportReadyNotifier::new(Arc::clone(&directory) as _, codes);
        let account = AccountId::from_uuid(Uuid::now_v7());

        assert_eq!(notifier.notify(&account).await.unwrap(), ExportReadyOutcome::NoLink);
        let expires = chrono::Utc::now() + chrono::Duration::days(7);
        directory.with_export_link(account, "https://exports.test/a.zip", expires);
        assert_eq!(notifier.notify(&account).await.unwrap(), ExportReadyOutcome::NoEmail);
        directory.with_contact(account, ContactDetails { email: Some("me@example.com".into()), phone: None });
        assert_eq!(notifier.notify(&account).await.unwrap(), ExportReadyOutcome::Emailed);
        assert_eq!(
            sender.export_notices(),
            vec![("me@example.com".to_owned(), "https://exports.test/a.zip".to_owned())]
        );
    }
}
