use std::sync::Arc;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::application::port::AccountRepository;
use crate::error::AccountError;

/// What one janitor pass did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct JanitorPass {
    pub anonymized: usize,
    /// Cancelled or changed concurrently since it was listed; retried next pass
    /// if still due.
    pub skipped: usize,
}

/// The GDPR janitor: anonymizes every account whose erasure grace period has
/// ended (`deletion_scheduled_at`, 30 days after the request) — the deletion
/// the app promises. Safe to run from every replica: each account is
/// re-checked after loading, and a concurrent write (another janitor, a
/// cancel) fails the optimistic CAS and is skipped.
pub struct AnonymizeDueAccounts {
    repo: Arc<dyn AccountRepository>,
}

impl AnonymizeDueAccounts {
    pub fn new(repo: Arc<dyn AccountRepository>) -> Self {
        Self { repo }
    }

    pub async fn run(&self, now: DateTime<Utc>, batch: i64) -> Result<JanitorPass, AccountError> {
        let mut pass = JanitorPass::default();
        for id in self.repo.list_due_for_anonymization(now, batch).await? {
            let Some(mut account) = self.repo.find_by_id(&id).await? else {
                continue;
            };
            if !account.is_due_for_anonymization(now) {
                pass.skipped += 1;
                continue;
            }
            account.anonymize(Uuid::now_v7())?;
            match self.repo.save(&account).await {
                Ok(()) => pass.anonymized += 1,
                Err(AccountError::ConcurrentModification) => pass.skipped += 1,
                Err(e) => return Err(e),
            }
        }
        Ok(pass)
    }
}
