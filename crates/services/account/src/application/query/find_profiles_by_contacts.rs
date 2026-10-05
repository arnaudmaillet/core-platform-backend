//! Finding the profiles of one's address book (#661).
//!
//! The app sends SHA-256 hashes of its contacts' normalized email addresses
//! (lower-cased, trimmed) and phone numbers (E.164); the server keeps none of
//! them. A contact matches an **active** account whose **verified** email or
//! phone hashes the same. Each matched account's profiles are then listed
//! when they are active and findable through that channel (`by_email` /
//! `by_phone`, profile's discovery settings), except any the caller blocks
//! or is blocked by (or a hidden one), and never the caller's own.
//!
//! Hashes of phone numbers hide nothing (a numbering plan hashes in
//! minutes), so lookups are bounded per account and UTC day
//! ([`DAILY_CONTACT_LOOKUPS`]): every submitted hash counts, matched or not,
//! reserved before anything is matched; past it, `ACC-7007`.

use std::collections::HashSet;
use std::sync::Arc;

use cqrs::{Envelope, Query, QueryHandler};

use crate::application::port::{ContactChannel, ContactIndex, ContactLookupQuota, DirectoryProfile, ProfileDirectory};
use crate::domain::value_object::AccountId;
use crate::error::AccountError;

/// Most hashes per call: one page of an address book.
pub const MAX_CONTACT_HASHES: usize = 1_000;

/// Most hashes one account looks up per UTC day: an address book or two.
pub const DAILY_CONTACT_LOOKUPS: i64 = 5_000;

pub struct FindProfilesByContactsQuery {
    pub account_id:   String,
    pub email_hashes: Vec<Vec<u8>>,
    pub phone_hashes: Vec<Vec<u8>>,
}

/// A profile found through one of the caller's contacts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FoundProfile {
    /// The hash the app sent, so it names the contact.
    pub hash:    Vec<u8>,
    pub channel: ContactChannel,
    pub profile: DirectoryProfile,
}

impl Query for FindProfilesByContactsQuery {
    type Response = Vec<FoundProfile>;
}

pub struct FindProfilesByContactsHandler {
    contacts:    Arc<dyn ContactIndex>,
    quota:       Arc<dyn ContactLookupQuota>,
    directory:   Option<Arc<dyn ProfileDirectory>>,
    daily_limit: i64,
}

impl FindProfilesByContactsHandler {
    /// Without a directory (profile / social-graph not configured), matching
    /// is unavailable (`ACC-7006`).
    pub fn new(
        contacts: Arc<dyn ContactIndex>,
        quota: Arc<dyn ContactLookupQuota>,
        directory: Option<Arc<dyn ProfileDirectory>>,
    ) -> Self {
        Self { contacts, quota, directory, daily_limit: DAILY_CONTACT_LOOKUPS }
    }

    /// A different daily budget (scenarios).
    pub fn with_daily_limit(mut self, daily_limit: i64) -> Self {
        self.daily_limit = daily_limit;
        self
    }
}

fn violation(message: &str) -> AccountError {
    AccountError::DomainViolation { field: "contact_hashes".to_owned(), message: message.to_owned() }
}

impl QueryHandler<FindProfilesByContactsQuery> for FindProfilesByContactsHandler {
    type Error = AccountError;

    async fn handle(&self, envelope: Envelope<FindProfilesByContactsQuery>) -> Result<Vec<FoundProfile>, AccountError> {
        let q = &envelope.payload;
        let caller = AccountId::try_from(q.account_id.as_str())?;
        if q.email_hashes.len() + q.phone_hashes.len() > MAX_CONTACT_HASHES {
            return Err(violation("at most 1000 hashes per call"));
        }
        if q.email_hashes.iter().chain(&q.phone_hashes).any(|h| h.len() != 32) {
            return Err(violation("each hash is a 32-byte SHA-256"));
        }
        if q.email_hashes.is_empty() && q.phone_hashes.is_empty() {
            return Ok(Vec::new());
        }
        let directory = self
            .directory
            .as_ref()
            .ok_or_else(|| AccountError::DirectoryUnavailable { reason: "no profile directory configured".to_owned() })?;
        let submitted = (q.email_hashes.len() + q.phone_hashes.len()) as i64;
        let today = chrono::Utc::now().date_naive();
        if !self.quota.reserve(&caller, today, submitted, self.daily_limit).await? {
            return Err(AccountError::ContactLookupQuotaExceeded { limit: self.daily_limit });
        }

        let matches = self.contacts.match_contacts(&q.email_hashes, &q.phone_hashes).await?;
        let mut found = Vec::new();
        let mut seen = HashSet::new();
        for m in matches.into_iter().filter(|m| m.account_id != caller) {
            for profile in directory.profiles_of(&m.account_id).await? {
                let findable = match m.channel {
                    ContactChannel::Email => profile.by_email,
                    ContactChannel::Phone => profile.by_phone,
                };
                if profile.active && findable && seen.insert((profile.profile_id.clone(), m.hash.clone())) {
                    found.push(FoundProfile { hash: m.hash.clone(), channel: m.channel, profile });
                }
            }
        }
        if found.is_empty() {
            return Ok(found);
        }
        let viewers: Vec<String> = directory.profiles_of(&caller).await?.into_iter().map(|p| p.profile_id).collect();
        let targets: Vec<String> = found.iter().map(|f| f.profile.profile_id.clone()).collect();
        let hidden = directory.hidden_from(&viewers, &targets).await?;
        found.retain(|f| !hidden.contains(&f.profile.profile_id));
        Ok(found)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use async_trait::async_trait;

    use super::*;
    use crate::application::port::ContactMatch;

    #[derive(Default)]
    struct Index(Vec<ContactMatch>);

    #[async_trait]
    impl ContactIndex for Index {
        async fn match_contacts(&self, email: &[Vec<u8>], phone: &[Vec<u8>]) -> Result<Vec<ContactMatch>, AccountError> {
            Ok(self
                .0
                .iter()
                .filter(|m| match m.channel {
                    ContactChannel::Email => email.contains(&m.hash),
                    ContactChannel::Phone => phone.contains(&m.hash),
                })
                .cloned()
                .collect())
        }
    }

    /// The daily budget, in memory.
    #[derive(Default)]
    struct Quota(Mutex<HashMap<AccountId, i64>>);

    #[async_trait]
    impl ContactLookupQuota for Quota {
        async fn reserve(&self, account: &AccountId, _: chrono::NaiveDate, n: i64, limit: i64) -> Result<bool, AccountError> {
            let mut used = self.0.lock().unwrap();
            let total = used.entry(*account).or_default();
            if *total + n > limit {
                return Ok(false);
            }
            *total += n;
            Ok(true)
        }
    }

    #[derive(Default)]
    struct Directory {
        profiles: HashMap<AccountId, Vec<DirectoryProfile>>,
        hidden:   HashSet<String>,
        asked:    Mutex<Vec<String>>,
    }

    #[async_trait]
    impl ProfileDirectory for Directory {
        async fn profiles_of(&self, account: &AccountId) -> Result<Vec<DirectoryProfile>, AccountError> {
            Ok(self.profiles.get(account).cloned().unwrap_or_default())
        }
        async fn hidden_from(&self, viewers: &[String], targets: &[String]) -> Result<HashSet<String>, AccountError> {
            self.asked.lock().unwrap().extend(viewers.iter().cloned());
            Ok(targets.iter().filter(|t| self.hidden.contains(*t)).cloned().collect())
        }
    }

    fn profile(id: &str, by_email: bool, by_phone: bool) -> DirectoryProfile {
        DirectoryProfile {
            profile_id: id.into(),
            handle: id.into(),
            display_name: id.into(),
            avatar_url: None,
            active: true,
            by_email,
            by_phone,
        }
    }

    fn hash(n: u8) -> Vec<u8> {
        vec![n; 32]
    }

    async fn find(handler: &FindProfilesByContactsHandler, caller: &AccountId, email: Vec<Vec<u8>>, phone: Vec<Vec<u8>>) -> Result<Vec<FoundProfile>, AccountError> {
        let query = FindProfilesByContactsQuery { account_id: caller.as_uuid().to_string(), email_hashes: email, phone_hashes: phone };
        handler.handle(Envelope::new(uuid::Uuid::now_v7(), query)).await
    }

    #[tokio::test]
    async fn a_contact_finds_the_profiles_findable_through_that_channel_except_blocked_ones() {
        let (me, friend, stranger) = (AccountId::new(), AccountId::new(), AccountId::new());
        let index = Index(vec![
            ContactMatch { account_id: friend, channel: ContactChannel::Email, hash: hash(1) },
            ContactMatch { account_id: stranger, channel: ContactChannel::Phone, hash: hash(2) },
            ContactMatch { account_id: me, channel: ContactChannel::Email, hash: hash(3) },
        ]);
        let mut hidden_profile = profile("friend-blocked", true, true);
        hidden_profile.handle = "blocked".into();
        let mut inactive = profile("friend-hidden", true, true);
        inactive.active = false;
        let directory = Directory {
            profiles: HashMap::from([
                (friend, vec![profile("friend-main", true, false), profile("friend-alt", false, true), hidden_profile, inactive]),
                (stranger, vec![profile("stranger-main", true, false)]),
                (me, vec![profile("me-main", true, true)]),
            ]),
            hidden: HashSet::from(["friend-blocked".to_owned()]),
            asked: Mutex::new(Vec::new()),
        };
        let handler =
            FindProfilesByContactsHandler::new(Arc::new(index), Arc::new(Quota::default()), Some(Arc::new(directory)));

        let found = find(&handler, &me, vec![hash(1), hash(3)], vec![hash(2)]).await.unwrap();
        let ids: Vec<_> = found.iter().map(|f| f.profile.profile_id.as_str()).collect();
        assert_eq!(ids, vec!["friend-main"], "email-findable, active, not blocked; never oneself; the stranger hides its phone");
        assert_eq!(found[0].hash, hash(1), "the app learns which contact it was");
        assert_eq!(found[0].channel, ContactChannel::Email);
    }

    #[tokio::test]
    async fn malformed_or_too_many_hashes_are_refused_and_no_directory_is_unavailable() {
        let me = AccountId::new();
        let handler = FindProfilesByContactsHandler::new(
            Arc::new(Index::default()),
            Arc::new(Quota::default()),
            Some(Arc::new(Directory::default())),
        );
        assert!(matches!(find(&handler, &me, vec![vec![1; 31]], vec![]).await, Err(AccountError::DomainViolation { .. })));
        let many = vec![hash(1); MAX_CONTACT_HASHES + 1];
        assert!(matches!(find(&handler, &me, many, vec![]).await, Err(AccountError::DomainViolation { .. })));
        assert!(find(&handler, &me, vec![], vec![]).await.unwrap().is_empty());

        let off = FindProfilesByContactsHandler::new(Arc::new(Index::default()), Arc::new(Quota::default()), None);
        assert!(matches!(find(&off, &me, vec![hash(1)], vec![]).await, Err(AccountError::DirectoryUnavailable { .. })));
    }

    /// Every submitted hash counts, matched or not; past the day's budget the
    /// call is refused before anything is matched.
    #[tokio::test]
    async fn the_daily_budget_counts_every_hash_and_refuses_past_it() {
        let me = AccountId::new();
        let handler = FindProfilesByContactsHandler::new(
            Arc::new(Index::default()),
            Arc::new(Quota::default()),
            Some(Arc::new(Directory::default())),
        )
        .with_daily_limit(5);
        find(&handler, &me, vec![hash(1), hash(2)], vec![hash(3)]).await.unwrap();
        let err = find(&handler, &me, vec![hash(4), hash(5), hash(6)], vec![]).await.unwrap_err();
        assert!(matches!(err, AccountError::ContactLookupQuotaExceeded { limit: 5 }), "{err:?}");
        find(&handler, &me, vec![hash(7), hash(8)], vec![]).await.expect("a refused call reserves nothing");
        assert!(find(&handler, &AccountId::new(), vec![hash(1); 5], vec![]).await.is_ok(), "per account");
    }
}
