//! Family supervision (#670), part 1: pairing. A parent and a teen pair
//! through an invite code either side creates; both see the link; either
//! ends it; it ends when the teen turns 18 or either account is erased.
//! Every start and end is published (`account.v1.events`) with both sides'
//! profiles, so each side can be told.

use std::sync::Arc;

use chrono::{DateTime, DurationRound, Utc};
use uuid::Uuid;

use crate::application::port::{
    AccountAges, DirectoryProfile, EventPublisher, Linked, ProfileDirectory, SupervisionStore,
};
use crate::domain::event::{
    DomainEvent, SupervisionEnded, SupervisionLimitsCleared, SupervisionLimitsSet, SupervisionStarted,
};
use crate::domain::supervision::{
    local_day, InviteCode, LimitsRecord, Supervision, SupervisionEnd, SupervisionInvite, SupervisionLimits,
    SupervisionRole, MAX_FAILED_ACCEPTS_PER_HOUR, MAX_REPORTED_MINUTES, MAX_SUPERVISORS,
};
use crate::domain::value_object::AccountId;
use crate::error::AccountError;

/// Supervisions the sweep ends per pass.
const SWEEP_BATCH: i64 = 200;

/// A supervision as one side sees it: the other side, and since when.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupervisionView {
    /// The other side's role: `Teen` for a supervisor's view.
    pub role:     SupervisionRole,
    pub account:  AccountId,
    pub since:    DateTime<Utc>,
    /// The other side's active profiles, to show who it is.
    pub profiles: Vec<DirectoryProfile>,
}

/// A teen's time today against their limit (#670 part 2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScreenTime {
    pub used_minutes:  i32,
    /// `None`: no daily limit.
    pub limit_minutes: Option<u16>,
    /// The app shows the pause screen.
    pub reached:       bool,
}

pub struct Supervisions {
    store:     Arc<dyn SupervisionStore>,
    ages:      Arc<dyn AccountAges>,
    profiles:  Arc<dyn ProfileDirectory>,
    publisher: Arc<dyn EventPublisher>,
}

impl Supervisions {
    pub fn new(
        store: Arc<dyn SupervisionStore>,
        ages: Arc<dyn AccountAges>,
        profiles: Arc<dyn ProfileDirectory>,
        publisher: Arc<dyn EventPublisher>,
    ) -> Self {
        Self { store, ages, profiles, publisher }
    }

    /// `account` invites the other side: the code to share (24 h, single use).
    pub async fn create_invite(&self, account: &str, role: SupervisionRole, now: DateTime<Utc>) -> Result<SupervisionInvite, AccountError> {
        let account = AccountId::try_from(account)?;
        let age = self.ages.age_bracket(&account, now.date_naive()).await?;
        let invite = SupervisionInvite::create(account, role, age, now)?;
        self.store.put_invite(&invite).await?;
        Ok(invite)
    }

    /// `account` accepts an invite: the pairing, seen from `account`.
    ///
    /// An unknown or expired code counts against the account's hourly cap
    /// (`ACC-3007` past [`MAX_FAILED_ACCEPTS_PER_HOUR`]). The invite is then
    /// claimed for `account` atomically — after the checks, so a wrong
    /// acceptor never spends it: a second acceptor racing on the same code
    /// gets `ACC-3001`; the same acceptor's retry completes.
    pub async fn accept(&self, account: &str, code: &str, now: DateTime<Utc>) -> Result<SupervisionView, AccountError> {
        let account = AccountId::try_from(account)?;
        let hour = now.duration_trunc(chrono::TimeDelta::hours(1)).unwrap_or(now);
        if self.store.failed_accepts(&account, hour).await? >= MAX_FAILED_ACCEPTS_PER_HOUR {
            return Err(AccountError::SupervisionAttemptsExceeded);
        }
        let found = match InviteCode::parse(code) {
            Some(code) => self.store.find_invite(&code).await?.map(|invite| (code, invite)),
            None => None,
        };
        let Some((code, invite)) = found else {
            self.store.record_failed_accept(&account, hour).await?;
            return Err(AccountError::SupervisionInviteInvalid);
        };
        let today = now.date_naive();
        // The creator must still fit their side (a teen may have turned 18).
        if !invite.role.fits(self.ages.age_bracket(&invite.creator, today).await?) {
            return Err(AccountError::SupervisionInviteInvalid);
        }
        let link = match invite.accept(account, self.ages.age_bracket(&account, today).await?, now) {
            Err(AccountError::SupervisionInviteInvalid) => {
                self.store.record_failed_accept(&account, hour).await?;
                return Err(AccountError::SupervisionInviteInvalid);
            }
            other => other?,
        };
        if !self.store.claim_invite(&code, &account).await? {
            return Err(AccountError::SupervisionInviteInvalid);
        }
        // Linked first, announced, then the invite spent: a retry of a
        // half-done accept (the invite still there) completes it, at the cost
        // of announcing twice.
        match self.store.link(&link, MAX_SUPERVISORS).await? {
            Linked::Created | Linked::Existing => {}
        }
        let (teen_profiles, supervisor_profiles) = self.both_profiles(&link).await?;
        self.publisher
            .publish(&DomainEvent::SupervisionStarted(SupervisionStarted {
                account_id: link.teen,
                supervisor_id: link.supervisor,
                teen_profile_ids: ids(&teen_profiles),
                supervisor_profile_ids: ids(&supervisor_profiles),
                occurred_at: now,
                correlation_id: Uuid::now_v7(),
            }))
            .await?;
        self.store.delete_invite(&code).await?;
        let (role, other) = link.counterpart(&account).ok_or(AccountError::SupervisionNotFound)?;
        let profiles = if other == link.teen { teen_profiles } else { supervisor_profiles };
        Ok(SupervisionView { role, account: other, since: link.since, profiles: active(profiles) })
    }

    /// `account`'s supervisions, both sides: its supervisors (it is a teen)
    /// and the teens it supervises.
    pub async fn list(&self, account: &str) -> Result<Vec<SupervisionView>, AccountError> {
        let account = AccountId::try_from(account)?;
        let mut links = self.store.supervisors_of(&account).await?;
        links.extend(self.store.teens_of(&account).await?);
        let mut views = Vec::with_capacity(links.len());
        for link in links {
            let Some((role, other)) = link.counterpart(&account) else { continue };
            let profiles = active(self.profiles.profiles_of(&other).await?);
            views.push(SupervisionView { role, account: other, since: link.since, profiles });
        }
        views.sort_by_key(|v| std::cmp::Reverse(v.since));
        Ok(views)
    }

    /// `account` ends its supervision with `other` (either side may);
    /// returns what is left. The other side is told.
    pub async fn end(&self, account: &str, other: &str, now: DateTime<Utc>) -> Result<Vec<SupervisionView>, AccountError> {
        let me = AccountId::try_from(account)?;
        let other = AccountId::try_from(other).map_err(|_| AccountError::SupervisionNotFound)?;
        // As the teen, then as the supervisor.
        let (link, end) = if let Some(link) = self.find(&me, &other).await? {
            (link, SupervisionEnd::ByTeen)
        } else if let Some(link) = self.find(&other, &me).await? {
            (link, SupervisionEnd::BySupervisor)
        } else {
            return Err(AccountError::SupervisionNotFound);
        };
        self.finish(&link, end, now).await?;
        self.list(account).await
    }

    /// The daily pass: supervisions whose teen turned 18 end; expired
    /// invites go. Returns how many supervisions ended.
    pub async fn sweep(&self, now: DateTime<Utc>) -> Result<usize, AccountError> {
        let due = self.store.came_of_age(now.date_naive(), SWEEP_BATCH).await?;
        for link in &due {
            self.finish(link, SupervisionEnd::CameOfAge, now).await?;
        }
        self.store.purge_expired_invites(now).await?;
        Ok(due.len())
    }

    /// Before `account` is erased: every supervision it is part of ends.
    pub async fn end_all(&self, account: &AccountId, now: DateTime<Utc>) -> Result<(), AccountError> {
        let mut links = self.store.supervisors_of(account).await?;
        links.extend(self.store.teens_of(account).await?);
        for link in &links {
            self.finish(link, SupervisionEnd::AccountDeleted, now).await?;
        }
        Ok(())
    }

    /// A supervisor sets its teen's limits (#670 part 2): one shared set,
    /// replacing the previous one; published so profile applies and locks them.
    pub async fn set_limits(
        &self,
        supervisor: &str,
        teen: &str,
        limits: SupervisionLimits,
        now: DateTime<Utc>,
    ) -> Result<LimitsRecord, AccountError> {
        let supervisor = AccountId::try_from(supervisor)?;
        let teen = AccountId::try_from(teen).map_err(|_| AccountError::SupervisionNotFound)?;
        if self.find(&teen, &supervisor).await?.is_none() {
            return Err(AccountError::SupervisionNotFound);
        }
        limits.validate()?;
        let record = LimitsRecord { limits, set_by: supervisor, set_at: now };
        self.store.put_limits(&teen, &record).await?;
        let l = &record.limits;
        self.publisher
            .publish(&DomainEvent::SupervisionLimitsSet(SupervisionLimitsSet {
                account_id: teen,
                set_by: supervisor,
                private_account: l.private_account,
                messages: l.messages.map(|a| a.as_str().to_owned()),
                comments: l.comments.map(|a| a.as_str().to_owned()),
                hidden_from_search: l.hidden_from_search,
                daily_minutes: l.daily_minutes,
                teen_profile_ids: ids(&self.profiles.profiles_of(&teen).await?),
                occurred_at: now,
                correlation_id: Uuid::now_v7(),
            }))
            .await?;
        Ok(record)
    }

    /// A teen's limits, read by the teen themselves or one of their
    /// supervisors (`teen` empty: the caller's own). `None`: no limits.
    pub async fn limits(&self, account: &str, teen: &str) -> Result<Option<LimitsRecord>, AccountError> {
        let account = AccountId::try_from(account)?;
        let teen = if teen.is_empty() { account } else { AccountId::try_from(teen).map_err(|_| AccountError::SupervisionNotFound)? };
        if teen != account && self.find(&teen, &account).await?.is_none() {
            return Err(AccountError::SupervisionNotFound);
        }
        self.store.limits(&teen).await
    }

    /// The app reports `minutes` more of use (at most 15 at a time) and learns
    /// whether the day's limit is reached. Only a teen with a daily limit is
    /// counted; the day is local to `timezone` (UTC when invalid).
    pub async fn report_time(&self, account: &str, minutes: u16, timezone: &str, now: DateTime<Utc>) -> Result<ScreenTime, AccountError> {
        let account = AccountId::try_from(account)?;
        if minutes > MAX_REPORTED_MINUTES {
            return Err(AccountError::DomainViolation {
                field:   "minutes".into(),
                message: format!("a report adds at most {MAX_REPORTED_MINUTES} minutes"),
            });
        }
        let Some(limit) = self.store.limits(&account).await?.and_then(|r| r.limits.daily_minutes) else {
            return Ok(ScreenTime { used_minutes: 0, limit_minutes: None, reached: false });
        };
        let used = self.store.add_usage(&account, local_day(now, timezone), i32::from(minutes)).await?;
        Ok(ScreenTime { used_minutes: used, limit_minutes: Some(limit), reached: used >= i32::from(limit) })
    }

    async fn find(&self, teen: &AccountId, supervisor: &AccountId) -> Result<Option<Supervision>, AccountError> {
        Ok(self.store.supervisors_of(teen).await?.into_iter().find(|l| l.supervisor == *supervisor))
    }

    /// Announced, then unlinked: a failed unlink retries and announces again
    /// (at least once), never an unannounced end.
    async fn finish(&self, link: &Supervision, end: SupervisionEnd, now: DateTime<Utc>) -> Result<(), AccountError> {
        let (teen_profiles, supervisor_profiles) = self.both_profiles(link).await?;
        self.publisher
            .publish(&DomainEvent::SupervisionEnded(SupervisionEnded {
                account_id: link.teen,
                supervisor_id: link.supervisor,
                ended_by: end.as_str().to_owned(),
                teen_profile_ids: ids(&teen_profiles),
                supervisor_profile_ids: ids(&supervisor_profiles),
                occurred_at: now,
                correlation_id: Uuid::now_v7(),
            }))
            .await?;
        self.store.unlink(&link.teen, &link.supervisor).await?;
        // The teen's last supervisor gone: their limits are lifted (the
        // settings keep their values, unlocked).
        if self.store.supervisors_of(&link.teen).await?.is_empty() && self.store.limits(&link.teen).await?.is_some() {
            self.publisher
                .publish(&DomainEvent::SupervisionLimitsCleared(SupervisionLimitsCleared {
                    account_id: link.teen,
                    teen_profile_ids: ids(&teen_profiles),
                    occurred_at: now,
                    correlation_id: Uuid::now_v7(),
                }))
                .await?;
            self.store.clear_limits(&link.teen).await?;
        }
        Ok(())
    }

    async fn both_profiles(&self, link: &Supervision) -> Result<(Vec<DirectoryProfile>, Vec<DirectoryProfile>), AccountError> {
        Ok((self.profiles.profiles_of(&link.teen).await?, self.profiles.profiles_of(&link.supervisor).await?))
    }
}

fn ids(profiles: &[DirectoryProfile]) -> Vec<String> {
    profiles.iter().map(|p| p.profile_id.clone()).collect()
}

fn active(profiles: Vec<DirectoryProfile>) -> Vec<DirectoryProfile> {
    profiles.into_iter().filter(|p| p.active).collect()
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::Mutex;

    use async_trait::async_trait;
    use chrono::{Duration, NaiveDate};

    use super::*;
    use crate::domain::value_object::AgeBracket;

    #[derive(Default)]
    struct Store {
        invites: Mutex<HashMap<String, SupervisionInvite>>,
        claims:  Mutex<HashMap<String, AccountId>>,
        limits:  Mutex<HashMap<AccountId, LimitsRecord>>,
        usage:   Mutex<HashMap<(AccountId, NaiveDate), i32>>,
        failures: Mutex<HashMap<(AccountId, DateTime<Utc>), i64>>,
        links:   Mutex<Vec<Supervision>>,
        /// Teens who are 18 on any day asked.
        adults:  Mutex<HashSet<AccountId>>,
    }

    #[async_trait]
    impl SupervisionStore for Store {
        async fn put_invite(&self, invite: &SupervisionInvite) -> Result<(), AccountError> {
            self.invites.lock().unwrap().insert(invite.code.as_str().to_owned(), invite.clone());
            Ok(())
        }
        async fn find_invite(&self, code: &InviteCode) -> Result<Option<SupervisionInvite>, AccountError> {
            Ok(self.invites.lock().unwrap().get(code.as_str()).cloned())
        }
        async fn delete_invite(&self, code: &InviteCode) -> Result<(), AccountError> {
            self.invites.lock().unwrap().remove(code.as_str());
            Ok(())
        }
        async fn claim_invite(&self, code: &InviteCode, acceptor: &AccountId) -> Result<bool, AccountError> {
            if !self.invites.lock().unwrap().contains_key(code.as_str()) {
                return Ok(false);
            }
            let mut claims = self.claims.lock().unwrap();
            Ok(*claims.entry(code.as_str().to_owned()).or_insert(*acceptor) == *acceptor)
        }
        async fn limits(&self, teen: &AccountId) -> Result<Option<LimitsRecord>, AccountError> {
            Ok(self.limits.lock().unwrap().get(teen).cloned())
        }
        async fn put_limits(&self, teen: &AccountId, record: &LimitsRecord) -> Result<(), AccountError> {
            self.limits.lock().unwrap().insert(*teen, record.clone());
            Ok(())
        }
        async fn clear_limits(&self, teen: &AccountId) -> Result<bool, AccountError> {
            Ok(self.limits.lock().unwrap().remove(teen).is_some())
        }
        async fn add_usage(&self, account: &AccountId, day: NaiveDate, minutes: i32) -> Result<i32, AccountError> {
            let mut usage = self.usage.lock().unwrap();
            let total = usage.entry((*account, day)).or_insert(0);
            *total += minutes;
            Ok(*total)
        }
        async fn failed_accepts(&self, account: &AccountId, hour: DateTime<Utc>) -> Result<i64, AccountError> {
            Ok(self.failures.lock().unwrap().get(&(*account, hour)).copied().unwrap_or(0))
        }
        async fn record_failed_accept(&self, account: &AccountId, hour: DateTime<Utc>) -> Result<(), AccountError> {
            *self.failures.lock().unwrap().entry((*account, hour)).or_insert(0) += 1;
            Ok(())
        }
        async fn link(&self, link: &Supervision, max: usize) -> Result<Linked, AccountError> {
            let mut links = self.links.lock().unwrap();
            if links.iter().any(|l| l.teen == link.teen && l.supervisor == link.supervisor) {
                return Ok(Linked::Existing);
            }
            if links.iter().filter(|l| l.teen == link.teen).count() >= max {
                return Err(AccountError::SupervisorLimitReached);
            }
            links.push(link.clone());
            Ok(Linked::Created)
        }
        async fn unlink(&self, teen: &AccountId, supervisor: &AccountId) -> Result<bool, AccountError> {
            let mut links = self.links.lock().unwrap();
            let before = links.len();
            links.retain(|l| !(l.teen == *teen && l.supervisor == *supervisor));
            Ok(links.len() < before)
        }
        async fn supervisors_of(&self, teen: &AccountId) -> Result<Vec<Supervision>, AccountError> {
            Ok(self.links.lock().unwrap().iter().filter(|l| l.teen == *teen).cloned().collect())
        }
        async fn teens_of(&self, supervisor: &AccountId) -> Result<Vec<Supervision>, AccountError> {
            Ok(self.links.lock().unwrap().iter().filter(|l| l.supervisor == *supervisor).cloned().collect())
        }
        async fn came_of_age(&self, _today: NaiveDate, _limit: i64) -> Result<Vec<Supervision>, AccountError> {
            let adults = self.adults.lock().unwrap();
            Ok(self.links.lock().unwrap().iter().filter(|l| adults.contains(&l.teen)).cloned().collect())
        }
        async fn purge_expired_invites(&self, now: DateTime<Utc>) -> Result<u64, AccountError> {
            let mut invites = self.invites.lock().unwrap();
            let before = invites.len();
            invites.retain(|_, i| i.expires_at > now);
            Ok((before - invites.len()) as u64)
        }
    }

    #[derive(Default)]
    struct Ages(Mutex<HashMap<AccountId, AgeBracket>>);

    #[async_trait]
    impl AccountAges for Ages {
        async fn age_bracket(&self, account: &AccountId, _today: NaiveDate) -> Result<Option<AgeBracket>, AccountError> {
            Ok(self.0.lock().unwrap().get(account).copied())
        }
    }

    struct Profiles;

    #[async_trait]
    impl ProfileDirectory for Profiles {
        async fn profiles_of(&self, account_id: &AccountId) -> Result<Vec<DirectoryProfile>, AccountError> {
            Ok(vec![DirectoryProfile {
                profile_id:   format!("p-{account_id}"),
                handle:       "handle".into(),
                display_name: "Name".into(),
                avatar_url:   None,
                active:       true,
                by_email:     false,
                by_phone:     false,
            }])
        }
        async fn hidden_from(&self, _: &[String], _: &[String]) -> Result<HashSet<String>, AccountError> {
            Ok(HashSet::new())
        }
    }

    #[derive(Default)]
    struct Published(Mutex<Vec<DomainEvent>>);

    #[async_trait]
    impl EventPublisher for Published {
        async fn publish(&self, event: &DomainEvent) -> Result<(), AccountError> {
            self.0.lock().unwrap().push(event.clone());
            Ok(())
        }
    }

    struct World {
        store:     Arc<Store>,
        ages:      Arc<Ages>,
        published: Arc<Published>,
        handler:   Supervisions,
    }

    fn world() -> World {
        let (store, ages, published) = (Arc::new(Store::default()), Arc::new(Ages::default()), Arc::new(Published::default()));
        let handler = Supervisions::new(
            Arc::clone(&store) as _,
            Arc::clone(&ages) as _,
            Arc::new(Profiles),
            Arc::clone(&published) as _,
        );
        World { store, ages, published, handler }
    }

    impl World {
        fn account(&self, age: AgeBracket) -> AccountId {
            let id = AccountId::new();
            self.ages.0.lock().unwrap().insert(id, age);
            id
        }

        async fn pair(&self, parent: AccountId, teen: AccountId) -> Result<SupervisionView, AccountError> {
            let invite = self.handler.create_invite(&parent.to_string(), SupervisionRole::Supervisor, Utc::now()).await?;
            self.handler.accept(&teen.to_string(), invite.code.as_str(), Utc::now()).await
        }

        fn events(&self) -> Vec<(String, Option<String>)> {
            self.published
                .0
                .lock()
                .unwrap()
                .iter()
                .map(|e| match e {
                    DomainEvent::SupervisionStarted(_) => ("started".into(), None),
                    DomainEvent::SupervisionEnded(e) => ("ended".into(), Some(e.ended_by.clone())),
                    other => (other.event_type().into(), None),
                })
                .collect()
        }
    }

    #[tokio::test]
    async fn a_parent_and_a_teen_pair_and_both_see_it() {
        let w = world();
        let (parent, teen) = (w.account(AgeBracket::Adult), w.account(AgeBracket::Teen13To15));
        let seen_by_teen = w.pair(parent, teen).await.unwrap();
        assert_eq!((seen_by_teen.role, seen_by_teen.account), (SupervisionRole::Supervisor, parent));
        assert_eq!(seen_by_teen.profiles[0].profile_id, format!("p-{parent}"));

        let parent_view = w.handler.list(&parent.to_string()).await.unwrap();
        assert_eq!((parent_view[0].role, parent_view[0].account), (SupervisionRole::Teen, teen));
        assert_eq!(w.handler.list(&teen.to_string()).await.unwrap().len(), 1, "the teen always sees it");

        let DomainEvent::SupervisionStarted(started) = &w.published.0.lock().unwrap()[0] else { panic!() };
        assert_eq!((started.account_id, started.supervisor_id), (teen, parent));
        assert_eq!(started.teen_profile_ids, vec![format!("p-{teen}")]);
        assert!(w.store.invites.lock().unwrap().is_empty(), "the code is spent");
    }

    #[tokio::test]
    async fn an_invite_is_single_use_and_two_supervisors_at_most() {
        let w = world();
        let teen = w.account(AgeBracket::Teen16To17);
        let invite = w.handler.create_invite(&teen.to_string(), SupervisionRole::Teen, Utc::now()).await.unwrap();
        let mum = w.account(AgeBracket::Adult);
        w.handler.accept(&mum.to_string(), invite.code.as_str(), Utc::now()).await.unwrap();
        assert!(matches!(
            w.handler.accept(&w.account(AgeBracket::Adult).to_string(), invite.code.as_str(), Utc::now()).await,
            Err(AccountError::SupervisionInviteInvalid)
        ));

        w.pair(w.account(AgeBracket::Adult), teen).await.unwrap();
        assert!(matches!(w.pair(w.account(AgeBracket::Adult), teen).await, Err(AccountError::SupervisorLimitReached)));
    }

    #[tokio::test]
    async fn either_side_ends_it_and_the_other_side_is_named() {
        let w = world();
        let (parent, teen) = (w.account(AgeBracket::Adult), w.account(AgeBracket::Teen13To15));
        w.pair(parent, teen).await.unwrap();
        assert!(w.handler.end(&teen.to_string(), &parent.to_string(), Utc::now()).await.unwrap().is_empty());
        w.pair(parent, teen).await.unwrap();
        w.handler.end(&parent.to_string(), &teen.to_string(), Utc::now()).await.unwrap();
        assert!(matches!(
            w.handler.end(&parent.to_string(), &teen.to_string(), Utc::now()).await,
            Err(AccountError::SupervisionNotFound)
        ));
        let ends: Vec<_> = w.events().into_iter().filter_map(|(kind, by)| by.filter(|_| kind == "ended")).collect();
        assert_eq!(ends, vec!["by_teen", "by_supervisor"]);
    }

    #[tokio::test]
    async fn it_ends_at_18_and_with_an_erased_account_and_expired_codes_go() {
        let w = world();
        let (parent, teen, other_teen) =
            (w.account(AgeBracket::Adult), w.account(AgeBracket::Teen16To17), w.account(AgeBracket::Teen13To15));
        w.pair(parent, teen).await.unwrap();
        w.pair(parent, other_teen).await.unwrap();

        w.store.adults.lock().unwrap().insert(teen);
        assert_eq!(w.handler.sweep(Utc::now()).await.unwrap(), 1);
        assert_eq!(w.handler.list(&parent.to_string()).await.unwrap().len(), 1);

        w.handler.end_all(&parent, Utc::now()).await.unwrap();
        assert!(w.handler.list(&other_teen.to_string()).await.unwrap().is_empty());
        let ends: Vec<_> = w.events().into_iter().filter_map(|(kind, by)| by.filter(|_| kind == "ended")).collect();
        assert_eq!(ends, vec!["came_of_age", "account_deleted"]);

        let invite = w.handler.create_invite(&parent.to_string(), SupervisionRole::Supervisor, Utc::now() - Duration::days(2)).await.unwrap();
        w.handler.sweep(Utc::now()).await.unwrap();
        assert!(w.store.find_invite(&invite.code).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn a_creator_who_no_longer_fits_voids_the_invite() {
        let w = world();
        let teen = w.account(AgeBracket::Teen16To17);
        let invite = w.handler.create_invite(&teen.to_string(), SupervisionRole::Teen, Utc::now()).await.unwrap();
        w.ages.0.lock().unwrap().insert(teen, AgeBracket::Adult);
        assert!(matches!(
            w.handler.accept(&w.account(AgeBracket::Adult).to_string(), invite.code.as_str(), Utc::now()).await,
            Err(AccountError::SupervisionInviteInvalid)
        ));
    }

    /// A code claimed by one acceptor is not another's: a leaked teen code
    /// racing the real parent never pairs both. The same acceptor's retry
    /// goes through.
    #[tokio::test]
    async fn a_claimed_code_is_one_acceptors_only() {
        let w = world();
        let teen = w.account(AgeBracket::Teen13To15);
        let invite = w.handler.create_invite(&teen.to_string(), SupervisionRole::Teen, Utc::now()).await.unwrap();
        let (parent, stranger) = (w.account(AgeBracket::Adult), w.account(AgeBracket::Adult));
        // The parent's claim lands first (as if their accept were mid-way).
        assert!(w.store.claim_invite(&invite.code, &parent).await.unwrap());
        assert!(matches!(
            w.handler.accept(&stranger.to_string(), invite.code.as_str(), Utc::now()).await,
            Err(AccountError::SupervisionInviteInvalid)
        ));
        w.handler.accept(&parent.to_string(), invite.code.as_str(), Utc::now()).await.expect("the claimant completes");
        assert_eq!(w.handler.list(&teen.to_string()).await.unwrap().len(), 1, "only the parent");
    }

    /// A wrong acceptor never spends the code; guessing is capped per hour.
    #[tokio::test]
    async fn a_wrong_acceptor_keeps_the_code_alive_and_guessing_is_capped() {
        let w = world();
        let teen = w.account(AgeBracket::Teen16To17);
        let invite = w.handler.create_invite(&teen.to_string(), SupervisionRole::Teen, Utc::now()).await.unwrap();
        let other_teen = w.account(AgeBracket::Teen13To15);
        assert!(matches!(
            w.handler.accept(&other_teen.to_string(), invite.code.as_str(), Utc::now()).await,
            Err(AccountError::SupervisionRoleNotAllowed { .. })
        ));
        let parent = w.account(AgeBracket::Adult);
        w.handler.accept(&parent.to_string(), invite.code.as_str(), Utc::now()).await.expect("still claimable");

        let guesser = w.account(AgeBracket::Adult);
        for _ in 0..MAX_FAILED_ACCEPTS_PER_HOUR {
            let wrong = InviteCode::generate();
            assert!(matches!(
                w.handler.accept(&guesser.to_string(), wrong.as_str(), Utc::now()).await,
                Err(AccountError::SupervisionInviteInvalid)
            ));
        }
        let fresh = w.handler.create_invite(&w.account(AgeBracket::Teen13To15).to_string(), SupervisionRole::Teen, Utc::now()).await.unwrap();
        assert!(matches!(
            w.handler.accept(&guesser.to_string(), fresh.code.as_str(), Utc::now()).await,
            Err(AccountError::SupervisionAttemptsExceeded)
        ), "even a right code, past the cap");
    }

    /// #670 part 2: a supervisor sets one shared set of limits; the teen and
    /// any supervisor read it; a stranger does not; the app counts time only
    /// under a daily limit; the last supervisor leaving lifts the limits.
    #[tokio::test]
    async fn limits_are_set_by_a_supervisor_read_by_both_and_lifted_with_the_last() {
        use crate::domain::supervision::AudienceFloor;

        let w = world();
        let (mum, dad, teen) = (w.account(AgeBracket::Adult), w.account(AgeBracket::Adult), w.account(AgeBracket::Teen13To15));
        w.pair(mum, teen).await.unwrap();
        w.pair(dad, teen).await.unwrap();
        let stranger = w.account(AgeBracket::Adult);

        // No limit yet: time is not counted.
        let free = w.handler.report_time(&teen.to_string(), 5, "Europe/Paris", Utc::now()).await.unwrap();
        assert_eq!((free.used_minutes, free.limit_minutes, free.reached), (0, None, false));

        let limits = SupervisionLimits {
            private_account: true,
            messages: Some(AudienceFloor::Mutuals),
            comments: Some(AudienceFloor::Followers),
            hidden_from_search: true,
            daily_minutes: Some(30),
        };
        assert!(matches!(
            w.handler.set_limits(&stranger.to_string(), &teen.to_string(), limits.clone(), Utc::now()).await,
            Err(AccountError::SupervisionNotFound)
        ));
        assert!(w.handler.set_limits(&mum.to_string(), &teen.to_string(), SupervisionLimits { daily_minutes: Some(5), ..Default::default() }, Utc::now()).await.is_err());
        w.handler.set_limits(&mum.to_string(), &teen.to_string(), limits.clone(), Utc::now()).await.unwrap();

        // One shared set: dad reads mum's, the teen reads it too.
        let seen = w.handler.limits(&dad.to_string(), &teen.to_string()).await.unwrap().unwrap();
        assert_eq!((seen.limits.clone(), seen.set_by), (limits.clone(), mum));
        assert_eq!(w.handler.limits(&teen.to_string(), "").await.unwrap().unwrap().limits, limits);
        assert!(w.handler.limits(&stranger.to_string(), &teen.to_string()).await.is_err());

        // Time counts towards the limit, all devices together.
        assert!(!w.handler.report_time(&teen.to_string(), 15, "Europe/Paris", Utc::now()).await.unwrap().reached);
        let full = w.handler.report_time(&teen.to_string(), 15, "Europe/Paris", Utc::now()).await.unwrap();
        assert_eq!((full.used_minutes, full.limit_minutes, full.reached), (30, Some(30), true));
        assert!(w.handler.report_time(&teen.to_string(), 60, "UTC", Utc::now()).await.is_err(), "15 at a time");

        // Mum leaves: dad still supervises, limits stay; dad leaves: lifted.
        w.handler.end(&mum.to_string(), &teen.to_string(), Utc::now()).await.unwrap();
        assert!(w.handler.limits(&teen.to_string(), "").await.unwrap().is_some());
        w.handler.end(&teen.to_string(), &dad.to_string(), Utc::now()).await.unwrap();
        assert!(w.handler.limits(&teen.to_string(), "").await.unwrap().is_none());
        let kinds: Vec<_> = w.published.0.lock().unwrap().iter().map(|e| e.event_type()).collect();
        assert_eq!(kinds.iter().filter(|k| **k == "account.supervision_limits_set").count(), 1);
        assert_eq!(kinds.last(), Some(&"account.supervision_limits_cleared"));
    }
}
