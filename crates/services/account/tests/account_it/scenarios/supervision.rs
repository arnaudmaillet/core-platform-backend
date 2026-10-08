//! #670 over real Postgres: a parent and a teen pair through an invite code;
//! both see the link; a teen has two supervisors at most; either side ends
//! it; it ends when the teen turns 18; expired codes go; the supervisor's
//! index follows the teen's rows.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::{Datelike, Duration, Utc};
use cqrs::{CommandBus, Envelope};
use postgres_storage::TransactionManager;
use uuid::Uuid;

use account::application::command::{CreateAccountCommand, Supervisions};
use account::application::port::{DirectoryProfile, EventPublisher, ProfileDirectory};
use account::domain::event::DomainEvent;
use account::domain::supervision::SupervisionRole;
use account::domain::value_object::AccountId;
use account::error::AccountError;
use account::infrastructure::persistence::{PgSupervisionStore, RepoAccountAges};

use crate::account_it::harness::{self, TestHarness, DEADLINE};

struct Profiles;

#[async_trait]
impl ProfileDirectory for Profiles {
    async fn profiles_of(&self, account_id: &AccountId) -> Result<Vec<DirectoryProfile>, AccountError> {
        Ok(vec![DirectoryProfile {
            profile_id:   format!("p-{account_id}"),
            handle:       "h".into(),
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
struct Published(Mutex<Vec<String>>);

#[async_trait]
impl EventPublisher for Published {
    async fn publish(&self, event: &DomainEvent) -> Result<(), AccountError> {
        if let DomainEvent::SupervisionEnded(e) = event {
            self.0.lock().unwrap().push(e.ended_by.clone());
        }
        Ok(())
    }
}

fn born_years_ago(years: i32) -> String {
    let today = Utc::now().date_naive();
    let dob = today.with_year(today.year() - years).unwrap_or(today - Duration::days(365 * years as i64));
    dob.format("%Y-%m-%d").to_string()
}

/// A new account of age `years`; its id.
async fn account(h: &TestHarness, years: i32) -> String {
    let identity = harness::random_identity();
    let cmd = CreateAccountCommand {
        identity_id: identity.clone(),
        email: harness::random_email(),
        phone: None,
        password_hash: None,
        country_of_residence: None,
        role: None,
        created_by: None,
        date_of_birth: Some(born_years_ago(years)),
    };
    h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), cmd)).await.expect("create");
    let identity_q = identity.clone();
    harness::await_until("account readable", DEADLINE, || {
        let identity_q = identity_q.clone();
        async move { h.get_by_identity(&identity_q).await.is_ok() }
    })
    .await;
    h.get_by_identity(&identity).await.unwrap().id
}

async fn pair(s: &Supervisions, parent: &str, teen: &str) -> Result<(), AccountError> {
    let invite = s.create_invite(parent, SupervisionRole::Supervisor, Utc::now()).await?;
    s.accept(teen, &invite.code.as_str().to_lowercase(), Utc::now()).await.map(|_| ())
}

#[tokio::test]
async fn a_parent_and_a_teen_pair_end_and_age_out_over_postgres() {
    let h = TestHarness::start().await;
    let published = Arc::new(Published::default());
    let supervisions = Supervisions::new(
        Arc::new(PgSupervisionStore::new(TransactionManager::new(h.pool.clone()))),
        Arc::new(RepoAccountAges(Arc::clone(&h.repository))),
        Arc::new(Profiles),
        Arc::clone(&published) as _,
    );
    let (mum, dad, aunt) = (account(&h, 40).await, account(&h, 41).await, account(&h, 35).await);
    let teen = account(&h, 15).await;

    // A teen cannot supervise; an adult cannot be supervised.
    assert!(matches!(
        supervisions.create_invite(&teen, SupervisionRole::Supervisor, Utc::now()).await,
        Err(AccountError::SupervisionRoleNotAllowed { .. })
    ));

    pair(&supervisions, &mum, &teen).await.expect("mum pairs");
    pair(&supervisions, &dad, &teen).await.expect("dad pairs");
    assert!(matches!(pair(&supervisions, &aunt, &teen).await, Err(AccountError::SupervisorLimitReached)));

    let teen_view = supervisions.list(&teen).await.unwrap();
    assert_eq!(teen_view.len(), 2, "the teen sees both");
    assert!(teen_view.iter().all(|v| v.role == SupervisionRole::Supervisor));
    let mum_view = supervisions.list(&mum).await.unwrap();
    assert_eq!((mum_view.len(), mum_view[0].account.to_string()), (1, teen.clone()));

    // The teen ends dad's; mum's stays.
    supervisions.end(&teen, &dad, Utc::now()).await.unwrap();
    assert!(supervisions.list(&dad).await.unwrap().is_empty());
    assert_eq!(supervisions.list(&teen).await.unwrap().len(), 1);

    // The teen turns 18: the sweep ends mum's too.
    sqlx::query("UPDATE accounts SET date_of_birth = $2 WHERE id = $1")
        .bind(Uuid::parse_str(&teen).unwrap())
        .bind(chrono::NaiveDate::parse_from_str(&born_years_ago(18), "%Y-%m-%d").unwrap())
        .execute(&h.pool)
        .await
        .unwrap();
    assert!(supervisions.sweep(Utc::now()).await.unwrap() >= 1);
    assert!(supervisions.list(&mum).await.unwrap().is_empty());
    assert!(supervisions.list(&teen).await.unwrap().is_empty());
    assert_eq!(*published.0.lock().unwrap(), vec!["by_teen", "came_of_age"]);

    // An expired invite goes with the sweep; a spent one cannot be reused.
    let other_teen = account(&h, 14).await;
    let stale = supervisions.create_invite(&aunt, SupervisionRole::Supervisor, Utc::now() - Duration::days(2)).await.unwrap();
    supervisions.sweep(Utc::now()).await.unwrap();
    assert!(matches!(
        supervisions.accept(&other_teen, stale.code.as_str(), Utc::now()).await,
        Err(AccountError::SupervisionInviteInvalid)
    ));
}

#[tokio::test]
async fn the_supervisor_index_follows_the_teens_rows() {
    let h = TestHarness::start().await;
    let supervisions = Supervisions::new(
        Arc::new(PgSupervisionStore::new(TransactionManager::new(h.pool.clone()))),
        Arc::new(RepoAccountAges(Arc::clone(&h.repository))),
        Arc::new(Profiles),
        Arc::new(Published::default()),
    );
    let (parent, teen) = (account(&h, 45).await, account(&h, 16).await);
    pair(&supervisions, &parent, &teen).await.unwrap();

    // A half-done ending (the teen's row gone, the index left behind) reads
    // as ended, and the stale index entry is dropped.
    sqlx::query("DELETE FROM supervisions WHERE teen_id = $1")
        .bind(Uuid::parse_str(&teen).unwrap())
        .execute(&h.pool)
        .await
        .unwrap();
    assert!(supervisions.list(&parent).await.unwrap().is_empty());
    let (left,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM supervisions_by_supervisor WHERE supervisor_id = $1")
        .bind(Uuid::parse_str(&parent).unwrap())
        .fetch_one(&h.pool)
        .await
        .unwrap();
    assert_eq!(left, 0);

    // The same pair again: a fresh link, not a duplicate.
    pair(&supervisions, &parent, &teen).await.unwrap();
    assert_eq!(supervisions.list(&parent).await.unwrap().len(), 1);
}

/// A teen's code racing between two adults pairs exactly one; failed codes
/// are capped per account per hour (#846 review).
#[tokio::test]
async fn one_code_pairs_one_acceptor_and_guessing_is_capped() {
    let h = TestHarness::start().await;
    let supervisions = Arc::new(Supervisions::new(
        Arc::new(PgSupervisionStore::new(TransactionManager::new(h.pool.clone()))),
        Arc::new(RepoAccountAges(Arc::clone(&h.repository))),
        Arc::new(Profiles),
        Arc::new(Published::default()),
    ));
    let teen = account(&h, 15).await;
    let (parent, stranger) = (account(&h, 40).await, account(&h, 33).await);
    let invite = supervisions.create_invite(&teen, SupervisionRole::Teen, Utc::now()).await.unwrap();
    let code = invite.code.as_str().to_owned();

    let (a, b) = tokio::join!(
        supervisions.accept(&parent, &code, Utc::now()),
        supervisions.accept(&stranger, &code, Utc::now()),
    );
    assert!(a.is_ok() != b.is_ok(), "exactly one pairs: {a:?} / {b:?}");
    let loser = if a.is_ok() { b } else { a };
    assert!(matches!(loser, Err(AccountError::SupervisionInviteInvalid)));
    assert_eq!(supervisions.list(&teen).await.unwrap().len(), 1);

    let guesser = account(&h, 50).await;
    for _ in 0..10 {
        let wrong = account::domain::supervision::InviteCode::generate();
        assert!(supervisions.accept(&guesser, wrong.as_str(), Utc::now()).await.is_err());
    }
    let other_teen = account(&h, 16).await;
    let fresh = supervisions.create_invite(&other_teen, SupervisionRole::Teen, Utc::now()).await.unwrap();
    assert!(matches!(
        supervisions.accept(&guesser, fresh.code.as_str(), Utc::now()).await,
        Err(AccountError::SupervisionAttemptsExceeded)
    ));
}

/// #670 part 2 over Postgres: a supervisor sets the teen's limits, the teen
/// reads them, time counts across reports up to the limit, and the limits go
/// with the last supervisor.
#[tokio::test]
async fn limits_and_screen_time_over_postgres() {
    use account::domain::supervision::{AudienceFloor, SupervisionLimits};

    let h = TestHarness::start().await;
    let supervisions = Supervisions::new(
        Arc::new(PgSupervisionStore::new(TransactionManager::new(h.pool.clone()))),
        Arc::new(RepoAccountAges(Arc::clone(&h.repository))),
        Arc::new(Profiles),
        Arc::new(Published::default()),
    );
    let (parent, teen) = (account(&h, 44).await, account(&h, 14).await);
    pair(&supervisions, &parent, &teen).await.unwrap();

    let limits = SupervisionLimits {
        private_account: true,
        messages: Some(AudienceFloor::NoOne),
        comments: None,
        hidden_from_search: true,
        daily_minutes: Some(20),
    };
    supervisions.set_limits(&parent, &teen, limits.clone(), Utc::now()).await.unwrap();
    let mine = supervisions.limits(&teen, "").await.unwrap().expect("set");
    assert_eq!(mine.limits, limits);
    assert_eq!(mine.set_by.to_string(), parent);

    let first = supervisions.report_time(&teen, 15, "Europe/Paris", Utc::now()).await.unwrap();
    assert_eq!((first.used_minutes, first.reached), (15, false));
    let second = supervisions.report_time(&teen, 5, "Europe/Paris", Utc::now()).await.unwrap();
    assert_eq!((second.used_minutes, second.limit_minutes, second.reached), (20, Some(20), true));
    // Part 3: the parent and the teen see the same days; yesterday too.
    supervisions.report_time(&teen, 7, "UTC", Utc::now() - Duration::days(1)).await.unwrap();
    let overview = supervisions.overview(&parent, &teen).await.unwrap();
    assert_eq!(overview, supervisions.overview(&teen, "").await.unwrap());
    let days: Vec<i32> = overview.screen_time.iter().map(|(_, minutes)| *minutes).collect();
    assert_eq!(days.len(), 2, "{:?}", overview.screen_time);
    assert!(overview.screen_time[0].0 > overview.screen_time[1].0, "most recent first");
    // An unsupervised account's time is not kept.
    assert_eq!(supervisions.report_time(&parent, 5, "UTC", Utc::now()).await.unwrap().limit_minutes, None);
    let (rows,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM screen_time WHERE account_id = $1")
        .bind(Uuid::parse_str(&parent).unwrap())
        .fetch_one(&h.pool)
        .await
        .unwrap();
    assert_eq!(rows, 0);

    supervisions.end(&parent, &teen, Utc::now()).await.unwrap();
    assert!(supervisions.limits(&teen, "").await.unwrap().is_none(), "lifted with the last supervisor");
}
