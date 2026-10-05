//! #661 over real Postgres: an account's contacts by hash. The hashes are
//! generated columns (migration 0007), so they follow every write; only a
//! verified contact of an active account matches, and the app's hash is
//! SHA-256 of the lower-cased, trimmed email / the E.164 number.

use cqrs::{CommandBus, Envelope};
use uuid::Uuid;

use account::application::command::{CreateAccountCommand, VerifyPhoneCommand};
use account::application::port::ContactChannel;

use crate::account_it::harness::{self, TestHarness, DEADLINE};

/// SHA-256 of `contact`, as the app computes it after normalizing.
async fn sha256(h: &TestHarness, contact: &str) -> Vec<u8> {
    let (hash,): (Vec<u8>,) = sqlx::query_as("SELECT sha256(convert_to($1, 'UTF8'))")
        .bind(contact)
        .fetch_one(&h.pool)
        .await
        .unwrap();
    hash
}

#[tokio::test]
async fn verified_contacts_of_active_accounts_match_their_hash() {
    let h = TestHarness::start().await;
    let (identity, email) = (harness::random_identity(), harness::random_email());
    h.create(&identity, &email).await;
    harness::await_until("account readable", DEADLINE, || async { h.get_by_identity(&identity).await.is_ok() }).await;
    let account = h.get_by_identity(&identity).await.unwrap();
    let email_hash = sha256(&h, &email.trim().to_lowercase()).await;

    // Unverified (and pending): no match.
    assert!(h.contacts.match_contacts(std::slice::from_ref(&email_hash), &[]).await.unwrap().is_empty());

    h.verify_email(&account.id).await.expect("verify email");
    let found = h.contacts.match_contacts(&[email_hash.clone(), vec![0; 32]], &[]).await.unwrap();
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].account_id.as_uuid().to_string(), account.id);
    assert_eq!((found[0].channel, found[0].hash.clone()), (ContactChannel::Email, email_hash.clone()));
    // An email hash is not a phone hash.
    assert!(h.contacts.match_contacts(&[], &[email_hash]).await.unwrap().is_empty());

    // A phone-only account, verified.
    let (identity, n) = (harness::random_identity(), Uuid::now_v7().as_u128() % 100_000_000);
    let phone = format!("+336{n:08}");
    h.command_bus
        .dispatch(Envelope::new(Uuid::now_v7(), CreateAccountCommand {
            identity_id: identity.clone(),
            email: String::new(),
            phone: Some(phone.clone()),
            password_hash: None,
            country_of_residence: Some("FR".to_owned()),
            role: None,
            created_by: None,
            date_of_birth: Some("1990-05-04".to_owned()),
        }))
        .await
        .expect("phone-only account");
    harness::await_until("account readable", DEADLINE, || async { h.get_by_identity(&identity).await.is_ok() }).await;
    let phone_account = h.get_by_identity(&identity).await.unwrap();
    let phone_hash = sha256(&h, &phone).await;
    assert!(h.contacts.match_contacts(&[], std::slice::from_ref(&phone_hash)).await.unwrap().is_empty(), "unverified");
    h.command_bus
        .dispatch(Envelope::new(Uuid::now_v7(), VerifyPhoneCommand { account_id: phone_account.id.clone() }))
        .await
        .expect("verify phone");
    let found = h.contacts.match_contacts(&[], &[phone_hash]).await.unwrap();
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].channel, ContactChannel::Phone);
    assert_eq!(found[0].account_id.as_uuid().to_string(), phone_account.id);
}

/// The daily budget over real Postgres: reservations add up per account and
/// day, one that would pass the limit reserves nothing, and concurrent
/// reservations never overshoot it.
#[tokio::test]
async fn the_daily_lookup_budget_is_atomic_and_never_overshot() {
    use account::domain::value_object::AccountId;

    let h = TestHarness::start().await;
    let (me, day) = (AccountId::new(), chrono::Utc::now().date_naive());
    assert!(h.quota.reserve(&me, day, 600, 1_000).await.unwrap());
    assert!(!h.quota.reserve(&me, day, 500, 1_000).await.unwrap(), "past the limit");
    assert!(h.quota.reserve(&me, day, 400, 1_000).await.unwrap(), "the refusal reserved nothing");
    assert!(!h.quota.reserve(&me, day, 1, 1_000).await.unwrap(), "spent");
    let tomorrow = day.succ_opt().unwrap();
    assert!(h.quota.reserve(&me, tomorrow, 1_000, 1_000).await.unwrap(), "a new day");

    // Twenty concurrent reservations of 100 against 1000: exactly ten pass.
    let other = AccountId::new();
    let attempts: Vec<_> = (0..20)
        .map(|_| {
            let quota = std::sync::Arc::clone(&h.quota);
            tokio::spawn(async move { quota.reserve(&other, day, 100, 1_000).await.unwrap() })
        })
        .collect();
    let mut granted = 0;
    for attempt in attempts {
        granted += usize::from(attempt.await.unwrap());
    }
    assert_eq!(granted, 10);
}
