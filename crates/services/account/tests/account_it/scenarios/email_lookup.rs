//! The account holding an email address, over real Postgres (auth's sign-up
//! uses it to spot an account created with another method): case-insensitive,
//! and NotFound for an address no account holds.

use cqrs::{Envelope, QueryBus};
use uuid::Uuid;

use account::application::query::GetAccountByEmailQuery;

use crate::account_it::harness::{self, TestHarness, DEADLINE};

#[tokio::test]
async fn an_account_is_found_by_its_email_whatever_the_case() {
    let h = TestHarness::start().await;
    let identity = format!("https://idp.test#{}", Uuid::now_v7());
    let email = format!("Someone.{}@Example.com", Uuid::now_v7().simple());
    h.create(&identity, &email).await;
    harness::await_until("account readable", DEADLINE, || async { h.get_by_identity(&identity).await.is_ok() }).await;
    let created = h.get_by_identity(&identity).await.unwrap();

    let found = h
        .query_bus
        .dispatch(Envelope::new(Uuid::now_v7(), GetAccountByEmailQuery { email: email.to_uppercase() }))
        .await
        .expect("found by email");
    assert_eq!(found.id, created.id);

    let missing = h
        .query_bus
        .dispatch(Envelope::new(Uuid::now_v7(), GetAccountByEmailQuery { email: format!("nobody.{}@example.com", Uuid::now_v7().simple()) }))
        .await;
    assert!(missing.is_err(), "no account holds that address");
}
