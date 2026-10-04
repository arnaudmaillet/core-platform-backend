//! Phone-only accounts over real Postgres (guest mode B4c): created with a
//! phone number and no email, activated by verifying the number, found by it,
//! and a number belongs to one account only.

use cqrs::{CommandBus, Envelope, QueryBus};
use uuid::Uuid;

use account::application::command::{CreateAccountCommand, VerifyPhoneCommand};
use account::application::query::GetAccountByPhoneQuery;
use account::domain::value_object::AccountStatus;

use crate::account_it::harness::{self, TestHarness, DEADLINE};

fn phone_only(identity: &str, phone: &str) -> Envelope<CreateAccountCommand> {
    Envelope::new(Uuid::now_v7(), CreateAccountCommand {
        identity_id: identity.to_owned(),
        email: String::new(),
        phone: Some(phone.to_owned()),
        password_hash: None,
        country_of_residence: Some("FR".to_owned()),
        role: None,
        created_by: None,
        date_of_birth: Some("1990-05-04".to_owned()),
    })
}

/// A random French mobile number.
fn random_phone() -> String {
    let n = Uuid::now_v7().as_u128() % 100_000_000;
    format!("+336{n:08}")
}

#[tokio::test]
async fn a_phone_only_account_is_created_activated_and_found_by_its_number() {
    let h = TestHarness::start().await;
    let identity = harness::random_identity();
    let phone = random_phone();

    h.command_bus.dispatch(phone_only(&identity, &phone)).await.expect("phone-only account");
    harness::await_until("account readable", DEADLINE, || async { h.get_by_identity(&identity).await.is_ok() }).await;
    let created = h.get_by_identity(&identity).await.unwrap();
    assert_eq!(created.email, "", "no email on file");
    assert_eq!(created.status, AccountStatus::PendingVerification.as_str());

    // Verifying the number activates it (the number is how it signed up).
    h.command_bus
        .dispatch(Envelope::new(Uuid::now_v7(), VerifyPhoneCommand { account_id: created.id.clone() }))
        .await
        .expect("verify phone");
    let active = h.get_by_identity(&identity).await.unwrap();
    assert_eq!(active.status, AccountStatus::Active.as_str());
    assert!(active.phone_verified);

    let found = h
        .query_bus
        .dispatch(Envelope::new(Uuid::now_v7(), GetAccountByPhoneQuery { phone: phone.clone() }))
        .await
        .expect("found by phone");
    assert_eq!(found.id, created.id);

    // The same number for another identity: refused.
    let err = h.command_bus.dispatch(phone_only(&harness::random_identity(), &phone)).await.unwrap_err();
    assert!(err.to_string().contains("phone number is already registered"), "{err}");

    // Neither an email nor a phone: refused.
    let mut neither = phone_only(&harness::random_identity(), &phone);
    neither.payload.phone = None;
    assert!(h.command_bus.dispatch(neither).await.is_err());
}
