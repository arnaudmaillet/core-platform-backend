//! #651 over real Postgres: an active account's email or phone is replaced by
//! one its holder proved (auth calls these over the mesh), set and verified at
//! once; another account's address is refused, and repeating it is a no-op.

use cqrs::{CommandBus, Envelope};
use error::AppError;
use uuid::Uuid;

use account::application::command::{ChangeEmailCommand, ChangePhoneCommand};

use crate::account_it::harness::{self, TestHarness, DEADLINE};

async fn active_account(h: &TestHarness) -> (String, String) {
    let (identity, email) = (harness::random_identity(), harness::random_email());
    h.create(&identity, &email).await;
    harness::await_until("account readable", DEADLINE, || async { h.get_by_identity(&identity).await.is_ok() }).await;
    let id = h.get_by_identity(&identity).await.unwrap().id;
    h.verify_email(&id).await.expect("activate");
    (identity, id)
}

fn random_phone() -> String {
    format!("+336{:08}", Uuid::now_v7().as_u128() % 100_000_000)
}

#[tokio::test]
async fn a_proven_email_or_phone_replaces_the_old_one() {
    let h = TestHarness::start().await;
    let (identity, id) = active_account(&h).await;
    let change_email = |email: &str| {
        Envelope::new(Uuid::now_v7(), ChangeEmailCommand { account_id: id.clone(), email: email.to_owned() })
    };

    let new_email = harness::random_email();
    h.command_bus.dispatch(change_email(&new_email)).await.expect("change email");
    let view = h.get_by_identity(&identity).await.unwrap();
    assert_eq!(view.email, new_email);
    assert!(view.email_verified, "proven: verified at once");
    h.command_bus.dispatch(change_email(&new_email)).await.expect("again: a no-op");

    let phone = random_phone();
    h.command_bus
        .dispatch(Envelope::new(Uuid::now_v7(), ChangePhoneCommand { account_id: id.clone(), phone: phone.clone() }))
        .await
        .expect("change phone");
    let view = h.get_by_identity(&identity).await.unwrap();
    assert_eq!(view.phone, Some(phone));
    assert!(view.phone_verified);
}

#[tokio::test]
async fn another_accounts_address_is_refused() {
    let h = TestHarness::start().await;
    let (_, first) = active_account(&h).await;
    let (other_identity, second) = active_account(&h).await;
    let taken_email = h.get_by_identity(&other_identity).await.unwrap().email;

    let err = h
        .command_bus
        .dispatch(Envelope::new(Uuid::now_v7(), ChangeEmailCommand { account_id: first.clone(), email: taken_email }))
        .await
        .unwrap_err();
    assert_eq!(err.error_code(), "ACC-1003");

    let phone = random_phone();
    h.command_bus
        .dispatch(Envelope::new(Uuid::now_v7(), ChangePhoneCommand { account_id: second, phone: phone.clone() }))
        .await
        .expect("second takes the number");
    let err = h
        .command_bus
        .dispatch(Envelope::new(Uuid::now_v7(), ChangePhoneCommand { account_id: first, phone }))
        .await
        .unwrap_err();
    assert_eq!(err.error_code(), "ACC-1004");
}
