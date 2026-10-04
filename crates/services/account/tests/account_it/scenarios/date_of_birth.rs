//! The minimum age over real Postgres: an account is created with its date of
//! birth (refused under 13), the age bracket is read back from it, and an
//! account created without one records it exactly once.

use chrono::{Datelike, Utc};
use cqrs::{CommandBus, Envelope};
use uuid::Uuid;

use account::application::command::{CreateAccountCommand, SetDateOfBirthCommand};
use account::domain::value_object::AgeBracket;

use crate::account_it::harness::{self, TestHarness, DEADLINE};

/// A date of birth `years` ago today (a birthday today).
fn born_years_ago(years: i32) -> String {
    let today = Utc::now().date_naive();
    let dob = today.with_year(today.year() - years).unwrap_or(today - chrono::Duration::days(365 * years as i64));
    dob.format("%Y-%m-%d").to_string()
}

fn create(identity: &str, email: &str, dob: Option<String>, country: Option<&str>) -> Envelope<CreateAccountCommand> {
    Envelope::new(
        Uuid::now_v7(),
        CreateAccountCommand {
            identity_id: identity.to_owned(),
            email: email.to_owned(),
            phone: None,
            password_hash: None,
            country_of_residence: country.map(str::to_owned),
            role: None,
            created_by: None,
            date_of_birth: dob,
        },
    )
}

async fn wait_for(h: &TestHarness, identity: &str) -> account::application::query::AccountView {
    let identity_q = identity.to_owned();
    harness::await_until("account readable", DEADLINE, || {
        let h = &h;
        let identity_q = identity_q.clone();
        async move { h.get_by_identity(&identity_q).await.is_ok() }
    })
    .await;
    h.get_by_identity(identity).await.unwrap()
}

#[tokio::test]
async fn the_minimum_age_holds_at_creation_and_on_a_late_date_of_birth() {
    let h = TestHarness::start().await;

    // Under 13: refused, nothing created.
    let child = harness::random_identity();
    let err = h
        .command_bus
        .dispatch(create(&child, &harness::random_email(), Some(born_years_ago(12)), None))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("minimum age"), "{err}");
    assert!(h.get_by_identity(&child).await.is_err());

    // 15 in Australia (minimum 16): refused; 15 in France: a 13–15 teen.
    let au = harness::random_identity();
    assert!(h
        .command_bus
        .dispatch(create(&au, &harness::random_email(), Some(born_years_ago(15)), Some("AU")))
        .await
        .is_err());
    let teen = harness::random_identity();
    h.command_bus
        .dispatch(create(&teen, &harness::random_email(), Some(born_years_ago(15)), Some("FR")))
        .await
        .expect("a 15-year-old in France");
    let view = wait_for(&h, &teen).await;
    assert_eq!(view.age_bracket, Some(AgeBracket::Teen13To15));
    assert_eq!(view.date_of_birth.map(|d| d.format("%Y-%m-%d").to_string()), Some(born_years_ago(15)));

    // Created without one: recorded once, then only support may change it.
    let legacy = harness::random_identity();
    h.command_bus.dispatch(create(&legacy, &harness::random_email(), None, None)).await.unwrap();
    let id = wait_for(&h, &legacy).await.id;
    let set = |dob: String| {
        Envelope::new(Uuid::now_v7(), SetDateOfBirthCommand { account_id: id.clone(), date_of_birth: dob })
    };
    assert!(h.command_bus.dispatch(set(born_years_ago(11))).await.is_err(), "under 13");
    h.command_bus.dispatch(set(born_years_ago(30))).await.expect("an adult");
    assert_eq!(wait_for(&h, &legacy).await.age_bracket, Some(AgeBracket::Adult));
    assert!(h.command_bus.dispatch(set(born_years_ago(20))).await.is_err(), "once only");
}
