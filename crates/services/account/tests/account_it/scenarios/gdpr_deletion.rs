//! The erasure lifecycle over real Postgres: a request deactivates the account
//! for its grace period, a cancel withdraws it, and the janitor anonymizes only
//! what is due — once.

use std::sync::Arc;

use chrono::Utc;
use cqrs::{CommandBus, Envelope};
use uuid::Uuid;

use account::application::command::{
    AnonymizeDueAccounts, CancelGdprDeletionCommand, JanitorPass, RequestGdprDeletionCommand,
    ResumeDeactivatedAccountCommand,
};
use account::application::query::get_gdpr_record::GetGdprRecordQuery;
use cqrs::QueryBus;

use crate::account_it::harness::{self, TestHarness, DEADLINE};

async fn active_account(h: &TestHarness) -> (String, String) {
    let identity = harness::random_identity();
    h.create(&identity, &harness::random_email()).await;
    let identity_q = identity.clone();
    harness::await_until("account readable", DEADLINE, || {
        let h = &h;
        let identity_q = identity_q.clone();
        async move { h.get_by_identity(&identity_q).await.is_ok() }
    })
    .await;
    let id = h.get_by_identity(&identity).await.unwrap().id;
    h.verify_email(&id).await.expect("activate");
    (id, identity)
}

async fn request_deletion(h: &TestHarness, id: &str) {
    h.command_bus
        .dispatch(Envelope::new(
            Uuid::now_v7(),
            RequestGdprDeletionCommand { account_id: id.to_owned(), retention_days: 30 },
        ))
        .await
        .expect("request deletion");
}

#[tokio::test]
async fn the_janitor_anonymizes_what_is_due_and_only_that() {
    let h = TestHarness::start().await;
    let (due, due_identity) = active_account(&h).await;
    let (cancelled, cancelled_identity) = active_account(&h).await;
    let (waiting, waiting_identity) = active_account(&h).await;

    for id in [&due, &cancelled, &waiting] {
        request_deletion(&h, id).await;
    }
    assert_eq!(h.get_by_identity(&due_identity).await.unwrap().status, "deactivated");
    h.command_bus
        .dispatch(Envelope::new(Uuid::now_v7(), CancelGdprDeletionCommand { account_id: cancelled.clone() }))
        .await
        .expect("cancel");

    // `due`'s 30 days have passed.
    sqlx::query("UPDATE accounts SET gdpr_deletion_scheduled_at = now() - interval '1 day' WHERE id = $1")
        .bind(Uuid::parse_str(&due).unwrap())
        .execute(&h.pool)
        .await
        .unwrap();

    let janitor = AnonymizeDueAccounts::new(Arc::clone(&h.repository));
    let pass = janitor.run(Utc::now(), 1_000).await.unwrap();
    // Other scenarios share the database: count only this test's accounts.
    let anonymized: Vec<Uuid> = sqlx::query_scalar(
        "SELECT id FROM accounts WHERE gdpr_anonymized_at IS NOT NULL AND id = ANY($1)",
    )
    .bind(vec![
        Uuid::parse_str(&due).unwrap(),
        Uuid::parse_str(&cancelled).unwrap(),
        Uuid::parse_str(&waiting).unwrap(),
    ])
    .fetch_all(&h.pool)
    .await
    .unwrap();
    assert_eq!(anonymized, vec![Uuid::parse_str(&due).unwrap()]);
    assert!(pass.anonymized >= 1);

    let erased = h.get_by_identity(&due_identity).await.unwrap();
    assert_eq!(erased.status, "deleted");
    assert_eq!(erased.email, format!("anonymized-{due}@anonymized.invalid"));
    assert_eq!(h.get_by_identity(&cancelled_identity).await.unwrap().status, "deactivated");

    // Signing back in (auth → ResumeDeactivatedAccount) withdraws a pending
    // deletion and reactivates, in one write.
    h.command_bus
        .dispatch(Envelope::new(Uuid::now_v7(), ResumeDeactivatedAccountCommand { account_id: waiting.clone() }))
        .await
        .expect("resume");
    assert_eq!(h.get_by_identity(&waiting_identity).await.unwrap().status, "active");
    let record = h
        .query_bus
        .dispatch(Envelope::new(Uuid::now_v7(), GetGdprRecordQuery { account_id: waiting.clone() }))
        .await
        .unwrap();
    assert!(record.deletion_requested_at.is_none() && record.deletion_scheduled_at.is_none());

    // Nothing of ours is left to do.
    let again = janitor.run(Utc::now(), 1_000).await.unwrap();
    let _: JanitorPass = again;
    let still: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM accounts WHERE id = $1 AND gdpr_anonymized_at IS NOT NULL",
    )
    .bind(Uuid::parse_str(&due).unwrap())
    .fetch_one(&h.pool)
    .await
    .unwrap();
    assert_eq!(still, 1);
}
