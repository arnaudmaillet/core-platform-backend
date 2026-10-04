//! GDPR Art. 7 over real Postgres: consents given and withdrawn land on the
//! record and, one row per effective change, in the timestamped history — in
//! the same transaction as the state they explain.

use cqrs::{CommandBus, Envelope, QueryBus};
use uuid::Uuid;

use account::application::command::UpdateConsentsCommand;
use account::application::query::get_gdpr_record::GetGdprRecordQuery;

use crate::account_it::harness::{self, TestHarness, DEADLINE};

fn consents(
    account_id: &str,
    marketing: Option<bool>,
    analytics: Option<bool>,
    version: Option<&str>,
) -> Envelope<UpdateConsentsCommand> {
    Envelope::new(
        Uuid::now_v7(),
        UpdateConsentsCommand {
            account_id: account_id.to_owned(),
            data_processing: None,
            marketing,
            analytics,
            policy_version: version.map(str::to_owned),
        },
    )
}

#[tokio::test]
async fn consents_are_given_withdrawn_and_kept_in_history() {
    let h = TestHarness::start().await;
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

    h.command_bus.dispatch(consents(&id, Some(true), Some(true), Some("PP-2026-10"))).await.unwrap();
    // Re-giving a given consent changes nothing, and writes nothing.
    h.command_bus.dispatch(consents(&id, Some(true), None, Some("PP-2026-10"))).await.unwrap();
    // Withdrawing is one call too.
    h.command_bus.dispatch(consents(&id, None, Some(false), None)).await.unwrap();

    let record = h
        .query_bus
        .dispatch(Envelope::new(Uuid::now_v7(), GetGdprRecordQuery { account_id: id.clone() }))
        .await
        .unwrap();
    assert!(record.marketing_consented_at.is_some());
    assert!(record.analytics_consented_at.is_none(), "withdrawn");
    assert_eq!(record.last_consent_version.as_deref(), Some("PP-2026-10"));

    let history: Vec<(String, bool, Option<String>)> = sqlx::query_as(
        "SELECT purpose, granted, policy_version FROM account_consent_history \
         WHERE account_id = $1 ORDER BY changed_at, purpose",
    )
    .bind(Uuid::parse_str(&id).unwrap())
    .fetch_all(&h.pool)
    .await
    .unwrap();
    assert_eq!(
        history,
        vec![
            ("analytics".to_owned(), true, Some("PP-2026-10".to_owned())),
            ("marketing".to_owned(), true, Some("PP-2026-10".to_owned())),
            ("analytics".to_owned(), false, None),
        ]
    );
}
