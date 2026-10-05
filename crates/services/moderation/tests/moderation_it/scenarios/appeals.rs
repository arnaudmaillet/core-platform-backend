//! Appeals against real Postgres (DSA Art. 20): one appeal per decision and
//! appellant (concurrent files store one), the appellant follows it — status,
//! then the reviewer's reasons — from the statement of reasons and their own
//! list, and the six-month window is enforced (MOD-5003).

use std::sync::Arc;

use tonic::{Code, Request};
use uuid::Uuid;

use moderation::infrastructure::grpc::proto;

use crate::moderation_it::harness::{subject, Harness};

fn as_account<T>(mut request: Request<T>, sub: &str) -> Request<T> {
    let raw: auth_context::OidcClaims =
        serde_json::from_value(serde_json::json!({ "sub": sub, "exp": 4_102_444_800_i64 })).unwrap();
    request.extensions_mut().insert(transport::grpc::edge::EdgePrincipal::new(Arc::new(
        auth_context::CurrentPrincipal {
            user_id: auth_context::PrincipalId::new(sub),
            tenant_id: None,
            permissions: vec![],
            raw_claims: raw,
        },
    )));
    request
}

/// A suspended actor and the decision behind it.
async fn suspended(h: &Harness) -> (String, String) {
    let subj = subject(proto::EntityType::Post, "feed");
    let actor = subj.actor_id.clone();
    let case_id = h
        .open_case(subj, proto::PolicyCategory::Harassment as i32)
        .await
        .expect("open case")
        .case
        .unwrap()
        .case_id;
    let decision = h
        .decide_case(&case_id, proto::ActionType::Suspend as i32, proto::PolicyCategory::Harassment as i32)
        .await
        .expect("decide")
        .decision
        .unwrap();
    (actor, decision.decision_id)
}

async fn my_appeals(h: &Harness, sub: &str) -> Vec<proto::AppealView> {
    let request = as_account(Request::new(proto::ListMyAppealsRequest { page_size: 0, page_token: String::new() }), sub);
    h.handler.list_my_appeals(request).await.expect("list my appeals").into_inner().appeals
}

#[tokio::test]
async fn one_appeal_per_decision_followed_to_its_reasoned_outcome() {
    let h = Harness::start().await;
    let (actor, decision_id) = suspended(&h).await;

    // Two concurrent files (a double tap): one appeal.
    let (a, b) = tokio::join!(h.file_appeal(&decision_id, &actor), h.file_appeal(&decision_id, &actor));
    let (a, b) = (a.expect("file").appeal.unwrap(), b.expect("file").appeal.unwrap());
    assert_eq!(a.appeal_id, b.appeal_id);
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM appeals WHERE decision_id = $1")
        .bind(Uuid::parse_str(&decision_id).unwrap())
        .fetch_one(&h.pool)
        .await
        .unwrap();
    assert_eq!(rows, 1);

    // The statement shows the deadline and the appeal to its owner.
    let request = as_account(Request::new(proto::GetStatementOfReasonsRequest { decision_id: decision_id.clone() }), &actor);
    let statement = h.handler.get_statement_of_reasons(request).await.unwrap().into_inner().statement.unwrap();
    let (decided, until) = (statement.decided_at.unwrap().seconds, statement.appealable_until.unwrap().seconds);
    assert_eq!(until - decided, 183 * 24 * 3600, "six months and a bit");
    assert_eq!(statement.appeal.unwrap().status, proto::AppealStatus::Filed as i32);

    // Their own list: filed, then upheld with the reviewer's reasons.
    let listed = my_appeals(&h, &actor).await;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].status, proto::AppealStatus::Filed as i32);
    assert!(my_appeals(&h, &Uuid::now_v7().to_string()).await.is_empty(), "nobody else's");

    h.resolve_appeal(&a.appeal_id, false).await.expect("uphold");
    let listed = my_appeals(&h, &actor).await;
    assert_eq!(listed[0].status, proto::AppealStatus::Upheld as i32);
    assert_eq!(listed[0].outcome, "reviewed");
    assert!(listed[0].resolved_at.is_some());

    // Filing again after the outcome: the same appeal, still upheld.
    let again = h.file_appeal(&decision_id, &actor).await.unwrap().appeal.unwrap();
    assert_eq!(again.appeal_id, a.appeal_id);
    assert_eq!(again.status, proto::AppealStatus::Upheld as i32);
}

#[tokio::test]
async fn an_appeal_past_the_window_is_refused() {
    let h = Harness::start().await;
    let (actor, decision_id) = suspended(&h).await;
    // The decision was taken seven months ago.
    sqlx::query("UPDATE decisions SET decided_at = decided_at - interval '210 days' WHERE id = $1")
        .bind(Uuid::parse_str(&decision_id).unwrap())
        .execute(&h.pool)
        .await
        .unwrap();

    let status = h.file_appeal(&decision_id, &actor).await.unwrap_err();
    // MOD-5003 AppealWindowClosed.
    assert_eq!(status.code(), Code::FailedPrecondition, "{status:?}");
    assert!(status.message().contains("window"), "{status:?}");
    assert!(my_appeals(&h, &actor).await.is_empty());
}
