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

/// Migration 0003 on old data (a restored backup) holding duplicate appeals:
/// it keeps one per (decision, appellant) — the resolved one — and then builds
/// the unique index instead of failing. Run in a scratch schema so the shared
/// database is untouched.
#[tokio::test]
async fn the_appeal_migration_survives_duplicates_in_old_data() {
    let h = Harness::start().await;
    let schema = format!("dedupe_{}", Uuid::now_v7().simple());
    let mut conn = h.pool.acquire().await.unwrap();
    sqlx::raw_sql(&format!(
        "CREATE SCHEMA {schema};
         CREATE TABLE {schema}.appeals (
             id UUID NOT NULL, decision_id UUID NOT NULL, actor_id UUID NOT NULL,
             statement TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'filed',
             filed_at TIMESTAMPTZ NOT NULL, resolved_at TIMESTAMPTZ, PRIMARY KEY (id));
         SET search_path TO {schema};"
    ))
    .execute(&mut *conn)
    .await
    .unwrap();

    let (decision, actor) = (Uuid::now_v7(), Uuid::now_v7());
    let (first, resolved, third) = (Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7());
    for (id, minutes, status) in [(first, 1, "filed"), (resolved, 2, "upheld"), (third, 3, "filed")] {
        sqlx::query(
            "INSERT INTO appeals (id, decision_id, actor_id, statement, status, filed_at, resolved_at)
             VALUES ($1, $2, $3, 'x', $4, now() + make_interval(mins => $5),
                     CASE WHEN $4 = 'upheld' THEN now() END)",
        )
        .bind(id)
        .bind(decision)
        .bind(actor)
        .bind(status)
        .bind(minutes)
        .execute(&mut *conn)
        .await
        .unwrap();
    }

    let migration = include_str!("../../../migrations/postgres/0003_appeal_outcome.sql");
    sqlx::raw_sql(migration).execute(&mut *conn).await.expect("migration 0003 on duplicates");

    let kept: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM appeals").fetch_all(&mut *conn).await.unwrap();
    assert_eq!(kept, vec![resolved], "the resolved appeal is kept");
    sqlx::raw_sql(&format!("SET search_path TO public; DROP SCHEMA {schema} CASCADE;"))
        .execute(&mut *conn)
        .await
        .unwrap();
}

/// DSA Art. 20(1) over real Postgres: a member's report leads to the decision
/// (ReportView.decision_id); the reporter appeals a dismissal; overturned, the
/// case is back under review (no decision on the report any more), and the new
/// decision is the one the report then leads to. A stranger cannot appeal.
#[tokio::test]
async fn a_reporter_appeals_the_dismissal_of_their_report() {
    use crate::moderation_it::harness::OWNER;
    use moderation::domain::value_object::{ActorId, CaseId, EntityType, SubjectRef};

    let h = Harness::start().await;
    let reporter = Uuid::now_v7().to_string();
    let post = format!("post-{}", Uuid::now_v7());
    let report = as_account(
        Request::new(proto::SubmitReportRequest {
            entity_type: proto::EntityType::Post as i32,
            entity_id: post.clone(),
            category: proto::PolicyCategory::Harassment as i32,
            reason: "abusive".into(),
            surface: "post_menu".into(),
        }),
        &reporter,
    );
    h.handler.submit_report(report).await.expect("report");
    let case_id = CaseId::for_subject(
        &SubjectRef::new(EntityType::Post, post.clone(), ActorId::from_uuid(OWNER), "post_menu").unwrap(),
    )
    .as_str();
    let my_report = || async {
        let request = as_account(Request::new(proto::ListMyReportsRequest { page_size: 0, page_token: String::new() }), &reporter);
        h.handler.list_my_reports(request).await.unwrap().into_inner().reports.remove(0)
    };
    assert!(my_report().await.decision_id.is_empty(), "not decided yet");

    // Dismissed: the report leads to the decision.
    h.decide_case(&case_id, proto::ActionType::NoAction as i32, proto::PolicyCategory::Harassment as i32)
        .await
        .expect("dismiss");
    let decided = my_report().await;
    assert_eq!(decided.status, proto::ReportStatus::NoViolation as i32);
    let dismissal = decided.decision_id;
    assert!(!dismissal.is_empty());

    let file = |sub: &str| {
        as_account(
            Request::new(proto::FileAppealRequest {
                decision_id: dismissal.clone(),
                actor_id: sub.to_owned(),
                statement: "it is abusive".into(),
            }),
            sub,
        )
    };
    let stranger = Uuid::now_v7().to_string();
    assert_eq!(h.handler.file_appeal(file(&stranger)).await.unwrap_err().code(), Code::NotFound);
    let appeal = h.handler.file_appeal(file(&reporter)).await.expect("the reporter appeals").into_inner().appeal.unwrap();
    assert!(appeal.by_reporter);
    assert_eq!(my_appeals(&h, &reporter).await.len(), 1);

    // Overturned: back under review, no decision to show.
    h.resolve_appeal(&appeal.appeal_id, true).await.expect("overturn");
    let reviewed = my_report().await;
    assert_eq!(reviewed.status, proto::ReportStatus::UnderReview as i32);
    assert!(reviewed.decision_id.is_empty());
    let listed = my_appeals(&h, &reporter).await;
    assert_eq!(listed[0].status, proto::AppealStatus::Overturned as i32);

    // Decided again: the report now leads to the new decision.
    let removal = h
        .decide_case(&case_id, proto::ActionType::RemoveContent as i32, proto::PolicyCategory::Harassment as i32)
        .await
        .expect("decide again")
        .decision
        .unwrap()
        .decision_id;
    let final_report = my_report().await;
    assert_eq!(final_report.status, proto::ReportStatus::ActionTaken as i32);
    assert_eq!(final_report.decision_id, removal);
}
