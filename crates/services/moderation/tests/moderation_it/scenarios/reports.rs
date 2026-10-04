//! Client reports (guest mode B7, DSA Art. 16) against real Postgres + Redis:
//! the reporter comes from the token, the reported account from the resolver,
//! and the per-reporter quota is real Redis counters.

use std::sync::Arc;

use tonic::{Code, Request};
use uuid::Uuid;

use moderation::infrastructure::grpc::proto;

use crate::moderation_it::harness::{Harness, REPORTS_PER_HOUR};

fn principal(sub: &str, kind: Option<&str>) -> transport::grpc::edge::EdgePrincipal {
    let mut claims = serde_json::json!({ "sub": sub, "exp": 4_102_444_800_i64 });
    if let Some(kind) = kind {
        claims["kind"] = serde_json::json!(kind);
    }
    let raw: auth_context::OidcClaims = serde_json::from_value(claims).unwrap();
    transport::grpc::edge::EdgePrincipal::new(Arc::new(auth_context::CurrentPrincipal {
        user_id: auth_context::PrincipalId::new(sub),
        tenant_id: None,
        permissions: vec![auth_context::Permission::new("read:public")],
        raw_claims: raw,
    }))
}

fn report_as(sub: &str, kind: Option<&str>, entity_id: &str) -> Request<proto::SubmitReportRequest> {
    let mut request = Request::new(proto::SubmitReportRequest {
        entity_type: proto::EntityType::Post as i32,
        entity_id: entity_id.into(),
        category: proto::PolicyCategory::Spam as i32,
        reason: "spam".into(),
        surface: "post_menu".into(),
    });
    request.extensions_mut().insert(principal(sub, kind));
    request
}

#[tokio::test]
async fn a_guest_report_opens_a_case_and_the_quota_holds_per_reporter() {
    let h = Harness::start().await;
    let guest = format!("guest:{}", Uuid::now_v7());
    let post = format!("post-{}", Uuid::now_v7());

    let first = h.handler.submit_report(report_as(&guest, Some("guest"), &post)).await.expect("guest report");
    let first = first.into_inner().report_id;
    for _ in 1..REPORTS_PER_HOUR {
        let again = h.handler.submit_report(report_as(&guest, Some("guest"), &post)).await.unwrap();
        assert_eq!(again.into_inner().report_id, first, "deterministic per reporter × subject");
    }
    let err = h.handler.submit_report(report_as(&guest, Some("guest"), &post)).await.unwrap_err();
    assert_eq!(err.code(), Code::ResourceExhausted);

    // Another reporter is not affected; unknown content is not found.
    let member = Uuid::now_v7().to_string();
    h.handler.submit_report(report_as(&member, None, &post)).await.expect("member report");
    let err = h.handler.submit_report(report_as(&member, None, "comment-x")).await.unwrap_err();
    assert_eq!(err.code(), Code::NotFound);

    // The case is in the review queue.
    let queue = h
        .handler
        .list_queue(Request::new(proto::ListQueueRequest { queue: "default".into(), page_size: 100, ..Default::default() }))
        .await
        .unwrap()
        .into_inner();
    assert!(
        queue.cases.iter().any(|c| c.subject.as_ref().is_some_and(|s| s.entity_id == post)),
        "the reported post has a case in the queue"
    );
}
