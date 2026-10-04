//! End-to-end decision lifecycle across all three stores: open a case, suspend
//! the actor (Postgres ledger + enforcement, Redis projection), then appeal and
//! overturn — reversing the enforcement, clearing the projection, and leaving the
//! append-only ledger holding both the original decision and its reversal.

use crate::moderation_it::harness::{subject, Harness};
use moderation::infrastructure::grpc::proto;

#[tokio::test]
async fn suspend_then_overturn_on_appeal() {
    let h = Harness::start().await;
    let subj = subject(proto::EntityType::Post, "feed");
    let actor = subj.actor_id.clone();

    // Open a case and suspend the actor.
    let opened = h
        .open_case(subj.clone(), proto::PolicyCategory::Harassment as i32)
        .await
        .expect("open case");
    assert!(opened.created);
    let case_id = opened.case.expect("case view").case_id;

    let decided = h
        .decide_case(&case_id, proto::ActionType::Suspend as i32, proto::PolicyCategory::Harassment as i32)
        .await
        .expect("decide");
    let decision = decided.decision.expect("decision recorded");
    assert!(decided.enforcement.is_some(), "suspend creates an enforcement");

    // Durably written: one decision, one active enforcement, one case.
    assert_eq!(h.count_decisions(&actor).await, 1);
    assert_eq!(h.count_enforcements(&actor, "active").await, 1);
    assert_eq!(h.count_cases(&actor).await, 1);

    // The Redis projection reports the actor restricted.
    let state = h.enforcement_state(&actor).await.expect("enforcement state");
    assert!(state.actor_restricted, "actor restricted on the hot-path projection");
    assert_eq!(state.active_enforcements.len(), 1);
    // The enforcement names the decision behind it (statement + appeal key).
    assert_eq!(state.active_enforcements[0].decision_id, decision.decision_id);

    // The DSA statement of reasons echoes the pinned policy version.
    let sor = h.statement_of_reasons(&decision.decision_id).await.expect("sor");
    let statement = sor.statement.expect("statement present");
    assert_eq!(statement.action, proto::ActionType::Suspend as i32);
    assert_eq!(statement.policy_version, "2026.06.1");

    // File then overturn the appeal.
    let appeal = h
        .file_appeal(&decision.decision_id, &actor)
        .await
        .expect("file appeal")
        .appeal
        .expect("appeal view");
    let resolved = h.resolve_appeal(&appeal.appeal_id, true).await.expect("resolve appeal");
    assert!(resolved.reversal.is_some(), "overturn records a reversal decision");

    // Append-only ledger now holds the original + the reversal; the enforcement is
    // reversed (no longer active) and the projection is cleared.
    assert_eq!(h.count_decisions(&actor).await, 2, "original + reversal");
    assert_eq!(h.count_enforcements(&actor, "active").await, 0);
    assert_eq!(h.count_enforcements(&actor, "reversed").await, 1);
    assert!(
        !h.enforcement_state(&actor).await.expect("state").actor_restricted,
        "projection cleared after overturn"
    );
}

#[tokio::test]
async fn dismissal_records_a_decision_but_no_enforcement() {
    let h = Harness::start().await;
    let subj = subject(proto::EntityType::Comment, "thread");
    let actor = subj.actor_id.clone();

    let opened = h
        .open_case(subj, proto::PolicyCategory::Spam as i32)
        .await
        .expect("open");
    let case_id = opened.case.unwrap().case_id;

    let decided = h
        .decide_case(&case_id, proto::ActionType::NoAction as i32, proto::PolicyCategory::Spam as i32)
        .await
        .expect("dismiss");
    assert!(decided.enforcement.is_none(), "no_action creates no enforcement");
    assert_eq!(h.count_decisions(&actor).await, 1);
    assert_eq!(h.count_enforcements(&actor, "active").await, 0);
}
