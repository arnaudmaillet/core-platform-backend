//! Global sign-out bumps the account generation and invalidates every session's
//! edge token at once for `Introspect` and `Refresh` — the account-wide kill switch.

use crate::auth_it::harness::{random_user, Harness};

#[tokio::test]
async fn logout_all_revokes_every_session_and_invalidates_tokens() {
    let h = Harness::start().await;
    let user = random_user();

    // Two devices for one account.
    let a = h.login(&user).await.expect("login a");
    let b = h.login(&user).await.expect("login b");
    let account_id = a.account_id.clone();
    let token_a = a.tokens.unwrap().access_token;
    let token_b = b.tokens.unwrap().access_token;
    assert_eq!(h.count_active_sessions(&account_id).await, 2);

    // Both tokens are active before the global logout.
    assert!(h.introspect(&token_a).await.unwrap().active);
    assert!(h.introspect(&token_b).await.unwrap().active);

    let out = h.logout_all(&account_id).await.expect("logout all");
    assert!(out.success);
    assert_eq!(out.sessions_revoked, 2);
    assert!(out.generation >= 1, "generation was bumped");

    // Every previously-minted edge token is now inactive (stale generation), and
    // no active sessions remain.
    assert!(!h.introspect(&token_a).await.unwrap().active);
    assert!(!h.introspect(&token_b).await.unwrap().active);
    assert_eq!(h.count_active_sessions(&account_id).await, 0);
    assert!(h.list_sessions(&account_id).await.unwrap().sessions.is_empty());
}
