//! Regression: the optimistic-CAS write path. The repository binds the
//! aggregate's PRE-mutation version (`version() - 1` — `touch()` already
//! bumped it in memory); binding `version()` made every mutation on every
//! account abort with ConcurrentModification while create+read stayed green —
//! found live by the prod E2E login drill (VerifyEmail could never
//! activate an account, so no login could ever succeed).

use crate::account_it::harness::{self, TestHarness, DEADLINE};

#[tokio::test]
async fn mutations_survive_the_optimistic_cas_across_reload_cycles() {
    let h = TestHarness::start().await;
    let identity = harness::random_identity();
    let email = harness::random_email();
    h.create(&identity, &email).await;

    let identity_q = identity.clone();
    harness::await_until("account readable", DEADLINE, || {
        let h = &h;
        let identity_q = identity_q.clone();
        async move { h.get_by_identity(&identity_q).await.is_ok() }
    })
    .await;
    let view = h.get_by_identity(&identity).await.expect("account exists");

    // First mutation of the account's life: PendingVerification -> Active.
    h.verify_email(&view.id).await.expect("verify_email must not abort on CAS");
    let view = h.get_by_identity(&identity).await.expect("account exists");
    assert_eq!(view.status, "active", "verify_email must activate the account");

    // Second, distinct mutation after a reload cycle: the CAS must keep holding.
    h.record_login(&view.id).await.expect("record_login must not abort on CAS");
}
