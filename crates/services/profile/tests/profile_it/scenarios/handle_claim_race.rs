//! Scenario — handle-claim race (concurrency).
//!
//! A handle is globally unique. When many accounts race to create a profile with
//! the *same* handle, the ScyllaDB LWT in `claim_handle` must let exactly one win;
//! every other create must surface `HandleAlreadyTaken`. This is the concurrency
//! axis: the uniqueness invariant must hold under a genuine simultaneous burst.

use std::sync::Arc;

use crate::profile_it::harness::{self, TestHarness};

const CONTENDERS: usize = 8;

#[tokio::test]
async fn concurrent_creates_of_same_handle_yield_exactly_one_winner() {
    let h = TestHarness::start().await;

    let handle = harness::random_handle();

    // Fire CONTENDERS concurrent creates, distinct accounts, identical handle.
    let mut handles = Vec::new();
    for i in 0..CONTENDERS {
        let bus = Arc::clone(&h.command_bus);
        let handle = handle.clone();
        let account = harness::random_account_id();
        let display = format!("contender-{i}");
        handles.push(tokio::spawn(async move {
            harness::dispatch_create(bus, &account, &handle, &display).await
        }));
    }

    let mut winners = 0;
    let mut losers = 0;
    for handle in handles {
        match handle.await.expect("join") {
            Ok(()) => winners += 1,
            Err(_) => losers += 1,
        }
    }

    assert_eq!(winners, 1, "exactly one create must win the handle claim");
    assert_eq!(losers, CONTENDERS - 1, "every other create must be rejected");

    // The handle resolves to a single, real profile.
    let view = h.get_by_handle(&handle).await;
    assert!(view.is_some(), "the claimed handle must resolve to the winning profile");
}

/// The losers of a claim race write nothing: no orphan profile row behind their
/// account, so `ListProfilesByAccount` (and the token's `pids`) stay clean.
#[tokio::test]
async fn losers_of_a_claim_race_leave_no_profile_behind() {
    let h = TestHarness::start().await;
    let handle = harness::random_handle();
    let accounts: Vec<String> = (0..CONTENDERS).map(|_| harness::random_account_id()).collect();

    let mut tasks = Vec::new();
    for account in &accounts {
        let (bus, handle, account) = (Arc::clone(&h.command_bus), handle.clone(), account.clone());
        tasks.push(tokio::spawn(async move { harness::dispatch_create(bus, &account, &handle, "racer").await }));
    }
    let mut listed = 0;
    for (task, account) in tasks.into_iter().zip(&accounts) {
        let won = task.await.expect("join").is_ok();
        let account_id = profile::domain::value_object::AccountId::try_from(account.as_str()).unwrap();
        let (profiles, _) = h.repository.list_by_account(&account_id, 50, None).await.expect("list");
        assert_eq!(profiles.len(), usize::from(won), "a loser has no profile, the winner one");
        listed += profiles.len();
    }
    assert_eq!(listed, 1);
}
