//! Scenario — sign-up's live handle check over real ScyllaDB: a free handle is
//! available (normalized), a claimed one is taken, a malformed one is invalid.

use cqrs::{Envelope, QueryBus};
use uuid::Uuid;

use profile::application::query::{CheckHandleAvailabilityQuery, HandleAvailability};

use crate::profile_it::harness::{self, TestHarness};

async fn check(h: &TestHarness, handle: &str) -> HandleAvailability {
    h.query_bus
        .dispatch(Envelope::new(Uuid::now_v7(), CheckHandleAvailabilityQuery { handle: handle.to_owned() }))
        .await
        .expect("check handle")
}

#[tokio::test]
async fn availability_follows_the_claims_and_the_rules() {
    let h = TestHarness::start().await;
    let handle = harness::random_handle();

    assert_eq!(check(&h, &handle.to_uppercase()).await, HandleAvailability::Available(handle.clone()));

    harness::dispatch_create(std::sync::Arc::clone(&h.command_bus), &harness::random_account_id(), &handle, "First")
        .await
        .expect("create");
    assert_eq!(check(&h, &handle).await, HandleAvailability::Taken(handle.clone()));

    assert!(matches!(check(&h, "a").await, HandleAvailability::Invalid(_)));
    assert!(matches!(check(&h, "bad..handle").await, HandleAvailability::Invalid(_)));
}

/// A handle released more than the reservation ago reads available AND can be
/// claimed (its tombstone is taken over); a fresh tombstone is still reserved.
#[tokio::test]
async fn an_expired_reservation_is_claimable_again() {
    use profile::application::port::HANDLE_RESERVATION_DAYS;

    let h = TestHarness::start().await;
    let handle = harness::random_handle();
    let bus = || std::sync::Arc::clone(&h.command_bus);
    harness::dispatch_create(bus(), &harness::random_account_id(), &handle, "Before").await.expect("create");
    let first = h.get_by_handle(&handle).await.expect("first owner").id;
    harness::dispatch_delete(bus(), &first).await.expect("delete releases the handle");

    // Freshly released: reserved for everyone.
    assert_eq!(check(&h, &handle).await, HandleAvailability::Taken(handle.clone()));
    assert!(harness::dispatch_create(bus(), &harness::random_account_id(), &handle, "Early").await.is_err());

    // Age the tombstone past the reservation.
    let aged = chrono::Utc::now() - chrono::Duration::days(HANDLE_RESERVATION_DAYS + 1);
    h.scylla
        .session
        .execute_unpaged(
            "UPDATE profile.profile_handles SET tombstoned_at = ? WHERE handle = ?",
            (scylla::value::CqlTimestamp(aged.timestamp_millis()), handle.clone()),
        )
        .await
        .expect("age the tombstone");

    assert_eq!(check(&h, &handle).await, HandleAvailability::Available(handle.clone()));
    let account = harness::random_account_id();
    harness::dispatch_create(bus(), &account, &handle, "After").await.expect("the expired handle is claimable");
    let now_owner = h.get_by_handle(&handle).await.expect("resolves to the new owner");
    assert_ne!(now_owner.id, first);
    assert_eq!(check(&h, &handle).await, HandleAvailability::Taken(handle.clone()));
}
