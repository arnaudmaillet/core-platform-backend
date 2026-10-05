//! Scenario — sign-up's live handle check over real ScyllaDB: a free handle is
//! available (normalized), a claimed one is taken, a malformed one is invalid.

use cqrs::{CommandBus, Envelope, QueryBus};
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

/// A rename interrupted between the profile save and the old handle's tombstone
/// leaves the old handle's claim live. Reads heal it: the old handle no longer
/// resolves to the renamed profile, and it enters its reservation.
#[tokio::test]
async fn a_claim_left_by_an_interrupted_rename_is_healed_on_read() {
    use profile::application::command::ChangeHandleCommand;

    let h = TestHarness::start().await;
    let (old, new) = (harness::random_handle(), harness::random_handle());
    harness::dispatch_create(std::sync::Arc::clone(&h.command_bus), &harness::random_account_id(), &old, "Renamed")
        .await
        .expect("create");
    let id = h.get_by_handle(&old).await.expect("created").id;
    h.command_bus
        .dispatch(Envelope::new(Uuid::now_v7(), ChangeHandleCommand { profile_id: id.clone(), new_handle: new.clone() }))
        .await
        .expect("rename");

    // Simulate the crash: the old handle's claim was never tombstoned (and is
    // older than the grace that protects a rename in flight).
    let long_ago = scylla::value::CqlTimestamp((chrono::Utc::now() - chrono::Duration::minutes(10)).timestamp_millis());
    let revive = || {
        h.scylla.session.execute_unpaged(
            "UPDATE profile.profile_handles SET tombstoned_at = null, created_at = ? WHERE handle = ?",
            (long_ago, old.clone()),
        )
    };
    revive().await.expect("revive the old claim");
    let tombstoned = || async {
        let rows = h
            .scylla
            .session
            .execute_unpaged("SELECT tombstoned_at FROM profile.profile_handles WHERE handle = ?", (old.clone(),))
            .await
            .unwrap()
            .into_rows_result()
            .unwrap();
        let (ts,): (Option<scylla::value::CqlTimestamp>,) = rows.first_row().unwrap();
        ts.is_some()
    };

    // A lookup by the old handle finds nothing and heals the row.
    assert!(h.get_by_handle(&old).await.is_none(), "the old handle must not resolve to the renamed profile");
    assert!(tombstoned().await, "the read tombstoned the stale claim");

    // The availability check heals too (and the handle is reserved, not free).
    revive().await.expect("revive again");
    assert_eq!(check(&h, &old).await, HandleAvailability::Taken(old.clone()));
    assert!(tombstoned().await, "the availability check tombstoned the stale claim");

    // The renamed profile is untouched.
    assert_eq!(h.get_by_handle(&new).await.expect("new handle").id, id);
}

/// A rename in flight claims the new handle before saving the profile: a read
/// in between sees a claim whose profile still carries the old handle. That
/// fresh claim must survive (the grace), or the rename would lose its handle.
#[tokio::test]
async fn a_rename_in_flight_keeps_its_new_claim() {
    use profile::domain::value_object::{AccountId, Handle, ProfileId};

    let h = TestHarness::start().await;
    let (old, new) = (harness::random_handle(), harness::random_handle());
    let account = harness::random_account_id();
    harness::dispatch_create(std::sync::Arc::clone(&h.command_bus), &account, &old, "Renaming")
        .await
        .expect("create");
    let view = h.get_by_handle(&old).await.expect("created");

    // ChangeHandle's first step: the claim on the new handle, profile not saved yet.
    let claimed = h
        .repository
        .claim_handle(
            &Handle::new(&new).unwrap(),
            ProfileId::try_from(view.id.as_str()).unwrap(),
            AccountId::try_from(account.as_str()).unwrap(),
        )
        .await
        .expect("claim");
    assert!(claimed);

    // Reads in that window: not resolvable yet, taken, and the claim stays live.
    assert!(h.get_by_handle(&new).await.is_none());
    assert_eq!(check(&h, &new).await, HandleAvailability::Taken(new.clone()));
    let rows = h
        .scylla
        .session
        .execute_unpaged("SELECT tombstoned_at FROM profile.profile_handles WHERE handle = ?", (new.clone(),))
        .await
        .unwrap()
        .into_rows_result()
        .unwrap();
    let (tombstoned_at,): (Option<scylla::value::CqlTimestamp>,) = rows.first_row().unwrap();
    assert!(tombstoned_at.is_none(), "a fresh claim (rename in flight) is not healed");
}
