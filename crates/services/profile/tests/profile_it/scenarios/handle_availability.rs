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
