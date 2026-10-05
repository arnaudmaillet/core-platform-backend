//! #661 over the real tables: a profile's QR / share-link token. Issued on
//! first ask and stable after; anyone resolves it to the profile as they may
//! see it; rotating it kills the old one at once; turning "reachable by QR"
//! off makes it resolve to nothing (but for the owner).

use cqrs::{CommandBus, Envelope, QueryBus};
use uuid::Uuid;

use profile::application::command::SetDiscoverySettingsCommand;
use profile::application::port::ProfileView;
use profile::application::query::{GetShareTokenQuery, ResolveShareTokenQuery, RotateShareTokenCommand};
use profile::domain::value_object::Viewer;

use crate::profile_it::harness::{self, TestHarness};

async fn token(h: &TestHarness, profile_id: &str) -> String {
    let query = GetShareTokenQuery { profile_id: profile_id.to_owned() };
    h.query_bus.dispatch(Envelope::new(Uuid::now_v7(), query)).await.expect("token")
}

async fn resolve(h: &TestHarness, token: &str, viewer: Viewer) -> Option<ProfileView> {
    let query = ResolveShareTokenQuery { token: token.to_owned(), viewer };
    h.query_bus.dispatch(Envelope::new(Uuid::now_v7(), query)).await.expect("resolve")
}

#[tokio::test]
async fn a_share_token_resolves_until_rotated_or_turned_off() {
    let h = TestHarness::start().await;
    let (account, handle) = (harness::random_account_id(), harness::random_handle());
    h.create(&account, &handle, "Alice").await;
    let profile = h.get_by_handle(&handle).await.expect("created");

    let first = token(&h, &profile.id).await;
    assert_eq!(first.len(), 22);
    assert_eq!(token(&h, &profile.id).await, first, "stable once issued");

    // Anyone resolves it — a stranger and an anonymous scan — as they may see it.
    let stranger = Viewer::Account(harness::random_account_id());
    let seen = resolve(&h, &first, stranger.clone()).await.expect("resolves");
    assert_eq!(seen.id, profile.id);
    assert!(seen.account_id.is_empty(), "as a stranger sees it: no owner-only fields");
    assert!(resolve(&h, &first, Viewer::Anonymous).await.is_some());
    assert!(resolve(&h, "not-a-token", stranger.clone()).await.is_none());

    // Rotated: the old one is dead at once, the new one works.
    h.command_bus
        .dispatch(Envelope::new(Uuid::now_v7(), RotateShareTokenCommand { profile_id: profile.id.clone() }))
        .await
        .expect("rotate");
    let second = token(&h, &profile.id).await;
    assert_ne!(second, first);
    assert!(resolve(&h, &first, stranger.clone()).await.is_none(), "revoked");
    assert!(resolve(&h, &second, stranger.clone()).await.is_some());

    // "Reachable by QR / shared link" off: nothing for others, the owner still.
    let off = SetDiscoverySettingsCommand { profile_id: profile.id.clone(), by_qr: Some(false), ..Default::default() };
    h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), off)).await.expect("by_qr off");
    assert!(resolve(&h, &second, stranger).await.is_none());
    assert!(resolve(&h, &second, Viewer::Account(account)).await.is_some(), "the owner");
}
