//! Scenario — an avatar's / banner's media asset (#907) round-trips through
//! ScyllaDB (the profile row and the account's index), and only the account's
//! own image is accepted.

use cqrs::{CommandBus, Envelope};
use uuid::Uuid;

use profile::application::command::{UpdateAvatarCommand, UpdateBannerCommand};
use profile::domain::value_object::AccountId;

use crate::profile_it::harness::{self, TestHarness};

const URL: &str = "https://cdn.example/avatar.jpg";

async fn set_avatar(h: &TestHarness, profile_id: &str, url: &str, asset: Option<&str>) -> Result<(), cqrs::CqrsError> {
    let cmd = UpdateAvatarCommand {
        profile_id:      profile_id.to_owned(),
        avatar_url:      Some(url.to_owned()),
        avatar_asset_id: asset.map(str::to_owned),
    };
    h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), cmd)).await
}

#[tokio::test]
async fn an_avatar_and_banner_keep_their_media_assets() {
    let h = TestHarness::start().await;
    let account = harness::random_account_id();
    let handle = harness::random_handle();
    h.create(&account, &handle, "Alice").await;
    let profile_id = h.get_by_handle(&handle).await.expect("created").id;

    let avatar = h.documents.upload(&account);
    let banner = h.documents.upload(&account);
    set_avatar(&h, &profile_id, URL, Some(&avatar)).await.expect("own image accepted");
    let cmd = UpdateBannerCommand {
        profile_id:      profile_id.clone(),
        banner_url:      Some("https://cdn.example/banner.jpg".to_owned()),
        banner_asset_id: Some(banner.clone()),
    };
    h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), cmd)).await.expect("own banner accepted");

    let view = h.get_by_id(&profile_id).await.expect("profile");
    assert_eq!(view.avatar_asset_id.as_deref(), Some(avatar.as_str()));
    assert_eq!(view.banner_asset_id.as_deref(), Some(banner.as_str()));

    // The account's profile list shows the new avatar and its asset.
    let account_id = AccountId::from_uuid(Uuid::parse_str(&account).unwrap());
    let (summaries, _) = h.repository.list_by_account(&account_id, 50, None).await.expect("list");
    assert_eq!(summaries[0].avatar_url.as_deref(), Some(URL));
    assert_eq!(summaries[0].avatar_asset_id.map(|a| a.as_str()), Some(avatar.clone()));

    // An avatar set by URL alone has no asset any more.
    set_avatar(&h, &profile_id, "https://cdn.example/other.jpg", None).await.expect("url only");
    assert_eq!(h.get_by_id(&profile_id).await.expect("profile").avatar_asset_id, None);
}

#[tokio::test]
async fn another_accounts_image_is_refused() {
    let h = TestHarness::start().await;
    let account = harness::random_account_id();
    let handle = harness::random_handle();
    h.create(&account, &handle, "Bob").await;
    let profile_id = h.get_by_handle(&handle).await.expect("created").id;

    let someone_elses = h.documents.upload(&harness::random_account_id());
    let err = set_avatar(&h, &profile_id, URL, Some(&someone_elses)).await.unwrap_err();
    assert!(err.to_string().contains("not a usable image"), "{err}");
    assert_eq!(h.get_by_id(&profile_id).await.expect("profile").avatar_url, None, "nothing changed");
}
