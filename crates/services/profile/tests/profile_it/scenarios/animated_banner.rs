//! Scenario — animated banners (#915, #947) over ScyllaDB: a video of the
//! account's own becomes the banner only for an account owning the item;
//! clearing both fields removes it; a still banner still works.

use cqrs::error::CqrsError;
use cqrs::{CommandBus, Envelope};
use error::AppError;
use uuid::Uuid;

use profile::application::command::UpdateBannerCommand;

use crate::profile_it::harness::{self, TestHarness};

async fn banner(h: &TestHarness, id: &str, url: Option<&str>, asset: Option<&str>) -> Result<(), CqrsError> {
    let cmd = UpdateBannerCommand {
        profile_id:      id.to_owned(),
        banner_url:      url.map(str::to_owned),
        banner_asset_id: asset.map(str::to_owned),
    };
    h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), cmd)).await
}

fn code(result: Result<(), CqrsError>) -> Option<&'static str> {
    match result {
        Err(CqrsError::Handler(e)) => Some(e.error_code()),
        Err(_) => Some("other"),
        Ok(()) => None,
    }
}

#[tokio::test]
async fn a_video_banner_needs_the_item_and_an_own_video() {
    let h = TestHarness::start().await;
    let account = harness::random_account_id();
    let handle = harness::random_handle();
    h.create(&account, &handle, "Alice").await;
    let id = h.get_by_handle(&handle).await.expect("created").id;
    let video = h.documents.upload(&account);
    let someone_elses = h.documents.upload(&harness::random_account_id());

    assert_eq!(code(banner(&h, &id, None, Some(&video)).await), Some("PRF-6007"), "the item first");
    h.wallet.granted.lock().unwrap().push((account.clone(), "banner.animated".into()));
    assert_eq!(code(banner(&h, &id, None, Some(&someone_elses)).await), Some("PRF-5007"), "its own video only");

    banner(&h, &id, None, Some(&video)).await.expect("an animated banner");
    let view = h.get_by_handle(&handle).await.unwrap();
    assert_eq!((view.banner_url.as_deref(), view.banner_asset_id.as_deref()), (None, Some(video.as_str())));

    banner(&h, &id, None, None).await.expect("cleared");
    let view = h.get_by_handle(&handle).await.unwrap();
    assert_eq!((view.banner_url, view.banner_asset_id), (None, None));

    banner(&h, &id, Some("https://cdn.example/b.jpg"), None).await.expect("a still banner");
    assert_eq!(h.get_by_handle(&handle).await.unwrap().banner_url.as_deref(), Some("https://cdn.example/b.jpg"));
}
