//! Scenario — animated avatars (#945, #948) over ScyllaDB: a clip of the
//! account's own becomes the avatar only for an account owning the item; a
//! photo replaces it.

use cqrs::error::CqrsError;
use cqrs::{CommandBus, Envelope};
use error::AppError;
use uuid::Uuid;

use profile::application::command::UpdateAvatarCommand;

use crate::profile_it::harness::{self, TestHarness};

async fn avatar(h: &TestHarness, id: &str, url: Option<&str>, asset: Option<&str>) -> Result<(), CqrsError> {
    let cmd = UpdateAvatarCommand {
        profile_id:      id.to_owned(),
        avatar_url:      url.map(str::to_owned),
        avatar_asset_id: asset.map(str::to_owned),
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
async fn a_clip_avatar_needs_the_item_and_an_own_clip() {
    let h = TestHarness::start().await;
    let account = harness::random_account_id();
    let handle = harness::random_handle();
    h.create(&account, &handle, "Alice").await;
    let id = h.get_by_handle(&handle).await.expect("created").id;
    let clip = h.documents.upload(&account);
    let someone_elses = h.documents.upload(&harness::random_account_id());

    assert_eq!(code(avatar(&h, &id, None, Some(&clip)).await), Some("PRF-6007"), "the item first");
    h.wallet.granted.lock().unwrap().push((account.clone(), "avatar.animated".into()));
    assert_eq!(code(avatar(&h, &id, None, Some(&someone_elses)).await), Some("PRF-5007"), "its own clip only");

    avatar(&h, &id, None, Some(&clip)).await.expect("an animated avatar");
    let view = h.get_by_handle(&handle).await.unwrap();
    assert_eq!((view.avatar_url.as_deref(), view.avatar_asset_id.as_deref()), (None, Some(clip.as_str())));

    avatar(&h, &id, Some("https://cdn.example/a.jpg"), None).await.expect("a photo again");
    let view = h.get_by_handle(&handle).await.unwrap();
    assert_eq!((view.avatar_url.as_deref(), view.avatar_asset_id), (Some("https://cdn.example/a.jpg"), None));
}
