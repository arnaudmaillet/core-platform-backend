//! #653 over real Postgres + MinIO: an account's assets, by id, each with a
//! signed download of its original that actually serves the bytes — what the
//! GDPR data export lists. A deleted asset is gone.

use crate::media_it::harness::Harness;

#[tokio::test]
async fn an_accounts_assets_list_by_id_with_working_downloads() {
    let h = Harness::start().await;
    let first = h.upload_and_process().await;
    let second = h.upload_and_process().await;

    let listed = h.list_by_owner(10, None, chrono::Duration::days(30)).await.expect("list");
    let mine: Vec<_> = listed.iter().map(|o| o.asset.id()).filter(|id| [first, second].contains(id)).collect();
    let mut expected = vec![first, second];
    expected.sort_by_key(|id| id.as_uuid());
    assert_eq!(mine, expected, "by id");
    let download = listed.iter().find(|o| o.asset.id() == first).unwrap().download.clone().expect("deliverable");
    assert!(download.url.contains("X-Amz-Expires=604800"), "capped at 7 days: {}", download.url);
    let bytes = reqwest::get(&download.url).await.unwrap();
    assert!(bytes.status().is_success(), "the signed URL serves the original");
    assert!(!bytes.bytes().await.unwrap().is_empty());

    // Paged by id: one asset a page from the first, the second comes later
    // (the harness owner is shared — other scenarios' assets may sit between).
    let mut after = expected[0];
    let next = loop {
        let page = h.list_by_owner(1, Some(after), chrono::Duration::hours(1)).await.unwrap();
        let id = page.first().expect("the second is still ahead").asset.id();
        assert!(id.as_uuid() > after.as_uuid(), "strictly after the cursor");
        if id == expected[1] {
            break id;
        }
        after = id;
    };
    assert_eq!(next, expected[1]);
    h.delete(first).await.expect("delete");
    let after = h.list_by_owner(10, None, chrono::Duration::hours(1)).await.unwrap();
    assert!(after.iter().all(|o| o.asset.id() != first));
}
