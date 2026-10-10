//! #876 over live Postgres + MinIO: a ticket retried under the same idempotency
//! key answers the asset the first one reserved — racing tickets reserve one
//! asset (the partial unique index), a pending one gets a fresh upload URL, a
//! committed one needs no upload, and deleting the asset frees the key.

use uuid::Uuid;

use media::domain::value_object::{AssetState, OwnerId};

use crate::media_it::harness::Harness;

#[tokio::test]
async fn a_retried_ticket_under_one_key_reserves_one_asset() {
    let h = Harness::start().await;
    let owner = OwnerId::from_uuid(Uuid::now_v7());
    let bytes = Harness::sample_jpeg(640, 480);
    let size = bytes.len() as u64;
    let key = format!("bubble-{}:{}", Uuid::now_v7(), &Harness::sha256_hex(&bytes)[..16]);

    // Racing tickets: one asset, every answer names it, each with an upload URL.
    let (a, b, c, d, e, f) = tokio::join!(
        h.issue_keyed(owner, size, &key),
        h.issue_keyed(owner, size, &key),
        h.issue_keyed(owner, size, &key),
        h.issue_keyed(owner, size, &key),
        h.issue_keyed(owner, size, &key),
        h.issue_keyed(owner, size, &key),
    );
    let outcomes: Vec<_> = [a, b, c, d, e, f].into_iter().map(|o| o.expect("keyed ticket")).collect();
    let asset_id = outcomes[0].asset_id;
    assert!(outcomes.iter().all(|o| o.asset_id == asset_id && !o.deduplicated && o.upload.is_some()));

    // The replay's URL uploads to the same asset, which then commits.
    let url = outcomes[5].upload.as_ref().unwrap().presigned.url.clone();
    assert!(h.put_to_url(&url, bytes).await);
    assert_eq!(h.commit(asset_id).await.unwrap().state(), AssetState::Uploaded);

    // Past pending: the same key needs no upload.
    let replay = h.issue_keyed(owner, size, &key).await.unwrap();
    assert_eq!(replay.asset_id, asset_id);
    assert!(replay.deduplicated);
    assert!(replay.upload.is_none());

    // Another key is another asset.
    let other = h.issue_keyed(owner, size, &format!("bubble-{}", Uuid::now_v7())).await.unwrap();
    assert_ne!(other.asset_id, asset_id);
}

#[tokio::test]
async fn deleting_the_asset_frees_its_key() {
    let h = Harness::start().await;
    let owner = OwnerId::from_uuid(Uuid::now_v7());
    let key = format!("clip-{}", Uuid::now_v7());

    let first = h.issue_keyed(owner, 1_000, &key).await.unwrap().asset_id;
    h.delete_as(owner, first).await.unwrap();

    let again = h.issue_keyed(owner, 1_000, &key).await.unwrap();
    assert_ne!(again.asset_id, first);
    assert!(again.upload.is_some());
}
