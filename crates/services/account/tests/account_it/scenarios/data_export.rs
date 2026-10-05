//! #653 over real Postgres + MinIO: an export asked for is built by the export
//! pass — the holder's account record plus the other services' files, zipped
//! into the private bucket — and its signed link (7 days) lands on the GDPR
//! record; the link serves the archive. A failing source leaves the export
//! pending (no partial archive); a new request makes it pending again.

use std::io::Read;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cqrs::{CommandBus, Envelope};
use uuid::Uuid;

use account::application::command::request_data_export::RequestDataExportCommand;
use account::application::command::ExportDueData;
use account::application::port::{ExportFile, ExportSources};
use account::application::query::{GdprRecordView, GetGdprRecordHandler, GetGdprRecordQuery};
use cqrs::QueryHandler;
use account::domain::value_object::AccountId;
use account::error::AccountError;
use account::infrastructure::export::{ExportStoreConfig, S3ExportStore};

use crate::account_it::harness::{self, TestHarness, DEADLINE};

/// One file per account (what the other services would hold); `down` fails.
#[derive(Default)]
struct Sources {
    down: AtomicBool,
}

#[async_trait]
impl ExportSources for Sources {
    async fn gather(&self, account_id: &AccountId, _now: DateTime<Utc>) -> Result<Vec<ExportFile>, AccountError> {
        if self.down.load(Ordering::SeqCst) {
            return Err(AccountError::DataExportUnavailable { reason: "post is down".into() });
        }
        Ok(vec![ExportFile::json("profiles/p1/posts.json", &serde_json::json!([{ "account": account_id.as_uuid(), "caption": "hello" }]))])
    }
}

async fn store() -> S3ExportStore {
    let endpoint = test_support::containers::minio_ready().await;
    let store = S3ExportStore::new(ExportStoreConfig {
        endpoint: endpoint.clone(),
        public_endpoint: endpoint,
        region: "us-east-1".into(),
        bucket: "gdpr-exports".into(),
        access_key: "minioadmin".into(),
        secret_key: "minioadmin".into(),
    })
    .expect("store");
    store.ensure_bucket().await.expect("bucket");
    store
}

async fn active_account(h: &TestHarness) -> String {
    let (identity, email) = (harness::random_identity(), harness::random_email());
    h.create(&identity, &email).await;
    harness::await_until("account readable", DEADLINE, || async { h.get_by_identity(&identity).await.is_ok() }).await;
    let id = h.get_by_identity(&identity).await.unwrap().id;
    h.verify_email(&id).await.expect("activate");
    id
}

/// The GDPR record as the holder reads it: the link signed on read.
async fn record(h: &TestHarness, store: &Arc<S3ExportStore>, id: &str) -> GdprRecordView {
    GetGdprRecordHandler::new(Arc::clone(&h.repository))
        .with_exports(Arc::clone(store) as _)
        .handle(Envelope::new(Uuid::now_v7(), GetGdprRecordQuery { account_id: id.to_owned() }))
        .await
        .unwrap()
}

async fn request_export(h: &TestHarness, id: &str) {
    h.command_bus
        .dispatch(Envelope::new(Uuid::now_v7(), RequestDataExportCommand { account_id: id.to_owned() }))
        .await
        .expect("request export");
}

#[tokio::test]
async fn a_requested_export_is_zipped_stored_and_linked_once() {
    let h = TestHarness::start().await;
    let sources = Arc::new(Sources::default());
    let store = Arc::new(store().await);
    let pass = ExportDueData::new(Arc::clone(&h.repository), Arc::clone(&sources) as _, Arc::clone(&store) as _);
    let id = active_account(&h).await;
    request_export(&h, &id).await;
    assert!(record(&h, &store, &id).await.data_export_url.is_none());

    // A source down: nothing is delivered, the export stays pending.
    sources.down.store(true, Ordering::SeqCst);
    let failed = pass.run(Utc::now(), 1000).await.unwrap();
    assert!(failed.retried >= 1);
    assert!(record(&h, &store, &id).await.data_export_url.is_none(), "no partial archive");

    sources.down.store(false, Ordering::SeqCst);
    let done = pass.run(Utc::now(), 1000).await.unwrap();
    assert!(done.delivered >= 1);
    let delivered = record(&h, &store, &id).await;
    let link = delivered.data_export_url.clone().expect("a link");
    let expires = delivered.data_export_expires_at.expect("an expiry");
    assert!(expires > Utc::now() + chrono::Duration::days(6), "valid ~7 days: {expires}");
    assert!(delivered.data_export_completed_at.is_some());
    // No bearer credential at rest: the row holds the object key only.
    let stored: Option<String> =
        sqlx::query_scalar("SELECT gdpr_data_export_key FROM accounts WHERE id = $1::uuid")
            .bind(&id)
            .fetch_one(&h.pool)
            .await
            .unwrap();
    let key = stored.expect("a key");
    assert!(key.starts_with("exports/") && !key.contains("X-Amz"), "{key}");

    // The link serves the archive: the account record (no secret) + the files.
    let bytes = reqwest::get(&link).await.unwrap().bytes().await.unwrap();
    let mut zip = zip::ZipArchive::new(std::io::Cursor::new(bytes.to_vec())).expect("a zip");
    let mut account = String::new();
    zip.by_name("account.json").unwrap().read_to_string(&mut account).unwrap();
    assert!(account.contains(&id) && account.contains("\"email\""), "{account}");
    assert!(!account.contains("password") && !account.contains("totp"), "no secrets: {account}");
    assert!(zip.by_name("profiles/p1/posts.json").is_ok());
    assert!(zip.by_name("README.txt").is_ok());

    // Delivered: the next pass leaves it alone; a new request re-opens it.
    let again = pass.run(Utc::now(), 1000).await.unwrap();
    let reread: Option<String> =
        sqlx::query_scalar("SELECT gdpr_data_export_key FROM accounts WHERE id = $1::uuid").bind(&id).fetch_one(&h.pool).await.unwrap();
    assert_eq!(reread.as_deref(), Some(key.as_str()), "not rebuilt: {again:?}");
    request_export(&h, &id).await;
    assert!(record(&h, &store, &id).await.data_export_url.is_none(), "a newer request hides the old link");
    pass.run(Utc::now(), 1000).await.unwrap();
    record(&h, &store, &id).await.data_export_url.expect("a new link");
    let rebuilt: Option<String> =
        sqlx::query_scalar("SELECT gdpr_data_export_key FROM accounts WHERE id = $1::uuid").bind(&id).fetch_one(&h.pool).await.unwrap();
    assert_ne!(rebuilt.as_deref(), Some(key.as_str()), "a new archive");
}
