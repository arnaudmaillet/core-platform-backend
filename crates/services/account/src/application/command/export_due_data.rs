//! The GDPR data export pass (#653, Art. 15/20): every account whose export
//! was asked for and not delivered since gets a ZIP of JSON files — its own
//! account record (no secrets), then everything the other services hold —
//! stored privately, its key recorded on the GDPR record (the link, valid 7
//! days, is signed when the record is read; `GdprDataExportCompleted` tells
//! auth to email it).

use std::io::Write;
use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use serde_json::json;
use uuid::Uuid;

use crate::application::port::{AccountRepository, ExportFile, ExportSources, ExportStore};
use crate::domain::aggregate::Account;
use crate::error::AccountError;

/// How long the download link lives (S3's presign cap).
pub const EXPORT_LINK_TTL_DAYS: i64 = 7;

/// What one export pass did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ExportPass {
    pub delivered: usize,
    /// A source or the store failed, or the account changed meanwhile: still
    /// pending, retried next pass.
    pub retried: usize,
}

pub struct ExportDueData {
    repo: Arc<dyn AccountRepository>,
    sources: Arc<dyn ExportSources>,
    store: Arc<dyn ExportStore>,
}

impl ExportDueData {
    pub fn new(repo: Arc<dyn AccountRepository>, sources: Arc<dyn ExportSources>, store: Arc<dyn ExportStore>) -> Self {
        Self { repo, sources, store }
    }

    /// Exports up to `batch` pending accounts. One failing account is logged
    /// and left pending; the others go on.
    pub async fn run(&self, now: DateTime<Utc>, batch: i64) -> Result<ExportPass, AccountError> {
        let mut pass = ExportPass::default();
        for id in self.repo.list_pending_exports(batch).await? {
            match self.export_one(&id, now).await {
                Ok(true) => pass.delivered += 1,
                Ok(false) => {}
                Err(error) => {
                    tracing::warn!(account.id = %id.as_uuid(), %error, "data export not delivered; retrying next pass");
                    pass.retried += 1;
                }
            }
        }
        Ok(pass)
    }

    /// `Ok(false)`: nothing pending anymore (delivered or anonymized meanwhile).
    async fn export_one(&self, id: &crate::domain::value_object::AccountId, now: DateTime<Utc>) -> Result<bool, AccountError> {
        let Some(mut account) = self.repo.find_by_id(id).await? else { return Ok(false) };
        if !account.gdpr().has_pending_export() || account.gdpr().is_anonymized() {
            return Ok(false);
        }
        let mut files = vec![ExportFile::json("account.json", &account_record(&account))];
        files.extend(self.sources.gather(id, now).await?);
        files.push(ExportFile { path: "README.txt".into(), content: readme(now).into_bytes() });

        let archive = zip_archive(&files)?;
        let key = format!("exports/{}/{}.zip", id.as_uuid(), Uuid::now_v7());
        self.store.put(&key, archive).await?;
        // Only the key is kept: the link is signed when the record is read.
        account.complete_gdpr_data_export(key, now + Duration::days(EXPORT_LINK_TTL_DAYS), Uuid::now_v7())?;
        // Version-checked: a request made meanwhile fails this and is rebuilt.
        self.repo.save(&account).await?;
        Ok(true)
    }
}

/// The holder's own account record, without anything secret (no password
/// hash, no MFA material) or internal (version, permission overrides).
fn account_record(account: &Account) -> serde_json::Value {
    let gdpr = account.gdpr();
    json!({
        "account_id": account.id().as_uuid(),
        "identity": account.identity_id().as_str(),
        "status": account.status().to_string(),
        "email": account.email().map(|e| e.as_str()),
        "email_verified": account.email_verified(),
        "phone": account.phone().map(|p| p.as_str()),
        "phone_verified": account.phone_verified(),
        "date_of_birth": account.date_of_birth(),
        "country_of_residence": account.country_of_residence().map(|c| c.as_str()),
        "roles": account.roles().iter().map(|r| r.to_string()).collect::<Vec<_>>(),
        "two_step_sign_in": account.mfa().is_enrolled(),
        "created_at": account.created_at(),
        "last_login_at": account.last_login_at(),
        "consents": {
            "data_processing_given_at": gdpr.data_processing_consented_at(),
            "marketing_given_at": gdpr.marketing_consented_at(),
            "analytics_given_at": gdpr.analytics_consented_at(),
            "policy_version": gdpr.last_consent_version(),
        },
        "deletion": {
            "requested_at": gdpr.deletion_requested_at(),
            "scheduled_at": gdpr.deletion_scheduled_at(),
        },
        "export_requested_at": gdpr.data_export_requested_at(),
    })
}

fn readme(now: DateTime<Utc>) -> String {
    format!(
        "Your data export ({now})\n\n\
         account.json        your account: contact details, consents, sign-in settings\n\
         profiles/           each of your profiles, and per profile:\n\
           posts.json        what you posted\n\
           comments.json     what you commented\n\
           recent_searches.json  what you searched for lately\n\
           social.json       who you follow, who follows you, who you blocked\n\
           conversations/    your conversations (one-to-one in full; in groups and\n\
                             channels your own messages, the others' as placeholders)\n\
         likes.json          the posts and comments you liked, and how many points\n\
         saves.json          the posts you saved, and with which profile\n\
         wallet.json         your points and gems, every movement, your stakes and how they settled\n\
         media.json          your photos and videos, each with a download link\n\n\
         Links in this archive work for {EXPORT_LINK_TTL_DAYS} days.\n"
    )
}

/// A ZIP of `files`, deflated.
fn zip_archive(files: &[ExportFile]) -> Result<Vec<u8>, AccountError> {
    let fail = |e: &dyn std::fmt::Display| AccountError::DomainViolation {
        field: "data_export.archive".into(),
        message: e.to_string(),
    };
    let mut zip = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    let options = zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Deflated);
    for file in files {
        zip.start_file(file.path.as_str(), options).map_err(|e| fail(&e))?;
        zip.write_all(&file.content).map_err(|e| fail(&e))?;
    }
    Ok(zip.finish().map_err(|e| fail(&e))?.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_archive_holds_every_file_readably() {
        let files = vec![
            ExportFile::json("account.json", &json!({"email": "me@example.com"})),
            ExportFile { path: "profiles/p1/posts.json".into(), content: b"[]".to_vec() },
        ];
        let bytes = zip_archive(&files).unwrap();
        let mut zip = zip::ZipArchive::new(std::io::Cursor::new(bytes)).unwrap();
        assert_eq!(zip.len(), 2);
        let mut account = String::new();
        std::io::Read::read_to_string(&mut zip.by_name("account.json").unwrap(), &mut account).unwrap();
        assert!(account.contains("me@example.com"));
        assert!(zip.by_name("profiles/p1/posts.json").is_ok());
    }
}
