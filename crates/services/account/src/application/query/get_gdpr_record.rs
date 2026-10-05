use std::sync::Arc;

use chrono::{DateTime, Utc};
use cqrs::{Envelope, Query, QueryHandler};
use uuid::Uuid;

use crate::application::port::{AccountRepository, ExportStore};
use crate::domain::value_object::AccountId;
use crate::error::AccountError;

#[derive(Debug, Clone)]
pub struct GdprRecordView {
    pub account_id: String,
    pub data_processing_consented_at: Option<DateTime<Utc>>,
    pub marketing_consented_at: Option<DateTime<Utc>>,
    pub analytics_consented_at: Option<DateTime<Utc>>,
    pub deletion_requested_at: Option<DateTime<Utc>>,
    pub deletion_scheduled_at: Option<DateTime<Utc>>,
    pub anonymized_at: Option<DateTime<Utc>>,
    pub data_export_requested_at: Option<DateTime<Utc>>,
    pub data_export_completed_at: Option<DateTime<Utc>>,
    /// The delivered export's download link and its expiry (#653): shown to
    /// the holder (edge) and to auth (mesh, which emails it).
    pub data_export_url: Option<String>,
    pub data_export_expires_at: Option<DateTime<Utc>>,
    pub last_consent_version: Option<String>,
}

#[derive(Debug, Clone)]
pub struct GetGdprRecordQuery {
    pub account_id: String,
}

impl Query for GetGdprRecordQuery {
    type Response = GdprRecordView;
}

pub struct GetGdprRecordHandler {
    repo: Arc<dyn AccountRepository>,
    /// Signs the delivered export's link on read (#653); `None`: no link.
    exports: Option<Arc<dyn ExportStore>>,
}

impl GetGdprRecordHandler {
    pub fn new(repo: Arc<dyn AccountRepository>) -> Self {
        Self { repo, exports: None }
    }

    /// Hands out the delivered export's link, signed per read.
    pub fn with_exports(mut self, exports: Arc<dyn ExportStore>) -> Self {
        self.exports = Some(exports);
        self
    }

    /// A link to the delivered export, valid for what is left of its 7 days
    /// — none while a newer request is being built, once expired, or without
    /// the store.
    fn export_link(&self, gdpr: &crate::domain::entity::GdprRecord) -> Option<(String, DateTime<Utc>)> {
        let (store, key, expires_at) = (self.exports.as_ref()?, gdpr.data_export_key()?, gdpr.data_export_expires_at()?);
        let left = expires_at - Utc::now();
        if gdpr.has_pending_export() || left <= chrono::Duration::seconds(60) {
            return None;
        }
        match store.signed_link(key, left) {
            Ok(link) => Some((link, expires_at)),
            Err(error) => {
                tracing::warn!(%error, "export link not signed");
                None
            }
        }
    }
}

impl QueryHandler<GetGdprRecordQuery> for GetGdprRecordHandler {
    type Error = AccountError;

    async fn handle(
        &self,
        envelope: Envelope<GetGdprRecordQuery>,
    ) -> Result<GdprRecordView, Self::Error> {
        let id_str = &envelope.payload.account_id;
        let uuid = id_str.parse::<Uuid>().map_err(|_| AccountError::DomainViolation {
            field: "account_id".into(),
            message: "invalid UUID format".into(),
        })?;
        let id = AccountId::from_uuid(uuid);
        let account = self
            .repo
            .find_by_id(&id)
            .await?
            .ok_or_else(|| AccountError::AccountNotFound { id: id_str.clone() })?;

        let gdpr = account.gdpr();
        let link = self.export_link(gdpr);
        Ok(GdprRecordView {
            account_id: id_str.clone(),
            data_processing_consented_at: gdpr.data_processing_consented_at(),
            marketing_consented_at: gdpr.marketing_consented_at(),
            analytics_consented_at: gdpr.analytics_consented_at(),
            deletion_requested_at: gdpr.deletion_requested_at(),
            deletion_scheduled_at: gdpr.deletion_scheduled_at(),
            anonymized_at: gdpr.anonymized_at(),
            data_export_requested_at: gdpr.data_export_requested_at(),
            data_export_completed_at: gdpr.data_export_completed_at(),
            data_export_expires_at: link.as_ref().map(|(_, at)| *at),
            data_export_url: link.map(|(url, _)| url),
            last_consent_version: gdpr.last_consent_version().map(str::to_owned),
        })
    }
}
pub type GetGdprRecordResponse = GdprRecordView;
