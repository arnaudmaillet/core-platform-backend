use std::sync::Arc;

use chrono::{DateTime, Utc};
use cqrs::{Envelope, Query, QueryHandler};
use uuid::Uuid;

use crate::application::port::AccountRepository;
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
}

impl GetGdprRecordHandler {
    pub fn new(repo: Arc<dyn AccountRepository>) -> Self {
        Self { repo }
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
            // A newer request is being built: the old link is not the answer.
            data_export_url: gdpr.data_export_url().filter(|_| !gdpr.has_pending_export()).map(str::to_owned),
            data_export_expires_at: gdpr.data_export_expires_at().filter(|_| !gdpr.has_pending_export()),
            last_consent_version: gdpr.last_consent_version().map(str::to_owned),
        })
    }
}
pub type GetGdprRecordResponse = GdprRecordView;
