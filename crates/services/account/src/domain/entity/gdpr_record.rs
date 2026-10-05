use chrono::{DateTime, Duration, Utc};

use crate::domain::value_object::ConsentPurpose;
use serde::{Deserialize, Serialize};

/// GDPR and data-protection state for an account.
///
/// Tracks consent timestamps, erasure requests, and anonymisation status.
/// The record is embedded inside the [`Account`] aggregate and is the
/// authoritative source of truth for all Art. 17 and Art. 20 obligations.
///
/// [`Account`]: crate::domain::aggregate::account::Account
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GdprRecord {
    /// Timestamp of explicit data-processing consent under Art. 7 GDPR.
    pub data_processing_consented_at: Option<DateTime<Utc>>,

    /// Timestamp of explicit marketing opt-in (separate from data processing).
    pub marketing_consented_at: Option<DateTime<Utc>>,

    /// Timestamp of the product-analytics opt-in.
    #[serde(default)]
    pub analytics_consented_at: Option<DateTime<Utc>>,

    /// IP address from which consent was recorded (evidence for regulators).
    /// Stored encrypted at rest.
    pub consent_ip: Option<String>,

    /// Version of the terms / privacy policy the user consented to,
    /// e.g. `"TOS-v3.2"`. Used to detect stale consent after policy updates.
    pub last_consent_version: Option<String>,

    /// Art. 17: Right to Erasure — timestamp of the deletion request.
    pub deletion_requested_at: Option<DateTime<Utc>>,

    /// Computed deadline after which anonymisation must have completed:
    /// `deletion_requested_at + retention_period`.
    pub deletion_scheduled_at: Option<DateTime<Utc>>,

    /// Timestamp at which PII scrubbing was completed. Once set, the record
    /// is considered permanently anonymised and the status transitions to
    /// `Deleted`.
    pub anonymized_at: Option<DateTime<Utc>>,

    /// Art. 20: Right to Data Portability — timestamp of the export request.
    pub data_export_requested_at: Option<DateTime<Utc>>,

    /// Timestamp at which the data export was delivered to the account holder.
    pub data_export_completed_at: Option<DateTime<Utc>>,

    /// The delivered export's download link (#653) — a credential, shown to
    /// the holder only — and when it stops working.
    pub data_export_url: Option<String>,
    pub data_export_expires_at: Option<DateTime<Utc>>,
}

impl GdprRecord {
    /// Reconstructs a GDPR record from persistence (no events emitted).
    #[allow(clippy::too_many_arguments)]
    pub fn reconstitute(
        data_processing_consented_at: Option<DateTime<Utc>>,
        marketing_consented_at: Option<DateTime<Utc>>,
        consent_ip: Option<String>,
        last_consent_version: Option<String>,
        deletion_requested_at: Option<DateTime<Utc>>,
        deletion_scheduled_at: Option<DateTime<Utc>>,
        anonymized_at: Option<DateTime<Utc>>,
        data_export_requested_at: Option<DateTime<Utc>>,
        data_export_completed_at: Option<DateTime<Utc>>,
    ) -> Self {
        Self {
            data_processing_consented_at,
            marketing_consented_at,
            analytics_consented_at: None,
            consent_ip,
            last_consent_version,
            deletion_requested_at,
            deletion_scheduled_at,
            anonymized_at,
            data_export_requested_at,
            data_export_completed_at,
            data_export_url: None,
            data_export_expires_at: None,
        }
    }

    /// Restores the delivered export's link from persistence (#653).
    pub fn with_data_export_delivery(mut self, url: Option<String>, expires_at: Option<DateTime<Utc>>) -> Self {
        self.data_export_url = url;
        self.data_export_expires_at = expires_at;
        self
    }

    pub fn data_export_url(&self) -> Option<&str> { self.data_export_url.as_deref() }
    pub fn data_export_expires_at(&self) -> Option<DateTime<Utc>> { self.data_export_expires_at }

    /// An export was asked for and not delivered since (#653).
    pub fn has_pending_export(&self) -> bool {
        match (self.data_export_requested_at, self.data_export_completed_at) {
            (Some(requested), Some(completed)) => requested > completed,
            (Some(_), None) => true,
            _ => false,
        }
    }

    pub fn data_processing_consented_at(&self) -> Option<DateTime<Utc>> { self.data_processing_consented_at }
    pub fn marketing_consented_at(&self) -> Option<DateTime<Utc>> { self.marketing_consented_at }
    pub fn analytics_consented_at(&self) -> Option<DateTime<Utc>> { self.analytics_consented_at }

    /// Restores the analytics consent from persistence (added after
    /// [`Self::reconstitute`]'s column set).
    pub fn with_analytics_consented_at(mut self, at: Option<DateTime<Utc>>) -> Self {
        self.analytics_consented_at = at;
        self
    }

    fn consent_slot(&mut self, purpose: ConsentPurpose) -> &mut Option<DateTime<Utc>> {
        match purpose {
            ConsentPurpose::DataProcessing => &mut self.data_processing_consented_at,
            ConsentPurpose::Marketing => &mut self.marketing_consented_at,
            ConsentPurpose::Analytics => &mut self.analytics_consented_at,
        }
    }

    /// Gives (`at` = when) or withdraws a consent. Returns `true` when that
    /// changed anything: re-giving a given consent keeps its original time.
    pub fn set_consent(&mut self, purpose: ConsentPurpose, granted: bool, at: DateTime<Utc>) -> bool {
        let slot = self.consent_slot(purpose);
        match (granted, slot.is_some()) {
            (true, false) => {
                *slot = Some(at);
                true
            }
            (false, true) => {
                *slot = None;
                true
            }
            _ => false,
        }
    }
    pub fn consent_ip_address(&self) -> Option<&str> { self.consent_ip.as_deref() }
    pub fn last_consent_version(&self) -> Option<&str> { self.last_consent_version.as_deref() }
    pub fn deletion_requested_at(&self) -> Option<DateTime<Utc>> { self.deletion_requested_at }
    pub fn deletion_scheduled_at(&self) -> Option<DateTime<Utc>> { self.deletion_scheduled_at }
    pub fn anonymized_at(&self) -> Option<DateTime<Utc>> { self.anonymized_at }
    pub fn data_export_requested_at(&self) -> Option<DateTime<Utc>> { self.data_export_requested_at }
    pub fn data_export_completed_at(&self) -> Option<DateTime<Utc>> { self.data_export_completed_at }

    /// Returns `true` once the PII scrub has completed.
    pub fn is_anonymized(&self) -> bool {
        self.anonymized_at.is_some()
    }

    /// Returns `true` if a deletion has been requested but not yet completed.
    pub fn has_pending_deletion(&self) -> bool {
        self.deletion_requested_at.is_some() && self.anonymized_at.is_none()
    }

    /// Records an erasure request and schedules the anonymisation deadline.
    ///
    /// `retention_days` is the legal retention period that must elapse before
    /// actual anonymisation may occur (typically 30 days for dispute resolution
    /// or 0 days for an immediate request without an outstanding dispute).
    pub fn request_deletion(&mut self, retention_days: u32) {
        let now = Utc::now();
        self.deletion_requested_at = Some(now);
        self.deletion_scheduled_at = Some(now + Duration::days(i64::from(retention_days)));
    }
}
