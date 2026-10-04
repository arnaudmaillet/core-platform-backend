use chrono::{DateTime, TimeZone, Utc};
use scylla::DeserializeRow;
use scylla::value::CqlTimestamp;
use serde_json;
use uuid::Uuid;

use crate::domain::aggregate::Profile;
use crate::domain::entity::ProfileLink;
use crate::domain::value_object::{
    AccountId, AvatarUrl, BannerUrl, Bio, DisplayName, Handle, InteractionSettings, Locale,
    DiscoverySettings, LocationSettings, MaskingReason, ProfileId,
    ProfileKind, ProfileStatus, ProfileVisibility, VerificationKind, WebsiteUrl,
};
use crate::error::ProfileError;

/// Flat ScyllaDB projection for `profile.profiles`.
///
/// All timestamps are stored as `bigint` (milliseconds since Unix epoch) and
/// lifted to `CqlTimestamp` for type-safe deserialization. JSON blobs for
/// `custom_links` are decoded from `text` columns.
#[derive(Debug, DeserializeRow)]
pub struct ProfileRow {
    pub profile_id:        Uuid,
    pub account_id:        Uuid,
    pub version:           i64,
    pub handle:            String,
    pub display_name:      String,
    pub bio:               Option<String>,
    pub avatar_url:        Option<String>,
    pub banner_url:        Option<String>,
    pub website_url:       Option<String>,
    pub custom_links: Option<String>,
    pub profile_kind:      String,
    pub visibility:        String,
    pub verified:          bool,
    pub verification_kind: Option<String>,
    /// `tinyint`; NULL on rows predating the tier column → treated as Standard.
    pub tier:              Option<i8>,
    pub locale:            String,
    pub timezone:          Option<String>,
    pub status:            String,
    pub suspension_reason: Option<String>,
    pub masked_at:         Option<CqlTimestamp>,
    pub masking_reason:    Option<String>,
    pub created_at:        CqlTimestamp,
    pub updated_at:        CqlTimestamp,
    pub deleted_at:        Option<CqlTimestamp>,
    /// JSON; NULL on rows predating the column → the defaults.
    pub interaction_settings: Option<String>,
    /// JSON; NULL on rows predating the column → the defaults.
    pub location_settings: Option<String>,
    /// JSON; NULL on rows predating the column → the defaults.
    pub discovery_settings: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
struct LinkJson {
    label: String,
    url:   String,
}

fn cql_ts_to_dt(ts: CqlTimestamp) -> Result<DateTime<Utc>, ProfileError> {
    Utc.timestamp_millis_opt(ts.0)
        .single()
        .ok_or_else(|| ProfileError::DomainViolation {
            field: "timestamp".to_string(),
            message: format!("invalid CQL timestamp: {}", ts.0),
        })
}

fn opt_cql_ts_to_dt(ts: Option<CqlTimestamp>) -> Result<Option<DateTime<Utc>>, ProfileError> {
    ts.map(cql_ts_to_dt).transpose()
}

impl TryFrom<ProfileRow> for Profile {
    type Error = ProfileError;

    fn try_from(row: ProfileRow) -> Result<Self, Self::Error> {
        let id = ProfileId::from_uuid(row.profile_id);
        let account_id = AccountId::from_uuid(row.account_id);

        let handle = Handle::new(&row.handle)?;
        let display_name = DisplayName::new(&row.display_name)?;
        let bio = row.bio.as_deref().map(Bio::new).transpose()?;
        let avatar_url = row.avatar_url.as_deref().map(AvatarUrl::new).transpose()?;
        let banner_url = row.banner_url.as_deref().map(BannerUrl::new).transpose()?;
        let website_url = row.website_url.as_deref().map(WebsiteUrl::new).transpose()?;

        let custom_links: Vec<ProfileLink> = match &row.custom_links {
            Some(json) if !json.is_empty() && json != "[]" => {
                let links: Vec<LinkJson> = serde_json::from_str(json).map_err(|e| {
                    ProfileError::DomainViolation {
                        field:   "custom_links".to_string(),
                        message: format!("JSON decode error: {e}"),
                    }
                })?;
                links
                    .into_iter()
                    .map(|l| {
                        let url = WebsiteUrl::new(&l.url)?;
                        ProfileLink::new(l.label, url)
                    })
                    .collect::<Result<Vec<_>, _>>()?
            }
            _ => Vec::new(),
        };

        let profile_kind = ProfileKind::try_from(row.profile_kind.as_str())?;
        let visibility = ProfileVisibility::try_from(row.visibility.as_str())?;
        let verification_kind = row.verification_kind
            .as_deref()
            .map(VerificationKind::try_from)
            .transpose()?;
        let status = ProfileStatus::try_from(row.status.as_str())?;
        let masking_reason = row.masking_reason
            .as_deref()
            .map(MaskingReason::try_from)
            .transpose()?;
        let locale = Locale::new(&row.locale)?;

        let created_at = cql_ts_to_dt(row.created_at)?;
        let updated_at = cql_ts_to_dt(row.updated_at)?;
        let masked_at = opt_cql_ts_to_dt(row.masked_at)?;
        let deleted_at = opt_cql_ts_to_dt(row.deleted_at)?;

        Ok(Profile::reconstitute(
            id,
            account_id,
            row.version,
            handle,
            display_name,
            bio,
            avatar_url,
            banner_url,
            website_url,
            custom_links,
            profile_kind,
            visibility,
            row.verified,
            verification_kind,
            row.tier.unwrap_or(0).clamp(0, 2) as u8,
            locale,
            row.timezone,
            status,
            row.suspension_reason,
            masked_at,
            masking_reason,
            created_at,
            updated_at,
            deleted_at,
        ).with_interaction(InteractionSettings::from_json(row.interaction_settings.as_deref()))
        .with_location(LocationSettings::from_json(row.location_settings.as_deref()))
        .with_discovery(DiscoverySettings::from_json(row.discovery_settings.as_deref())))
    }
}
