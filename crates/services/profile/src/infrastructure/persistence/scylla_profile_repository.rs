use std::sync::Arc;

use async_trait::async_trait;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use chrono::{TimeZone, Utc};
use scylla::observability::history::HistoryListener;
use scylla::statement::unprepared::Statement;
use scylla::value::CqlTimestamp;
use scylla::{DeserializeRow, SerializeRow};
use uuid::Uuid;

use scylla_storage::{ProfileKind as ScyllaProfileKind, ScyllaClient, ScyllaStorageError};

use crate::application::port::{ProfileRepository, ProfileSummary};
use crate::domain::aggregate::Profile;
use crate::domain::entity::ProfileLink;
use crate::domain::value_object::{AccountId, Handle, ProfileId};
use crate::error::ProfileError;
use crate::infrastructure::persistence::model::ProfileRow;

// ── Page-token type ───────────────────────────────────────────────────────────

#[derive(serde::Serialize, serde::Deserialize)]
struct PageToken {
    created_at_ms: i64,
}

// ── Serialization structs for large INSERT/UPDATE ─────────────────────────────

/// Values for the 23-column INSERT into `profile.profiles`.
///
/// Uses `enforce_order` so the fields serialize in declaration order,
/// matching the positional `?` placeholders in the CQL.
#[derive(SerializeRow)]
#[scylla(flavor = "enforce_order")]
struct ProfileInsert {
    profile_id:        Uuid,
    account_id:        Uuid,
    version:           i64,
    handle:            String,
    display_name:      String,
    bio:               Option<String>,
    avatar_url:        Option<String>,
    banner_url:        Option<String>,
    website_url:       Option<String>,
    custom_links:      String,
    profile_kind:      String,
    visibility:        String,
    verified:          bool,
    verification_kind: Option<String>,
    locale:            String,
    timezone:          Option<String>,
    status:            String,
    suspension_reason: Option<String>,
    masked_at:         Option<CqlTimestamp>,
    masking_reason:    Option<String>,
    created_at:        CqlTimestamp,
    updated_at:        CqlTimestamp,
    deleted_at:        Option<CqlTimestamp>,
    tier:              i8,
    interaction_settings: String,
}

/// Values for the 21-column LWT UPDATE of `profile.profiles`.
///
/// Field order matches the SET clause then WHERE + IF clause.
#[derive(SerializeRow)]
// skip_name_checks is REQUIRED with enforce_order here: binding is positional
// but the driver still name-checks by default, and these field names diverge
// from the bind markers on purpose (`version` appears twice in the statement —
// SET's new value vs IF's expected value — so the fields are new_version /
// expected_version). Same class as the soak's social-graph FollowRow finding.
#[scylla(flavor = "enforce_order", skip_name_checks)]
struct ProfileUpdate {
    handle:            String,
    display_name:      String,
    bio:               Option<String>,
    avatar_url:        Option<String>,
    banner_url:        Option<String>,
    website_url:       Option<String>,
    custom_links:      String,
    visibility:        String,
    verified:          bool,
    verification_kind: Option<String>,
    locale:            String,
    timezone:          Option<String>,
    status:            String,
    suspension_reason: Option<String>,
    masked_at:         Option<CqlTimestamp>,
    masking_reason:    Option<String>,
    updated_at:        CqlTimestamp,
    deleted_at:        Option<CqlTimestamp>,
    tier:              i8,
    interaction_settings: String,
    new_version:       i64,
    profile_id:        Uuid,
    expected_version:  i64,
}

// ── Error helpers ─────────────────────────────────────────────────────────────


/// Extracts the `[applied]` flag from an LWT result. The row's SHAPE varies:
/// bare `(bool)` when applied, `[applied]` + the existing row's columns on
/// conflict — and Scylla 5.4 returns the full column list even on success.
/// Strict tuple typing (`(bool,)`) type-checks the WHOLE row and fails on
/// column count (found by the suites' first CI run; prod runs the same 5.4 —
/// profile creation had never been exercised live). Read column 0 untyped.
fn lwt_applied(
    rows: scylla::response::query_result::QueryRowsResult,
    ctx: &'static str,
) -> Result<bool, ProfileError> {
    let row = rows
        .maybe_first_row::<scylla::value::Row>()
        .map_err(|e| row_err(ctx, e))?;
    Ok(matches!(
        row.and_then(|r| r.columns.into_iter().next().flatten()),
        Some(scylla::value::CqlValue::Boolean(true))
    ))
}

fn scylla_err(e: scylla::errors::ExecutionError) -> ProfileError {
    ProfileError::Storage(ScyllaStorageError::from(e))
}

fn row_err(ctx: &'static str, e: impl ToString) -> ProfileError {
    ProfileError::DomainViolation {
        field: ctx.to_owned(),
        message: e.to_string(),
    }
}

// ── Repository ────────────────────────────────────────────────────────────────

pub struct ScyllaProfileRepository {
    client: Arc<ScyllaClient>,
}

impl ScyllaProfileRepository {
    pub fn new(client: Arc<ScyllaClient>) -> Self {
        Self { client }
    }

    fn fast_stmt(&self, cql: &str) -> Statement {
        let mut s = Statement::new(cql);
        s.set_execution_profile_handle(Some(
            self.client.profiles.get(ScyllaProfileKind::Fast)
                .clone()
                .into_handle_with_label("fast".to_string()),
        ));
        s.set_history_listener(
            Arc::clone(&self.client.history_listener) as Arc<dyn HistoryListener>,
        );
        s
    }

    fn strict_stmt(&self, cql: &str) -> Statement {
        let mut s = Statement::new(cql);
        s.set_execution_profile_handle(Some(
            self.client.profiles.get(ScyllaProfileKind::Strict)
                .clone()
                .into_handle_with_label("strict".to_string()),
        ));
        s.set_history_listener(
            Arc::clone(&self.client.history_listener) as Arc<dyn HistoryListener>,
        );
        s
    }

    fn dt_ms(dt: chrono::DateTime<Utc>) -> CqlTimestamp {
        CqlTimestamp(dt.timestamp_millis())
    }

    fn links_to_json(links: &[ProfileLink]) -> String {
        #[derive(serde::Serialize)]
        struct J<'a> { label: &'a str, url: &'a str }
        let items: Vec<_> = links
            .iter()
            .map(|l| J { label: &l.label, url: l.url.as_str() })
            .collect();
        serde_json::to_string(&items).unwrap_or_else(|_| "[]".to_string())
    }
}

#[async_trait]
impl ProfileRepository for ScyllaProfileRepository {
    async fn save(&self, profile: &Profile) -> Result<(), ProfileError> {
        if profile.version() == 0 {
            let stmt = self.strict_stmt(
                "INSERT INTO profile.profiles \
                 (profile_id, account_id, version, handle, display_name, bio, avatar_url, \
                  banner_url, website_url, custom_links, profile_kind, visibility, verified, \
                  verification_kind, locale, timezone, status, suspension_reason, masked_at, \
                  masking_reason, created_at, updated_at, deleted_at, tier, interaction_settings) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            );
            let values = ProfileInsert {
                profile_id:        profile.id().as_uuid(),
                account_id:        profile.account_id().as_uuid(),
                version:           0i64,
                handle:            profile.handle().as_str().to_owned(),
                display_name:      profile.display_name().as_str().to_owned(),
                bio:               profile.bio().map(|b| b.as_str().to_owned()),
                avatar_url:        profile.avatar_url().map(|u| u.as_str().to_owned()),
                banner_url:        profile.banner_url().map(|u| u.as_str().to_owned()),
                website_url:       profile.website_url().map(|u| u.as_str().to_owned()),
                custom_links:      Self::links_to_json(profile.custom_links()),
                profile_kind:      profile.profile_kind().as_str().to_owned(),
                visibility:        profile.visibility().as_str().to_owned(),
                verified:          profile.verified(),
                verification_kind: profile.verification_kind().map(|v| v.as_str().to_owned()),
                locale:            profile.locale().as_str().to_owned(),
                timezone:          profile.timezone().map(str::to_owned),
                status:            profile.status().as_str().to_owned(),
                suspension_reason: profile.suspension_reason().map(str::to_owned),
                masked_at:         profile.masked_at().map(Self::dt_ms),
                masking_reason:    profile.masking_reason().map(|r| r.as_str().to_owned()),
                created_at:        Self::dt_ms(profile.created_at()),
                updated_at:        Self::dt_ms(profile.updated_at()),
                deleted_at:        profile.deleted_at().map(Self::dt_ms),
                tier:              profile.tier() as i8,
                interaction_settings: profile.interaction().to_json(),
            };
            self.client.session.execute_unpaged(stmt, values).await.map_err(scylla_err)?;
        } else {
            // LWT UPDATE: `version()` was already incremented by `touch()`.
            let stmt = self.strict_stmt(
                "UPDATE profile.profiles \
                 SET handle = ?, display_name = ?, bio = ?, avatar_url = ?, banner_url = ?, \
                     website_url = ?, custom_links = ?, visibility = ?, verified = ?, \
                     verification_kind = ?, locale = ?, timezone = ?, status = ?, \
                     suspension_reason = ?, masked_at = ?, masking_reason = ?, \
                     updated_at = ?, deleted_at = ?, tier = ?, interaction_settings = ?, version = ? \
                 WHERE profile_id = ? \
                 IF version = ?",
            );
            let values = ProfileUpdate {
                handle:            profile.handle().as_str().to_owned(),
                display_name:      profile.display_name().as_str().to_owned(),
                bio:               profile.bio().map(|b| b.as_str().to_owned()),
                avatar_url:        profile.avatar_url().map(|u| u.as_str().to_owned()),
                banner_url:        profile.banner_url().map(|u| u.as_str().to_owned()),
                website_url:       profile.website_url().map(|u| u.as_str().to_owned()),
                custom_links:      Self::links_to_json(profile.custom_links()),
                visibility:        profile.visibility().as_str().to_owned(),
                verified:          profile.verified(),
                verification_kind: profile.verification_kind().map(|v| v.as_str().to_owned()),
                locale:            profile.locale().as_str().to_owned(),
                timezone:          profile.timezone().map(str::to_owned),
                status:            profile.status().as_str().to_owned(),
                suspension_reason: profile.suspension_reason().map(str::to_owned),
                masked_at:         profile.masked_at().map(Self::dt_ms),
                masking_reason:    profile.masking_reason().map(|r| r.as_str().to_owned()),
                updated_at:        Self::dt_ms(profile.updated_at()),
                deleted_at:        profile.deleted_at().map(Self::dt_ms),
                tier:              profile.tier() as i8,
                interaction_settings: profile.interaction().to_json(),
                new_version:       profile.version(),
                profile_id:        profile.id().as_uuid(),
                expected_version:  profile.version() - 1,
            };
            let result = self.client.session
                .execute_unpaged(stmt, values).await.map_err(scylla_err)?;
            let applied = lwt_applied(
                result.into_rows_result().map_err(|e| row_err("save_ltw_rows", e))?,
                "save_ltw_deser",
            )?;
            if !applied {
                return Err(ProfileError::ConcurrentModification);
            }
        }
        Ok(())
    }

    async fn find_by_id(&self, id: &ProfileId) -> Result<Option<Profile>, ProfileError> {
        let stmt = self.fast_stmt(
            "SELECT profile_id, account_id, version, handle, display_name, bio, avatar_url, \
                    banner_url, website_url, custom_links, profile_kind, visibility, verified, \
                    verification_kind, tier, locale, timezone, status, suspension_reason, masked_at, \
                    masking_reason, created_at, updated_at, deleted_at, interaction_settings \
             FROM profile.profiles WHERE profile_id = ?",
        );
        let result = self.client.session
            .execute_unpaged(stmt, (id.as_uuid(),))
            .await.map_err(scylla_err)?;

        let row = result
            .into_rows_result().map_err(|e| row_err("find_by_id_rows", e))?
            .maybe_first_row::<ProfileRow>().map_err(|e| row_err("find_by_id_deser", e))?;

        row.map(Profile::try_from).transpose()
    }

    async fn find_by_handle(&self, handle: &Handle) -> Result<Option<Profile>, ProfileError> {
        #[derive(DeserializeRow)]
        struct HandleLookup {
            profile_id:    Uuid,
            tombstoned_at: Option<CqlTimestamp>,
        }

        let stmt = self.fast_stmt(
            "SELECT profile_id, tombstoned_at FROM profile.profile_handles WHERE handle = ?",
        );
        let result = self.client.session
            .execute_unpaged(stmt, (handle.as_str().to_owned(),))
            .await.map_err(scylla_err)?;

        let row = result
            .into_rows_result().map_err(|e| row_err("find_handle_rows", e))?
            .maybe_first_row::<HandleLookup>().map_err(|e| row_err("find_handle_deser", e))?;

        let profile_id = match row {
            None => return Ok(None),
            Some(r) if r.tombstoned_at.is_some() => return Ok(None),
            Some(r) => ProfileId::from_uuid(r.profile_id),
        };

        self.find_by_id(&profile_id).await
    }

    async fn list_by_account(
        &self,
        account_id: &AccountId,
        limit: i32,
        page_token: Option<&str>,
    ) -> Result<(Vec<ProfileSummary>, Option<String>), ProfileError> {
        #[derive(DeserializeRow)]
        struct IndexRow {
            profile_id:   Uuid,
            handle:       String,
            display_name: String,
            avatar_url:   Option<String>,
            profile_kind: String,
            visibility:   String,
            status:       String,
            created_at:   CqlTimestamp,
        }

        // CQL `LIMIT` is a native `int` (i32); binding an i64 makes the driver
        // serialize it as BigInt and the query fails type-checking. Keep it i32.
        let limit = limit.clamp(1, 100);

        let token = page_token
            .map(|t| -> Result<PageToken, ProfileError> {
                let bytes = URL_SAFE_NO_PAD.decode(t).map_err(|_| ProfileError::DomainViolation {
                    field: "page_token".to_string(),
                    message: "invalid page token encoding".to_string(),
                })?;
                serde_json::from_slice(&bytes).map_err(|_| ProfileError::DomainViolation {
                    field: "page_token".to_string(),
                    message: "invalid page token format".to_string(),
                })
            })
            .transpose()?;

        let rows_result = if let Some(ref tok) = token {
            let stmt = self.fast_stmt(
                "SELECT profile_id, handle, display_name, avatar_url, profile_kind, \
                        visibility, status, created_at \
                 FROM profile.profiles_by_account \
                 WHERE account_id = ? AND created_at < ? LIMIT ?",
            );
            self.client.session
                .execute_unpaged(stmt, (account_id.as_uuid(), CqlTimestamp(tok.created_at_ms), limit))
                .await.map_err(scylla_err)?
                .into_rows_result().map_err(|e| row_err("list_rows", e))?
        } else {
            let stmt = self.fast_stmt(
                "SELECT profile_id, handle, display_name, avatar_url, profile_kind, \
                        visibility, status, created_at \
                 FROM profile.profiles_by_account \
                 WHERE account_id = ? LIMIT ?",
            );
            self.client.session
                .execute_unpaged(stmt, (account_id.as_uuid(), limit))
                .await.map_err(scylla_err)?
                .into_rows_result().map_err(|e| row_err("list_rows", e))?
        };

        let rows: Vec<IndexRow> = rows_result
            .rows::<IndexRow>().map_err(|e| row_err("list_deser", e))?
            .collect::<Result<Vec<_>, _>>().map_err(|e| row_err("list_row", e))?;

        let total = rows.len();
        let mut summaries = Vec::with_capacity(total);
        let mut last_created_at_ms = 0i64;

        for row in &rows {
            last_created_at_ms = row.created_at.0;
            let created_at = Utc.timestamp_millis_opt(row.created_at.0)
                .single()
                .ok_or_else(|| ProfileError::DomainViolation {
                    field: "created_at".to_string(),
                    message: format!("invalid timestamp {}", row.created_at.0),
                })?;
            summaries.push(ProfileSummary {
                profile_id:   ProfileId::from_uuid(row.profile_id),
                handle:       row.handle.clone(),
                display_name: row.display_name.clone(),
                avatar_url:   row.avatar_url.clone(),
                profile_kind: row.profile_kind.clone(),
                visibility:   row.visibility.clone(),
                status:       row.status.clone(),
                created_at,
            });
        }

        let next_page_token = if total == limit as usize {
            let tok = PageToken { created_at_ms: last_created_at_ms };
            let json = serde_json::to_vec(&tok).unwrap_or_default();
            Some(URL_SAFE_NO_PAD.encode(json))
        } else {
            None
        };

        Ok((summaries, next_page_token))
    }

    async fn claim_handle(
        &self,
        handle: &Handle,
        profile_id: ProfileId,
        account_id: AccountId,
    ) -> Result<bool, ProfileError> {
        let now = Self::dt_ms(Utc::now());
        let stmt = self.strict_stmt(
            "INSERT INTO profile.profile_handles \
             (handle, profile_id, account_id, created_at) \
             VALUES (?, ?, ?, ?) IF NOT EXISTS",
        );
        let result = self.client.session
            .execute_unpaged(stmt, (handle.as_str().to_owned(), profile_id.as_uuid(), account_id.as_uuid(), now))
            .await.map_err(scylla_err)?;

        lwt_applied(
            result.into_rows_result().map_err(|e| row_err("claim_ltw_rows", e))?,
            "claim_ltw_deser",
        )
    }

    async fn tombstone_handle(&self, handle: &Handle) -> Result<(), ProfileError> {
        let now = Self::dt_ms(Utc::now());
        let stmt = self.strict_stmt(
            "UPDATE profile.profile_handles SET tombstoned_at = ? WHERE handle = ?",
        );
        self.client.session
            .execute_unpaged(stmt, (now, handle.as_str().to_owned()))
            .await.map_err(scylla_err)?;
        Ok(())
    }

    async fn handle_is_available(&self, handle: &Handle) -> Result<bool, ProfileError> {
        #[derive(DeserializeRow)]
        struct TombRow { tombstoned_at: Option<CqlTimestamp> }

        let stmt = self.fast_stmt(
            "SELECT tombstoned_at FROM profile.profile_handles WHERE handle = ?",
        );
        let result = self.client.session
            .execute_unpaged(stmt, (handle.as_str().to_owned(),))
            .await.map_err(scylla_err)?;

        let row = result
            .into_rows_result().map_err(|e| row_err("handle_avail_rows", e))?
            .maybe_first_row::<TombRow>().map_err(|e| row_err("handle_avail_deser", e))?;

        match row {
            None => Ok(true),
            Some(TombRow { tombstoned_at: None }) => Ok(false),
            Some(TombRow { tombstoned_at: Some(ts) }) => {
                let days_elapsed = (Utc::now().timestamp_millis() - ts.0) / 86_400_000;
                Ok(days_elapsed >= 30)
            }
        }
    }

    async fn save_account_index(&self, profile: &Profile) -> Result<(), ProfileError> {
        let stmt = self.strict_stmt(
            "INSERT INTO profile.profiles_by_account \
             (account_id, created_at, profile_id, handle, display_name, avatar_url, \
              profile_kind, visibility, status) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        );
        let values = (
            profile.account_id().as_uuid(),
            Self::dt_ms(profile.created_at()),
            profile.id().as_uuid(),
            profile.handle().as_str().to_owned(),
            profile.display_name().as_str().to_owned(),
            profile.avatar_url().map(|u| u.as_str().to_owned()),
            profile.profile_kind().as_str().to_owned(),
            profile.visibility().as_str().to_owned(),
            profile.status().as_str().to_owned(),
        );
        self.client.session.execute_unpaged(stmt, values).await.map_err(scylla_err)?;
        Ok(())
    }

    async fn delete_account_index(&self, profile: &Profile) -> Result<(), ProfileError> {
        let stmt = self.strict_stmt(
            "DELETE FROM profile.profiles_by_account \
             WHERE account_id = ? AND created_at = ? AND profile_id = ?",
        );
        let values = (
            profile.account_id().as_uuid(),
            Self::dt_ms(profile.created_at()),
            profile.id().as_uuid(),
        );
        self.client.session.execute_unpaged(stmt, values).await.map_err(scylla_err)?;
        Ok(())
    }
}
