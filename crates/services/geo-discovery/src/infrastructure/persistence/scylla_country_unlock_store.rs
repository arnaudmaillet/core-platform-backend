use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use scylla::observability::history::HistoryListener;
use scylla::statement::unprepared::Statement;
use scylla::value::CqlTimestamp;
use scylla::DeserializeRow;
use scylla_storage::{ProfileKind as ScyllaProfileKind, ScyllaClient, ScyllaStorageError};
use uuid::Uuid;

use crate::application::port::{AccountCountries, CountryUnlockStore};
use crate::domain::value_object::CountryCode;
use crate::error::GeoDiscoveryError;

/// Scylla adapter for [`CountryUnlockStore`] (`geo_discovery.country_unlocks`,
/// migration 0009): one partition per account, the home country a static
/// column set once (LWT), one row per unlocked country.
pub struct ScyllaCountryUnlockStore {
    client: Arc<ScyllaClient>,
}

impl ScyllaCountryUnlockStore {
    pub fn new(client: Arc<ScyllaClient>) -> Self {
        Self { client }
    }

    fn stmt(&self, cql: &str, kind: ScyllaProfileKind, label: &str) -> Statement {
        let mut s = Statement::new(cql);
        s.set_execution_profile_handle(Some(
            self.client.profiles.get(kind).clone().into_handle_with_label(label.to_string()),
        ));
        s.set_history_listener(Arc::clone(&self.client.history_listener) as Arc<dyn HistoryListener>);
        s
    }
}

fn scylla_err(e: scylla::errors::ExecutionError) -> GeoDiscoveryError {
    GeoDiscoveryError::Scylla(ScyllaStorageError::from(e))
}

fn row_err(ctx: &'static str, e: impl ToString) -> GeoDiscoveryError {
    GeoDiscoveryError::DomainViolation { field: ctx.to_owned(), message: e.to_string() }
}

#[async_trait]
impl CountryUnlockStore for ScyllaCountryUnlockStore {
    async fn get(&self, account: Uuid) -> Result<AccountCountries, GeoDiscoveryError> {
        #[derive(DeserializeRow)]
        struct Row {
            home_country: Option<String>,
            country:      Option<String>,
        }
        // Strict: an unlock just bought shows on the very next read.
        let stmt = self.stmt(
            "SELECT home_country, country FROM geo_discovery.country_unlocks WHERE account_id = ?",
            ScyllaProfileKind::Strict,
            "strict",
        );
        let rows = self
            .client
            .session
            .execute_unpaged(stmt, (account,))
            .await
            .map_err(scylla_err)?
            .into_rows_result()
            .map_err(|e| row_err("country_unlocks:rows", e))?;
        let mut countries = AccountCountries::default();
        for row in rows.rows::<Row>().map_err(|e| row_err("country_unlocks:iter", e))? {
            let row = row.map_err(|e| row_err("country_unlocks:deser", e))?;
            // An unreadable code unlocks nothing.
            if let Some(home) = row.home_country.as_deref().and_then(|c| CountryCode::try_from(c).ok()) {
                countries.home = Some(home);
            }
            if let Some(country) = row.country.as_deref().and_then(|c| CountryCode::try_from(c).ok()) {
                countries.unlocked.push(country);
            }
        }
        Ok(countries)
    }

    async fn set_home_once(&self, account: Uuid, country: CountryCode) -> Result<CountryCode, GeoDiscoveryError> {
        let stmt = self.stmt(
            "UPDATE geo_discovery.country_unlocks SET home_country = ? WHERE account_id = ? IF home_country = null",
            ScyllaProfileKind::Strict,
            "strict",
        );
        self.client.session.execute_unpaged(stmt, (country.as_str(), account)).await.map_err(scylla_err)?;
        // Applied or not, the stored one is the home country.
        Ok(self.get(account).await?.home.unwrap_or(country))
    }

    async fn add(&self, account: Uuid, country: CountryCode, price: i64, at: DateTime<Utc>) -> Result<(), GeoDiscoveryError> {
        let stmt = self.stmt(
            "INSERT INTO geo_discovery.country_unlocks (account_id, country, price, unlocked_at) VALUES (?, ?, ?, ?)",
            ScyllaProfileKind::Strict,
            "strict",
        );
        self.client
            .session
            .execute_unpaged(stmt, (account, country.as_str(), price, CqlTimestamp(at.timestamp_millis())))
            .await
            .map_err(scylla_err)?;
        Ok(())
    }
}
