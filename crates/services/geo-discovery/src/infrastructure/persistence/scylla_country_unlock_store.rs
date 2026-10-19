use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use scylla::observability::history::HistoryListener;
use scylla::statement::unprepared::Statement;
use scylla::value::CqlTimestamp;
use scylla::DeserializeRow;
use scylla_storage::{ProfileKind as ScyllaProfileKind, ScyllaClient, ScyllaStorageError};
use uuid::Uuid;

use crate::application::port::{AccountCountries, CountryUnlockStore, VisaPurchase};
use crate::domain::value_object::CountryCode;
use crate::error::GeoDiscoveryError;

/// How long a pending purchase's agreed price is kept.
const PENDING_TTL_SECS: i32 = 24 * 3600;

/// Scylla adapter for [`CountryUnlockStore`] (`geo_discovery.country_unlocks`,
/// migration 0009): one partition per account, the home country a static
/// column set once (LWT), one row per unlocked (or pending) country; a 24 h
/// visa is the row's `visa_until` (#910, migration 0011), its purchases in
/// `country_visa_purchases`.
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
            price:        Option<i64>,
            pending:      Option<bool>,
            visa_until:   Option<CqlTimestamp>,
        }
        // Strict: an unlock just bought shows on the very next read.
        let stmt = self.stmt(
            "SELECT home_country, country, price, pending, visa_until FROM geo_discovery.country_unlocks \
             WHERE account_id = ?",
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
                // `add` writes `pending = false`; a row with neither is a visa only.
                match row.pending {
                    Some(true) => countries.pending.push((country, row.price.unwrap_or(0))),
                    Some(false) => countries.unlocked.push(country),
                    None => {}
                }
                if let Some(until) = row.visa_until.and_then(|t| DateTime::from_timestamp_millis(t.0)) {
                    countries.visas.push((country, until));
                }
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

    async fn mark_pending(&self, account: Uuid, country: CountryCode, price: i64) -> Result<(), GeoDiscoveryError> {
        let stmt = self.stmt(
            "INSERT INTO geo_discovery.country_unlocks (account_id, country, price, pending) VALUES (?, ?, ?, true) \
             USING TTL ?",
            ScyllaProfileKind::Strict,
            "strict",
        );
        self.client
            .session
            .execute_unpaged(stmt, (account, country.as_str(), price, PENDING_TTL_SECS))
            .await
            .map_err(scylla_err)?;
        Ok(())
    }

    async fn clear_pending(&self, account: Uuid, country: CountryCode) -> Result<(), GeoDiscoveryError> {
        // Only a pending purchase: an unlock is never undone, and a running
        // visa on the row stays (#910).
        let stmt = self.stmt(
            "DELETE price, pending FROM geo_discovery.country_unlocks WHERE account_id = ? AND country = ? \
             IF pending = true",
            ScyllaProfileKind::Strict,
            "strict",
        );
        self.client.session.execute_unpaged(stmt, (account, country.as_str())).await.map_err(scylla_err)?;
        Ok(())
    }

    async fn add(&self, account: Uuid, country: CountryCode, price: i64, at: DateTime<Utc>) -> Result<(), GeoDiscoveryError> {
        // Every column rewritten without a TTL: a pending row becomes durable;
        // a running visa to it ends (#910).
        let stmt = self.stmt(
            "INSERT INTO geo_discovery.country_unlocks (account_id, country, price, unlocked_at, pending, visa_until) \
             VALUES (?, ?, ?, ?, false, null)",
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

    async fn visa_purchase(&self, account: Uuid, key: &str) -> Result<Option<VisaPurchase>, GeoDiscoveryError> {
        #[derive(DeserializeRow)]
        struct Row {
            country: Option<String>,
            price:   Option<i64>,
            until:   Option<CqlTimestamp>,
            settled: Option<bool>,
        }
        let stmt = self.stmt(
            "SELECT country, price, until, settled FROM geo_discovery.country_visa_purchases \
             WHERE account_id = ? AND purchase_key = ?",
            ScyllaProfileKind::Strict,
            "strict",
        );
        let rows = self
            .client
            .session
            .execute_unpaged(stmt, (account, key))
            .await
            .map_err(scylla_err)?
            .into_rows_result()
            .map_err(|e| row_err("country_visa_purchases:rows", e))?;
        let Some(row) = rows.maybe_first_row::<Row>().map_err(|e| row_err("country_visa_purchases:deser", e))? else {
            return Ok(None);
        };
        // An unreadable row is no purchase: the retry agrees one anew.
        let purchase = (|| {
            Some(VisaPurchase {
                country: CountryCode::try_from(row.country.as_deref()?).ok()?,
                price:   row.price?,
                until:   DateTime::from_timestamp_millis(row.until?.0)?,
                settled: row.settled.unwrap_or(false),
            })
        })();
        Ok(purchase)
    }

    async fn record_visa_purchase(&self, account: Uuid, key: &str, purchase: VisaPurchase) -> Result<(), GeoDiscoveryError> {
        let stmt = self.stmt(
            "INSERT INTO geo_discovery.country_visa_purchases (account_id, purchase_key, country, price, until, settled) \
             VALUES (?, ?, ?, ?, ?, ?)",
            ScyllaProfileKind::Strict,
            "strict",
        );
        let values = (
            account,
            key,
            purchase.country.as_str(),
            purchase.price,
            CqlTimestamp(purchase.until.timestamp_millis()),
            purchase.settled,
        );
        self.client.session.execute_unpaged(stmt, values).await.map_err(scylla_err)?;
        Ok(())
    }

    async fn clear_visa_purchase(&self, account: Uuid, key: &str) -> Result<(), GeoDiscoveryError> {
        let stmt = self.stmt(
            "DELETE FROM geo_discovery.country_visa_purchases WHERE account_id = ? AND purchase_key = ?",
            ScyllaProfileKind::Strict,
            "strict",
        );
        self.client.session.execute_unpaged(stmt, (account, key)).await.map_err(scylla_err)?;
        Ok(())
    }

    async fn grant_visa(&self, account: Uuid, country: CountryCode, until: DateTime<Utc>) -> Result<(), GeoDiscoveryError> {
        // A later end already recorded (another purchase) stays.
        if self.get(account).await?.visas.iter().any(|(c, end)| *c == country && *end >= until) {
            return Ok(());
        }
        let ttl = i32::try_from((until - Utc::now()).num_seconds().max(1)).unwrap_or(i32::MAX);
        let stmt = self.stmt(
            "UPDATE geo_discovery.country_unlocks USING TTL ? SET visa_until = ? WHERE account_id = ? AND country = ?",
            ScyllaProfileKind::Strict,
            "strict",
        );
        self.client
            .session
            .execute_unpaged(stmt, (ttl, CqlTimestamp(until.timestamp_millis()), account, country.as_str()))
            .await
            .map_err(scylla_err)?;
        Ok(())
    }
}
