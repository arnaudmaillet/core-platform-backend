//! Country access from location: the device says which country it is in (a
//! code derived on the device, never a coordinate); the claim is granted only
//! when the request's network (GeoIP) is in that country or a neighbouring one.
//! The grant is what a guest's map is limited to.

use std::net::IpAddr;
use std::sync::Arc;

use crate::application::port::{CountryGrantStore, CountryUnlockStore, GeoIp};
use crate::domain::country_atlas::CountryAtlas;
use crate::domain::value_object::{decide, CountryAccessOutcome, CountryCode, MapScope};
use crate::error::GeoDiscoveryError;

pub struct ResolveCountryAccess {
    pub geo_ip: Arc<dyn GeoIp>,
    pub grants: Arc<dyn CountryGrantStore>,
    pub atlas:  &'static CountryAtlas,
}

impl ResolveCountryAccess {
    /// Records the outcome for `principal` (the token `sub`): a granted country
    /// replaces the previous grant; anything else clears it (location off, or a
    /// claim the network does not back: nothing opens from location).
    pub async fn handle(
        &self,
        principal: &str,
        claimed:   Option<&str>,
        client_ip: Option<IpAddr>,
    ) -> Result<CountryAccessOutcome, GeoDiscoveryError> {
        if principal.is_empty() {
            return Err(GeoDiscoveryError::CountryAccessNeedsCaller);
        }
        let claimed = match claimed.map(str::trim).filter(|c| !c.is_empty()) {
            Some(code) => {
                let code = CountryCode::try_from(code)?;
                if !self.atlas.knows(code) {
                    return Err(GeoDiscoveryError::InvalidCountryCode(code.to_string()));
                }
                Some(code)
            }
            None => None,
        };
        let network = client_ip.and_then(|ip| self.geo_ip.country_of(ip, claimed));
        let outcome = decide(claimed, network, |a, b| self.atlas.neighbours(a, b));
        match outcome {
            CountryAccessOutcome::Granted(country) => self.grants.set(principal, country).await?,
            CountryAccessOutcome::Mismatch => {
                tracing::info!(
                    claimed = %claimed.map(|c| c.to_string()).unwrap_or_default(),
                    network = %network.map(|c| c.to_string()).unwrap_or_default(),
                    "country claim not backed by the network"
                );
                self.grants.clear(principal).await?
            }
            CountryAccessOutcome::NotSent | CountryAccessOutcome::Unverifiable => self.grants.clear(principal).await?,
        }
        Ok(outcome)
    }
}

/// Which posts a reader's map shows, by where they were published.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CountryFilter {
    /// No country filter.
    Open,
    /// Nothing (a guest with no granted country).
    Nothing,
    /// Posts in these countries (a point near a border counts for both); at
    /// sea too when `open_sea` (a member's map).
    Only { countries: Vec<CountryCode>, open_sea: bool },
}

impl CountryFilter {
    /// Whether a post at `(lat, lng)` is on the map.
    pub fn admits(&self, atlas: &CountryAtlas, lat: f64, lng: f64) -> bool {
        match self {
            Self::Open => true,
            Self::Nothing => false,
            Self::Only { countries, open_sea } => {
                countries.iter().any(|c| atlas.contains(*c, lat, lng))
                    || (*open_sea && atlas.country_at(lat, lng).is_none())
            }
        }
    }
}

/// The country filter of a reader's map: a guest's granted country; a
/// member's home and unlocked countries (#665); nothing else.
pub async fn country_filter(
    grants:  &dyn CountryGrantStore,
    unlocks: &dyn CountryUnlockStore,
    scope:   &MapScope,
) -> Result<CountryFilter, GeoDiscoveryError> {
    Ok(match scope {
        MapScope::All => CountryFilter::Open,
        MapScope::Guest(principal) if principal.is_empty() => CountryFilter::Nothing,
        MapScope::Guest(principal) => match grants.get(principal).await? {
            Some(country) => CountryFilter::Only { countries: vec![country], open_sea: false },
            None => CountryFilter::Nothing,
        },
        MapScope::Member(account) => CountryFilter::Only { countries: unlocks.get(*account).await?.all(), open_sea: true },
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use async_trait::async_trait;

    use super::*;
    use crate::application::country_unlocks::fakes::MemUnlocks;

    #[derive(Default)]
    pub struct MemGrants(pub Mutex<HashMap<String, CountryCode>>);

    #[async_trait]
    impl CountryGrantStore for MemGrants {
        async fn get(&self, p: &str) -> Result<Option<CountryCode>, GeoDiscoveryError> {
            Ok(self.0.lock().unwrap().get(p).copied())
        }
        async fn set(&self, p: &str, c: CountryCode) -> Result<(), GeoDiscoveryError> {
            self.0.lock().unwrap().insert(p.to_owned(), c);
            Ok(())
        }
        async fn clear(&self, p: &str) -> Result<(), GeoDiscoveryError> {
            self.0.lock().unwrap().remove(p);
            Ok(())
        }
    }

    /// 203.0.113.x is France, 198.51.100.x the United States, anything else unknown.
    struct Ranges;

    impl GeoIp for Ranges {
        fn country_of(&self, ip: IpAddr, _: Option<CountryCode>) -> Option<CountryCode> {
            let s = ip.to_string();
            let code = if s.starts_with("203.0.113.") { "FR" } else if s.starts_with("198.51.100.") { "US" } else { return None };
            CountryCode::try_from(code).ok()
        }
    }

    fn service(grants: Arc<MemGrants>) -> ResolveCountryAccess {
        ResolveCountryAccess { geo_ip: Arc::new(Ranges), grants, atlas: CountryAtlas::embedded() }
    }

    fn ip(s: &str) -> Option<IpAddr> {
        Some(s.parse().unwrap())
    }

    #[tokio::test]
    async fn a_backed_claim_is_granted_and_replaces_the_previous_one() {
        let grants = Arc::new(MemGrants::default());
        let s = service(Arc::clone(&grants));
        let fr = CountryCode::try_from("FR").unwrap();
        let es = CountryCode::try_from("ES").unwrap();

        assert_eq!(s.handle("guest:1", Some("fr"), ip("203.0.113.9")).await.unwrap(), CountryAccessOutcome::Granted(fr));
        assert_eq!(
            country_filter(grants.as_ref(), &MemUnlocks::default(), &MapScope::Guest("guest:1".into())).await.unwrap(),
            CountryFilter::Only { countries: vec![fr], open_sea: false }
        );

        // A French network backs Spain (neighbours): the new country replaces France.
        assert_eq!(s.handle("guest:1", Some("ES"), ip("203.0.113.9")).await.unwrap(), CountryAccessOutcome::Granted(es));
        assert_eq!(grants.get("guest:1").await.unwrap(), Some(es));
    }

    #[tokio::test]
    async fn an_unbacked_unverifiable_or_absent_claim_closes_the_map() {
        let grants = Arc::new(MemGrants::default());
        let s = service(Arc::clone(&grants));
        let fr = CountryCode::try_from("FR").unwrap();
        for (claim, from, outcome) in [
            (Some("FR"), ip("198.51.100.4"), CountryAccessOutcome::Mismatch),
            (Some("FR"), ip("192.0.2.1"), CountryAccessOutcome::Unverifiable),
            (Some("FR"), None, CountryAccessOutcome::Unverifiable),
            (None, ip("203.0.113.9"), CountryAccessOutcome::NotSent),
        ] {
            grants.set("guest:2", fr).await.unwrap();
            assert_eq!(s.handle("guest:2", claim, from).await.unwrap(), outcome);
            assert_eq!(grants.get("guest:2").await.unwrap(), None, "{outcome:?} clears the grant");
        }
    }

    #[tokio::test]
    async fn bad_codes_and_callers_are_refused() {
        let s = service(Arc::new(MemGrants::default()));
        assert!(matches!(s.handle("guest:3", Some("FRA"), None).await, Err(GeoDiscoveryError::InvalidCountryCode(_))));
        assert!(matches!(s.handle("guest:3", Some("ZZ"), None).await, Err(GeoDiscoveryError::InvalidCountryCode(_))));
        assert!(matches!(s.handle("", Some("FR"), None).await, Err(GeoDiscoveryError::CountryAccessNeedsCaller)));
    }

    #[tokio::test]
    async fn members_and_the_mesh_are_not_limited_an_anonymous_guest_sees_nothing() {
        let grants = MemGrants::default();
        let unlocks = MemUnlocks::default();
        assert_eq!(country_filter(&grants, &unlocks, &MapScope::All).await.unwrap(), CountryFilter::Open);
        assert_eq!(country_filter(&grants, &unlocks, &MapScope::Guest(String::new())).await.unwrap(), CountryFilter::Nothing);
        assert_eq!(country_filter(&grants, &unlocks, &MapScope::Guest("guest:9".into())).await.unwrap(), CountryFilter::Nothing);
    }

    /// #665: a member's map shows its home and unlocked countries, and the
    /// sea; nothing else.
    #[tokio::test]
    async fn a_members_map_shows_its_countries_and_the_sea() {
        let unlocks = MemUnlocks::default();
        let me = uuid::Uuid::now_v7();
        let fr = CountryCode::try_from("FR").unwrap();
        let es = CountryCode::try_from("ES").unwrap();
        unlocks.set_home_once(me, fr).await.unwrap();
        unlocks.add(me, es, 15, chrono::Utc::now()).await.unwrap();
        let filter = country_filter(&MemGrants::default(), &unlocks, &MapScope::Member(me)).await.unwrap();
        let atlas = CountryAtlas::embedded();
        assert!(filter.admits(atlas, 48.8566, 2.3522), "Paris: home");
        assert!(filter.admits(atlas, 40.4168, -3.7038), "Madrid: unlocked");
        assert!(!filter.admits(atlas, 52.52, 13.405), "Berlin: locked");
        assert!(filter.admits(atlas, 45.0, -30.0), "mid-Atlantic: the sea");
    }
}
