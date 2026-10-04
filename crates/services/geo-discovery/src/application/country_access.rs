//! Country access from location: the device says which country it is in (a
//! code derived on the device, never a coordinate); the claim is granted only
//! when the request's network (GeoIP) is in that country or a neighbouring one.
//! The grant is what a guest's map is limited to.

use std::net::IpAddr;
use std::sync::Arc;

use crate::application::port::{CountryGrantStore, GeoIp};
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

/// The country a reader's map is limited to: `None` = no limit; `Some(None)` =
/// nothing (a guest with no granted country); `Some(Some(c))` = only `c`.
pub async fn country_limit(
    grants: &dyn CountryGrantStore,
    scope:  &MapScope,
) -> Result<Option<Option<CountryCode>>, GeoDiscoveryError> {
    match scope {
        MapScope::All => Ok(None),
        MapScope::Guest(principal) if principal.is_empty() => Ok(Some(None)),
        MapScope::Guest(principal) => Ok(Some(grants.get(principal).await?)),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use async_trait::async_trait;

    use super::*;

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
        assert_eq!(country_limit(grants.as_ref(), &MapScope::Guest("guest:1".into())).await.unwrap(), Some(Some(fr)));

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
        assert_eq!(country_limit(&grants, &MapScope::All).await.unwrap(), None);
        assert_eq!(country_limit(&grants, &MapScope::Guest(String::new())).await.unwrap(), Some(None));
        assert_eq!(country_limit(&grants, &MapScope::Guest("guest:9".into())).await.unwrap(), Some(None));
    }
}
