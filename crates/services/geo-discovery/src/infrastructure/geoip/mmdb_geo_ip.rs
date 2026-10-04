//! A MaxMind DB file (DB-IP Lite country, or MaxMind GeoLite2-Country — same
//! format, same `country.iso_code` path), loaded once at startup.
//!
//! Without a file (none configured, or unreadable), every lookup answers
//! "unknown": nothing is verified, so nothing is granted — fail closed.
//!
//! A private, loopback or link-local address never reaches us from the ALB in
//! a deployed environment (it forwards the client's public address). The
//! local fleet, where every caller is on a Docker network, can map those to a
//! fixed country, or to `*` = whatever the device claims
//! (`GEO_GEOIP_PRIVATE_NETWORK_COUNTRY`): never set it in a deployed env.

use std::net::IpAddr;

use maxminddb::{geoip2, Reader};

use crate::application::port::GeoIp;
use crate::domain::value_object::CountryCode;

/// What a private-network address resolves to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrivateNetworkCountry {
    /// Unknown (the default): not verifiable.
    Unknown,
    /// Always this country.
    Fixed(CountryCode),
    /// Whatever the device claims (local development only).
    AsClaimed,
}

impl PrivateNetworkCountry {
    pub fn parse(value: Option<&str>) -> Self {
        match value.map(str::trim).filter(|v| !v.is_empty()) {
            None => Self::Unknown,
            Some("*") => Self::AsClaimed,
            Some(code) => CountryCode::try_from(code).map_or(Self::Unknown, Self::Fixed),
        }
    }
}

pub struct MmdbGeoIp {
    reader:  Option<Reader<Vec<u8>>>,
    private: PrivateNetworkCountry,
}

impl MmdbGeoIp {
    /// Loads `path` when set. A missing or unreadable file is logged and
    /// leaves the service without GeoIP (nothing is granted) rather than
    /// failing the boot: the map itself keeps working.
    pub fn load(path: Option<&str>, private: PrivateNetworkCountry) -> Self {
        let reader = path.filter(|p| !p.trim().is_empty()).and_then(|p| match Reader::open_readfile(p) {
            Ok(reader) => {
                tracing::info!(path = p, "GeoIP database loaded");
                Some(reader)
            }
            Err(e) => {
                tracing::error!(path = p, error = %e, "GeoIP database unreadable: country access grants nothing");
                None
            }
        });
        if reader.is_none() && path.is_none() {
            tracing::warn!("no GeoIP database configured: country access grants nothing");
        }
        Self { reader, private }
    }
}

/// Addresses that cannot be a client seen by the ALB.
pub fn is_private(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => v4.is_private() || v4.is_loopback() || v4.is_link_local() || v4.is_unspecified(),
        IpAddr::V6(v6) => {
            v6.is_loopback()
                || v6.is_unspecified()
                || (v6.segments()[0] & 0xfe00) == 0xfc00 // unique local
                || (v6.segments()[0] & 0xffc0) == 0xfe80 // link local
                || v6.to_ipv4_mapped().is_some_and(|v4| is_private(IpAddr::V4(v4)))
        }
    }
}

impl GeoIp for MmdbGeoIp {
    fn country_of(&self, ip: IpAddr, claimed: Option<CountryCode>) -> Option<CountryCode> {
        if is_private(ip) {
            return match self.private {
                PrivateNetworkCountry::Fixed(code) => Some(code),
                PrivateNetworkCountry::AsClaimed => claimed,
                PrivateNetworkCountry::Unknown => None,
            };
        }
        let reader = self.reader.as_ref()?;
        let record: geoip2::Country = reader.lookup(ip).ok()?.decode().ok()??;
        record.country.iso_code.and_then(|code| CountryCode::try_from(code).ok())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn private_addresses_never_hit_the_database() {
        let none = MmdbGeoIp::load(None, PrivateNetworkCountry::Unknown);
        assert_eq!(none.country_of("10.0.3.7".parse().unwrap(), None), None);
        assert_eq!(none.country_of("8.8.8.8".parse().unwrap(), None), None, "no database, no answer");

        let fr = CountryCode::try_from("FR").unwrap();
        let fixed = MmdbGeoIp::load(None, PrivateNetworkCountry::Fixed(fr));
        for ip in ["172.18.0.5", "192.168.1.2", "127.0.0.1", "::1", "fd00::1", "::ffff:10.0.0.1"] {
            assert_eq!(fixed.country_of(ip.parse().unwrap(), None), Some(fr), "{ip}");
        }
        assert_eq!(fixed.country_of("8.8.8.8".parse().unwrap(), Some(fr)), None);

        let us = CountryCode::try_from("US").unwrap();
        let claimed = MmdbGeoIp::load(None, PrivateNetworkCountry::AsClaimed);
        assert_eq!(claimed.country_of("172.18.0.5".parse().unwrap(), Some(us)), Some(us));
        assert_eq!(claimed.country_of("8.8.8.8".parse().unwrap(), Some(us)), None, "public addresses are never taken on trust");
    }

    #[test]
    fn the_private_network_setting_parses_leniently() {
        assert_eq!(PrivateNetworkCountry::parse(None), PrivateNetworkCountry::Unknown);
        assert_eq!(PrivateNetworkCountry::parse(Some(" ")), PrivateNetworkCountry::Unknown);
        assert_eq!(PrivateNetworkCountry::parse(Some("*")), PrivateNetworkCountry::AsClaimed);
        assert_eq!(
            PrivateNetworkCountry::parse(Some("fr")),
            PrivateNetworkCountry::Fixed(CountryCode::try_from("FR").unwrap())
        );
        assert_eq!(PrivateNetworkCountry::parse(Some("france")), PrivateNetworkCountry::Unknown);
    }
}
