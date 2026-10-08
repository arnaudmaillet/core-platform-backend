//! Country access: the ISO code, and the rule that decides whether the country
//! a device says it is in is granted.

use std::fmt;

use crate::error::GeoDiscoveryError;

/// An ISO 3166-1 alpha-2 country code, upper case.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CountryCode([u8; 2]);

impl CountryCode {
    pub fn as_str(&self) -> &str {
        // Two ASCII upper-case letters by construction.
        std::str::from_utf8(&self.0).unwrap_or("??")
    }
}

impl TryFrom<&str> for CountryCode {
    type Error = GeoDiscoveryError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let bytes = value.trim().as_bytes();
        match bytes {
            [a, b] if a.is_ascii_alphabetic() && b.is_ascii_alphabetic() => {
                Ok(Self([a.to_ascii_uppercase(), b.to_ascii_uppercase()]))
            }
            _ => Err(GeoDiscoveryError::InvalidCountryCode(value.to_owned())),
        }
    }
}

impl fmt::Display for CountryCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// What became of a device's claim to be in a country.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CountryAccessOutcome {
    /// The claim matches the request's network country (or a neighbour of it):
    /// the country is open.
    Granted(CountryCode),
    /// No claim (location off or unavailable): nothing is open from location.
    NotSent,
    /// The network says another, non-neighbouring country.
    Mismatch,
    /// The request's network country is unknown (no GeoIP data for the address,
    /// or no GeoIP database at all): nothing can be verified, nothing is granted.
    Unverifiable,
}

/// The grant rule. `claimed` is the country the device derived from its
/// location; `network` the request's GeoIP country; `neighbours` whether two
/// countries share a border. Granted when equal or neighbours (a border town,
/// a phone on a neighbouring network); never on an unknown network.
pub fn decide(
    claimed:    Option<CountryCode>,
    network:    Option<CountryCode>,
    neighbours: impl Fn(CountryCode, CountryCode) -> bool,
) -> CountryAccessOutcome {
    let Some(claimed) = claimed else {
        return CountryAccessOutcome::NotSent;
    };
    let Some(network) = network else {
        return CountryAccessOutcome::Unverifiable;
    };
    if claimed == network || neighbours(claimed, network) {
        CountryAccessOutcome::Granted(claimed)
    } else {
        CountryAccessOutcome::Mismatch
    }
}

/// What part of the map a reader may see.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MapScope {
    /// Everything (the mesh; members while the member filter is off).
    All,
    /// A guest session: only the country granted to this principal (its token
    /// `sub`), nothing without one.
    Guest(String),
    /// A member account (#665, with the member filter on): its home and
    /// unlocked countries, and posts at sea.
    Member(uuid::Uuid),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cc(code: &str) -> CountryCode {
        CountryCode::try_from(code).unwrap()
    }

    #[test]
    fn codes_are_two_letters_upper_cased() {
        assert_eq!(cc(" fr ").as_str(), "FR");
        for bad in ["", "F", "FRA", "F1", "é"] {
            assert!(CountryCode::try_from(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn the_claim_is_granted_on_a_matching_or_neighbouring_network_only() {
        let neighbours = |a: CountryCode, b: CountryCode| {
            let pair = [a.as_str(), b.as_str()];
            pair == ["FR", "ES"] || pair == ["ES", "FR"]
        };
        assert_eq!(decide(Some(cc("FR")), Some(cc("FR")), neighbours), CountryAccessOutcome::Granted(cc("FR")));
        assert_eq!(decide(Some(cc("ES")), Some(cc("FR")), neighbours), CountryAccessOutcome::Granted(cc("ES")));
        assert_eq!(decide(Some(cc("US")), Some(cc("FR")), neighbours), CountryAccessOutcome::Mismatch);
        assert_eq!(decide(Some(cc("FR")), None, neighbours), CountryAccessOutcome::Unverifiable);
        assert_eq!(decide(None, Some(cc("FR")), neighbours), CountryAccessOutcome::NotSent);
    }
}
