use std::net::IpAddr;

use crate::domain::value_object::CountryCode;

/// IP → country (GeoIP). `None` when the address is not in the database, or
/// when there is no database: an unknown network verifies nothing.
///
/// `claimed` is the country the device says it is in; only a local-development
/// rule for private addresses may answer with it.
pub trait GeoIp: Send + Sync + 'static {
    fn country_of(&self, ip: IpAddr, claimed: Option<CountryCode>) -> Option<CountryCode>;
}
