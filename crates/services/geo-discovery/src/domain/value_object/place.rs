use crate::domain::value_object::{CountryCode, GeoCoordinate, H3Index, H3Resolution};
use crate::error::GeoDiscoveryError;

/// What a place page lists (#833): the posts of one H3 cell — at a
/// resolution the map indexes (R5 a city, R7 a district, R9 a street) — or of
/// a whole country.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Place {
    Cell { index: H3Index, resolution: H3Resolution },
    Country(CountryCode),
}

impl Place {
    /// A cell by its raw index; GEO-1002 unless it is an R5, R7 or R9 cell.
    pub fn cell(raw: i64) -> Result<Self, GeoDiscoveryError> {
        let index = H3Index::from_i64(raw)?;
        let resolution = index.resolution().ok_or(GeoDiscoveryError::InvalidH3Index(raw))?;
        Ok(Self::Cell { index, resolution })
    }

    /// The cell at `resolution` around a point (the client names a city by its
    /// centre and a scale).
    pub fn around(lat: f64, lng: f64, resolution: H3Resolution) -> Result<Self, GeoDiscoveryError> {
        let coord = GeoCoordinate::new(lat, lng)?;
        Ok(Self::Cell { index: H3Index::encode(&coord, resolution), resolution })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cells_are_the_indexed_resolutions_only() {
        let paris = Place::around(48.8566, 2.3522, H3Resolution::R5).unwrap();
        let Place::Cell { index, resolution } = paris else { panic!("a cell") };
        assert_eq!(resolution, H3Resolution::R5);
        assert_eq!(Place::cell(index.as_i64()).unwrap(), paris);

        // An R6 cell is not indexed.
        let r6 = h3o::LatLng::new(48.8566, 2.3522).unwrap().to_cell(h3o::Resolution::Six);
        assert!(matches!(Place::cell(u64::from(r6) as i64), Err(GeoDiscoveryError::InvalidH3Index(_))));
        assert!(Place::cell(42).is_err());
        assert!(Place::around(95.0, 0.0, H3Resolution::R7).is_err());
    }
}
