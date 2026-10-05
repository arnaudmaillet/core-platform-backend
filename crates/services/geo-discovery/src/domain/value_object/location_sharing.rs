//! A post author's location sharing (#657), projected from profile's
//! `ProfileLocationSettingsChanged`: ghost mode, city-level precision and who
//! sees the location.

use serde::{Deserialize, Serialize};

use crate::domain::value_object::{GeoCoordinate, H3Index, H3Resolution};

/// How an author shares where their posts were made. The default (no
/// projection row) shares precisely.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct LocationSharing {
    /// Ghost mode: the author's posts leave everyone else's map.
    pub ghost: bool,
    /// City level: others see the author's posts only at the coarse band (R5,
    /// ~87 km²), at the cell's centre — never the point or a finer cell.
    pub city: bool,
    /// Who sees the author's posts on the map at all.
    pub audience: LocationAudience,
}

impl LocationSharing {
    pub fn is_default(&self) -> bool {
        *self == Self::default()
    }
}

/// Who sees where an author's posts were made (#657). Outside it, the posts
/// leave the reader's map.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum LocationAudience {
    #[default]
    Everyone,
    /// Readers following the author.
    Followers,
    /// Readers the author follows back.
    Mutuals,
}

impl LocationAudience {
    pub fn as_tinyint(self) -> i8 {
        match self {
            Self::Everyone => 0,
            Self::Followers => 1,
            Self::Mutuals => 2,
        }
    }

    /// The stored value; anything unknown (or absent) is everyone.
    pub fn from_tinyint(v: Option<i8>) -> Self {
        match v {
            Some(1) => Self::Followers,
            Some(2) => Self::Mutuals,
            _ => Self::Everyone,
        }
    }

    /// Does it take a reader who `follows` the author, and is `mutual` with
    /// it? A reader nobody knows (the mesh, an anonymous one) neither.
    pub fn admits(self, follows: bool, mutual: bool) -> bool {
        match self {
            Self::Everyone => true,
            Self::Followers => follows,
            Self::Mutuals => mutual,
        }
    }
}

/// The city-level stand-in for a point: the centre of its R5 cell.
pub fn city_point(lat: f64, lng: f64) -> Option<(f64, f64)> {
    let coord = GeoCoordinate::new(lat, lng).ok()?;
    Some(H3Index::encode(&coord, H3Resolution::R5).center())
}

/// The city-level stand-in for an R7 cell: the R7 cell at the centre of its R5
/// parent, so a card names the same cell for every post in that city.
pub fn city_r7(h3_r7: i64) -> Option<i64> {
    let cell = H3Index::from_i64(h3_r7).ok()?;
    let (lat, lng) = cell.parent(H3Resolution::R5).center();
    let coord = GeoCoordinate::new(lat, lng).ok()?;
    Some(H3Index::encode(&coord, H3Resolution::R7).as_i64())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_audience_admits_exactly_who_it_names() {
        use LocationAudience::*;
        for (audience, stranger, follower, mutual) in
            [(Everyone, true, true, true), (Followers, false, true, true), (Mutuals, false, false, true)]
        {
            assert_eq!(audience.admits(false, false), stranger, "{audience:?}");
            assert_eq!(audience.admits(true, false), follower, "{audience:?}");
            assert_eq!(audience.admits(true, true), mutual, "{audience:?}");
            assert_eq!(LocationAudience::from_tinyint(Some(audience.as_tinyint())), audience);
        }
        assert_eq!(LocationAudience::from_tinyint(None), Everyone);
    }

    #[test]
    fn nearby_points_share_one_city_point_and_one_city_cell() {
        // Two points ~300 m apart in central Paris.
        let a = city_point(48.8566, 2.3522).unwrap();
        let b = city_point(48.8590, 2.3550).unwrap();
        assert_eq!(a, b);
        assert_ne!(a, (48.8566, 2.3522), "never the post's own point");

        let r7 = |lat, lng| {
            H3Index::encode(&GeoCoordinate::new(lat, lng).unwrap(), H3Resolution::R7).as_i64()
        };
        assert_eq!(city_r7(r7(48.8566, 2.3522)), city_r7(r7(48.8590, 2.3550)));
    }
}
