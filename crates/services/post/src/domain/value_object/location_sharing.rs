//! An author's location sharing (#657), projected from profile's
//! `ProfileLocationSettingsChanged`: what a post's location looks like to
//! anyone but its author.

use h3o::{LatLng, Resolution};

use crate::domain::value_object::GeoPoint;

/// How an author shares where their posts were made. The default (no
/// projection row) shares precisely.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LocationSharing {
    /// Ghost mode: the author's posts carry no location for anyone else.
    pub ghost: bool,
    /// City level: others see the centre of the post's ~87 km² cell (H3 R5,
    /// the map's coarse band) — never the point itself.
    pub city: bool,
    /// Who sees the location at all.
    pub audience: LocationAudience,
}

/// Who sees where an author's posts were made (#657). Outside it, a reader
/// sees the post without its location.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
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

    /// Does it take a reader who `follows` the author / is `mutual` with it?
    pub fn admits(self, follows: bool, mutual: bool) -> bool {
        match self {
            Self::Everyone => true,
            Self::Followers => follows,
            Self::Mutuals => mutual,
        }
    }
}

impl LocationSharing {
    /// `point` as a reader other than the author sees it.
    pub fn shown(&self, point: GeoPoint) -> Option<GeoPoint> {
        if self.ghost {
            return None;
        }
        if !self.city {
            return Some(point);
        }
        let cell = LatLng::new(point.lat(), point.lng()).ok()?.to_cell(Resolution::Five);
        let centre = LatLng::from(cell);
        GeoPoint::new(centre.lat(), centre.lng()).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn paris() -> GeoPoint {
        GeoPoint::new(48.8566, 2.3522).unwrap()
    }

    #[test]
    fn the_default_shows_the_point() {
        assert_eq!(LocationSharing::default().shown(paris()), Some(paris()));
    }

    #[test]
    fn ghost_shows_nothing_whatever_the_precision() {
        for city in [false, true] {
            assert_eq!(LocationSharing { ghost: true, city, ..LocationSharing::default() }.shown(paris()), None);
        }
    }

    #[test]
    fn city_level_shows_one_centre_for_nearby_points_never_the_point() {
        let city = LocationSharing { city: true, ..LocationSharing::default() };
        let a = city.shown(paris()).unwrap();
        // ~300 m away, same city cell.
        let b = city.shown(GeoPoint::new(48.8590, 2.3550).unwrap()).unwrap();
        assert_eq!(a, b);
        assert_ne!(a, paris());
    }
}
