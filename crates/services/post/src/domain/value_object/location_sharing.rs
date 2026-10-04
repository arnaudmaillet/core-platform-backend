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
            assert_eq!(LocationSharing { ghost: true, city }.shown(paris()), None);
        }
    }

    #[test]
    fn city_level_shows_one_centre_for_nearby_points_never_the_point() {
        let city = LocationSharing { ghost: false, city: true };
        let a = city.shown(paris()).unwrap();
        // ~300 m away, same city cell.
        let b = city.shown(GeoPoint::new(48.8590, 2.3550).unwrap()).unwrap();
        assert_eq!(a, b);
        assert_ne!(a, paris());
    }
}
