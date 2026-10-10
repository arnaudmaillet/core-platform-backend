use crate::error::PostError;

/// Client-supplied post location (WGS-84 decimal degrees).
///
/// Optional on a post: text posts carry no location. When present, it is
/// denormalized onto `PostPublished` so geo-discovery can spatially index the
/// post. Posts without a location are simply not geo-indexed.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GeoPoint {
    lat: f64,
    lng: f64,
}

impl GeoPoint {
    pub fn new(lat: f64, lng: f64) -> Result<Self, PostError> {
        if !lat.is_finite() || !(-90.0..=90.0).contains(&lat) {
            return Err(PostError::DomainViolation {
                field:   "lat".into(),
                message: "lat must be a finite value in [-90, 90]".into(),
            });
        }
        if !lng.is_finite() || !(-180.0..=180.0).contains(&lng) {
            return Err(PostError::DomainViolation {
                field:   "lng".into(),
                message: "lng must be a finite value in [-180, 180]".into(),
            });
        }
        Ok(Self { lat, lng })
    }

    pub fn lat(&self) -> f64 {
        self.lat
    }

    pub fn lng(&self) -> f64 {
        self.lng
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_valid_coordinates() {
        let p = GeoPoint::new(48.8566, 2.3522).expect("valid");
        assert_eq!(p.lat(), 48.8566);
        assert_eq!(p.lng(), 2.3522);
    }

    #[test]
    fn rejects_out_of_range_lat() {
        assert!(GeoPoint::new(90.1, 0.0).is_err());
        assert!(GeoPoint::new(-90.1, 0.0).is_err());
    }

    #[test]
    fn rejects_out_of_range_lng() {
        assert!(GeoPoint::new(0.0, 180.1).is_err());
        assert!(GeoPoint::new(0.0, -180.1).is_err());
    }

    #[test]
    fn rejects_non_finite() {
        assert!(GeoPoint::new(f64::NAN, 0.0).is_err());
        assert!(GeoPoint::new(0.0, f64::INFINITY).is_err());
    }
}
