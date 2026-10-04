use std::fmt;

use crate::domain::value_object::{GeoCoordinate, H3Resolution};
use crate::error::GeoDiscoveryError;

/// A validated H3 hexagonal cell index.
///
/// Wraps `h3o::CellIndex`. Valid H3 indices have bit 63 = 0, so the cast
/// between u64 and i64 (ScyllaDB `bigint`) is lossless and round-trips exactly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct H3Index(h3o::CellIndex);

impl H3Index {
    /// Encodes a validated coordinate to the H3 cell at the given resolution.
    pub fn encode(coord: &GeoCoordinate, resolution: H3Resolution) -> Self {
        let latlng = h3o::LatLng::new(coord.lat, coord.lng)
            // Safety: GeoCoordinate invariants guarantee lat/lng are within h3o bounds.
            .expect("GeoCoordinate invariants violated");
        Self(latlng.to_cell(resolution.as_h3o()))
    }

    /// Returns the parent cell at the given (coarser) resolution.
    pub fn parent(&self, resolution: H3Resolution) -> Self {
        Self(self.0.parent(resolution.as_h3o())
            .expect("parent resolution must be coarser than cell resolution"))
    }

    /// The cell's centre `(lat, lng)`.
    pub fn center(&self) -> (f64, f64) {
        let ll = h3o::LatLng::from(self.0);
        (ll.lat(), ll.lng())
    }

    /// Raw signed integer representation for ScyllaDB `bigint` storage.
    pub fn as_i64(&self) -> i64 {
        u64::from(self.0) as i64
    }

    /// Raw unsigned representation for Redis key construction and h3o operations.
    pub fn as_u64(&self) -> u64 {
        u64::from(self.0)
    }

    /// Returns all cells within k-ring distance of this cell.
    pub fn grid_disk(&self, k: u32) -> Vec<H3Index> {
        self.0.grid_disk::<Vec<_>>(k).into_iter().map(H3Index).collect()
    }

    /// Reconstructs from a ScyllaDB `bigint`. Returns an error for invalid indices.
    pub fn from_i64(v: i64) -> Result<Self, GeoDiscoveryError> {
        h3o::CellIndex::try_from(v as u64)
            .map(Self)
            .map_err(|_| GeoDiscoveryError::InvalidH3Index(v))
    }
}

impl fmt::Display for H3Index {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_u64())
    }
}
