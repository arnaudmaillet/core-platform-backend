//! The world's country borders (`data/countries.json`, embedded): which country
//! a point is in, and which countries share a border.
//!
//! The borders are the iOS app's own (Natural Earth 1:50m, simplified at 0.02°),
//! so the server and the device put a post in the same country. Containment is
//! even-odd over every ring of a polygon (holes — lakes, enclaves — are
//! outside), like the app's `CountryAtlas.contains`.

use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;

use crate::domain::value_object::CountryCode;

const BORDERS: &str = include_str!("../../data/countries.json");

/// Simplification tolerance of the borders, in degrees: a point this close to a
/// country's outline still counts as inside it (a beach, a border town).
const EDGE_TOLERANCE_DEG: f64 = 0.02;

/// Two countries with outline points this close share a border. Wider than the
/// simplification of both sides so a shared border always matches; a strait
/// narrower than this (Øresund, Johor) reads as a border too.
const NEIGHBOUR_DISTANCE_DEG: f64 = 0.05;

type Ring = Vec<(f64, f64)>; // (lng, lat)

/// Outline points per grid cell: `(cell x, cell y) → [(country, lng, lat)]`.
type PointGrid = HashMap<(i64, i64), Vec<(CountryCode, f64, f64)>>;

struct Country {
    /// Each polygon: outer ring, then holes.
    polygons: Vec<Vec<Ring>>,
    /// (min_lng, min_lat, max_lng, max_lat).
    bbox:     (f64, f64, f64, f64),
}

pub struct CountryAtlas {
    countries:  HashMap<CountryCode, Country>,
    neighbours: HashSet<(CountryCode, CountryCode)>,
}

impl CountryAtlas {
    /// The embedded atlas, parsed once.
    pub fn embedded() -> &'static CountryAtlas {
        static ATLAS: OnceLock<CountryAtlas> = OnceLock::new();
        ATLAS.get_or_init(|| Self::parse(BORDERS).expect("embedded data/countries.json is valid"))
    }

    fn parse(json: &str) -> Result<Self, String> {
        let raw: HashMap<String, Vec<Vec<Vec<[f64; 2]>>>> =
            serde_json::from_str(json).map_err(|e| e.to_string())?;
        let mut countries = HashMap::with_capacity(raw.len());
        for (code, polygons) in raw {
            let code = CountryCode::try_from(code.as_str()).map_err(|e| e.to_string())?;
            let polygons: Vec<Vec<Ring>> = polygons
                .into_iter()
                .map(|rings| rings.into_iter().map(|ring| ring.into_iter().map(|[x, y]| (x, y)).collect()).collect())
                .collect();
            let mut bbox = (f64::MAX, f64::MAX, f64::MIN, f64::MIN);
            for &(x, y) in polygons.iter().flatten().flatten() {
                bbox = (bbox.0.min(x), bbox.1.min(y), bbox.2.max(x), bbox.3.max(y));
            }
            countries.insert(code, Country { polygons, bbox });
        }
        let neighbours = Self::border_pairs(&countries);
        Ok(Self { countries, neighbours })
    }

    /// Pairs of countries whose outlines come within [`NEIGHBOUR_DISTANCE_DEG`]:
    /// every outline point is hashed into a grid of that size and compared with
    /// the points of the eight cells around it.
    fn border_pairs(countries: &HashMap<CountryCode, Country>) -> HashSet<(CountryCode, CountryCode)> {
        let cell = |v: f64| (v / NEIGHBOUR_DISTANCE_DEG).floor() as i64;
        let mut grid: PointGrid = HashMap::new();
        for (code, country) in countries {
            for &(x, y) in country.polygons.iter().flatten().flatten() {
                grid.entry((cell(x), cell(y))).or_default().push((*code, x, y));
            }
        }
        let mut pairs = HashSet::new();
        let limit = NEIGHBOUR_DISTANCE_DEG * NEIGHBOUR_DISTANCE_DEG;
        for (&(cx, cy), points) in &grid {
            for dx in -1..=1 {
                for dy in -1..=1 {
                    let Some(others) = grid.get(&(cx + dx, cy + dy)) else { continue };
                    for &(a, ax, ay) in points {
                        for &(b, bx, by) in others {
                            if a != b && !pairs.contains(&(a, b)) && (ax - bx).powi(2) + (ay - by).powi(2) <= limit {
                                pairs.insert((a, b));
                                pairs.insert((b, a));
                            }
                        }
                    }
                }
            }
        }
        pairs
    }

    pub fn knows(&self, code: CountryCode) -> bool {
        self.countries.contains_key(&code)
    }

    /// Whether two countries share a border (symmetric; a country is not its
    /// own neighbour).
    pub fn neighbours(&self, a: CountryCode, b: CountryCode) -> bool {
        self.neighbours.contains(&(a, b))
    }

    /// Whether a point is in `code`, or within [`EDGE_TOLERANCE_DEG`] of its
    /// outline. Unknown codes contain nothing.
    pub fn contains(&self, code: CountryCode, lat: f64, lng: f64) -> bool {
        let Some(country) = self.countries.get(&code) else {
            return false;
        };
        let (min_x, min_y, max_x, max_y) = country.bbox;
        let t = EDGE_TOLERANCE_DEG;
        if lng < min_x - t || lng > max_x + t || lat < min_y - t || lat > max_y + t {
            return false;
        }
        let inside = country.polygons.iter().any(|polygon| {
            polygon.iter().filter(|ring| ring_contains(ring, lng, lat)).count() % 2 == 1
        });
        inside || country.polygons.iter().flatten().any(|ring| ring_within(ring, lng, lat, t))
    }

    /// The country a point is in (strictly inside, no tolerance), if any.
    pub fn country_at(&self, lat: f64, lng: f64) -> Option<CountryCode> {
        self.countries.iter().find_map(|(code, country)| {
            let (min_x, min_y, max_x, max_y) = country.bbox;
            if lng < min_x || lng > max_x || lat < min_y || lat > max_y {
                return None;
            }
            country
                .polygons
                .iter()
                .any(|polygon| polygon.iter().filter(|ring| ring_contains(ring, lng, lat)).count() % 2 == 1)
                .then_some(*code)
        })
    }
}

/// Ray casting (even-odd) on one ring.
fn ring_contains(ring: &Ring, x: f64, y: f64) -> bool {
    let mut inside = false;
    let mut j = ring.len().wrapping_sub(1);
    for i in 0..ring.len() {
        let (xi, yi) = ring[i];
        let (xj, yj) = ring[j];
        if (yi > y) != (yj > y) && x < (xj - xi) * (y - yi) / (yj - yi) + xi {
            inside = !inside;
        }
        j = i;
    }
    inside
}

/// Whether the point is within `tolerance` of the ring's outline.
fn ring_within(ring: &Ring, x: f64, y: f64, tolerance: f64) -> bool {
    let limit = tolerance * tolerance;
    let mut j = ring.len().wrapping_sub(1);
    for i in 0..ring.len() {
        if segment_distance_sq(ring[j], ring[i], (x, y)) <= limit {
            return true;
        }
        j = i;
    }
    false
}

fn segment_distance_sq((ax, ay): (f64, f64), (bx, by): (f64, f64), (px, py): (f64, f64)) -> f64 {
    let (dx, dy) = (bx - ax, by - ay);
    let len = dx * dx + dy * dy;
    let t = if len == 0.0 { 0.0 } else { (((px - ax) * dx + (py - ay) * dy) / len).clamp(0.0, 1.0) };
    let (cx, cy) = (ax + t * dx, ay + t * dy);
    (px - cx).powi(2) + (py - cy).powi(2)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cc(code: &str) -> CountryCode {
        CountryCode::try_from(code).unwrap()
    }

    #[test]
    fn points_land_in_their_country() {
        let atlas = CountryAtlas::embedded();
        assert_eq!(atlas.country_at(48.8566, 2.3522), Some(cc("FR"))); // Paris
        assert_eq!(atlas.country_at(40.4168, -3.7038), Some(cc("ES"))); // Madrid
        assert_eq!(atlas.country_at(-33.8688, 151.2093), Some(cc("AU"))); // Sydney
        assert_eq!(atlas.country_at(45.0, -30.0), None); // mid-Atlantic
        assert!(atlas.contains(cc("FR"), 48.8566, 2.3522));
        assert!(!atlas.contains(cc("ES"), 48.8566, 2.3522));
        assert!(!atlas.contains(cc("ZZ"), 48.8566, 2.3522));
    }

    #[test]
    fn a_point_just_off_the_simplified_coast_still_counts() {
        let atlas = CountryAtlas::embedded();
        // Nice's seafront, a few hundred metres out: outside the simplified
        // outline maybe, but within its tolerance.
        assert!(atlas.contains(cc("FR"), 43.690, 7.265));
    }

    #[test]
    fn land_neighbours_border_overseas_countries_do_not() {
        let atlas = CountryAtlas::embedded();
        for (a, b) in [("FR", "ES"), ("FR", "DE"), ("FR", "BE"), ("US", "CA"), ("US", "MX"), ("PT", "ES")] {
            assert!(atlas.neighbours(cc(a), cc(b)), "{a}-{b}");
            assert!(atlas.neighbours(cc(b), cc(a)), "{b}-{a}");
        }
        for (a, b) in [("FR", "US"), ("PT", "FR"), ("JP", "CN"), ("AU", "NZ")] {
            assert!(!atlas.neighbours(cc(a), cc(b)), "{a}-{b}");
        }
        assert!(!atlas.neighbours(cc("FR"), cc("FR")));
    }
}
