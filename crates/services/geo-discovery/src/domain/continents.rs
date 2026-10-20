//! Which continent each country of the atlas belongs to (`data/continents.json`,
//! embedded; #912): the continent badges' rule, and the table the app draws its
//! progress from (`ListContinents`), so the two never disagree.
//!
//! Seven continents, codes `AF`, `AN`, `AS`, `EU`, `NA`, `OC`, `SA`. Natural
//! Earth's own continents (the atlas's source), with its "Seven seas" islands
//! put with their nearest continent.

use std::collections::HashMap;
use std::sync::OnceLock;

use crate::domain::value_object::CountryCode;

const TABLE: &str = include_str!("../../data/continents.json");

pub struct Continents {
    /// Continent code → its countries, sorted.
    members: Vec<(String, Vec<CountryCode>)>,
}

impl Continents {
    /// The embedded table, parsed once.
    pub fn embedded() -> &'static Continents {
        static TABLE_ONCE: OnceLock<Continents> = OnceLock::new();
        TABLE_ONCE.get_or_init(|| Self::parse(TABLE).expect("embedded data/continents.json is valid"))
    }

    fn parse(json: &str) -> Result<Self, String> {
        let raw: HashMap<String, Vec<String>> = serde_json::from_str(json).map_err(|e| e.to_string())?;
        let mut members: Vec<(String, Vec<CountryCode>)> = raw
            .into_iter()
            .map(|(continent, codes)| {
                let codes = codes.iter().map(|c| CountryCode::try_from(c.as_str()).map_err(|e| e.to_string())).collect::<Result<Vec<_>, _>>()?;
                Ok((continent, codes))
            })
            .collect::<Result<_, String>>()?;
        members.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(Self { members })
    }

    /// Every continent and its countries, by code.
    pub fn all(&self) -> &[(String, Vec<CountryCode>)] {
        &self.members
    }

    /// A continent's badge (`badge.continent_eu`).
    pub fn badge_id(continent: &str) -> String {
        format!("badge.continent_{}", continent.to_ascii_lowercase())
    }

    /// The continents whose every country is in `countries`.
    pub fn completed(&self, countries: &[CountryCode]) -> Vec<&str> {
        self.members
            .iter()
            .filter(|(_, members)| members.iter().all(|c| countries.contains(c)))
            .map(|(continent, _)| continent.as_str())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::country_atlas::CountryAtlas;

    fn cc(code: &str) -> CountryCode {
        CountryCode::try_from(code).unwrap()
    }

    #[test]
    fn every_atlas_country_is_on_exactly_one_continent() {
        let table = Continents::embedded();
        let codes: Vec<&str> = table.all().iter().map(|(c, _)| c.as_str()).collect();
        assert_eq!(codes, ["AF", "AN", "AS", "EU", "NA", "OC", "SA"]);
        let mut all: Vec<CountryCode> = table.all().iter().flat_map(|(_, m)| m.iter().copied()).collect();
        let total = all.len();
        all.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        all.dedup();
        assert_eq!(all.len(), total, "no country on two continents");
        let atlas = CountryAtlas::embedded();
        assert!(all.iter().all(|c| atlas.knows(*c)), "only atlas countries");
        assert_eq!(total, 237, "the whole atlas");
    }

    #[test]
    fn a_continent_completes_when_all_its_countries_are_held() {
        let table = Continents::embedded();
        let antarctica = [cc("AQ"), cc("HM"), cc("TF")];
        assert_eq!(table.completed(&antarctica), ["AN"]);
        assert!(table.completed(&antarctica[..2]).is_empty());
        assert_eq!(Continents::badge_id("EU"), "badge.continent_eu");
    }
}
