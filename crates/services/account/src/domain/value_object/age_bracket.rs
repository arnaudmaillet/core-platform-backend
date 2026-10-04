use chrono::{Datelike, NaiveDate};

use crate::domain::value_object::CountryCode;
use crate::error::AccountError;

/// The youngest age at which anyone may hold an account.
pub const MINIMUM_AGE: u32 = 13;

/// The oldest plausible date of birth, in years — anything older is a typo.
const MAXIMUM_AGE: u32 = 120;

/// Countries whose law sets a higher minimum age for social media.
const MINIMUM_AGE_16: &[&str] = &["AU"];

/// The holder's age bracket on a given day: 13–17 ⇒ teen protections.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AgeBracket {
    Teen13To15,
    Teen16To17,
    Adult,
}

impl AgeBracket {
    /// The bracket of someone born on `dob`, on `today`. Below the minimum
    /// (never stored, see [`check_date_of_birth`]) reads as the youngest
    /// bracket: the protective default.
    pub fn on(dob: NaiveDate, today: NaiveDate) -> Self {
        match age_on(dob, today) {
            Some(16..=17) => Self::Teen16To17,
            Some(age) if age >= 18 => Self::Adult,
            _ => Self::Teen13To15,
        }
    }

    pub fn is_minor(&self) -> bool {
        !matches!(self, Self::Adult)
    }
}

/// Completed years between `dob` and `today`; `None` when `dob` is in the future.
pub fn age_on(dob: NaiveDate, today: NaiveDate) -> Option<u32> {
    if dob > today {
        return None;
    }
    let mut years = today.year() - dob.year();
    if (today.month(), today.day()) < (dob.month(), dob.day()) {
        years -= 1;
    }
    u32::try_from(years).ok()
}

/// The minimum age where the holder lives (13; 16 in Australia).
pub fn minimum_age(country: Option<&CountryCode>) -> u32 {
    match country {
        Some(c) if MINIMUM_AGE_16.contains(&c.as_str()) => 16,
        _ => MINIMUM_AGE,
    }
}

/// A date of birth the account may record: in the past, plausible, and at
/// least the minimum age for the holder's country of residence.
pub fn check_date_of_birth(
    dob: NaiveDate,
    country: Option<&CountryCode>,
    today: NaiveDate,
) -> Result<(), AccountError> {
    let age = age_on(dob, today).ok_or_else(|| AccountError::DomainViolation {
        field: "date_of_birth".into(),
        message: "date_of_birth is in the future".into(),
    })?;
    if age > MAXIMUM_AGE {
        return Err(AccountError::DomainViolation {
            field: "date_of_birth".into(),
            message: format!("date_of_birth is more than {MAXIMUM_AGE} years ago"),
        });
    }
    let minimum = minimum_age(country);
    if age < minimum {
        return Err(AccountError::AgeBelowMinimum { minimum });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(y: i32, m: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, day).unwrap()
    }

    #[test]
    fn age_counts_completed_years_only() {
        let today = d(2026, 10, 4);
        assert_eq!(age_on(d(2013, 10, 4), today), Some(13), "13th birthday today");
        assert_eq!(age_on(d(2013, 10, 5), today), Some(12), "the day before");
        assert_eq!(age_on(d(2027, 1, 1), today), None);
        // A 29 February birthday turns a year older on 1 March in common years.
        assert_eq!(age_on(d(2008, 2, 29), d(2026, 2, 28)), Some(17));
        assert_eq!(age_on(d(2008, 2, 29), d(2026, 3, 1)), Some(18));
    }

    #[test]
    fn brackets_follow_birthdays() {
        let today = d(2026, 10, 4);
        assert_eq!(AgeBracket::on(d(2013, 10, 4), today), AgeBracket::Teen13To15);
        assert_eq!(AgeBracket::on(d(2010, 10, 4), today), AgeBracket::Teen16To17);
        assert_eq!(AgeBracket::on(d(2008, 10, 5), today), AgeBracket::Teen16To17);
        assert_eq!(AgeBracket::on(d(2008, 10, 4), today), AgeBracket::Adult);
        assert!(AgeBracket::Teen16To17.is_minor() && !AgeBracket::Adult.is_minor());
    }

    #[test]
    fn the_minimum_age_is_13_and_16_in_australia() {
        let today = d(2026, 10, 4);
        let au = CountryCode::new("AU").unwrap();
        let fr = CountryCode::new("FR").unwrap();
        assert!(check_date_of_birth(d(2013, 10, 4), None, today).is_ok());
        assert!(check_date_of_birth(d(2013, 10, 4), Some(&fr), today).is_ok());
        assert!(matches!(
            check_date_of_birth(d(2013, 10, 5), None, today),
            Err(AccountError::AgeBelowMinimum { minimum: 13 })
        ));
        assert!(matches!(
            check_date_of_birth(d(2011, 1, 1), Some(&au), today),
            Err(AccountError::AgeBelowMinimum { minimum: 16 })
        ));
        assert!(check_date_of_birth(d(2010, 10, 4), Some(&au), today).is_ok());
        assert!(check_date_of_birth(d(2030, 1, 1), None, today).is_err(), "future");
        assert!(check_date_of_birth(d(1890, 1, 1), None, today).is_err(), "implausible");
    }
}
