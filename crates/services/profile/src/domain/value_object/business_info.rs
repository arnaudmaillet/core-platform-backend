use serde::{Deserialize, Serialize};

use crate::error::ProfileError;

/// The public contact card of a business (brand) profile (#668).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BusinessInfo {
    /// E.g. "Restaurant", "Musician" (≤ 64 chars).
    pub category:      String,
    pub contact_email: Option<String>,
    pub contact_phone: Option<String>,
}

fn violation(field: &str, message: &str) -> ProfileError {
    ProfileError::DomainViolation { field: field.into(), message: message.into() }
}

impl BusinessInfo {
    pub fn new(category: String, contact_email: Option<String>, contact_phone: Option<String>) -> Result<Self, ProfileError> {
        let category = category.trim().to_owned();
        if category.is_empty() || category.chars().count() > 64 {
            return Err(violation("business.category", "1–64 characters"));
        }
        let contact_email = contact_email.map(|e| e.trim().to_owned()).filter(|e| !e.is_empty());
        if let Some(email) = &contact_email
            && (email.len() > 254 || !email.contains('@') || email.contains(char::is_whitespace))
        {
            return Err(violation("business.contact_email", "not an email address"));
        }
        let contact_phone = contact_phone.map(|p| p.trim().to_owned()).filter(|p| !p.is_empty());
        if let Some(phone) = &contact_phone
            && (phone.len() > 32 || !phone.chars().all(|c| c.is_ascii_digit() || "+ -().".contains(c)))
        {
            return Err(violation("business.contact_phone", "not a phone number"));
        }
        Ok(Self { category, contact_email, contact_phone })
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }

    pub fn from_json(json: Option<&str>) -> Option<Self> {
        json.and_then(|j| serde_json::from_str(j).ok())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_business_card_is_validated() {
        let ok = BusinessInfo::new(" Café ".into(), Some("hi@cafe.fr".into()), Some("+33 1 23 45 67 89".into())).unwrap();
        assert_eq!(ok.category, "Café");
        assert_eq!(BusinessInfo::from_json(Some(&ok.to_json())), Some(ok));
        assert!(BusinessInfo::new("".into(), None, None).is_err());
        assert!(BusinessInfo::new("Shop".into(), Some("nope".into()), None).is_err());
        assert!(BusinessInfo::new("Shop".into(), None, Some("call me".into())).is_err());
        assert_eq!(BusinessInfo::new("Shop".into(), Some(" ".into()), None).unwrap().contact_email, None);
    }
}
