use std::collections::HashSet;

use async_trait::async_trait;

use crate::domain::value_object::AccountId;
use crate::error::AccountError;

/// How a contact matched: its email address or its phone number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ContactChannel {
    Email,
    Phone,
}

/// An active account whose **verified** contact hashes to `hash` (#661).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContactMatch {
    pub account_id: AccountId,
    pub channel:    ContactChannel,
    /// SHA-256 of the normalized contact, as the app sent it.
    pub hash:       Vec<u8>,
}

/// The accounts' contacts, by hash: SHA-256 of a lower-cased, trimmed email
/// address or of an E.164 phone number. Only verified contacts of active
/// accounts match.
#[async_trait]
pub trait ContactIndex: Send + Sync + 'static {
    async fn match_contacts(
        &self,
        email_hashes: &[Vec<u8>],
        phone_hashes: &[Vec<u8>],
    ) -> Result<Vec<ContactMatch>, AccountError>;
}

/// One profile of an account, with how it may be found (#661).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectoryProfile {
    pub profile_id:   String,
    pub handle:       String,
    pub display_name: String,
    pub avatar_url:   Option<String>,
    /// Active (not hidden, suspended or deleted).
    pub active:       bool,
    /// Findable through its account's email address / phone number.
    pub by_email:     bool,
    pub by_phone:     bool,
}

/// The fleet's view of profiles, over the mesh: an account's profiles
/// (profile), and who is hidden from whom (social-graph: a block either way,
/// or a hidden profile).
#[async_trait]
pub trait ProfileDirectory: Send + Sync + 'static {
    async fn profiles_of(&self, account_id: &AccountId) -> Result<Vec<DirectoryProfile>, AccountError>;

    /// The `targets` that `viewers` (one account's profiles) may not see at all.
    async fn hidden_from(&self, viewers: &[String], targets: &[String]) -> Result<HashSet<String>, AccountError>;
}
