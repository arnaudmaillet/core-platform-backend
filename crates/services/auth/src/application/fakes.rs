//! In-memory fakes for every outbound port, plus a [`Fixture`] that wires them
//! into the handlers. Test-only (`#[cfg(test)]`) — they prove the application
//! layer works against the port contracts with no real backend, which is exactly
//! what makes the abstraction credible before the Phase 4 adapters exist.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use uuid::Uuid;

use super::policy::SessionPolicy;
use super::port::{
    AccountActivation, AccountDirectory, AccountSnapshot, AuthnGrant, CredentialAdmin, EventPublisher,
    GeneratedRefresh, IdentityProvider, NormalizedClaims, ProfileDirectory,
    RefreshTokenRepository, SessionCache, SessionRepository, SubjectLinkRepository, TokenMinter,
};
use crate::domain::aggregate::{RefreshToken, Session, SubjectLink};
use crate::domain::event::DomainEvent;
use crate::domain::value_object::{
    AccessTokenClaims, AccountId, AgeBracket, Generation, IdpSubject, Permission, ProfileId,
    RefreshTokenHash, SessionId, SessionStatus,
};
use crate::error::AuthError;

/// A fixed reference instant for deterministic tests.
pub fn t0() -> DateTime<Utc> {
    DateTime::parse_from_rfc3339("2026-06-25T12:00:00Z").unwrap().with_timezone(&Utc)
}

// ─── IdentityProvider ────────────────────────────────────────────────────────

pub struct StubIdentityProvider {
    claims: Mutex<Option<NormalizedClaims>>,
    /// When set, a password grant must present exactly this password.
    password: Mutex<Option<String>>,
}

impl StubIdentityProvider {
    pub fn returning(issuer: &str, subject: &str) -> Self {
        Self {
            claims: Mutex::new(Some(NormalizedClaims {
                issuer: issuer.to_owned(),
                subject: subject.to_owned(),
            })),
            password: Mutex::new(None),
        }
    }

    pub fn failing() -> Self {
        Self { claims: Mutex::new(None), password: Mutex::new(None) }
    }

    /// Password grants succeed only with `password` from now on.
    pub fn with_password(&self, password: &str) {
        *self.password.lock().unwrap() = Some(password.to_owned());
    }
}

#[async_trait]
impl IdentityProvider for StubIdentityProvider {
    async fn authenticate(&self, grant: AuthnGrant) -> Result<NormalizedClaims, AuthError> {
        if let (AuthnGrant::Password { password, .. }, Some(expected)) =
            (&grant, self.password.lock().unwrap().as_ref())
            && password != expected
        {
            return Err(AuthError::IdpAuthenticationFailed);
        }
        self.claims
            .lock()
            .unwrap()
            .clone()
            .ok_or(AuthError::IdpAuthenticationFailed)
    }
}

// ─── AccountDirectory ────────────────────────────────────────────────────────

/// (sealed seed, unused backup-code hashes), as `account` keeps them.
pub type StoredMfa = (Vec<u8>, Vec<String>);

pub struct StubAccountDirectory {
    subjects: Mutex<HashMap<IdpSubject, AccountId>>,
    snapshots: Mutex<HashMap<AccountId, AccountSnapshot>>,
    /// Every account looked up, in order.
    looked_up: Mutex<Vec<AccountId>>,
    /// Every deactivated account resumed, in order.
    resumed: Mutex<Vec<AccountId>>,
    /// When set, resuming fails as if the account was suspended meanwhile.
    refuse_resume: std::sync::atomic::AtomicBool,
    /// email → holder (lower-cased).
    emails: Mutex<HashMap<String, super::port::EmailHolder>>,
    /// Every account provisioned, in order.
    provisioned: Mutex<Vec<super::port::NewAccount>>,
    /// account → its email / phone (#651).
    contacts: Mutex<HashMap<AccountId, super::port::ContactDetails>>,
    /// When set, `change_contact` fails as if another account won a race.
    refuse_contact_changes: std::sync::atomic::AtomicBool,
    /// account → (sealed seed, unused backup-code hashes) (#649).
    mfa: Mutex<HashMap<AccountId, StoredMfa>>,
    /// account → its delivered export's (link, expiry) (#653).
    export_links: Mutex<HashMap<AccountId, (String, DateTime<Utc>)>>,
}

impl Default for StubAccountDirectory {
    fn default() -> Self {
        Self::new()
    }
}

impl StubAccountDirectory {
    pub fn new() -> Self {
        Self {
            subjects: Mutex::new(HashMap::new()),
            snapshots: Mutex::new(HashMap::new()),
            looked_up: Mutex::new(Vec::new()),
            resumed: Mutex::new(Vec::new()),
            refuse_resume: std::sync::atomic::AtomicBool::new(false),
            emails: Mutex::new(HashMap::new()),
            provisioned: Mutex::new(Vec::new()),
            contacts: Mutex::new(HashMap::new()),
            refuse_contact_changes: std::sync::atomic::AtomicBool::new(false),
            mfa: Mutex::new(HashMap::new()),
            export_links: Mutex::new(HashMap::new()),
        }
    }

    /// The account's delivered export, as `account` would sign it.
    pub fn with_export_link(&self, account_id: AccountId, link: &str, expires_at: DateTime<Utc>) {
        self.export_links.lock().unwrap().insert(account_id, (link.to_owned(), expires_at));
    }

    /// Turns two-step sign-in on for a known account: `sealed` as the cipher
    /// sealed the seed, `code_hashes` its backup codes' hashes.
    pub fn with_mfa(&self, account_id: AccountId, sealed: Vec<u8>, code_hashes: Vec<String>) {
        self.mfa.lock().unwrap().insert(account_id, (sealed, code_hashes));
        if let Some(snapshot) = self.snapshots.lock().unwrap().get_mut(&account_id) {
            snapshot.mfa_enrolled = true;
        }
    }

    /// The unused backup-code hashes of an account.
    pub fn recovery_codes_left(&self, account_id: &AccountId) -> usize {
        self.mfa.lock().unwrap().get(account_id).map_or(0, |(_, codes)| codes.len())
    }

    /// Every later `change_contact` fails (`EmailAlreadyRegistered`).
    pub fn refuse_contact_changes(&self) {
        self.refuse_contact_changes.store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Sets an account's email / phone as `account` holds them.
    pub fn with_contact(&self, account_id: AccountId, contact: super::port::ContactDetails) {
        self.contacts.lock().unwrap().insert(account_id, contact);
    }

    pub fn contact_of(&self, account_id: &AccountId) -> super::port::ContactDetails {
        self.contacts.lock().unwrap().get(account_id).cloned().unwrap_or_default()
    }

    /// The accounts provisioned so far.
    pub fn provisioned(&self) -> Vec<super::port::NewAccount> {
        self.provisioned.lock().unwrap().clone()
    }

    /// Registers `email` as held by an account bound to `subject` (created with
    /// another method).
    pub fn with_email(&self, email: &str, subject: &IdpSubject, account_id: AccountId) {
        self.subjects.lock().unwrap().insert(subject.clone(), account_id);
        self.emails.lock().unwrap().insert(
            email.to_lowercase(),
            super::port::EmailHolder { account_id, identity_id: subject.to_string() },
        );
    }

    /// The deactivated accounts resumed so far.
    pub fn resumed(&self) -> Vec<AccountId> {
        self.resumed.lock().unwrap().clone()
    }

    /// Changes a known account's activation (e.g. deactivated after login).
    pub fn set_activation(&self, account_id: &AccountId, activation: AccountActivation) {
        if let Some(snapshot) = self.snapshots.lock().unwrap().get_mut(account_id) {
            snapshot.activation = activation;
        }
    }

    /// Sets a known account's age bracket (as `account` would compute it).
    pub fn set_age_bracket(&self, account_id: &AccountId, age_bracket: Option<AgeBracket>) {
        if let Some(snapshot) = self.snapshots.lock().unwrap().get_mut(account_id) {
            snapshot.age_bracket = age_bracket;
        }
    }

    /// Makes every later resume fail (the account left `Deactivated`).
    pub fn refuse_resume(&self) {
        self.refuse_resume.store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// The accounts looked up so far.
    pub fn lookups(&self) -> Vec<AccountId> {
        self.looked_up.lock().unwrap().clone()
    }

    /// Pre-binds a subject to a known account with the given activation + perms.
    pub fn with_account(
        &self,
        subject: &IdpSubject,
        account_id: AccountId,
        activation: AccountActivation,
        permissions: Vec<Permission>,
    ) {
        self.subjects.lock().unwrap().insert(subject.clone(), account_id);
        self.snapshots
            .lock()
            .unwrap()
            .insert(account_id, AccountSnapshot { activation, permissions, age_bracket: None, mfa_enrolled: false });
    }
}

#[async_trait]
impl AccountDirectory for StubAccountDirectory {
    async fn resolve_or_provision(&self, subject: &IdpSubject) -> Result<AccountId, AuthError> {
        let mut subjects = self.subjects.lock().unwrap();
        if let Some(id) = subjects.get(subject) {
            return Ok(*id);
        }
        let id = AccountId::from_uuid(Uuid::now_v7());
        subjects.insert(subject.clone(), id);
        self.snapshots.lock().unwrap().insert(
            id,
            AccountSnapshot {
                activation: AccountActivation::Active,
                permissions: Vec::new(),
                age_bracket: None,
                mfa_enrolled: false,
            },
        );
        Ok(id)
    }

    async fn lookup(&self, account_id: &AccountId) -> Result<AccountSnapshot, AuthError> {
        self.looked_up.lock().unwrap().push(*account_id);
        Ok(self.snapshots.lock().unwrap().get(account_id).cloned().unwrap_or(AccountSnapshot {
            activation: AccountActivation::Active,
            permissions: Vec::new(),
            age_bracket: None,
            mfa_enrolled: false,
        }))
    }

    async fn provision(&self, account: &super::port::NewAccount) -> Result<AccountId, AuthError> {
        // The minimum age, as `account` enforces it (13).
        let dob = chrono::NaiveDate::parse_from_str(&account.date_of_birth, "%Y-%m-%d")
            .map_err(|_| AuthError::DomainViolation { field: "date_of_birth".into(), message: "bad date".into() })?;
        let today = Utc::now().date_naive();
        let age = today.years_since(dob).unwrap_or(0);
        if age < 13 {
            return Err(AuthError::AgeBelowMinimum);
        }
        let email = account.email.as_deref().map(str::to_lowercase);
        if let Some(email) = &email
            && let Some(holder) = self.emails.lock().unwrap().get(email)
            && holder.identity_id != account.subject.to_string()
        {
            return Err(AuthError::EmailAlreadyRegistered);
        }
        if let Some(phone) = &account.phone
            && let Some(holder) = self.emails.lock().unwrap().get(phone)
            && holder.identity_id != account.subject.to_string()
        {
            return Err(AuthError::PhoneAlreadyRegistered);
        }
        let mut subjects = self.subjects.lock().unwrap();
        let id = *subjects.entry(account.subject.clone()).or_insert_with(|| AccountId::from_uuid(Uuid::now_v7()));
        let activation = if account.email_verified || account.phone_verified {
            AccountActivation::Active
        } else {
            AccountActivation::Inactive { reason: "pending_verification".into() }
        };
        self.snapshots
            .lock()
            .unwrap()
            .insert(id, AccountSnapshot { activation, permissions: Vec::new(), age_bracket: None, mfa_enrolled: false });
        // Emails and numbers share one index here (a number never looks like an email).
        for key in email.into_iter().chain(account.phone.clone()) {
            self.emails.lock().unwrap().insert(
                key,
                super::port::EmailHolder { account_id: id, identity_id: account.subject.to_string() },
            );
        }
        self.provisioned.lock().unwrap().push(account.clone());
        Ok(id)
    }

    async fn find_by_email(&self, email: &str) -> Result<Option<super::port::EmailHolder>, AuthError> {
        Ok(self.emails.lock().unwrap().get(&email.to_lowercase()).cloned())
    }

    async fn find_by_phone(&self, phone: &str) -> Result<Option<super::port::EmailHolder>, AuthError> {
        Ok(self.emails.lock().unwrap().get(phone).cloned())
    }

    async fn contact(&self, account_id: &AccountId) -> Result<super::port::ContactDetails, AuthError> {
        Ok(self.contact_of(account_id))
    }

    async fn change_contact(
        &self,
        account_id: &AccountId,
        channel: super::port::VerificationChannel,
        destination: &str,
    ) -> Result<(), AuthError> {
        if self.refuse_contact_changes.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(AuthError::EmailAlreadyRegistered);
        }
        let key = destination.to_lowercase();
        if let Some(holder) = self.emails.lock().unwrap().get(&key)
            && holder.account_id != *account_id
        {
            return Err(match channel {
                super::port::VerificationChannel::Email => AuthError::EmailAlreadyRegistered,
                super::port::VerificationChannel::Sms => AuthError::PhoneAlreadyRegistered,
            });
        }
        let mut contacts = self.contacts.lock().unwrap();
        let contact = contacts.entry(*account_id).or_default();
        match channel {
            super::port::VerificationChannel::Email => contact.email = Some(destination.to_owned()),
            super::port::VerificationChannel::Sms => contact.phone = Some(destination.to_owned()),
        }
        Ok(())
    }

    async fn export_link(&self, account_id: &AccountId) -> Result<Option<(String, DateTime<Utc>)>, AuthError> {
        Ok(self.export_links.lock().unwrap().get(account_id).cloned())
    }

    async fn mfa_secret(&self, account_id: &AccountId) -> Result<super::port::MfaSecret, AuthError> {
        Ok(match self.mfa.lock().unwrap().get(account_id) {
            Some((sealed, codes)) => super::port::MfaSecret {
                enrolled: true,
                sealed_seed: sealed.clone(),
                recovery_codes_remaining: codes.len() as u32,
            },
            None => super::port::MfaSecret::default(),
        })
    }

    async fn consume_recovery_code(&self, account_id: &AccountId, code_hash: &str) -> Result<bool, AuthError> {
        let mut mfa = self.mfa.lock().unwrap();
        let Some((_, codes)) = mfa.get_mut(account_id) else { return Ok(false) };
        Ok(match codes.iter().position(|c| c == code_hash) {
            Some(at) => {
                codes.remove(at);
                true
            }
            None => false,
        })
    }

    async fn enroll_mfa(&self, account_id: &AccountId, sealed_seed: &[u8], code_hashes: &[String]) -> Result<(), AuthError> {
        if self.mfa.lock().unwrap().contains_key(account_id) {
            return Err(AuthError::MfaAlreadyEnabled);
        }
        self.with_mfa(*account_id, sealed_seed.to_vec(), code_hashes.to_vec());
        Ok(())
    }

    async fn revoke_mfa(&self, account_id: &AccountId) -> Result<(), AuthError> {
        if self.mfa.lock().unwrap().remove(account_id).is_none() {
            return Err(AuthError::MfaNotEnabled);
        }
        if let Some(snapshot) = self.snapshots.lock().unwrap().get_mut(account_id) {
            snapshot.mfa_enrolled = false;
        }
        Ok(())
    }

    async fn replace_recovery_codes(&self, account_id: &AccountId, code_hashes: &[String]) -> Result<(), AuthError> {
        match self.mfa.lock().unwrap().get_mut(account_id) {
            Some((_, codes)) => {
                *codes = code_hashes.to_vec();
                Ok(())
            }
            None => Err(AuthError::MfaNotEnabled),
        }
    }

    async fn resume_deactivated(&self, account_id: &AccountId) -> Result<(), AuthError> {
        if self.refuse_resume.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(AuthError::AccountNotActive { current: "not_resumable".into() });
        }
        self.resumed.lock().unwrap().push(*account_id);
        if let Some(snapshot) = self.snapshots.lock().unwrap().get_mut(account_id) {
            snapshot.activation = AccountActivation::Active;
        }
        Ok(())
    }
}

// ─── ProfileDirectory ────────────────────────────────────────────────────────

/// Profiles per account; `failing()` simulates a `profile` outage (the handlers
/// must degrade to an empty `pids` claim, never fail the mint).
pub struct StubProfileDirectory {
    profiles: Mutex<HashMap<AccountId, Vec<ProfileId>>>,
    failing: bool,
}

impl Default for StubProfileDirectory {
    fn default() -> Self {
        Self::new()
    }
}

impl StubProfileDirectory {
    pub fn new() -> Self {
        Self { profiles: Mutex::new(HashMap::new()), failing: false }
    }

    pub fn failing() -> Self {
        Self { profiles: Mutex::new(HashMap::new()), failing: true }
    }

    pub fn with_profiles(&self, account_id: AccountId, ids: Vec<ProfileId>) {
        self.profiles.lock().unwrap().insert(account_id, ids);
    }
}

#[async_trait]
impl ProfileDirectory for StubProfileDirectory {
    async fn list_profile_ids(&self, account_id: &AccountId) -> Result<Vec<ProfileId>, AuthError> {
        if self.failing {
            return Err(AuthError::ProfileDirectoryUnavailable);
        }
        Ok(self.profiles.lock().unwrap().get(account_id).cloned().unwrap_or_default())
    }
}

// ─── SubjectLinkRepository ───────────────────────────────────────────────────

pub struct InMemorySubjectLinkRepository {
    links: Mutex<HashMap<IdpSubject, SubjectLink>>,
}

impl Default for InMemorySubjectLinkRepository {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemorySubjectLinkRepository {
    pub fn new() -> Self {
        Self { links: Mutex::new(HashMap::new()) }
    }
}

#[async_trait]
impl SubjectLinkRepository for InMemorySubjectLinkRepository {
    async fn find_by_subject(
        &self,
        subject: &IdpSubject,
    ) -> Result<Option<SubjectLink>, AuthError> {
        Ok(self.links.lock().unwrap().get(subject).cloned())
    }

    async fn find_by_account(&self, account_id: &AccountId) -> Result<Vec<SubjectLink>, AuthError> {
        Ok(self.links.lock().unwrap().values().filter(|l| l.account_id() == *account_id).cloned().collect())
    }

    async fn delete(&self, subject: &IdpSubject) -> Result<(), AuthError> {
        self.links.lock().unwrap().remove(subject);
        Ok(())
    }

    async fn save(&self, link: &SubjectLink) -> Result<(), AuthError> {
        let mut links = self.links.lock().unwrap();
        if links.contains_key(link.subject()) {
            return Err(AuthError::SubjectAlreadyLinked {
                iss: link.subject().issuer().to_owned(),
                sub: link.subject().subject().to_owned(),
            });
        }
        // Persistence does not round-trip pending events.
        let mut stored = link.clone();
        let _ = stored.drain_events();
        links.insert(stored.subject().clone(), stored);
        Ok(())
    }
}

// ─── SessionRepository ───────────────────────────────────────────────────────

pub struct InMemorySessionRepository {
    sessions: Mutex<HashMap<SessionId, Session>>,
}

impl Default for InMemorySessionRepository {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemorySessionRepository {
    pub fn new() -> Self {
        Self { sessions: Mutex::new(HashMap::new()) }
    }

    pub fn count(&self) -> usize {
        self.sessions.lock().unwrap().len()
    }
}

#[async_trait]
impl SessionRepository for InMemorySessionRepository {
    async fn save(&self, session: &Session) -> Result<(), AuthError> {
        let mut stored = session.clone();
        let _ = stored.drain_events(); // events do not survive persistence
        self.sessions.lock().unwrap().insert(stored.id(), stored);
        Ok(())
    }

    async fn find_by_id(&self, id: &SessionId) -> Result<Option<Session>, AuthError> {
        Ok(self.sessions.lock().unwrap().get(id).cloned())
    }

    async fn list_active_by_account(
        &self,
        account_id: &AccountId,
    ) -> Result<Vec<Session>, AuthError> {
        Ok(self
            .sessions
            .lock()
            .unwrap()
            .values()
            .filter(|s| s.account_id() == *account_id && s.status() == SessionStatus::Active)
            .cloned()
            .collect())
    }

    async fn device_history(
        &self,
        account_id: &AccountId,
        device_id: Option<&str>,
    ) -> Result<super::port::DeviceHistory, AuthError> {
        let sessions = self.sessions.lock().unwrap();
        let mut mine: Vec<&Session> = sessions.values().filter(|s| s.account_id() == *account_id).collect();
        mine.sort_by_key(|s| std::cmp::Reverse(s.issued_at()));
        Ok(super::port::DeviceHistory {
            any_session: !mine.is_empty(),
            seen_device: device_id.is_some_and(|id| mine.iter().any(|s| s.device().device_id() == Some(id))),
            recent_without_device_id: !mine.is_empty()
                && mine.iter().take(super::port::RECENT_SESSIONS as usize).all(|s| s.device().device_id().is_none()),
        })
    }
}

// ─── RefreshTokenRepository ──────────────────────────────────────────────────

pub struct InMemoryRefreshTokenRepository {
    tokens: Mutex<HashMap<RefreshTokenHash, RefreshToken>>,
}

impl Default for InMemoryRefreshTokenRepository {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryRefreshTokenRepository {
    pub fn new() -> Self {
        Self { tokens: Mutex::new(HashMap::new()) }
    }
}

#[async_trait]
impl RefreshTokenRepository for InMemoryRefreshTokenRepository {
    async fn save(&self, token: &RefreshToken) -> Result<(), AuthError> {
        self.tokens.lock().unwrap().insert(token.token_hash().clone(), token.clone());
        Ok(())
    }

    async fn find_by_hash(
        &self,
        hash: &RefreshTokenHash,
    ) -> Result<Option<RefreshToken>, AuthError> {
        Ok(self.tokens.lock().unwrap().get(hash).cloned())
    }

    async fn revoke_all_for_session(&self, session_id: &SessionId) -> Result<(), AuthError> {
        // Modelled as deletion: a subsequent lookup misses, i.e. is invalid.
        self.tokens.lock().unwrap().retain(|_, t| t.session_id() != *session_id);
        Ok(())
    }
}

// ─── SessionCache ────────────────────────────────────────────────────────────

pub struct InMemorySessionCache {
    generations: Mutex<HashMap<AccountId, Generation>>,
    blacklist: Mutex<HashSet<SessionId>>,
}

impl Default for InMemorySessionCache {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemorySessionCache {
    pub fn new() -> Self {
        Self { generations: Mutex::new(HashMap::new()), blacklist: Mutex::new(HashSet::new()) }
    }
}

#[async_trait]
impl SessionCache for InMemorySessionCache {
    async fn current_generation(&self, account_id: &AccountId) -> Result<Generation, AuthError> {
        Ok(self.generations.lock().unwrap().get(account_id).copied().unwrap_or(Generation::INITIAL))
    }

    async fn bump_generation(&self, account_id: &AccountId) -> Result<Generation, AuthError> {
        let mut gens = self.generations.lock().unwrap();
        let next = gens.get(account_id).copied().unwrap_or(Generation::INITIAL).next();
        gens.insert(*account_id, next);
        Ok(next)
    }

    async fn blacklist_session(
        &self,
        session_id: &SessionId,
        _ttl: Duration,
    ) -> Result<(), AuthError> {
        self.blacklist.lock().unwrap().insert(*session_id);
        Ok(())
    }

    async fn is_blacklisted(&self, session_id: &SessionId) -> Result<bool, AuthError> {
        Ok(self.blacklist.lock().unwrap().contains(session_id))
    }
}

// ─── TokenMinter ─────────────────────────────────────────────────────────────

pub struct StubTokenMinter {
    issued: Mutex<HashMap<String, AccessTokenClaims>>,
}

impl Default for StubTokenMinter {
    fn default() -> Self {
        Self::new()
    }
}

impl StubTokenMinter {
    pub fn new() -> Self {
        Self { issued: Mutex::new(HashMap::new()) }
    }
}

#[async_trait]
impl TokenMinter for StubTokenMinter {
    async fn mint_access(&self, claims: &AccessTokenClaims) -> Result<String, AuthError> {
        let token = format!("access-{}", Uuid::now_v7());
        self.issued.lock().unwrap().insert(token.clone(), claims.clone());
        Ok(token)
    }

    async fn verify_access(&self, token: &str) -> Result<AccessTokenClaims, AuthError> {
        self.issued
            .lock()
            .unwrap()
            .get(token)
            .cloned()
            .ok_or(AuthError::IdpTokenRejected)
    }

    fn generate_refresh(&self) -> Result<GeneratedRefresh, AuthError> {
        let plaintext = Uuid::now_v7().to_string();
        let hash = self.hash_refresh(&plaintext)?;
        Ok(GeneratedRefresh { plaintext, hash })
    }

    fn hash_refresh(&self, plaintext: &str) -> Result<RefreshTokenHash, AuthError> {
        // Deterministic so generate→store and present→lookup agree.
        RefreshTokenHash::new(format!("h:{plaintext}"))
    }
}

// ─── GuestRegistry ───────────────────────────────────────────────────────────

#[derive(Default)]
pub struct InMemoryGuestRegistry {
    pub records: Mutex<Vec<super::port::GuestRecord>>,
    /// (guest, account) upgrades recorded.
    pub upgrades: Mutex<Vec<(AccountId, AccountId)>>,
}

#[async_trait]
impl super::port::GuestRegistry for InMemoryGuestRegistry {
    async fn record(&self, guest: &super::port::GuestRecord) -> Result<(), AuthError> {
        let mut records = self.records.lock().unwrap();
        if !records.iter().any(|r| r.guest_id == guest.guest_id) {
            records.push(guest.clone());
        }
        Ok(())
    }

    async fn mark_upgraded(
        &self,
        guest_id: &AccountId,
        account_id: &AccountId,
        _at: DateTime<Utc>,
    ) -> Result<(), AuthError> {
        self.upgrades.lock().unwrap().push((*guest_id, *account_id));
        Ok(())
    }

    async fn purge_stale(
        &self,
        seen_before: DateTime<Utc>,
        _now: DateTime<Utc>,
        limit: i64,
    ) -> Result<u64, AuthError> {
        let upgraded: Vec<AccountId> = self.upgrades.lock().unwrap().iter().map(|(g, _)| *g).collect();
        let mut records = self.records.lock().unwrap();
        let before = records.len();
        let mut left = limit;
        records.retain(|r| {
            let stale = left > 0 && r.first_seen_at < seen_before && !upgraded.contains(&r.guest_id);
            if stale {
                left -= 1;
            }
            !stale
        });
        Ok((before - records.len()) as u64)
    }
}

// ─── EventPublisher ──────────────────────────────────────────────────────────

pub struct RecordingEventPublisher {
    events: Mutex<Vec<DomainEvent>>,
}

impl Default for RecordingEventPublisher {
    fn default() -> Self {
        Self::new()
    }
}

impl RecordingEventPublisher {
    pub fn new() -> Self {
        Self { events: Mutex::new(Vec::new()) }
    }

    pub fn event_types(&self) -> Vec<&'static str> {
        self.events.lock().unwrap().iter().map(|e| e.event_type()).collect()
    }

    pub fn count(&self) -> usize {
        self.events.lock().unwrap().len()
    }
}

#[async_trait]
impl EventPublisher for RecordingEventPublisher {
    async fn publish(&self, event: &DomainEvent) -> Result<(), AuthError> {
        self.events.lock().unwrap().push(event.clone());
        Ok(())
    }
}

// ─── CredentialAdmin ─────────────────────────────────────────────────────────

/// Knows every subject as `user`; records each password set, or refuses them
/// all with a policy reason.
#[derive(Default)]
pub struct StubCredentialAdmin {
    set: Mutex<Vec<(IdpSubject, String)>>,
    refuse: Mutex<Option<String>>,
    deleted: Mutex<Vec<IdpSubject>>,
    idp_down: std::sync::atomic::AtomicBool,
    /// The emails set, per subject (#651).
    emails: Mutex<Vec<(IdpSubject, String)>>,
}

impl StubCredentialAdmin {
    /// The passwords set so far, per subject.
    pub fn passwords_set(&self) -> Vec<(IdpSubject, String)> {
        self.set.lock().unwrap().clone()
    }

    /// The emails set so far, per subject.
    pub fn emails_set(&self) -> Vec<(IdpSubject, String)> {
        self.emails.lock().unwrap().clone()
    }

    /// Refuse every new password as the IdP policy would.
    pub fn refuse_with(&self, reason: &str) {
        *self.refuse.lock().unwrap() = Some(reason.to_owned());
    }

    /// The IdP users deleted so far.
    pub fn deleted_users(&self) -> Vec<IdpSubject> {
        self.deleted.lock().unwrap().clone()
    }

    /// Makes the IdP unreachable (or reachable again).
    pub fn idp_down(&self, down: bool) {
        self.idp_down.store(down, std::sync::atomic::Ordering::SeqCst);
    }
}

#[async_trait]
impl CredentialAdmin for StubCredentialAdmin {
    async fn login_name(&self, _subject: &IdpSubject) -> Result<String, AuthError> {
        Ok("user".to_owned())
    }

    async fn set_password(&self, subject: &IdpSubject, new_password: &str) -> Result<(), AuthError> {
        if let Some(reason) = self.refuse.lock().unwrap().clone() {
            return Err(AuthError::PasswordRejected { reason });
        }
        self.set.lock().unwrap().push((subject.clone(), new_password.to_owned()));
        Ok(())
    }

    async fn delete_user(&self, subject: &IdpSubject) -> Result<(), AuthError> {
        if self.idp_down.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(AuthError::IdpUnavailable);
        }
        self.deleted.lock().unwrap().push(subject.clone());
        Ok(())
    }

    async fn set_email(&self, subject: &IdpSubject, email: &str) -> Result<(), AuthError> {
        self.emails.lock().unwrap().push((subject.clone(), email.to_owned()));
        Ok(())
    }
}

// ─── Two-step sign-in (#649) ─────────────────────────────────────────────────

/// A cipher for tests: "seals" by prefixing, hashes by tagging. `unkeyed()`
/// is a deployment without the seed key.
pub struct FakeSeedCipher {
    keyed: bool,
}

impl Default for FakeSeedCipher {
    fn default() -> Self {
        Self { keyed: true }
    }
}

impl FakeSeedCipher {
    pub fn unkeyed() -> Self {
        Self { keyed: false }
    }
}

impl super::port::MfaSeedCipher for FakeSeedCipher {
    fn seal(&self, seed: &[u8]) -> Result<Vec<u8>, AuthError> {
        if !self.keyed {
            return Err(AuthError::MfaUnavailable);
        }
        Ok([b"sealed:".as_slice(), seed].concat())
    }

    fn open(&self, sealed: &[u8]) -> Result<Vec<u8>, AuthError> {
        match (self.keyed, sealed.strip_prefix(b"sealed:".as_slice())) {
            (true, Some(seed)) => Ok(seed.to_vec()),
            _ => Err(AuthError::MfaUnavailable),
        }
    }

    fn code_hash(&self, normalized_code: &str) -> Result<String, AuthError> {
        if !self.keyed {
            return Err(AuthError::MfaUnavailable);
        }
        Ok(format!("hash:{normalized_code}"))
    }
}

/// Two-step state in memory (TTLs ignored, failures never expire).
#[derive(Default)]
pub struct InMemoryMfaStore {
    pending: Mutex<HashMap<String, super::port::PendingLogin>>,
    enrolments: Mutex<HashMap<AccountId, Vec<u8>>>,
    steps: Mutex<std::collections::HashSet<(AccountId, i64)>>,
    failures: Mutex<HashMap<AccountId, u32>>,
}

#[async_trait]
impl super::port::MfaStore for InMemoryMfaStore {
    async fn save_pending_login(
        &self,
        token_hash: &str,
        login: &super::port::PendingLogin,
        _ttl_secs: u64,
    ) -> Result<(), AuthError> {
        self.pending.lock().unwrap().insert(token_hash.to_owned(), login.clone());
        Ok(())
    }

    async fn pending_login(&self, token_hash: &str) -> Result<Option<super::port::PendingLogin>, AuthError> {
        Ok(self.pending.lock().unwrap().get(token_hash).cloned())
    }

    async fn take_pending_login(&self, token_hash: &str) -> Result<Option<super::port::PendingLogin>, AuthError> {
        Ok(self.pending.lock().unwrap().remove(token_hash))
    }

    async fn claim_step(&self, account: &AccountId, step: i64, _ttl_secs: u64) -> Result<bool, AuthError> {
        Ok(self.steps.lock().unwrap().insert((*account, step)))
    }

    /// One increment under the lock: atomic, like the Redis script.
    async fn reserve_attempt(&self, account: &AccountId, window_secs: u64) -> Result<(u32, i64), AuthError> {
        let mut failures = self.failures.lock().unwrap();
        let n = failures.entry(*account).or_insert(0);
        *n += 1;
        Ok((*n, window_secs as i64))
    }

    async fn clear_failures(&self, account: &AccountId) -> Result<(), AuthError> {
        self.failures.lock().unwrap().remove(account);
        Ok(())
    }

    async fn save_pending_enrollment(&self, account: &AccountId, sealed_seed: &[u8], _ttl_secs: u64) -> Result<(), AuthError> {
        self.enrolments.lock().unwrap().insert(*account, sealed_seed.to_vec());
        Ok(())
    }

    async fn pending_enrollment(&self, account: &AccountId) -> Result<Option<Vec<u8>>, AuthError> {
        Ok(self.enrolments.lock().unwrap().get(account).cloned())
    }

    async fn discard_pending_enrollment(&self, account: &AccountId) -> Result<(), AuthError> {
        self.enrolments.lock().unwrap().remove(account);
        Ok(())
    }
}

// ─── Fixture ─────────────────────────────────────────────────────────────────

/// Bundles concrete fakes and builds handlers wired to them. Handlers receive
/// the fakes as `Arc<dyn Port>`; tests keep the concrete `Arc`s to assert on
/// recorded state (published events, generation, stored sessions).
pub struct Fixture {
    pub idp: Arc<StubIdentityProvider>,
    pub credentials: Arc<StubCredentialAdmin>,
    pub directory: Arc<StubAccountDirectory>,
    pub profiles: Arc<StubProfileDirectory>,
    pub links: Arc<InMemorySubjectLinkRepository>,
    pub sessions: Arc<InMemorySessionRepository>,
    pub refresh_tokens: Arc<InMemoryRefreshTokenRepository>,
    pub cache: Arc<InMemorySessionCache>,
    pub minter: Arc<StubTokenMinter>,
    pub publisher: Arc<RecordingEventPublisher>,
    pub guests: Arc<InMemoryGuestRegistry>,
    pub policy: SessionPolicy,
    /// Two-step sign-in (#649): the cipher seals with a prefix; turn it on for
    /// an account with [`Fixture::enroll_mfa`].
    pub mfa_cipher: Arc<FakeSeedCipher>,
    pub mfa_store: Arc<InMemoryMfaStore>,
    pub mfa: Arc<super::command::MfaVerifier>,
}

impl Default for Fixture {
    fn default() -> Self {
        Self::new()
    }
}

impl Fixture {
    /// Default: IdP returns `(iss, sub)`, directory auto-provisions active accounts.
    pub fn new() -> Self {
        let directory = Arc::new(StubAccountDirectory::new());
        let (mfa_cipher, mfa_store) = (Arc::new(FakeSeedCipher::default()), Arc::new(InMemoryMfaStore::default()));
        let mfa = Arc::new(super::command::MfaVerifier::new(
            Arc::clone(&directory) as _,
            Arc::clone(&mfa_cipher) as _,
            Arc::clone(&mfa_store) as _,
            super::command::MfaPolicy::default(),
        ));
        Self {
            idp: Arc::new(StubIdentityProvider::returning("https://idp.test", "sub-123")),
            credentials: Arc::new(StubCredentialAdmin::default()),
            directory,
            profiles: Arc::new(StubProfileDirectory::new()),
            links: Arc::new(InMemorySubjectLinkRepository::new()),
            sessions: Arc::new(InMemorySessionRepository::new()),
            refresh_tokens: Arc::new(InMemoryRefreshTokenRepository::new()),
            cache: Arc::new(InMemorySessionCache::new()),
            minter: Arc::new(StubTokenMinter::new()),
            publisher: Arc::new(RecordingEventPublisher::new()),
            guests: Arc::new(InMemoryGuestRegistry::default()),
            policy: SessionPolicy::test_default(),
            mfa_cipher,
            mfa_store,
            mfa,
        }
    }

    /// Turns two-step sign-in on for `account`: returns its TOTP seed; its
    /// backup codes are `abcde-fghjk` and `mnpqr-stuvw`.
    pub fn enroll_mfa(&self, account: AccountId) -> crate::domain::value_object::TotpSecret {
        use super::port::MfaSeedCipher;
        let seed = crate::domain::value_object::TotpSecret::generate();
        self.directory.with_mfa(
            account,
            self.mfa_cipher.seal(seed.as_bytes()).unwrap(),
            vec![self.mfa_cipher.code_hash("abcdefghjk").unwrap(), self.mfa_cipher.code_hash("mnpqrstuvw").unwrap()],
        );
        seed
    }

    pub fn start_guest_handler(&self) -> super::command::StartGuestSessionHandler {
        super::command::StartGuestSessionHandler::new(
            Arc::clone(&self.sessions) as _,
            Arc::clone(&self.refresh_tokens) as _,
            Arc::clone(&self.cache) as _,
            Arc::clone(&self.minter) as _,
            Arc::clone(&self.guests) as _,
            self.policy.clone(),
            true,
        )
    }

    pub fn login_handler(&self) -> super::command::LoginHandler {
        super::command::LoginHandler::new(
            Arc::clone(&self.idp) as _,
            Arc::clone(&self.directory) as _,
            Arc::clone(&self.profiles) as _,
            Arc::clone(&self.links) as _,
            Arc::clone(&self.sessions) as _,
            Arc::clone(&self.refresh_tokens) as _,
            Arc::clone(&self.cache) as _,
            Arc::clone(&self.minter) as _,
            Arc::clone(&self.publisher) as _,
            self.policy.clone(),
        )
        .with_mfa(Arc::clone(&self.mfa))
    }

    pub fn refresh_handler(&self) -> super::command::RefreshHandler {
        super::command::RefreshHandler::new(
            Arc::clone(&self.directory) as _,
            Arc::clone(&self.profiles) as _,
            Arc::clone(&self.sessions) as _,
            Arc::clone(&self.refresh_tokens) as _,
            Arc::clone(&self.cache) as _,
            Arc::clone(&self.minter) as _,
            Arc::clone(&self.publisher) as _,
            self.policy.clone(),
        )
    }

    pub fn logout_handler(&self) -> super::command::LogoutHandler {
        super::command::LogoutHandler::new(
            Arc::clone(&self.sessions) as _,
            Arc::clone(&self.refresh_tokens) as _,
            Arc::clone(&self.cache) as _,
            Arc::clone(&self.publisher) as _,
            self.policy.clone(),
        )
    }

    pub fn logout_all_handler(&self) -> super::command::LogoutAllSessionsHandler {
        super::command::LogoutAllSessionsHandler::new(
            Arc::clone(&self.sessions) as _,
            Arc::clone(&self.refresh_tokens) as _,
            Arc::clone(&self.cache) as _,
            Arc::clone(&self.publisher) as _,
            self.policy.clone(),
        )
    }

    pub fn introspect_handler(&self) -> super::query::IntrospectHandler {
        super::query::IntrospectHandler::new(
            Arc::clone(&self.minter) as _,
            Arc::clone(&self.cache) as _,
        )
    }

    pub fn list_sessions_handler(&self) -> super::query::ListSessionsHandler {
        super::query::ListSessionsHandler::new(Arc::clone(&self.sessions) as _)
    }

    pub fn change_password_handler(&self) -> super::command::ChangePasswordHandler {
        super::command::ChangePasswordHandler::new(
            Arc::clone(&self.idp) as _,
            Arc::clone(&self.credentials) as _,
            Arc::clone(&self.sessions) as _,
            Arc::clone(&self.refresh_tokens) as _,
            Arc::clone(&self.cache) as _,
            Arc::clone(&self.publisher) as _,
            self.policy.clone(),
        )
    }

    pub fn verify_credentials_handler(&self) -> super::command::VerifyCredentialsHandler {
        super::command::VerifyCredentialsHandler::new(
            Arc::clone(&self.idp) as _,
            Arc::clone(&self.credentials) as _,
            Arc::clone(&self.directory) as _,
            Arc::clone(&self.profiles) as _,
            Arc::clone(&self.sessions) as _,
            Arc::clone(&self.cache) as _,
            Arc::clone(&self.minter) as _,
            self.policy.clone(),
        )
        .with_mfa(Arc::clone(&self.mfa))
    }
}

// ─── One-time codes ──────────────────────────────────────────────────────────

struct StoredChallenge {
    challenge: super::port::PendingChallenge,
    attempts_left: u32,
}

/// Challenges in memory (no expiry) and a per-address send counter.
#[derive(Default)]
pub struct InMemoryVerificationStore {
    challenges: Mutex<HashMap<String, StoredChallenge>>,
    sends: Mutex<HashMap<String, u32>>,
    failures: Mutex<HashMap<String, u32>>,
    sms_today: Mutex<HashMap<String, u32>>,
}

impl InMemoryVerificationStore {
    /// SMS counted against today's service budget.
    pub fn sms_sent_today(&self) -> u32 {
        self.sms_today.lock().unwrap().values().sum()
    }

    pub fn len(&self) -> usize {
        self.challenges.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[async_trait]
impl super::port::VerificationStore for InMemoryVerificationStore {
    async fn admit_send(
        &self,
        destination_key: &str,
        limits: super::port::SendLimits,
    ) -> Result<super::port::SendAdmission, AuthError> {
        let mut sends = self.sends.lock().unwrap();
        let n = sends.entry(destination_key.to_owned()).or_default();
        *n += 1;
        Ok(if *n > limits.per_hour.min(limits.per_day) {
            super::port::SendAdmission::Refused { retry_after_secs: 3600 }
        } else {
            super::port::SendAdmission::Allowed
        })
    }

    async fn save(&self, challenge: &super::port::PendingChallenge, _ttl: Duration, max_attempts: u32) -> Result<(), AuthError> {
        self.challenges.lock().unwrap().insert(
            challenge.challenge_id.clone(),
            StoredChallenge { challenge: challenge.clone(), attempts_left: max_attempts },
        );
        Ok(())
    }

    async fn record_failure(&self, destination_key: &str, _window: Duration) -> Result<u32, AuthError> {
        let mut failures = self.failures.lock().unwrap();
        let n = failures.entry(destination_key.to_owned()).or_default();
        *n += 1;
        Ok(*n)
    }

    async fn failures(&self, destination_key: &str) -> Result<(u32, i64), AuthError> {
        Ok((self.failures.lock().unwrap().get(destination_key).copied().unwrap_or(0), 86_400))
    }

    async fn consume(&self, challenge_id: &str, code_hash: &str) -> Result<super::port::ConsumeOutcome, AuthError> {
        let mut challenges = self.challenges.lock().unwrap();
        let Some(stored) = challenges.get_mut(challenge_id) else { return Ok(super::port::ConsumeOutcome::Unknown) };
        let destination_key = stored.challenge.destination_key.clone();
        if stored.challenge.code_hash == code_hash {
            let stored = challenges.remove(challenge_id).unwrap();
            return Ok(super::port::ConsumeOutcome::Verified {
                destination: super::port::VerifiedDestination {
                    channel: stored.challenge.channel,
                    destination: stored.challenge.destination,
                },
                destination_key,
            });
        }
        let (stored_channel, stored_destination, stored_locale) =
            (stored.challenge.channel, stored.challenge.destination.clone(), stored.challenge.locale.clone());
        stored.attempts_left = stored.attempts_left.saturating_sub(1);
        if stored.attempts_left == 0 {
            challenges.remove(challenge_id);
        }
        Ok(super::port::ConsumeOutcome::Miss {
            destination_key,
            destination: super::port::VerifiedDestination {
                channel: stored_channel,
                destination: stored_destination,
            },
            locale: stored_locale,
        })
    }

    async fn discard(&self, challenge_id: &str) -> Result<(), AuthError> {
        self.challenges.lock().unwrap().remove(challenge_id);
        Ok(())
    }

    async fn reserve_sms(
        &self,
        country: &str,
        budget: super::port::SmsBudget,
    ) -> Result<super::port::SmsReservation, AuthError> {
        let mut sent = self.sms_today.lock().unwrap();
        if sent.get(country).copied().unwrap_or(0) >= budget.country_daily {
            return Ok(super::port::SmsReservation::CountryExhausted);
        }
        if sent.values().sum::<u32>() >= budget.daily {
            return Ok(super::port::SmsReservation::Exhausted);
        }
        *sent.entry(country.to_owned()).or_default() += 1;
        Ok(super::port::SmsReservation::Reserved)
    }

    async fn refund_sms(&self, country: &str) -> Result<(), AuthError> {
        if let Some(n) = self.sms_today.lock().unwrap().get_mut(country) {
            *n = n.saturating_sub(1);
        }
        Ok(())
    }
}

/// Records every code "sent"; `fail()` makes later sends fail.
#[derive(Default)]
pub struct RecordingCodeSender {
    sent: Mutex<Vec<(String, String, Option<String>)>>,
    notices: Mutex<Vec<(String, Option<String>)>>,
    /// (changed channel, email told) of every contact-changed notice (#651).
    contact_notices: Mutex<Vec<(super::port::VerificationChannel, String)>>,
    /// (email told, device, ip) of every new-sign-in notice (#649).
    login_notices: Mutex<Vec<LoginNotice>>,
    /// (email told, change) of every two-step change notice (#649).
    mfa_notices: Mutex<Vec<(String, super::port::MfaChange)>>,
    /// (email told, link) of every export-ready notice (#653).
    export_notices: Mutex<Vec<(String, String)>>,
    failing: std::sync::atomic::AtomicBool,
}

/// (email told, device, ip) of a new-sign-in notice.
pub type LoginNotice = (String, Option<String>, Option<String>);

impl RecordingCodeSender {
    /// The last (destination, code, locale) sent.
    pub fn last(&self) -> Option<(String, String, Option<String>)> {
        self.sent.lock().unwrap().last().cloned()
    }

    pub fn fail(&self) {
        self.failing.store(true, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn recover(&self) {
        self.failing.store(false, std::sync::atomic::Ordering::SeqCst);
    }

    /// The (email told, link) of every export-ready notice.
    pub fn export_notices(&self) -> Vec<(String, String)> {
        self.export_notices.lock().unwrap().clone()
    }

    /// The (email told, change) of every two-step change notice.
    pub fn mfa_notices(&self) -> Vec<(String, super::port::MfaChange)> {
        self.mfa_notices.lock().unwrap().clone()
    }

    /// The (email told, device, ip) of every new-sign-in notice.
    pub fn login_notices(&self) -> Vec<LoginNotice> {
        self.login_notices.lock().unwrap().clone()
    }

    /// The (changed channel, email told) of every contact-changed notice.
    pub fn contact_notices(&self) -> Vec<(super::port::VerificationChannel, String)> {
        self.contact_notices.lock().unwrap().clone()
    }

    /// The (destination, locale) of every lockout notice sent.
    pub fn notices(&self) -> Vec<(String, Option<String>)> {
        self.notices.lock().unwrap().clone()
    }
}

#[async_trait]
impl super::port::CodeSender for RecordingCodeSender {
    async fn send(
        &self,
        _channel: super::port::VerificationChannel,
        destination: &str,
        code: &str,
        locale: Option<&str>,
    ) -> Result<(), AuthError> {
        if self.failing.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(AuthError::VerificationSendFailed);
        }
        self.sent.lock().unwrap().push((destination.to_owned(), code.to_owned(), locale.map(str::to_owned)));
        Ok(())
    }

    async fn send_lockout_notice(
        &self,
        _channel: super::port::VerificationChannel,
        destination: &str,
        locale: Option<&str>,
    ) -> Result<(), AuthError> {
        self.notices.lock().unwrap().push((destination.to_owned(), locale.map(str::to_owned)));
        Ok(())
    }

    async fn send_contact_changed_notice(
        &self,
        changed: super::port::VerificationChannel,
        email: &str,
        _locale: Option<&str>,
    ) -> Result<(), AuthError> {
        self.contact_notices.lock().unwrap().push((changed, email.to_owned()));
        Ok(())
    }

    async fn send_new_login_notice(
        &self,
        email: &str,
        device: Option<&str>,
        ip: Option<&str>,
        _locale: Option<&str>,
    ) -> Result<(), AuthError> {
        self.login_notices.lock().unwrap().push((email.to_owned(), device.map(str::to_owned), ip.map(str::to_owned)));
        Ok(())
    }

    async fn send_mfa_changed_notice(
        &self,
        email: &str,
        change: super::port::MfaChange,
        _locale: Option<&str>,
    ) -> Result<(), AuthError> {
        self.mfa_notices.lock().unwrap().push((email.to_owned(), change));
        Ok(())
    }

    async fn send_export_ready_notice(
        &self,
        email: &str,
        link: &str,
        _expires_at: DateTime<Utc>,
        _locale: Option<&str>,
    ) -> Result<(), AuthError> {
        self.export_notices.lock().unwrap().push((email.to_owned(), link.to_owned()));
        Ok(())
    }
}

/// Server-issued sign-in nonces, in memory.
#[derive(Default)]
pub struct InMemoryNonceStore {
    issued: Mutex<std::collections::HashSet<String>>,
}

#[async_trait]
impl super::port::FederatedNonceStore for InMemoryNonceStore {
    async fn issue(&self, nonce_hash: &str, _ttl: Duration) -> Result<(), AuthError> {
        self.issued.lock().unwrap().insert(nonce_hash.to_owned());
        Ok(())
    }

    async fn consume(&self, nonce_hash: &str) -> Result<bool, AuthError> {
        Ok(self.issued.lock().unwrap().remove(nonce_hash))
    }
}
