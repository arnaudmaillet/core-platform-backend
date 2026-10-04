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
        }
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
            .insert(account_id, AccountSnapshot { activation, permissions, age_bracket: None });
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
            .insert(id, AccountSnapshot { activation, permissions: Vec::new(), age_bracket: None });
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
}

impl StubCredentialAdmin {
    /// The passwords set so far, per subject.
    pub fn passwords_set(&self) -> Vec<(IdpSubject, String)> {
        self.set.lock().unwrap().clone()
    }

    /// Refuse every new password as the IdP policy would.
    pub fn refuse_with(&self, reason: &str) {
        *self.refuse.lock().unwrap() = Some(reason.to_owned());
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
}

impl Default for Fixture {
    fn default() -> Self {
        Self::new()
    }
}

impl Fixture {
    /// Default: IdP returns `(iss, sub)`, directory auto-provisions active accounts.
    pub fn new() -> Self {
        Self {
            idp: Arc::new(StubIdentityProvider::returning("https://idp.test", "sub-123")),
            credentials: Arc::new(StubCredentialAdmin::default()),
            directory: Arc::new(StubAccountDirectory::new()),
            profiles: Arc::new(StubProfileDirectory::new()),
            links: Arc::new(InMemorySubjectLinkRepository::new()),
            sessions: Arc::new(InMemorySessionRepository::new()),
            refresh_tokens: Arc::new(InMemoryRefreshTokenRepository::new()),
            cache: Arc::new(InMemorySessionCache::new()),
            minter: Arc::new(StubTokenMinter::new()),
            publisher: Arc::new(RecordingEventPublisher::new()),
            guests: Arc::new(InMemoryGuestRegistry::default()),
            policy: SessionPolicy::test_default(),
        }
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
    sms_today: Mutex<u32>,
}

impl InMemoryVerificationStore {
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
        stored.attempts_left = stored.attempts_left.saturating_sub(1);
        if stored.attempts_left == 0 {
            challenges.remove(challenge_id);
        }
        Ok(super::port::ConsumeOutcome::Miss { destination_key })
    }

    async fn discard(&self, challenge_id: &str) -> Result<(), AuthError> {
        self.challenges.lock().unwrap().remove(challenge_id);
        Ok(())
    }

    async fn reserve_sms(&self, daily_budget: u32) -> Result<bool, AuthError> {
        let mut sent = self.sms_today.lock().unwrap();
        *sent += 1;
        Ok(*sent <= daily_budget)
    }
}

/// Records every code "sent"; `fail()` makes later sends fail.
#[derive(Default)]
pub struct RecordingCodeSender {
    sent: Mutex<Vec<(String, String, Option<String>)>>,
    failing: std::sync::atomic::AtomicBool,
}

impl RecordingCodeSender {
    /// The last (destination, code, locale) sent.
    pub fn last(&self) -> Option<(String, String, Option<String>)> {
        self.sent.lock().unwrap().last().cloned()
    }

    pub fn fail(&self) {
        self.failing.store(true, std::sync::atomic::Ordering::SeqCst);
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
}
