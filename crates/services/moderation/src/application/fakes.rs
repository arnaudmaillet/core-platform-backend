//! In-memory fakes for every outbound port, plus a [`Fixture`] that wires them
//! into the handlers. Test-only (`#[cfg(test)]`) — they prove the application
//! layer works against the port contracts with no real backend, which is exactly
//! what makes the abstraction credible before the Phase 4 adapters exist.

// Fakes are constructed explicitly via `new()` / `Fixture::new()`; a `Default`
// impl would add noise without a caller.
#![allow(clippy::new_without_default)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use super::policy::ModerationPolicy;
use super::port::{
    AccountDirectory, AppealRepository, CaseRepository, ClassifierGateway, ContentHash,
    CorpusMatch, DecisionRepository, EnforcementProjection, EnforcementRepository, EventPublisher,
    FiledReport, PenaltyRepository, ReportCursor, ReportRepository, ScreenCorpus,
};
use crate::domain::aggregate::{Appeal, Case, Decision, EnforcementAction, PenaltyLedger, Report};
use crate::domain::event::DomainEvent;
use crate::domain::value_object::{
    ActorId, AppealId, CaseId, CaseStatus, DecisionId, EnforcementId, EnforcementStatus,
    EnforcementVersion, PolicyCategory, ReportId, ReporterKind, SubjectRef,
};
use crate::error::ModerationError;

/// A fixed reference instant for deterministic tests.
pub fn t0() -> DateTime<Utc> {
    DateTime::parse_from_rfc3339("2026-06-25T12:00:00Z").unwrap().with_timezone(&Utc)
}

// ─── CaseRepository ────────────────────────────────────────────────────────────

pub struct InMemoryCaseRepository {
    cases: Mutex<HashMap<CaseId, Case>>,
}

impl InMemoryCaseRepository {
    pub fn new() -> Self {
        Self { cases: Mutex::new(HashMap::new()) }
    }

    pub fn count(&self) -> usize {
        self.cases.lock().unwrap().len()
    }
}

#[async_trait]
impl CaseRepository for InMemoryCaseRepository {
    async fn save(&self, case: &Case) -> Result<(), ModerationError> {
        let mut stored = case.clone();
        let _ = stored.drain_events(); // events do not survive persistence
        self.cases.lock().unwrap().insert(stored.id(), stored);
        Ok(())
    }

    async fn find_by_id(&self, id: &CaseId) -> Result<Option<Case>, ModerationError> {
        Ok(self.cases.lock().unwrap().get(id).cloned())
    }

    async fn list_queue(
        &self,
        queue: &str,
        status: Option<CaseStatus>,
        limit: usize,
    ) -> Result<Vec<Case>, ModerationError> {
        Ok(self
            .cases
            .lock()
            .unwrap()
            .values()
            .filter(|c| c.queue() == queue && status.is_none_or(|s| c.status() == s))
            .take(limit)
            .cloned()
            .collect())
    }
}

// ─── DecisionRepository (append-only) ──────────────────────────────────────────

pub struct InMemoryDecisionRepository {
    decisions: Mutex<HashMap<DecisionId, Decision>>,
}

impl InMemoryDecisionRepository {
    pub fn new() -> Self {
        Self { decisions: Mutex::new(HashMap::new()) }
    }

    pub fn count(&self) -> usize {
        self.decisions.lock().unwrap().len()
    }
}

#[async_trait]
impl DecisionRepository for InMemoryDecisionRepository {
    async fn append(&self, decision: &Decision) -> Result<(), ModerationError> {
        self.decisions.lock().unwrap().insert(decision.id(), decision.clone());
        Ok(())
    }

    async fn find_by_id(&self, id: &DecisionId) -> Result<Option<Decision>, ModerationError> {
        Ok(self.decisions.lock().unwrap().get(id).cloned())
    }
}

// ─── ReportRepository ──────────────────────────────────────────────────────────

/// Joins each report to its case in `cases`, like the SQL adapter's LEFT JOIN.
pub struct InMemoryReportRepository {
    reports: Mutex<HashMap<ReportId, Report>>,
    cases: Arc<InMemoryCaseRepository>,
}

impl InMemoryReportRepository {
    pub fn new(cases: Arc<InMemoryCaseRepository>) -> Self {
        Self { reports: Mutex::new(HashMap::new()), cases }
    }

    pub fn count(&self) -> usize {
        self.reports.lock().unwrap().len()
    }
}

#[async_trait]
impl ReportRepository for InMemoryReportRepository {
    async fn record(&self, report: &Report) -> Result<(), ModerationError> {
        self.reports.lock().unwrap().entry(report.id()).or_insert_with(|| report.clone());
        Ok(())
    }

    async fn list_for_reporter(
        &self,
        kind: ReporterKind,
        reporter_id: &ActorId,
        after: Option<ReportCursor>,
        limit: usize,
    ) -> Result<Vec<FiledReport>, ModerationError> {
        let key = |r: &Report| (r.reported_at(), r.id().as_uuid());
        let mut mine: Vec<Report> = self
            .reports
            .lock()
            .unwrap()
            .values()
            .filter(|r| r.reporter_kind() == kind && r.reporter_id() == *reporter_id)
            .filter(|r| after.is_none_or(|c| key(r) < (c.reported_at, c.id.as_uuid())))
            .cloned()
            .collect();
        mine.sort_by_key(|r| std::cmp::Reverse(key(r)));
        mine.truncate(limit);
        let mut filed = Vec::with_capacity(mine.len());
        for report in mine {
            let case_status = self
                .cases
                .find_by_id(&CaseId::for_subject(report.subject()))
                .await?
                .map(|c| c.status());
            filed.push(FiledReport { report, case_status });
        }
        Ok(filed)
    }
}

// ─── EnforcementRepository ─────────────────────────────────────────────────────

pub struct InMemoryEnforcementRepository {
    enforcements: Mutex<HashMap<EnforcementId, EnforcementAction>>,
}

impl InMemoryEnforcementRepository {
    pub fn new() -> Self {
        Self { enforcements: Mutex::new(HashMap::new()) }
    }
}

#[async_trait]
impl EnforcementRepository for InMemoryEnforcementRepository {
    async fn save(&self, enforcement: &EnforcementAction) -> Result<(), ModerationError> {
        let mut stored = enforcement.clone();
        let _ = stored.drain_events();
        self.enforcements.lock().unwrap().insert(stored.id(), stored);
        Ok(())
    }

    async fn find_by_id(
        &self,
        id: &EnforcementId,
    ) -> Result<Option<EnforcementAction>, ModerationError> {
        Ok(self.enforcements.lock().unwrap().get(id).cloned())
    }

    async fn next_version(
        &self,
        subject: &SubjectRef,
    ) -> Result<EnforcementVersion, ModerationError> {
        let key = subject.canonical_key();
        let max = self
            .enforcements
            .lock()
            .unwrap()
            .values()
            .filter(|e| e.subject().canonical_key() == key)
            .map(|e| e.version().value())
            .max();
        Ok(match max {
            Some(v) => EnforcementVersion::from_i64(v).next(),
            None => EnforcementVersion::INITIAL,
        })
    }

    async fn list_active_for_actor(
        &self,
        actor_id: &ActorId,
    ) -> Result<Vec<EnforcementAction>, ModerationError> {
        Ok(self
            .enforcements
            .lock()
            .unwrap()
            .values()
            .filter(|e| e.actor_id() == *actor_id && e.status() == EnforcementStatus::Active)
            .cloned()
            .collect())
    }
}

// ─── PenaltyRepository ─────────────────────────────────────────────────────────

pub struct InMemoryPenaltyRepository {
    ledgers: Mutex<HashMap<ActorId, PenaltyLedger>>,
}

impl InMemoryPenaltyRepository {
    pub fn new() -> Self {
        Self { ledgers: Mutex::new(HashMap::new()) }
    }
}

#[async_trait]
impl PenaltyRepository for InMemoryPenaltyRepository {
    async fn load(&self, actor_id: &ActorId) -> Result<PenaltyLedger, ModerationError> {
        Ok(self
            .ledgers
            .lock()
            .unwrap()
            .get(actor_id)
            .cloned()
            .unwrap_or_else(|| PenaltyLedger::empty(*actor_id)))
    }

    async fn save(&self, ledger: &PenaltyLedger) -> Result<(), ModerationError> {
        self.ledgers.lock().unwrap().insert(ledger.actor_id(), ledger.clone());
        Ok(())
    }
}

// ─── AppealRepository ──────────────────────────────────────────────────────────

pub struct InMemoryAppealRepository {
    appeals: Mutex<HashMap<AppealId, Appeal>>,
}

impl InMemoryAppealRepository {
    pub fn new() -> Self {
        Self { appeals: Mutex::new(HashMap::new()) }
    }
}

#[async_trait]
impl AppealRepository for InMemoryAppealRepository {
    async fn save(&self, appeal: &Appeal) -> Result<(), ModerationError> {
        let mut stored = appeal.clone();
        let _ = stored.drain_events();
        self.appeals.lock().unwrap().insert(stored.id(), stored);
        Ok(())
    }

    async fn file(&self, appeal: &Appeal) -> Result<Appeal, ModerationError> {
        if let Some(existing) = self.find_for(&appeal.decision_id(), &appeal.actor_id()).await? {
            return Ok(existing);
        }
        self.save(appeal).await?;
        Ok(self.appeals.lock().unwrap()[&appeal.id()].clone())
    }

    async fn find_by_id(&self, id: &AppealId) -> Result<Option<Appeal>, ModerationError> {
        Ok(self.appeals.lock().unwrap().get(id).cloned())
    }

    async fn find_for(&self, decision: &DecisionId, appellant: &ActorId) -> Result<Option<Appeal>, ModerationError> {
        Ok(self
            .appeals
            .lock()
            .unwrap()
            .values()
            .find(|a| a.decision_id() == *decision && a.actor_id() == *appellant)
            .cloned())
    }

    async fn list_for_appellant(
        &self,
        appellant: &ActorId,
        after: Option<crate::application::port::AppealCursor>,
        limit: usize,
    ) -> Result<Vec<Appeal>, ModerationError> {
        let mut mine: Vec<Appeal> =
            self.appeals.lock().unwrap().values().filter(|a| a.actor_id() == *appellant).cloned().collect();
        mine.sort_by_key(|a| std::cmp::Reverse((a.filed_at(), a.id().as_uuid())));
        Ok(mine
            .into_iter()
            .filter(|a| after.is_none_or(|c| (a.filed_at(), a.id().as_uuid()) < (c.filed_at, c.id.as_uuid())))
            .take(limit)
            .collect())
    }
}

// ─── EnforcementProjection ─────────────────────────────────────────────────────

pub struct InMemoryEnforcementProjection {
    /// actor → (last version written, restricted).
    state: Mutex<HashMap<ActorId, (EnforcementVersion, bool)>>,
}

impl InMemoryEnforcementProjection {
    pub fn new() -> Self {
        Self { state: Mutex::new(HashMap::new()) }
    }
}

#[async_trait]
impl EnforcementProjection for InMemoryEnforcementProjection {
    async fn set_actor_restriction(
        &self,
        actor_id: &ActorId,
        version: EnforcementVersion,
        _expires_at: Option<DateTime<Utc>>,
    ) -> Result<(), ModerationError> {
        let mut state = self.state.lock().unwrap();
        // Ignore a stale (lower-versioned) write.
        if state.get(actor_id).is_none_or(|(v, _)| version >= *v) {
            state.insert(*actor_id, (version, true));
        }
        Ok(())
    }

    async fn clear_actor_restriction(
        &self,
        actor_id: &ActorId,
        version: EnforcementVersion,
    ) -> Result<(), ModerationError> {
        let mut state = self.state.lock().unwrap();
        if state.get(actor_id).is_none_or(|(v, _)| version >= *v) {
            state.insert(*actor_id, (version, false));
        }
        Ok(())
    }

    async fn is_actor_restricted(&self, actor_id: &ActorId) -> Result<bool, ModerationError> {
        Ok(self.state.lock().unwrap().get(actor_id).map(|(_, r)| *r).unwrap_or(false))
    }
}

// ─── ScreenCorpus ──────────────────────────────────────────────────────────────

pub struct StubScreenCorpus {
    known_bad: Mutex<HashMap<String, (Vec<PolicyCategory>, String)>>,
    unavailable: Mutex<bool>,
    /// When set, the lookup sleeps this long before answering — used to drive the
    /// Screen hard-timeout path.
    delay: Mutex<Option<std::time::Duration>>,
}

impl StubScreenCorpus {
    pub fn new() -> Self {
        Self {
            known_bad: Mutex::new(HashMap::new()),
            unavailable: Mutex::new(false),
            delay: Mutex::new(None),
        }
    }

    /// Registers a hash value as a known-bad match for the given categories.
    pub fn add_known_bad(&self, hash_value: &str, categories: Vec<PolicyCategory>, reference: &str) {
        self.known_bad
            .lock()
            .unwrap()
            .insert(hash_value.to_owned(), (categories, reference.to_owned()));
    }

    pub fn set_unavailable(&self) {
        *self.unavailable.lock().unwrap() = true;
    }

    /// Makes the corpus answer after `delay` — slower than the screen timeout, to
    /// exercise the fail-closed-on-timeout path.
    pub fn set_delay(&self, delay: std::time::Duration) {
        *self.delay.lock().unwrap() = Some(delay);
    }
}

#[async_trait]
impl ScreenCorpus for StubScreenCorpus {
    async fn screen(
        &self,
        hashes: &[ContentHash],
        _text: Option<&str>,
        _categories: &[PolicyCategory],
    ) -> Result<Option<CorpusMatch>, ModerationError> {
        let delay = *self.delay.lock().unwrap();
        if let Some(d) = delay {
            tokio::time::sleep(d).await;
        }
        if *self.unavailable.lock().unwrap() {
            return Err(ModerationError::ScreenUnavailable);
        }
        let known = self.known_bad.lock().unwrap();
        for h in hashes {
            if let Some((categories, reference)) = known.get(&h.value) {
                return Ok(Some(CorpusMatch {
                    categories: categories.clone(),
                    reference: reference.clone(),
                }));
            }
        }
        Ok(None)
    }
}

// ─── ClassifierGateway ─────────────────────────────────────────────────────────

pub struct StubClassifierGateway {
    requests: Mutex<usize>,
}

impl StubClassifierGateway {
    pub fn new() -> Self {
        Self { requests: Mutex::new(0) }
    }

    pub fn request_count(&self) -> usize {
        *self.requests.lock().unwrap()
    }
}

#[async_trait]
impl ClassifierGateway for StubClassifierGateway {
    async fn request_classification(&self, _subject: &SubjectRef) -> Result<(), ModerationError> {
        *self.requests.lock().unwrap() += 1;
        Ok(())
    }
}

// ─── AccountDirectory ──────────────────────────────────────────────────────────

pub struct StubAccountDirectory {
    known: Mutex<bool>,
}

impl StubAccountDirectory {
    pub fn new() -> Self {
        Self { known: Mutex::new(true) }
    }

    pub fn set_known(&self, known: bool) {
        *self.known.lock().unwrap() = known;
    }
}

#[async_trait]
impl AccountDirectory for StubAccountDirectory {
    async fn actor_exists(&self, _actor_id: &ActorId) -> Result<bool, ModerationError> {
        Ok(*self.known.lock().unwrap())
    }
}

// ─── EventPublisher ────────────────────────────────────────────────────────────

pub struct RecordingEventPublisher {
    events: Mutex<Vec<DomainEvent>>,
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

    /// A clone of every published event, for asserting on payload content.
    pub fn events(&self) -> Vec<DomainEvent> {
        self.events.lock().unwrap().clone()
    }

    /// Resets recorded events — used by tests that assert only on events emitted
    /// after a setup phase (e.g. open-then-decide).
    pub fn clear(&self) {
        self.events.lock().unwrap().clear();
    }
}

#[async_trait]
impl EventPublisher for RecordingEventPublisher {
    async fn publish(&self, event: &DomainEvent) -> Result<(), ModerationError> {
        self.events.lock().unwrap().push(event.clone());
        Ok(())
    }
}

// ─── Fixture ───────────────────────────────────────────────────────────────────

/// Bundles concrete fakes and builds handlers wired to them. Handlers receive the
/// fakes as `Arc<dyn Port>`; tests keep the concrete `Arc`s to assert on recorded
/// state (published events, projection flags, stored aggregates).
/// Content id → responsible account; unknown ids are "not found".
#[derive(Default)]
pub struct StubSubjectResolver {
    pub owners: Mutex<HashMap<String, ActorId>>,
}

impl StubSubjectResolver {
    pub fn own(&self, entity_id: &str, account: ActorId) {
        self.owners.lock().unwrap().insert(entity_id.to_owned(), account);
    }
}

#[async_trait]
impl super::port::SubjectResolver for StubSubjectResolver {
    async fn responsible_account(
        &self,
        _entity_type: crate::domain::value_object::EntityType,
        entity_id: &str,
    ) -> Result<Option<ActorId>, ModerationError> {
        Ok(self.owners.lock().unwrap().get(entity_id).copied())
    }
}

/// Admits up to `limit` reports per reporter key.
pub struct CountingReportQuota {
    limit: u32,
    counts: Mutex<HashMap<String, u32>>,
}

impl CountingReportQuota {
    pub fn with_limit(limit: u32) -> Self {
        Self { limit, counts: Mutex::new(HashMap::new()) }
    }
}

#[async_trait]
impl super::port::ReportRateLimiter for CountingReportQuota {
    async fn admit(&self, reporter_key: &str) -> Result<bool, ModerationError> {
        let mut counts = self.counts.lock().unwrap();
        let n = counts.entry(reporter_key.to_owned()).or_insert(0);
        *n += 1;
        Ok(*n <= self.limit)
    }
}

pub struct Fixture {
    pub cases: Arc<InMemoryCaseRepository>,
    pub reports: Arc<InMemoryReportRepository>,
    pub decisions: Arc<InMemoryDecisionRepository>,
    pub enforcements: Arc<InMemoryEnforcementRepository>,
    pub penalties: Arc<InMemoryPenaltyRepository>,
    pub appeals: Arc<InMemoryAppealRepository>,
    pub projection: Arc<InMemoryEnforcementProjection>,
    pub corpus: Arc<StubScreenCorpus>,
    pub classifiers: Arc<StubClassifierGateway>,
    pub accounts: Arc<StubAccountDirectory>,
    pub publisher: Arc<RecordingEventPublisher>,
    pub subjects: Arc<StubSubjectResolver>,
    pub report_quota: Arc<CountingReportQuota>,
    pub policy: ModerationPolicy,
}

impl Fixture {
    pub fn new() -> Self {
        let cases = Arc::new(InMemoryCaseRepository::new());
        Self {
            reports: Arc::new(InMemoryReportRepository::new(Arc::clone(&cases))),
            cases,
            decisions: Arc::new(InMemoryDecisionRepository::new()),
            enforcements: Arc::new(InMemoryEnforcementRepository::new()),
            penalties: Arc::new(InMemoryPenaltyRepository::new()),
            appeals: Arc::new(InMemoryAppealRepository::new()),
            projection: Arc::new(InMemoryEnforcementProjection::new()),
            corpus: Arc::new(StubScreenCorpus::new()),
            classifiers: Arc::new(StubClassifierGateway::new()),
            accounts: Arc::new(StubAccountDirectory::new()),
            publisher: Arc::new(RecordingEventPublisher::new()),
            subjects: Arc::new(StubSubjectResolver::default()),
            report_quota: Arc::new(CountingReportQuota::with_limit(20)),
            policy: ModerationPolicy::test_default(),
        }
    }

    pub fn screen_handler(&self) -> super::command::ScreenHandler {
        super::command::ScreenHandler::new(
            Arc::clone(&self.corpus) as _,
            Arc::clone(&self.decisions) as _,
            Arc::clone(&self.enforcements) as _,
            Arc::clone(&self.penalties) as _,
            Arc::clone(&self.projection) as _,
            Arc::clone(&self.publisher) as _,
            self.policy.clone(),
        )
    }

    pub fn open_case_handler(&self) -> super::command::OpenCaseHandler {
        super::command::OpenCaseHandler::new(
            Arc::clone(&self.cases) as _,
            Arc::clone(&self.publisher) as _,
        )
    }

    pub fn assign_case_handler(&self) -> super::command::AssignCaseHandler {
        super::command::AssignCaseHandler::new(Arc::clone(&self.cases) as _)
    }

    pub fn decide_handler(&self) -> super::command::DecideCaseHandler {
        super::command::DecideCaseHandler::new(
            Arc::clone(&self.cases) as _,
            Arc::clone(&self.decisions) as _,
            Arc::clone(&self.enforcements) as _,
            Arc::clone(&self.penalties) as _,
            Arc::clone(&self.projection) as _,
            Arc::clone(&self.accounts) as _,
            Arc::clone(&self.publisher) as _,
            self.policy.clone(),
        )
    }

    pub fn ingest_report_handler(&self) -> super::command::IngestReportHandler {
        super::command::IngestReportHandler::new(
            Arc::clone(&self.reports) as _,
            Arc::clone(&self.cases) as _,
            Arc::clone(&self.publisher) as _,
            Arc::clone(&self.classifiers) as _,
        )
    }

    pub fn ingest_signal_handler(&self) -> super::command::IngestSignalHandler {
        super::command::IngestSignalHandler::new(
            Arc::clone(&self.cases) as _,
            Arc::clone(&self.publisher) as _,
        )
    }

    pub fn file_appeal_handler(&self) -> super::command::FileAppealHandler {
        super::command::FileAppealHandler::new(
            Arc::clone(&self.decisions) as _,
            Arc::clone(&self.appeals) as _,
            Arc::clone(&self.cases) as _,
        )
    }

    pub fn resolve_appeal_handler(&self) -> super::command::ResolveAppealHandler {
        super::command::ResolveAppealHandler::new(
            Arc::clone(&self.appeals) as _,
            Arc::clone(&self.decisions) as _,
            Arc::clone(&self.enforcements) as _,
            Arc::clone(&self.cases) as _,
            Arc::clone(&self.projection) as _,
            Arc::clone(&self.publisher) as _,
        )
    }

    pub fn list_queue_handler(&self) -> super::query::ListQueueHandler {
        super::query::ListQueueHandler::new(Arc::clone(&self.cases) as _)
    }

    pub fn enforcement_state_handler(&self) -> super::query::GetEnforcementStateHandler {
        super::query::GetEnforcementStateHandler::new(
            Arc::clone(&self.projection) as _,
            Arc::clone(&self.enforcements) as _,
        )
    }

    pub fn statement_of_reasons_handler(&self) -> super::query::GetStatementOfReasonsHandler {
        super::query::GetStatementOfReasonsHandler::new(Arc::clone(&self.decisions) as _, Arc::clone(&self.appeals) as _)
    }

    pub fn submit_report_handler(&self) -> super::command::SubmitReportHandler {
        super::command::SubmitReportHandler::new(
            Arc::clone(&self.subjects) as _,
            Arc::clone(&self.report_quota) as _,
            Arc::new(self.ingest_report_handler()),
        )
    }

    pub fn list_my_reports_handler(&self) -> super::query::ListMyReportsHandler {
        super::query::ListMyReportsHandler::new(Arc::clone(&self.reports) as _)
    }
}
