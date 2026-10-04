//! Write use-cases. Like `auth`, these are explicit application-service handlers
//! (not `cqrs::CommandHandler`) because they return rich outputs — a decision, an
//! enforcement, a screen verdict — that a `Result<(), E>` command cannot carry.
//! Each takes a [`cqrs::Envelope`] so the `correlation_id` threads into the domain
//! events it emits, and an injected `now` for deterministic tests.

pub mod appeal;
pub mod decide_case;
pub mod ingest;
pub mod screen;
pub mod submit_report;

pub use appeal::{
    FileAppealCommand, FileAppealHandler, ResolveAppealCommand, ResolveAppealHandler,
    ResolveAppealOutcome,
};
pub use decide_case::{
    AssignCaseCommand, AssignCaseHandler, DecideCaseCommand, DecideCaseHandler, DecideOutcome,
    OpenCaseCommand, OpenCaseHandler, OpenedCase,
};
pub use ingest::{
    IngestReportCommand, IngestReportHandler, IngestSignalCommand, IngestSignalHandler,
    ReportOrigin,
};
pub use submit_report::{Reporter, SubmitReportCommand, SubmitReportHandler};
pub use screen::{ScreenCommand, ScreenHandler, ScreenOutcome, ScreenVerdict};

use uuid::Uuid;

use crate::domain::aggregate::Decision;
use crate::domain::event::{DecisionRecorded, DomainEvent};

/// Build the `DecisionRecorded` compliance-evidence event from a just-recorded
/// `Decision`, threading the command's `correlation_id`. Used at every site that
/// appends a decision (automated screen, human review, appeal reversal) so the
/// `audit` plane captures who decided, under what authority and with what reason.
pub(crate) fn decision_recorded(decision: &Decision, correlation_id: Uuid) -> DomainEvent {
    DomainEvent::DecisionRecorded(DecisionRecorded {
        decision_id: decision.id(),
        subject: decision.subject().clone(),
        author: decision.author().clone(),
        action: decision.action(),
        category: decision.category(),
        policy_version: decision.policy_version().clone(),
        rationale: decision.rationale().to_owned(),
        reverses: decision.reverses(),
        occurred_at: decision.decided_at(),
        correlation_id,
    })
}
