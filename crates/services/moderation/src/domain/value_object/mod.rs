//! Value objects for the moderation domain — small, validated, immutable types
//! the aggregates are built from. Each enforces its own invariants at
//! construction so an invalid value is unrepresentable upstream.

pub mod action_type;
pub mod appeal_status;
pub mod case_status;
pub mod confidence;
pub mod enforcement_status;
pub mod enforcement_version;
pub mod entity_type;
pub mod ids;
pub mod penalty_policy;
pub mod policy_category;
pub mod policy_version;
pub mod report_status;
pub mod reporter_kind;
pub mod signal;
pub mod strike;
pub mod subject_ref;

pub use action_type::ActionType;
pub use appeal_status::AppealStatus;
pub use case_status::CaseStatus;
pub use confidence::Confidence;
pub use enforcement_status::EnforcementStatus;
pub use enforcement_version::EnforcementVersion;
pub use entity_type::EntityType;
pub use ids::{ActorId, AppealId, CaseId, DecisionId, EnforcementId, ReportId, MODERATION_NAMESPACE};
pub use penalty_policy::PenaltyPolicy;
pub use policy_category::PolicyCategory;
pub use policy_version::PolicyVersion;
pub use report_status::ReportStatus;
pub use reporter_kind::ReporterKind;
pub use signal::Signal;
pub use strike::Strike;
pub use subject_ref::SubjectRef;
