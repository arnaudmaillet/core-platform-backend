//! Flat row projections of the moderation tables. Domain reconstruction
//! (validation, value-object construction) happens in each `TryFrom`, keeping
//! persistence free of domain logic.

pub mod appeal_row;
pub mod case_row;
pub mod decision_row;
pub mod enforcement_row;
pub mod penalty_row;
pub mod report_row;

pub use appeal_row::AppealRow;
pub use case_row::CaseRow;
pub use decision_row::DecisionRow;
pub use enforcement_row::EnforcementRow;
pub use penalty_row::PenaltyRow;
pub use report_row::FiledReportRow;

use uuid::Uuid;

use crate::domain::value_object::{ActorId, EntityType, SubjectRef};
use crate::error::ModerationError;

/// Reconstructs a [`SubjectRef`] from its four flat columns.
pub(crate) fn subject_from(
    entity_type: &str,
    entity_id: String,
    actor_id: Uuid,
    surface: String,
) -> Result<SubjectRef, ModerationError> {
    SubjectRef::new(
        EntityType::try_from(entity_type)?,
        entity_id,
        ActorId::from_uuid(actor_id),
        surface,
    )
}
