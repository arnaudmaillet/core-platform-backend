//! Read use-cases. These are genuinely read-only, so they implement
//! [`cqrs::QueryHandler`] and ride the query bus like the rest of the fleet.

pub mod enforcement_state;
pub mod list_my_appeals;
pub mod list_my_reports;
pub mod list_queue;
pub mod statement_of_reasons;

pub use enforcement_state::{
    EnforcementStateView, GetEnforcementStateHandler, GetEnforcementStateQuery,
};
pub use list_my_appeals::{ListMyAppealsHandler, ListMyAppealsQuery, MyAppealsPage};
pub use list_my_reports::{ListMyReportsHandler, ListMyReportsQuery, MyReport, MyReportsPage};
pub use list_queue::{ListQueueHandler, ListQueueQuery};
pub use statement_of_reasons::{
    GetStatementOfReasonsHandler, GetStatementOfReasonsQuery, StatementOfReasons,
};
