//! Write use-cases — explicit application-service handlers (not `cqrs::Command`),
//! each taking a [`cqrs::Envelope`] and an injected `now`, returning a rich
//! outcome. They orchestrate the domain over the outbound ports and publish
//! lifecycle events durable-first.

pub mod apply_moderation;
mod asset_objects;
pub mod commit_upload;
pub mod delete_asset;
pub mod document_retention;
pub mod erase_owner;
pub mod issue_ticket;
pub mod process_asset;
pub mod transcode_asset;

pub use apply_moderation::{
    ApplyModerationCommand, ApplyModerationHandler, ApplyModerationOutcome, ModerationAction,
};
pub use commit_upload::{CommitUploadCommand, CommitUploadHandler};
pub use delete_asset::{DeleteAssetCommand, DeleteAssetHandler, DeleteOutcome};
pub use document_retention::DocumentRetention;
pub use erase_owner::{ErasedMedia, OwnerErasure};
pub use issue_ticket::{
    IssueUploadTicketCommand, IssueUploadTicketHandler, IssueUploadTicketOutcome, PreparedUpload,
};
pub use process_asset::{ProcessAssetCommand, ProcessAssetHandler, ProcessOutcome};
pub use transcode_asset::{TranscodeAssetCommand, TranscodeAssetHandler};
