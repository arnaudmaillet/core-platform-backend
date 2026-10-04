pub mod account_activated;
pub mod account_created;
pub mod account_deactivated;
pub mod account_deleted;
pub mod account_suspended;
pub mod email_changed;
pub mod email_verified;
pub mod consents_updated;
pub mod gdpr_data_export_requested;
pub mod gdpr_deletion_cancelled;
pub mod gdpr_deletion_requested;
pub mod kyc_status_changed;
pub mod mfa_enrolled;
pub mod mfa_revoked;
pub mod password_changed;
pub mod phone_changed;
pub mod role_assigned;
pub mod role_revoked;

pub use account_activated::AccountActivated;
pub use account_created::AccountCreated;
pub use account_deactivated::AccountDeactivated;
pub use account_deleted::AccountDeleted;
pub use account_suspended::AccountSuspended;
pub use email_changed::EmailChanged;
pub use email_verified::EmailVerified;
pub use consents_updated::{ConsentChange, ConsentsUpdated};
pub use gdpr_data_export_requested::GdprDataExportRequested;
pub use gdpr_deletion_cancelled::GdprDeletionCancelled;
pub use gdpr_deletion_requested::GdprDeletionRequested;
pub use kyc_status_changed::KycStatusChanged;
pub use mfa_enrolled::MfaEnrolled;
pub use mfa_revoked::MfaRevoked;
pub use password_changed::PasswordChanged;
pub use phone_changed::PhoneChanged;
pub use role_assigned::RoleAssigned;
pub use role_revoked::RoleRevoked;

use serde::{Deserialize, Serialize};

/// Sealed sum type of all domain events emitted by the Account aggregate.
///
/// Consumers (e.g. the event bus adapter) pattern-match on this enum to
/// determine routing keys and serialization targets.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DomainEvent {
    AccountCreated(AccountCreated),
    EmailVerified(EmailVerified),
    PasswordChanged(PasswordChanged),
    EmailChanged(EmailChanged),
    PhoneChanged(PhoneChanged),
    MfaEnrolled(MfaEnrolled),
    MfaRevoked(MfaRevoked),
    RoleAssigned(RoleAssigned),
    RoleRevoked(RoleRevoked),
    AccountSuspended(AccountSuspended),
    AccountActivated(AccountActivated),
    AccountDeactivated(AccountDeactivated),
    AccountDeleted(AccountDeleted),
    KycStatusChanged(KycStatusChanged),
    GdprDeletionRequested(GdprDeletionRequested),
    GdprDataExportRequested(GdprDataExportRequested),
    ConsentsUpdated(ConsentsUpdated),
    GdprDeletionCancelled(GdprDeletionCancelled),
}

impl DomainEvent {
    pub fn event_type(&self) -> &'static str {
        match self {
            Self::AccountCreated(_)          => "account.created",
            Self::EmailVerified(_)           => "account.email_verified",
            Self::PasswordChanged(_)         => "account.password_changed",
            Self::EmailChanged(_)            => "account.email_changed",
            Self::PhoneChanged(_)            => "account.phone_changed",
            Self::MfaEnrolled(_)             => "account.mfa_enrolled",
            Self::MfaRevoked(_)              => "account.mfa_revoked",
            Self::RoleAssigned(_)            => "account.role_assigned",
            Self::RoleRevoked(_)             => "account.role_revoked",
            Self::AccountSuspended(_)        => "account.suspended",
            Self::AccountActivated(_)        => "account.activated",
            Self::AccountDeactivated(_)      => "account.deactivated",
            Self::AccountDeleted(_)          => "account.deleted",
            Self::KycStatusChanged(_)        => "account.kyc_status_changed",
            Self::GdprDeletionRequested(_)   => "account.gdpr_deletion_requested",
            Self::GdprDataExportRequested(_) => "account.gdpr_data_export_requested",
            Self::ConsentsUpdated(_)         => "account.consents_updated",
            Self::GdprDeletionCancelled(_)   => "account.gdpr_deletion_cancelled",
        }
    }
}
