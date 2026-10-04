pub mod handle_changed;
pub mod interaction_settings_changed;
pub mod location_settings_changed;
pub mod profile_created;
pub mod profile_deleted;
pub mod profile_hidden;
pub mod profile_restored;
pub mod profile_updated;
pub mod profile_verified;
pub mod tier_changed;
pub mod visibility_changed;

pub use handle_changed::HandleChanged;
pub use interaction_settings_changed::InteractionSettingsChanged;
pub use location_settings_changed::LocationSettingsChanged;
pub use profile_created::ProfileCreated;
pub use profile_deleted::ProfileDeleted;
pub use profile_hidden::ProfileHidden;
pub use profile_restored::ProfileRestored;
pub use profile_updated::ProfileUpdated;
pub use profile_verified::ProfileVerified;
pub use tier_changed::TierChanged;
pub use visibility_changed::VisibilityChanged;

#[derive(Debug, Clone)]
pub enum DomainEvent {
    ProfileCreated(ProfileCreated),
    ProfileUpdated(ProfileUpdated),
    HandleChanged(HandleChanged),
    ProfileHidden(ProfileHidden),
    ProfileRestored(ProfileRestored),
    ProfileVerified(ProfileVerified),
    ProfileDeleted(ProfileDeleted),
    TierChanged(TierChanged),
    InteractionSettingsChanged(InteractionSettingsChanged),
    LocationSettingsChanged(LocationSettingsChanged),
    VisibilityChanged(VisibilityChanged),
}
