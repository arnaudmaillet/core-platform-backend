//! Scenario groups for the account live suite, mapping to the testing standard's
//! axes: concurrency (uniqueness race) and durable persistence.

mod persistence_roundtrip;
mod uniqueness_race;
mod mutation_roundtrip;
mod consents;
mod gdpr_deletion;
mod date_of_birth;
mod email_lookup;
