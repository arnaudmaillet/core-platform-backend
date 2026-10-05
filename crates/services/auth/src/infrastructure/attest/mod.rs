//! Device attestation (guest mode B5b): proving a request comes from a genuine
//! install of our app on a real device.

pub mod app_attest;

pub use app_attest::{AppAttestVerifier, AttestEnvironment, AttestError, AttestedKey};
