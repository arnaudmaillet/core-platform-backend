//! GDPR data export adapters (#653).

pub mod s3_export_store;

pub use s3_export_store::{ExportStoreConfig, S3ExportStore};
