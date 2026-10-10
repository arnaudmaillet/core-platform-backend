//! Live scenarios, grouped by concern. Each boots (shares) the MinIO + Postgres +
//! Redis containers via the harness and drives the real adapters end-to-end.

mod moderation;
mod pipeline;
mod validation;
mod assets_by_owner;
mod private_documents;
mod upload_keys;
