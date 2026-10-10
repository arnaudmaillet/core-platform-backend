//! The search application layer — use-case orchestration over the domain and ports.
//!
//! ## Handler shape
//! The single write use case ([`command::ProjectionHandler`]) is a plain
//! application-service struct (not a `cqrs::CommandHandler`): it returns a rich
//! [`command::ApplyOutcome`] a `Result<(), E>` could not carry, takes a
//! [`cqrs::Envelope`] for the correlation id, and an injected `now`. The read use
//! cases ([`query::SearchHandler`] / [`query::SuggestHandler`]) implement
//! [`cqrs::QueryHandler`] and ride the query bus.
//!
//! ## Ports
//! Every external dependency is an `async_trait` port in [`port`], injected as an
//! `Arc<dyn …>` at the composition root, so the handlers never name a concrete
//! adapter. A version-aware in-memory fake of [`port::SearchIndex`] backs the unit
//! tests.

pub mod command;
pub mod hashtags;
pub mod port;
pub mod query;
pub mod reindex;

#[cfg(test)]
pub mod fakes;

pub use command::{ApplyOutcome, ProjectionHandler};
pub use query::{RunSearch, RunSuggest, SearchHandler, SuggestHandler};
pub use reindex::{ReindexReport, Reindexer};
