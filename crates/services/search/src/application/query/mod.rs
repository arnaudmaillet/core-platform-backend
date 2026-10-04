//! Read-side use cases. Both implement [`cqrs::QueryHandler`] and ride the query
//! bus; each is a thin delegate over the [`SearchIndex`](crate::application::port::SearchIndex)
//! port — the engine owns matching and ranking.

pub mod audience;
pub mod search;
pub mod suggest;

pub use audience::{filter_results, filter_suggestions};
pub use search::{RunSearch, SearchHandler};
pub use suggest::{RunSuggest, SuggestHandler};
