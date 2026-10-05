//! Scenario groups for the comment live suite, mapping to the testing standard's
//! axes: dual-table consistency / threading and the tombstone-vs-purge deletion
//! invariant.

mod dual_table_threading;
mod tombstone_vs_purge;
mod read_gate;
mod hidden_words;
mod held_comments;
mod restricted_comments;
mod gate_over_grpc;
mod comments_by_author;
