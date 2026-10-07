use std::sync::Arc;

use scylla::observability::history::HistoryListener;
use scylla::statement::batch::{Batch, BatchType};
use scylla::statement::unprepared::Statement;
use scylla_storage::{ProfileKind, ScyllaClient, ScyllaStorageError};

use crate::error::ChatError;

/// Maps a driver execution error into the service error contract, preserving the
/// underlying storage error code and retryability.
pub(crate) fn scylla_err(e: scylla::errors::ExecutionError) -> ChatError {
    ChatError::Scylla(ScyllaStorageError::from(e))
}

/// Maps a row deserialization/iteration failure into a domain violation tagged
/// with the call site (`ctx`) for diagnostics.
pub(crate) fn row_err(ctx: &'static str, e: impl ToString) -> ChatError {
    ChatError::DomainViolation { field: ctx.to_owned(), message: e.to_string() }
}

/// Builds a statement bound to the **Strict** execution profile (LocalQuorum).
/// Use for member writes and any read that must be linearizable within the DC.
pub(crate) fn strict(client: &ScyllaClient, cql: &str) -> Statement {
    profiled(client, cql, ProfileKind::Strict, "strict")
}

/// Builds a statement bound to the **Fast** execution profile (LocalOne +
/// speculative execution). Use for tail-latency-sensitive reads — in particular
/// the passive guest history reads, which tolerate eventual consistency and
/// benefit from spreading across replicas.
pub(crate) fn fast(client: &ScyllaClient, cql: &str) -> Statement {
    profiled(client, cql, ProfileKind::Fast, "fast")
}

/// Builds a statement bound to the **Analytical** execution profile (Quorum,
/// extended timeout). Use for admin/analytics scans (e.g. paginating the full
/// audience subscription set) that tolerate higher latency.
pub(crate) fn analytical(client: &ScyllaClient, cql: &str) -> Statement {
    profiled(client, cql, ProfileKind::Analytical, "analytical")
}

fn profiled(client: &ScyllaClient, cql: &str, kind: ProfileKind, label: &str) -> Statement {
    let mut stmt = Statement::new(cql);
    stmt.set_execution_profile_handle(Some(
        client
            .profiles
            .get(kind)
            .clone()
            .into_handle_with_label(label.to_string()),
    ));
    stmt.set_history_listener(Arc::clone(&client.history_listener) as Arc<dyn HistoryListener>);
    stmt
}

/// An empty **logged** batch on the Strict profile: its statements apply
/// together even if the coordinator dies mid-write (a table and its index).
pub(crate) fn strict_batch(client: &ScyllaClient) -> Batch {
    let mut batch = Batch::new(BatchType::Logged);
    batch.set_execution_profile_handle(Some(
        client.profiles.get(ProfileKind::Strict).clone().into_handle_with_label("strict-batch".to_string()),
    ));
    batch.set_history_listener(Arc::clone(&client.history_listener) as Arc<dyn HistoryListener>);
    batch
}

/// The `[applied]` flag of an LWT result. The row's shape varies (bare
/// `[applied]`, or with the existing row's columns — and Scylla 5.4 returns
/// the columns even on success), so only column 0 is read, untyped.
pub(crate) fn lwt_applied(rows: scylla::response::query_result::QueryRowsResult, ctx: &'static str) -> Result<bool, ChatError> {
    let row = rows.maybe_first_row::<scylla::value::Row>().map_err(|e| row_err(ctx, e))?;
    Ok(matches!(
        row.and_then(|r| r.columns.into_iter().next().flatten()),
        Some(scylla::value::CqlValue::Boolean(true))
    ))
}
