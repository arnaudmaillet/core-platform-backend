//! Migration runners for the ephemeral nodes.
//!
//! Both runners read the *real* migration assets from disk at runtime (not
//! `include_str!`), so a malformed migration fails the suite loudly — the test
//! exercises exactly what gets deployed.

use std::fs;
use std::sync::Arc;

use scylla_storage::{ScyllaConfig, ScyllaSessionBuilder};

/// Applies every `.cql` migration in `migrations_dir` (lexical order) against the
/// single-node ScyllaDB at `contact_point`, then blocks on schema agreement.
///
/// ## Single-node replication adaptation
///
/// A production `0001_create_keyspace.cql` typically provisions the keyspace with
/// `NetworkTopologyStrategy {datacenter1: 3}`, and the durable writer uses a
/// LocalQuorum profile that needs two live replicas — impossible on a single-node
/// container. We therefore rewrite *only* the keyspace's replication clause to
/// `SimpleStrategy RF=1`; every table statement is applied verbatim. This is a
/// test-topology adaptation, not a schema change — cluster slot/replication
/// behaviour belongs to a separate multi-node CI guard.
pub async fn scylla_apply(contact_point: &str, keyspace: &str, migrations_dir: &str) {
    let config = ScyllaConfig {
        contact_points: vec![contact_point.to_owned()],
        keyspace: None,
        ..ScyllaConfig::default()
    };
    let client = Arc::new(
        ScyllaSessionBuilder::new(config)
            .build()
            .await
            .expect("integration: failed to connect to ScyllaDB for migration"),
    );
    let session = client.session.get_session();

    for statement in load_cql_statements(migrations_dir, keyspace) {
        if let Err(e) = session.query_unpaged(statement.as_str(), &[]).await {
            // Mirror the production migrator's idempotent-ALTER tolerance. Scylla
            // has no `ALTER TABLE ADD IF NOT EXISTS`, so a per-column `ADD`
            // migration that re-adds a column the (updated) CREATE TABLE already
            // defines errors with "conflicts with an existing column". The geo /
            // post migration sets are deliberately authored to apply idempotently
            // to both fresh and legacy clusters (see the per-column ADD files),
            // which requires treating that specific case as a benign no-op rather
            // than a failure. Every other error still fails the suite loudly.
            let msg = e.to_string();
            let is_idempotent_add = statement.to_uppercase().contains("ALTER TABLE")
                && (msg.contains("conflicts with an existing column")
                    || msg.contains("already exists"));
            if is_idempotent_add {
                continue;
            }
            panic!("migration statement failed: {e}\n--- statement ---\n{statement}");
        }
    }

    session
        .await_schema_agreement()
        .await
        .expect("integration: schema did not converge after migrations");
}


/// Collects migration files with `ext` under `dir`, recursing one level: most
/// services keep a flat `migrations/`, but dual-store services (moderation)
/// split into `migrations/{postgres,scylla}` — the old non-recursive read_dir
/// silently loaded NOTHING for those, so their suites ran against an empty
/// schema (found on the suites' first CI run: relation "cases" did not exist).
fn collect_migration_files(dir: &str, ext: &str) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let entries = fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("cannot read migrations dir '{dir}': {e}"));
    for entry in entries {
        let path = entry.expect("bad dir entry").path();
        if path.is_dir() {
            let nested = fs::read_dir(&path)
                .unwrap_or_else(|e| panic!("cannot read migrations dir '{}': {e}", path.display()));
            for nested_entry in nested {
                let nested_path = nested_entry.expect("bad dir entry").path();
                if nested_path.extension().is_some_and(|x| x == ext) {
                    out.push(nested_path);
                }
            }
        } else if path.extension().is_some_and(|x| x == ext) {
            out.push(path);
        }
    }
    out
}

/// Reads `migrations_dir`, strips `--` line comments, splits on `;`, and adapts
/// the keyspace replication for a single node.
fn load_cql_statements(migrations_dir: &str, keyspace: &str) -> Vec<String> {
    let mut files = collect_migration_files(migrations_dir, "cql");
    files.sort();

    let mut statements = Vec::new();
    for path in files {
        let raw = fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("cannot read migration '{}': {e}", path.display()));

        let code: String = raw
            .lines()
            .map(strip_inline_comment)
            .filter(|line| !line.trim().is_empty())
            .collect::<Vec<_>>()
            .join("\n");

        for piece in split_statements(&code) {
            statements.push(adapt_keyspace(&piece, keyspace));
        }
    }
    statements
}

/// Splits CQL on the `;` that end statements — never one inside a
/// single-quoted literal (`''` is an escaped quote), like the production
/// migrator (`crates/apps/migrator`): a `;` in a table's `comment = '…'`
/// split it here only, and failed a suite that prod would have applied.
fn split_statements(code: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut in_string = false;
    let mut chars = code.chars().peekable();
    while let Some(c) = chars.next() {
        if in_string {
            current.push(c);
            if c == '\'' {
                if chars.peek() == Some(&'\'') {
                    current.push(chars.next().expect("peeked"));
                } else {
                    in_string = false;
                }
            }
            continue;
        }
        match c {
            '\'' => {
                in_string = true;
                current.push(c);
            }
            ';' => {
                let statement = current.trim();
                if !statement.is_empty() {
                    statements.push(statement.to_owned());
                }
                current.clear();
            }
            _ => current.push(c),
        }
    }
    let tail = current.trim();
    if !tail.is_empty() {
        statements.push(tail.to_owned());
    }
    statements
}

/// Truncates a line at the first `--` that sits OUTSIDE a single-quoted CQL
/// string literal. The naive full-line filter left inline comments in place,
/// and a `;` inside one (e.g. "-- watermark (UUID v7); NULL while private")
/// split the surrounding CREATE TABLE mid-definition — six services' suites
/// failed on their first-ever CI run because of exactly that.
fn strip_inline_comment(line: &str) -> &str {
    let bytes = line.as_bytes();
    let mut in_string = false;
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' => in_string = !in_string,
            b'-' if !in_string && i + 1 < bytes.len() && bytes[i + 1] == b'-' => {
                return &line[..i];
            }
            _ => {}
        }
        i += 1;
    }
    line
}

/// Rewrites a `CREATE KEYSPACE` statement to single-node `SimpleStrategy RF=1`;
/// passes every other statement through unchanged.
fn adapt_keyspace(statement: &str, keyspace: &str) -> String {
    if statement.to_uppercase().contains("CREATE KEYSPACE") {
        format!(
            "CREATE KEYSPACE IF NOT EXISTS {keyspace} \
             WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}} \
             AND durable_writes = true"
        )
    } else {
        statement.to_owned()
    }
}

/// Applies every `.sql` migration in `migrations_dir` (lexical order) against the
/// Postgres database at `url`.
///
/// Each file is executed via the simple-query protocol ([`sqlx::raw_sql`]) so a
/// single file may contain multiple statements (and constructs that embed `;`,
/// like function bodies) without a fragile hand-rolled split.
pub async fn postgres_apply(url: &str, migrations_dir: &str) {
    use sqlx::Executor as _;

    let pool = sqlx::PgPool::connect(url)
        .await
        .expect("integration: failed to connect to Postgres for migration");

    let mut files = collect_migration_files(migrations_dir, "sql");
    files.sort();

    for path in files {
        let sql = fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("cannot read migration '{}': {e}", path.display()));

        pool.execute(sqlx::raw_sql(&sql))
            .await
            .unwrap_or_else(|e| {
                panic!("migration '{}' failed: {e}", path.display())
            });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_semicolon_inside_a_literal_does_not_split_a_statement() {
        let code = "CREATE TABLE t (a int PRIMARY KEY) WITH comment = 'one; two, it''s fine';\nCREATE TABLE u (b int PRIMARY KEY);";
        let statements = split_statements(code);
        assert_eq!(statements.len(), 2, "{statements:?}");
        assert!(statements[0].ends_with("'one; two, it''s fine'"));
        assert_eq!(statements[1], "CREATE TABLE u (b int PRIMARY KEY)");
    }
}
