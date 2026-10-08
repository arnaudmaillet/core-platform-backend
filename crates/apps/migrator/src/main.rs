//! `migrator` — the fleet schema-migration runner.
//!
//! Applies each service's migrations against the right backend (ScyllaDB for the
//! `.cql` services, PostgreSQL for `account`'s `.sql`). Migrations are **embedded
//! at compile time** (`include_dir!`), so the binary is self-contained — no
//! source tree or mounted volume needed in the container. Application is
//! **tracked and idempotent**: each applied `(service, version)` is recorded, so
//! re-running skips what's already done.
//!
//! Usage:
//! ```text
//! migrator              # migrate every service
//! migrator chat         # migrate one service
//! ```
//!
//! Connection settings come from the same env vars the services use
//! (`SCYLLA_*`, `PG_*` / `DATABASE_URL`).
//!
//! Scylla files are split into statements with a quote-aware splitter (see
//! [`split_statements`]) that ignores `;` inside string literals — several `comment
//! = '...'` clauses contain semicolons. Postgres files run whole via `sqlx::raw_sql`.

use std::collections::BTreeSet;

use anyhow::{Context, Result};
use include_dir::{include_dir, Dir};
use postgres_storage::{PgPoolBuilder, PostgresConfig};
use scylla_storage::{ScyllaConfig, ScyllaSessionBuilder};

/// Which backend a service's migrations target.
enum Store {
    Scylla,
    Postgres,
}

struct ServiceMigrations {
    name: &'static str,
    store: Store,
    dir: Dir<'static>,
}

/// Every service's embedded migration tree, in dependency-free order (each
/// service's keyspace/schema is self-contained).
fn services() -> Vec<ServiceMigrations> {
    vec![
        ServiceMigrations { name: "chat",          store: Store::Scylla,   dir: include_dir!("$CARGO_MANIFEST_DIR/../../services/chat/migrations") },
        ServiceMigrations { name: "profile",       store: Store::Scylla,   dir: include_dir!("$CARGO_MANIFEST_DIR/../../services/profile/migrations") },
        ServiceMigrations { name: "social-graph",  store: Store::Scylla,   dir: include_dir!("$CARGO_MANIFEST_DIR/../../services/social-graph/migrations") },
        ServiceMigrations { name: "post",          store: Store::Scylla,   dir: include_dir!("$CARGO_MANIFEST_DIR/../../services/post/migrations") },
        ServiceMigrations { name: "engagement",    store: Store::Scylla,   dir: include_dir!("$CARGO_MANIFEST_DIR/../../services/engagement/migrations") },
        ServiceMigrations { name: "comment",       store: Store::Scylla,   dir: include_dir!("$CARGO_MANIFEST_DIR/../../services/comment/migrations") },
        ServiceMigrations { name: "geo-discovery", store: Store::Scylla,   dir: include_dir!("$CARGO_MANIFEST_DIR/../../services/geo-discovery/migrations") },
        ServiceMigrations { name: "notification",  store: Store::Scylla,   dir: include_dir!("$CARGO_MANIFEST_DIR/../../services/notification/migrations") },
        ServiceMigrations { name: "timeline",      store: Store::Scylla,   dir: include_dir!("$CARGO_MANIFEST_DIR/../../services/timeline/migrations") },
        // `counter` is the only dual-store service: its warm ledger is Postgres and
        // its cold time-series is Scylla, so it registers one entry per backend
        // (each pointing at the matching `migrations/<store>` subdir).
        ServiceMigrations { name: "counter-timeseries", store: Store::Scylla,   dir: include_dir!("$CARGO_MANIFEST_DIR/../../services/counter/migrations/scylla") },
        // `moderation` is dual-store like `counter`: its decision/enforcement SoR is
        // Postgres and its append-only history feed is Scylla. Same split: one entry
        // per backend, each pointing at the matching `migrations/<store>` subdir.
        ServiceMigrations { name: "moderation-history", store: Store::Scylla,   dir: include_dir!("$CARGO_MANIFEST_DIR/../../services/moderation/migrations/scylla") },
        ServiceMigrations { name: "account",            store: Store::Postgres, dir: include_dir!("$CARGO_MANIFEST_DIR/../../services/account/migrations") },
        ServiceMigrations { name: "counter",            store: Store::Postgres, dir: include_dir!("$CARGO_MANIFEST_DIR/../../services/counter/migrations/postgres") },
        ServiceMigrations { name: "audit",              store: Store::Postgres, dir: include_dir!("$CARGO_MANIFEST_DIR/../../services/audit/migrations") },
        ServiceMigrations { name: "moderation",         store: Store::Postgres, dir: include_dir!("$CARGO_MANIFEST_DIR/../../services/moderation/migrations/postgres") },
        ServiceMigrations { name: "auth",               store: Store::Postgres, dir: include_dir!("$CARGO_MANIFEST_DIR/../../services/auth/migrations") },
        ServiceMigrations { name: "media",              store: Store::Postgres, dir: include_dir!("$CARGO_MANIFEST_DIR/../../services/media/migrations") },
        ServiceMigrations { name: "wallet",             store: Store::Postgres, dir: include_dir!("$CARGO_MANIFEST_DIR/../../services/wallet/migrations") },
    ]
}

#[tokio::main]
async fn main() -> Result<()> {
    let filter = std::env::args().nth(1);

    let all = services();
    let targets: Vec<&ServiceMigrations> = all
        .iter()
        .filter(|s| filter.as_deref().is_none_or(|f| f == s.name))
        .collect();

    if targets.is_empty() {
        anyhow::bail!(
            "unknown service '{}'; known: {}",
            filter.unwrap_or_default(),
            all.iter().map(|s| s.name).collect::<Vec<_>>().join(", ")
        );
    }

    let scylla: Vec<&ServiceMigrations> =
        targets.iter().copied().filter(|s| matches!(s.store, Store::Scylla)).collect();
    let postgres: Vec<&ServiceMigrations> =
        targets.iter().copied().filter(|s| matches!(s.store, Store::Postgres)).collect();

    if !scylla.is_empty() {
        migrate_scylla(&scylla).await.context("scylla migrations")?;
    }
    if !postgres.is_empty() {
        migrate_postgres(&postgres).await.context("postgres migrations")?;
    }

    println!("migrations complete");
    Ok(())
}

// ── ScyllaDB ─────────────────────────────────────────────────────────────────

async fn migrate_scylla(targets: &[&ServiceMigrations]) -> Result<()> {
    // No keyspace on the session: the service keyspaces are created by their own
    // 0001 migration, and every statement is keyspace-qualified.
    let mut cfg = ScyllaConfig::from_env();
    cfg.keyspace = None;
    let client = ScyllaSessionBuilder::new(cfg).build().await.context("connect")?;
    let session = client.session.get_session();

    // Tracker (SimpleStrategy RF1 — bookkeeping, not service data).
    session
        .query_unpaged(
            "CREATE KEYSPACE IF NOT EXISTS migrations WITH replication = \
             {'class': 'SimpleStrategy', 'replication_factor': 1}",
            (),
        )
        .await
        .context("create migrations keyspace")?;
    session
        .query_unpaged(
            "CREATE TABLE IF NOT EXISTS migrations.applied \
             (service text, version text, applied_at timestamp, PRIMARY KEY ((service), version))",
            (),
        )
        .await
        .context("create migrations.applied")?;

    for svc in targets {
        println!("[scylla] {}", svc.name);

        let result = session
            .query_unpaged("SELECT version FROM migrations.applied WHERE service = ?", (svc.name,))
            .await
            .context("read applied versions")?
            .into_rows_result()
            .context("applied: rows")?;
        let mut applied = BTreeSet::new();
        for row in result.rows::<(String,)>().context("applied: typed")? {
            applied.insert(row.context("applied: deser")?.0);
        }

        for (version, content) in sorted_migrations(&svc.dir) {
            if applied.contains(&version) {
                println!("  [skip] {version}");
                continue;
            }
            for stmt in split_statements(&content) {
                match session.query_unpaged(stmt.as_str(), ()).await {
                    Ok(_) => {}
                    // Idempotency for statements that can't express `IF NOT EXISTS`
                    // (Scylla has no `ALTER TABLE ADD IF NOT EXISTS`): re-adding an
                    // existing column on a fresh cluster is a benign no-op. Versioning
                    // already prevents intentional re-runs, so this only covers the
                    // fresh-cluster case, not masking real drift.
                    Err(e) if is_already_exists(&e.to_string()) => {
                        println!("  [exists] {version}: {}", first_line(&stmt));
                    }
                    Err(e) => {
                        return Err(e).with_context(|| {
                            format!("{}/{version}: {}", svc.name, first_line(&stmt))
                        });
                    }
                }
            }
            session
                .query_unpaged(
                    "INSERT INTO migrations.applied (service, version, applied_at) \
                     VALUES (?, ?, toTimestamp(now()))",
                    (svc.name, version.as_str()),
                )
                .await
                .with_context(|| format!("record {}/{version}", svc.name))?;
            println!("  [done] {version}");
        }
    }

    Ok(())
}

// ── PostgreSQL ───────────────────────────────────────────────────────────────

async fn migrate_postgres(targets: &[&ServiceMigrations]) -> Result<()> {
    let pool = PgPoolBuilder::build(PostgresConfig::from_env()).await.context("connect")?;

    sqlx::raw_sql(
        "CREATE TABLE IF NOT EXISTS schema_migrations \
         (service text NOT NULL, version text NOT NULL, \
          applied_at timestamptz NOT NULL DEFAULT now(), \
          PRIMARY KEY (service, version))",
    )
    .execute(&pool)
    .await
    .context("create schema_migrations")?;

    for svc in targets {
        println!("[postgres] {}", svc.name);

        let rows: Vec<(String,)> =
            sqlx::query_as("SELECT version FROM schema_migrations WHERE service = $1")
                .bind(svc.name)
                .fetch_all(&pool)
                .await
                .context("read applied versions")?;
        let applied: BTreeSet<String> = rows.into_iter().map(|r| r.0).collect();

        for (version, content) in sorted_migrations(&svc.dir) {
            if applied.contains(&version) {
                println!("  [skip] {version}");
                continue;
            }
            // `raw_sql` runs the whole file (multiple statements) in one round-trip.
            sqlx::raw_sql(&content)
                .execute(&pool)
                .await
                .with_context(|| format!("{}/{version}", svc.name))?;
            sqlx::query("INSERT INTO schema_migrations (service, version) VALUES ($1, $2)")
                .bind(svc.name)
                .bind(&version)
                .execute(&pool)
                .await
                .with_context(|| format!("record {}/{version}", svc.name))?;
            println!("  [done] {version}");
        }
    }

    Ok(())
}

// ── Helpers ──────────────────────────────────────────────────────────────────

/// Returns `(version, contents)` for each `.cql`/`.sql` file, ordered by filename
/// (which is the numeric migration prefix).
fn sorted_migrations(dir: &Dir<'static>) -> Vec<(String, String)> {
    let mut files: Vec<(String, String)> = dir
        .files()
        .filter(|f| {
            matches!(f.path().extension().and_then(|e| e.to_str()), Some("cql") | Some("sql"))
        })
        .filter_map(|f| {
            let name = f.path().file_name()?.to_string_lossy().into_owned();
            let body = f.contents_utf8()?.to_owned();
            Some((name, body))
        })
        .collect();
    files.sort_by(|a, b| a.0.cmp(&b.0));
    files
}

/// Splits a migration file into individual statements on `;`, ignoring `;` inside
/// single-quoted string literals (e.g. a `comment = '... ; ...'` clause) and
/// stripping `--` line comments. Single-pass and quote-aware; handles CQL's `''`
/// escape for a literal quote inside a string.
fn split_statements(content: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut in_string = false;
    let mut chars = content.chars().peekable();

    while let Some(c) = chars.next() {
        if in_string {
            current.push(c);
            if c == '\'' {
                // `''` is an escaped quote, not the end of the string.
                if chars.peek() == Some(&'\'') {
                    current.push(chars.next().unwrap());
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
            // `--` line comment: skip to end of line (the newline is handled next).
            '-' if chars.peek() == Some(&'-') => {
                chars.next();
                while chars.peek().is_some_and(|&n| n != '\n') {
                    chars.next();
                }
            }
            ';' => {
                let stmt = current.trim();
                if !stmt.is_empty() {
                    statements.push(stmt.to_owned());
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

/// Whether a Scylla error is a benign "this DDL target already exists" — used to
/// keep non-`IF NOT EXISTS` statements (notably `ALTER TABLE ... ADD`) idempotent
/// on a fresh cluster.
fn is_already_exists(msg: &str) -> bool {
    let m = msg.to_ascii_lowercase();
    m.contains("already exists") || m.contains("conflicts with an existing column")
}

/// First non-empty line of a statement, for error context.
fn first_line(stmt: &str) -> &str {
    stmt.lines().map(str::trim).find(|l| !l.is_empty()).unwrap_or(stmt)
}

#[cfg(test)]
mod tests {
    use super::split_statements;

    #[test]
    fn ignores_semicolons_inside_string_literals() {
        let cql = "CREATE TABLE t (id uuid PRIMARY KEY) \
                   AND comment = 'scores; and more'; CREATE TABLE u (id uuid PRIMARY KEY);";
        let stmts = split_statements(cql);
        assert_eq!(stmts.len(), 2, "got: {stmts:?}");
        assert!(stmts[0].contains("'scores; and more'"));
        assert!(stmts[1].starts_with("CREATE TABLE u"));
    }

    #[test]
    fn strips_line_comments_and_blank_statements() {
        let cql = "-- header\nCREATE KEYSPACE ks; -- trailing\n\n;";
        let stmts = split_statements(cql);
        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].starts_with("CREATE KEYSPACE ks"));
    }
}
