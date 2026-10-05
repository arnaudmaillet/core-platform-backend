//! Lazy, shared ephemeral backends for a test binary.
//!
//! Every backend is booted through a process-wide [`OnceCell`] and shared by all
//! scenarios linked into the same test binary. Because each service compiles its
//! own test binary, "process-wide" is effectively "per service" — exactly the
//! one-container-set-per-service property the standard requires.
//!
//! ## Redis image override
//!
//! The `testcontainers-modules` redis module defaults to `redis:5.0`, which
//! predates sharded pub/sub (`SSUBSCRIBE`/`SPUBLISH`/`SUNSUBSCRIBE`, Redis 7.0).
//! Several services depend on those, so we pin a 7.x image explicitly for all.
//!
//! ## Teardown
//!
//! The containers live in statics, which Rust never drops — so testcontainers'
//! drop-time removal never fires and, left alone, every run would leak its
//! whole container set. (Leaked containers pile up fast; see the Scylla
//! `--reactor-backend` note for why that used to break every Scylla suite.)
//!
//! Every container is therefore created with the [`OWNER_LABEL`] label (value:
//! this process's PID), and a reaper removes everything carrying this PID's
//! label — including a container still mid-boot — when the test binary ends:
//!
//! - **Normal exit** (all tests done, pass or fail): from an `atexit` hook.
//! - **SIGINT / SIGTERM / SIGQUIT** (Ctrl-C mid-suite): from a signal thread,
//!   which then re-raises the signal so the process still dies of it.
//!
//! Both run after (or outside) the tokio runtimes the containers were created
//! on, so the reaper shells out to the `docker` CLI. testcontainers' own
//! `watchdog` feature is deliberately not used: it panics (and from then on
//! swallows Ctrl-C) when a container it tracks was already removed, which is
//! what happens to one interrupted mid-boot.
//!
//! A SIGKILL or abort skips both paths. Anything left behind can be swept with
//! `docker rm -f -v $(docker ps -aq --filter label=core-platform.test-support.pid)`.

use std::io::Write;
use std::process::{Command, Stdio};
use std::sync::Once;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use signal_hook::consts::{SIGINT, SIGQUIT, SIGTERM};
use signal_hook::iterator::Signals;
use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, ContainerRequest, GenericImage, Image, ImageExt};
use testcontainers_modules::kafka::apache::{KAFKA_PORT, Kafka};
use testcontainers_modules::minio::MinIO;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::scylladb::ScyllaDB;
use tokio::sync::OnceCell;

use crate::migrate;

/// Internal Redis port; the 7.x image exposes it and testcontainers maps it.
const REDIS_PORT: u16 = 6379;
/// Internal ScyllaDB CQL port.
const SCYLLA_CQL_PORT: u16 = 9042;
/// Internal PostgreSQL port.
const POSTGRES_PORT: u16 = 5432;
/// Internal OpenSearch REST port.
const OPENSEARCH_PORT: u16 = 9200;
/// Internal MinIO S3 API port.
const MINIO_PORT: u16 = 9000;
/// MinIO image: the community-maintained `pgsty/minio` fork on Docker Hub (see
/// [`minio_ready`] for why upstream `minio/minio` is unusable).
const MINIO_IMAGE: &str = "pgsty/minio";
/// Release tag + multi-arch (amd64/arm64) index digest. The digest makes the pin
/// immutable: a re-pushed tag fails the pull instead of silently changing the
/// backend under the suites.
const MINIO_TAG: &str = "RELEASE.2026-08-04T00-00-00Z\
     @sha256:b6bfe7239bfc83fb90d31612d9704d86039dd714f7904b3f1ad68f211e602372";

static SCYLLA: OnceCell<ContainerAsync<ScyllaDB>> = OnceCell::const_new();
static REDIS: OnceCell<ContainerAsync<GenericImage>> = OnceCell::const_new();
static KAFKA: OnceCell<ContainerAsync<Kafka>> = OnceCell::const_new();
static POSTGRES: OnceCell<ContainerAsync<Postgres>> = OnceCell::const_new();
static OPENSEARCH: OnceCell<ContainerAsync<GenericImage>> = OnceCell::const_new();
static MINIO: OnceCell<ContainerAsync<MinIO>> = OnceCell::const_new();

static SCYLLA_MIGRATED: OnceCell<()> = OnceCell::const_new();
static POSTGRES_MIGRATED: OnceCell<()> = OnceCell::const_new();

/// Label stamped on every container this crate boots; the value is the PID of
/// the test process that owns it (see the module docs, "Teardown").
pub const OWNER_LABEL: &str = "core-platform.test-support.pid";

static REAPER: Once = Once::new();
/// Set by the signal reaper: the process is about to die, boot nothing new.
static INTERRUPTED: AtomicBool = AtomicBool::new(false);

// ── Lifecycle ────────────────────────────────────────────────────────────────

/// Starts `request` labelled with [`OWNER_LABEL`], with the reaper armed first
/// so a container interrupted mid-boot is removed too.
async fn boot<I: Image>(
    request: impl Into<ContainerRequest<I>>,
    backend: &str,
) -> ContainerAsync<I> {
    REAPER.call_once(arm_reaper);
    if INTERRUPTED.load(Ordering::SeqCst) {
        // A signal reaper is tearing down; a boot now (typically a test
        // retrying the `OnceCell` init the reaper just broke) would only leak
        // a fresh container. Park until the process dies.
        std::future::pending::<()>().await;
    }
    let request: ContainerRequest<I> = request.into();
    ensure_image(&request.descriptor()).await;
    request
        .with_label(OWNER_LABEL, std::process::id().to_string())
        .start()
        .await
        .unwrap_or_else(|e| panic!("failed to start the {backend} test container: {e:?}"))
}

/// Pull attempts [`ensure_image`] makes before handing over to testcontainers.
const PULL_ATTEMPTS: u32 = 4;
/// Linear backoff step between pull attempts (5s, 10s, 15s).
const PULL_BACKOFF: Duration = Duration::from_secs(5);

/// `docker pull` stderr fragments that mean retrying is pointless.
const PERMANENT_PULL_ERRORS: &[&str] = &[
    "not found",
    "manifest unknown",
    "denied",
    "unauthorized",
    NO_DOCKER_CLI,
];
/// [`docker`]'s error when the CLI itself can't be spawned.
const NO_DOCKER_CLI: &str = "could not run the docker CLI";

/// Makes sure `descriptor` (`name:tag[@digest]`) is in the local image store,
/// pulling it with retries if not, so a registry hiccup doesn't fail a suite.
///
/// testcontainers pulls a missing image exactly once and turns any transport
/// error into a boot failure — CI lost whole suites to a Docker Hub stream cut
/// mid-pull (`PullImage { .. "bytes remaining on stream" }`, 2026-10-04) and to
/// timeouts. Pulling here first, with backoff, absorbs those. Docker keeps the
/// layers that did complete, so a retry resumes rather than restarts.
///
/// An image that is already present costs one local `docker image inspect` —
/// no registry round-trip, so offline runs on a warm cache are unchanged. This
/// never fails: on a permanent error (unknown image, denied, no `docker`
/// CLI) or after the last attempt, it falls through to testcontainers' own
/// pull, whose error the caller reports exactly as before.
///
/// [`boot`] calls it for every backend; a harness that starts its own
/// containers (e.g. transport's Kafka) can call it before `start()`.
pub async fn ensure_image(descriptor: &str) {
    if docker(&["image", "inspect", "--format", "{{.Id}}", descriptor])
        .await
        .is_ok()
    {
        return;
    }
    for attempt in 1..=PULL_ATTEMPTS {
        match docker(&["pull", "--quiet", descriptor]).await {
            Ok(()) => return,
            Err(e) => {
                let _ = writeln!(
                    std::io::stderr(),
                    "test-support: pulling {descriptor} failed (attempt {attempt}/{PULL_ATTEMPTS}): {e}"
                );
                // A missing image or tag, an auth wall, or no CLI won't heal
                // on retry: hand it straight to testcontainers to report.
                if PERMANENT_PULL_ERRORS.iter().any(|p| e.contains(p)) {
                    return;
                }
                if attempt < PULL_ATTEMPTS {
                    tokio::time::sleep(PULL_BACKOFF * attempt).await;
                }
            }
        }
    }
}

/// Runs `docker <args>` off the async runtime; `Err` carries stderr (or the
/// spawn error) for the retry notice.
async fn docker(args: &[&str]) -> Result<(), String> {
    let args: Vec<String> = args.iter().map(|a| (*a).to_owned()).collect();
    let output = tokio::task::spawn_blocking(move || {
        Command::new("docker")
            .args(&args)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .output()
    })
    .await
    .map_err(|e| e.to_string())?
    .map_err(|e| format!("{NO_DOCKER_CLI}: {e}"))?;
    if output.status.success() {
        Ok(())
    } else {
        Err(String::from_utf8_lossy(&output.stderr).trim().to_owned())
    }
}

/// Runs [`reap`] at normal exit (`atexit`) and on SIGINT/SIGTERM/SIGQUIT.
fn arm_reaper() {
    extern "C" fn reap_at_exit() {
        reap();
    }
    // SAFETY: `reap_at_exit` is a plain `extern "C" fn()` that never unwinds.
    if unsafe { libc::atexit(reap_at_exit) } != 0 {
        warn("could not register the exit-time container reaper");
    }

    let spawned = Signals::new([SIGINT, SIGTERM, SIGQUIT]).and_then(|mut signals| {
        std::thread::Builder::new()
            .name("test-support-reaper".into())
            .spawn(move || {
                if let Some(signal) = signals.forever().next() {
                    // Stop new boots, then sweep until a pass finds nothing,
                    // catching a create that was already in flight.
                    INTERRUPTED.store(true, Ordering::SeqCst);
                    for _ in 0..5 {
                        if reap() == 0 {
                            break;
                        }
                        std::thread::sleep(Duration::from_millis(250));
                    }
                    // Restores the default disposition and re-raises: the
                    // process dies of the signal, as if never intercepted.
                    let _ = signal_hook::low_level::emulate_default_handler(signal);
                }
            })
    });
    if let Err(e) = spawned {
        warn(&format!(
            "could not install the signal-time container reaper: {e}"
        ));
    }
}

/// Force-removes (with anonymous volumes) every container labelled with this
/// process's PID and returns how many it found. Never panics: it runs inside an
/// `extern "C"` `atexit` hook, where an unwind would abort.
fn reap() -> usize {
    let owner = format!("label={OWNER_LABEL}={}", std::process::id());
    let listed = Command::new("docker")
        .args(["ps", "--all", "--quiet", "--filter", &owner])
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .output();
    let ids = match listed {
        Ok(out) if out.status.success() => String::from_utf8_lossy(&out.stdout).into_owned(),
        other => {
            warn(&format!(
                "could not list this run's test containers ({other:?})"
            ));
            return 0;
        }
    };
    let ids: Vec<&str> = ids.split_whitespace().collect();
    if ids.is_empty() {
        return 0;
    }

    let removed = Command::new("docker")
        .args(["rm", "--force", "--volumes"])
        .args(&ids)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
    if !matches!(removed, Ok(status) if status.success()) {
        warn(&format!(
            "could not remove test containers {ids:?} ({removed:?})"
        ));
    }
    ids.len()
}

/// Best-effort stderr notice with the manual cleanup command; never panics.
fn warn(what: &str) {
    let _ = writeln!(
        std::io::stderr(),
        "test-support: {what}; remove leftovers with \
         `docker rm -f -v $(docker ps -aq --filter label={OWNER_LABEL})`"
    );
}

// ── ScyllaDB ─────────────────────────────────────────────────────────────────

/// Boots ScyllaDB (once) and returns its `host:port` contact point.
///
/// `--developer-mode 1 --smp 1 --reactor-backend epoll` makes a single-node boot
/// fast and reliable on untuned hosts (CI / macOS), several at a time.
pub async fn scylla_contact_point() -> String {
    let container = SCYLLA
        .get_or_init(|| async {
            // Tag pinned to the version PROD runs (core-platform-infra: k8s/base/infra/
            // scylla-cluster-prod → 5.4.0) — bump in lockstep. The module's
            // floating default drifted to a release whose CQL parser rejects
            // six services' migrations: local runs kept passing on cached old
            // images while fresh CI pulls failed (first-ever CI execution of
            // these suites, 2026-07-05).
            //
            // `--reactor-backend epoll`: the default linux-aio backend reserves
            // ~51k of the Docker host's 65536 `fs.aio-max-nr` events per node, so
            // a second concurrent Scylla (another suite, worktree, or a leaked
            // container) fails to boot with `StartupTimeout`. epoll needs 1024
            // and boots as fast; CQL semantics are unchanged.
            let request = ScyllaDB::default().with_tag("5.4.0").with_cmd([
                "--developer-mode",
                "1",
                "--smp",
                "1",
                "--reactor-backend",
                "epoll",
            ]);
            boot(request, "ScyllaDB").await
        })
        .await;

    let port = container
        .get_host_port_ipv4(SCYLLA_CQL_PORT)
        .await
        .expect("failed to resolve the mapped ScyllaDB port");
    format!("127.0.0.1:{port}")
}

/// Boots ScyllaDB (once), applies the service's `.cql` migrations from
/// `migrations_dir` (once, with the single-node `SimpleStrategy RF=1` rewrite of
/// the `keyspace`), and returns the contact point.
///
/// `keyspace` is the name the service's `0001_create_keyspace.cql` provisions;
/// `migrations_dir` is typically `concat!(env!("CARGO_MANIFEST_DIR"), "/migrations")`
/// so the suite exercises exactly the DDL that ships.
pub async fn scylla_ready(keyspace: &str, migrations_dir: &str) -> String {
    let contact_point = scylla_contact_point().await;

    SCYLLA_MIGRATED
        .get_or_init(|| {
            let cp = contact_point.clone();
            let keyspace = keyspace.to_owned();
            let dir = migrations_dir.to_owned();
            async move { migrate::scylla_apply(&cp, &keyspace, &dir).await }
        })
        .await;

    contact_point
}

// ── Redis ────────────────────────────────────────────────────────────────────

/// Boots a Redis 7.x node (once) and returns its `host:port`.
pub async fn redis_endpoint() -> String {
    let container = REDIS
        .get_or_init(|| async {
            boot(
                GenericImage::new("redis", "7-alpine")
                    .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections")),
                "Redis",
            )
            .await
        })
        .await;

    let port = container
        .get_host_port_ipv4(REDIS_PORT)
        .await
        .expect("failed to resolve the mapped Redis port");
    format!("127.0.0.1:{port}")
}

// ── OpenSearch ───────────────────────────────────────────────────────────────

/// Boots a single-node OpenSearch (once, security plugin disabled so it speaks
/// plain HTTP on 9200) and returns its base URL, e.g. `http://127.0.0.1:49xxx`.
///
/// The search service's adapter creates its own indices/aliases at boot
/// (`IndexAdmin::ensure_indices`), so there is no migration step here — unlike
/// Scylla/Postgres. A small JVM heap keeps the container light on CI / laptops.
pub async fn opensearch_ready() -> String {
    let container = OPENSEARCH
        .get_or_init(|| async {
            boot(
                GenericImage::new("opensearchproject/opensearch", "2.15.0")
                    .with_wait_for(WaitFor::message_on_stdout("] started"))
                    .with_env_var("discovery.type", "single-node")
                    .with_env_var("DISABLE_SECURITY_PLUGIN", "true")
                    .with_env_var("DISABLE_INSTALL_DEMO_CONFIG", "true")
                    .with_env_var("OPENSEARCH_JAVA_OPTS", "-Xms512m -Xmx512m"),
                "OpenSearch",
            )
            .await
        })
        .await;

    let port = container
        .get_host_port_ipv4(OPENSEARCH_PORT)
        .await
        .expect("failed to resolve the mapped OpenSearch port");
    format!("http://127.0.0.1:{port}")
}

// ── MinIO (S3-compatible object storage) ─────────────────────────────────────

/// Boots a single MinIO container (once) and returns its S3 API base URL, e.g.
/// `http://127.0.0.1:49xxx`. Default credentials are `minioadmin:minioadmin`.
///
/// There is no migration step — the media adapter creates its bucket idempotently
/// (`S3Client::ensure_bucket`) at harness start, the object-storage analogue of how
/// the OpenSearch adapter self-creates its indices.
pub async fn minio_ready() -> String {
    let container = MINIO
        .get_or_init(|| async {
            // Upstream MinIO stopped publishing anonymously pullable images:
            // Docker Hub's `minio/minio` went away on 2026-09-16 (PR #644 moved
            // to quay.io), then `quay.io/minio/minio` started answering 401 to
            // anonymous pulls on 2026-10-04 — each time breaking every
            // MinIO-backed suite. `pgsty/minio` is a community build of the same
            // server: same `server /data` CLI, same `API:` readiness line on
            // stderr, same `minioadmin:minioadmin` defaults, so the module's
            // wait condition and the suites' static keys are unchanged.
            boot(
                MinIO::default().with_name(MINIO_IMAGE).with_tag(MINIO_TAG),
                "MinIO",
            )
            .await
        })
        .await;

    let port = container
        .get_host_port_ipv4(MINIO_PORT)
        .await
        .expect("failed to resolve the mapped MinIO port");
    format!("http://127.0.0.1:{port}")
}

// ── Kafka ────────────────────────────────────────────────────────────────────

/// Boots the Kafka broker (once) and returns its bootstrap `host:port`.
///
/// The `apache/kafka-native` image advertises `127.0.0.1:<mapped port>`, so
/// clients must dial that exact host.
pub async fn kafka_brokers() -> String {
    let container = KAFKA
        .get_or_init(|| async { boot(Kafka::default(), "Kafka").await })
        .await;

    let port = container
        .get_host_port_ipv4(KAFKA_PORT)
        .await
        .expect("failed to resolve the mapped Kafka port");
    format!("127.0.0.1:{port}")
}

/// Synchronously creates each topic (partitions=1, RF=1) and waits for the broker
/// to confirm, so a freshly built consumer/producer never races auto-creation.
/// A topic left over from an earlier scenario is treated as success.
pub async fn ensure_topics(brokers: &str, topics: &[&str]) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("failed to build the Kafka AdminClient");

    let new_topics: Vec<NewTopic> = topics
        .iter()
        .map(|name| NewTopic::new(name, 1, TopicReplication::Fixed(1)))
        .collect();

    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(10)));
    let results = admin
        .create_topics(&new_topics, &opts)
        .await
        .expect("the create_topics request failed");

    for result in results {
        if let Err((topic, code)) = result
            && code != rdkafka::types::RDKafkaErrorCode::TopicAlreadyExists
        {
            panic!("failed to create topic '{topic}': {code}");
        }
    }
}

// ── Postgres ─────────────────────────────────────────────────────────────────

/// Boots PostgreSQL (once), applies the service's `.sql` migrations from
/// `migrations_dir` (once), and returns a connection URL.
///
/// Unlike ScyllaDB there is no replication rewrite — a single Postgres node is a
/// faithful production analogue. The default image credentials are
/// `postgres:postgres` / database `postgres`.
pub async fn postgres_ready(migrations_dir: &str) -> String {
    let container = POSTGRES
        .get_or_init(|| async {
            // Pinned to prod's major (CNPG ghcr postgresql:16) — same
            // lockstep rule as the Scylla tag above.
            boot(Postgres::default().with_tag("16"), "Postgres").await
        })
        .await;

    let port = container
        .get_host_port_ipv4(POSTGRES_PORT)
        .await
        .expect("failed to resolve the mapped Postgres port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");

    POSTGRES_MIGRATED
        .get_or_init(|| {
            let url = url.clone();
            let dir = migrations_dir.to_owned();
            async move { migrate::postgres_apply(&url, &dir).await }
        })
        .await;

    url
}
