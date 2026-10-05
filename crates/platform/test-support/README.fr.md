---
i18n:
  source: ./README.md
  source_sha256: 23f35611a7c37440ad0802aac4fe79697d1e53629304a089205a591cf699bf9b
  translated_at: 2026-10-05
  status: complete
---
> 🇫🇷 Traduction française — la version **anglaise** [`README.md`](./README.md) fait foi.
> En cas de divergence, l'anglais prime. Les contrats (codes d'erreur, variables
> d'environnement, signatures, identifiants) sont volontairement laissés en anglais.

# `test-support` — Échafaudage de tests d'intégration partagé : conteneurs, migrations, et la primitive d'attente anti-flake

> **Fiche crate**
>
> | | |
> |---|---|
> | **Rôle** | `platform` — épine dorsale de test **dev-only** (jamais liée dans un binaire de service) |
> | **Package** | `test-support` (dir : `crates/platform/test-support`) |
> | **Consommé par** | la suite d'intégration live de chaque service (`tests/<svc>_it/`), en `[dev-dependency]` |
> | **Dépend de** | `testcontainers(-modules)`, `rdkafka`, `tokio`, `scylla(-storage)`, `sqlx`, `tracing`, `libc` + `signal-hook` (reaper de conteneurs) |
> | **Stabilité** | contrat stable |
> | **Feature flags** | aucun |
> | **Propriétaire** | `<TODO: équipe>` · `<TODO: #canal-slack>` |

---

## 🎯 Vue d'ensemble & rôle

`test-support` est l'épine dorsale backend-agnostique sur laquelle la suite de tests live de chaque
service est bâtie. Il possède les parties *identiques* d'un service à l'autre — orchestration de
conteneurs, runners de migration, et la primitive de synchronisation — pour que le `tests/<svc>_it/` de
chaque service ne porte que ce qui est irréductiblement spécifique (son graphe de racine de composition
et ses scénarios).

**Frontière architecturale** — **dev-only** : c'est une `[dev-dependency]` et ne doit **jamais** être
liée dans un binaire de service. Il fournit l'infrastructure, pas la logique de test ; la discipline de
namespacing qui donne l'isolation parallèle vit dans le harnais de chaque service, pas ici.

---

## 📐 Architecture & décisions clés

Les six piliers (extraits de la suite gold-standard `chat`) :

- **Un jeu de conteneurs par binaire de test** — chaque backend boote paresseusement via un
  `tokio::sync::OnceCell` et est partagé par chaque scénario du binaire ; Kafka/Postgres ne bootent que
  quand un scénario le demande pour la première fois. Les statics ne sont jamais droppés : un reaper
  supprime donc chaque conteneur labellisé avec le PID du process (`core-platform.test-support.pid`) à la
  sortie normale (`atexit`) et sur SIGINT/SIGTERM/SIGQUIT — le jeu vit exactement aussi longtemps que le
  binaire de test.
- **Zéro conflit de port** — chaque endpoint est résolu depuis le **port hôte mappé assigné par l'OS** ;
  rien n'est lié statiquement, donc les suites tournent en concurrence.
- **Migrations appliquées exactement une fois** — derrière un `OnceCell`, avec l'adaptation de réplication
  single-node (ScyllaDB `SimpleStrategy RF=1`) ou un runner SQL brut (Postgres).
- **Zéro sleep fixe** — `await_until` est l'*unique* primitive de synchronisation : les assertions
  pollent l'état observable contre une deadline, ne `sleep` jamais une durée fixe. C'est la règle
  anti-flake.
- **Isolation par namespacing, pas teardown** — les scénarios génèrent des clés UUID fraîches pour que la
  suite tourne en parallèle contre les conteneurs partagés (la discipline vit dans chaque harnais ; ce
  crate fournit l'infra).
- **Un hoquet de registry ne fait pas échouer une suite** — avant chaque boot, `ensure_image` vérifie le
  store d'images local et, si l'image manque, la `docker pull` en 4 tentatives avec backoff linéaire (un
  flux coupé ou un timeout de Docker Hub faisait échouer tout le job CI). Une image en cache coûte un
  `docker image inspect` local ; une erreur permanente (tag inconnu, accès refusé) n'est pas retentée et
  remonte via testcontainers exactement comme avant.

---

## 🔌 API publique & contrat

```rust
// containers.rs — lazy, OnceCell-backed, OS-mapped-port endpoints
pub async fn scylla_contact_point() -> String;
pub async fn scylla_ready(keyspace: &str, migrations_dir: &str) -> String;   // boot + migrate once
pub async fn redis_endpoint() -> String;
pub async fn kafka_brokers() -> String;
pub async fn ensure_topics(brokers: &str, topics: &[&str]);
pub async fn postgres_ready(migrations_dir: &str) -> String;                 // boot + migrate once
pub async fn ensure_image(descriptor: &str);                                  // pull-if-missing, retried (boot calls it)
pub const OWNER_LABEL: &str = "core-platform.test-support.pid";              // on every container; value = PID

// migrate.rs — idempotent runners (single-node adaptation)
pub async fn scylla_apply(contact_point: &str, keyspace: &str, migrations_dir: &str);
pub async fn postgres_apply(url: &str, migrations_dir: &str);

// wait.rs — THE synchronization primitive
pub async fn await_until<F, Fut>(label: &str, deadline: Duration, probe: F)   // re-exported at crate root
where F: FnMut() -> Fut, Fut: Future<Output = bool>;
```

> **Contrat :** `scylla_ready`/`postgres_ready` sont les points d'entrée canoniques — ils bootent le
> conteneur (une fois) *et* appliquent les migrations (une fois) avant de renvoyer le endpoint. Ne jamais
> ajouter de `sleep` fixe dans un harnais ; exprimer l'attente comme une sonde `await_until` sur l'état
> observable.

---

## 📦 Intégration

```toml
[dev-dependencies]                       # dev-only — NEVER a normal dependency
test-support = { workspace = true }
```

```rust
use test_support::{containers, await_until};
use std::time::Duration;

let contact = containers::scylla_ready("chat", "migrations").await;   // boots + migrates once
// ... drive the service's App::build against `contact`, run a scenario, then assert without sleeping:
await_until("message visible to guest", Duration::from_secs(5), || async {
    guest_history(&client).await.len() == 1
}).await;
```

---

## ⚙️ Configuration & feature flags

Aucun — pas de variables d'environnement ni de features cargo. Les endpoints sont découverts depuis les
conteneurs bootés (ports mappés par l'OS) ; les seuls prérequis runtime sont un **daemon Docker en cours
d'exécution** et la **CLI `docker` dans le `PATH`** (le reaper de fin de process et le pré-pull d'images l'invoquent — voir
Piège 5).

---

## 🧪 Tests

```bash
cargo clippy -p test-support --all-targets
# Exercised transitively by each service's suite, e.g.:
cargo test -p chat --features integration        # boots containers via test-support
```

Ce crate est de l'échafaudage — sa propre surface est couverte à travers les suites de service qui le
consomment.

---

## 🚨 Pièges / FAQ

> Les arêtes vives. Une entrée par piège réel.

**1. Un build a tiré `testcontainers` dans un binaire de service.**
`test-support` est **dev-only** — il doit apparaître sous `[dev-dependencies]`, jamais `[dependencies]`.
Le lier dans un binaire de service traîne l'échafaudage de test `testcontainers`/`rdkafka` en production.

**2. Test flaky qui passe en local, échoue en CI.**
Presque toujours un `sleep` fixe en course avec un conteneur lent. Le remplacer par `await_until(label,
deadline, probe)` pollant l'état observable réel — c'est tout le contrat anti-flake.

**3. La migration ScyllaDB échoue sur une erreur de réplication en single-node.**
Les runners adaptent le DDL au single-node `SimpleStrategy RF=1` ; si vous contournez
`scylla_apply`/`scylla_ready` et lancez du DDL `NetworkTopologyStrategy` brut, il ne satisfera pas le RF
sur un nœud. Passer par le runner.

**4. Deux scénarios interfèrent avec les données l'un de l'autre.**
L'isolation est par **namespacing, pas teardown** — chaque scénario doit générer des clés/topics UUID
frais. Les conteneurs sont partagés sur le binaire par conception ; ne pas compter sur une ardoise propre
entre scénarios.

**5. Des conteneurs de test restent actifs / les boots Scylla échouent en `StartupTimeout`.**
Chaque run supprime ses propres conteneurs à la sortie ou sur Ctrl-C ; seuls un SIGKILL, un abort, ou une
CLI `docker` absente (un avertissement `test-support:` sur stderr) les laissent derrière. Balayer tous les
restes, quel que soit le run :

```bash
docker rm -f -v $(docker ps -aq --filter label=core-platform.test-support.pid)
```

Scylla tourne avec `--reactor-backend epoll` (≈1k des 65536 événements `fs.aio-max-nr` de l'hôte au lieu
de ≈51k en linux-aio), si bien que plusieurs suites Scylla — ou worktrees — tournent côte à côte. Un log
de conteneur `Could not setup Async I/O … aio-max-nr` signifie que des conteneurs non labellisés d'un
checkout plus ancien tiennent le budget : supprimer à la main les conteneurs `scylladb/scylla` périmés.
