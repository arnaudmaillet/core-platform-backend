---
i18n:
  source: ./README.md
  source_sha256: 9bccff4e98dbcf49a4980cebb7655915a6529ff87643b2874aa21a6386648257
  translated_at: 2026-10-08
  status: complete
---
> 🇫🇷 Traduction française — la version **anglaise** [`README.md`](./README.md) fait foi.
> En cas de divergence, l'anglais prime. Les contrats (codes d'erreur, variables
> d'environnement, topics Kafka, identifiants) sont volontairement laissés en anglais.

# `engagement` — Scoring de réactions pondéré & compteurs d'interaction à fort volume

> **Fiche service**
>
> | | |
> |---|---|
> | **Propriétaire** | `<TODO: équipe>` · `<TODO: #canal-slack>` |
> | **Astreinte / escalade** | `<TODO: rotation-astreinte>` → `<TODO: politique-escalade>` |
> | **Palier (Tier)** | **TIER-1** — colonne vertébrale d'interaction temps réel ; dégradable vers le ledger durable |
> | **Binaire déployable** | `crates/apps/engagement-server` (crate bibliothèque : `crates/services/engagement`) |
> | **Bases de données** | Redis (chemin chaud faisant autorité) · ScyllaDB keyspace `engagement` (ledger durable) |
> | **Asynchrone** | publie `engagement.reactions` (+ `engagement.score_updated`) · consomme `comment.created` / `comment.deleted` |
> | **Appelants amont** | `<TODO: passerelle>` |
> | **Dépendances aval** | Redis, ScyllaDB, Kafka |
> | **SLO** | swap de réaction p99 **< 5 ms** (zéro Scylla sur le chemin chaud) · lecture de snapshot p99 ~0,3 ms |

---

## 🎯 Vue d'ensemble & rôle du service

`engagement` est la colonne vertébrale d'interaction temps réel. Pour chaque post publié, il possède
trois catégories de données : **réactions pondérées** (emoji/icône/gif, une active par
`(post_id, profile_id)`), **compteurs à fort volume** (vues/partages, incrémentés dans Redis et flushés
en write-behind vers Scylla), et **comptes de commentaires** (ingérés réactivement depuis `comment.*`).

Le problème difficile qu'il résout est **des millions de réactions concurrentes sans Paxos** : un
read-modify-write naïf ou un LWT ScyllaDB s'effondrerait sous les tempêtes de bascule rapide. Il résout
cela avec un **swap atomique Lua Redis-primaire** — l'exécution mono-thread de Redis sérialise tous les
swaps d'une paire `(post, profile)`, avec zéro lecture Scylla sur le chemin chaud — et un chemin
**write-behind Kafka** qui persiste le ledger durable de façon asynchrone.

**Objectifs fondamentaux :** swap de réaction sub-ms sans Scylla sur le chemin chaud ; pas de courses de
bascule rapide ; write-behind Kafka pour le streaming multi-régions et la durabilité au crash. **Redis
fait autorité ; les compteurs Scylla sont des analytics approximatifs.**

---

## 📐 Architecture & concepts

```
WRITE PATH (hot, <5ms): gRPC ─► Upsert/RemoveReaction, RecordView/Share
                           ─► RedisScoreStore (Lua EVAL / INCR, one round-trip)
                           ─► KafkaProducer(engagement.reactions, key post_id:profile_id)

WRITE-BEHIND (async): ReactionWriteBehindWorker  (consumes engagement.reactions → Scylla post_reactions, idempotent ; chaque écriture porte l'heure de son événement : le dernier événement l'emporte, quel que soit l'ordre d'arrivée)
                      CounterFlushWorker (every 5s) (DirtyPostTracker → Redis GETSET 0 → Scylla counters)
                      CommentEventConsumer (consumes comment.created/deleted → Redis INCR/DECR + Scylla counter)

READ PATH: GetPostEngagement ─► RedisScoreStore::get_snapshot (4 parallel GETs, ~0.3ms p99)
```

**Disposition des clés Redis :** `engagement:r:{post}:{profile}` (HASH, réaction par profil = source du
swap) ; `engagement:scores:{post}` (HASH, scores pondérés faisant autorité) ;
`engagement:views/shares/comments:{post}` (compteurs). **ScyllaDB :** `engagement.post_reactions` (ledger
durable, PK `((post_id), profile_id)`), `engagement.reactions_by_profile` (son miroir par profil, PK
`((profile_id), post_id)`, écrit et supprimé avec lui dans un même LOGGED BATCH),
`engagement.post_interaction_counters` (table de compteurs approximative).

**Les réactions d'un profil (#653).** `ListReactionsByProfile(profile_id, limit, page_token)` est **mesh
uniquement** (jamais sur l'edge) : l'export de données RGPD lit à quels posts un profil a réagi, avec
quelle réaction et quand, paginé par id de post (le jeton est le dernier id de post). Elle lit le ledger,
donc nécessite le chemin write-behind (Kafka + Scylla) ; sans lui la RPC répond `ENG-5003`
(`UNAVAILABLE`). Les réactions antérieures à `reactions_by_profile` sont indexées par un backfill
optionnel et idempotent au démarrage (`ENGAGEMENT_BACKFILL_REACTIONS_BY_PROFILE=true` : un parcours
paginé de `post_reactions`).

> **Invariants** (et où ils sont imposés) : une réaction active par `(post_id, profile_id)` — imposée
> atomiquement par le swap Lua ; les swaps concurrents pour la même paire sont sérialisés par le contexte
> Lua mono-thread de Redis ; l'UPSERT du ledger est idempotent (re-livraison sûre).

---

## 📊 Objectifs de niveau de service (SLO)

| SLI | Objectif | Fenêtre | Mesuré par |
|---|---|---|---|
| Swap de réaction p99 (chemin chaud) | **< 5 ms** | 1 h | `engagement_reaction_upsert_duration_ms` |
| `GetPostEngagement` p99 | ~0,3 ms (cible < 5 ms) | 1 h | histogramme de lecture de snapshot |
| Lag de flush des compteurs | `< <TODO>` posts | direct | `engagement_counter_flush_lag_posts` |
| Lag du consommateur write-behind | `< <TODO>` | direct | `engagement_write_behind_consumer_lag` |
| Durabilité (réactions) | ledger à terme cohérent | — | Kafka at-least-once → UPSERT idempotent |

**Budget d'erreur :** `<TODO>`. **En cas d'épuisement :** `<TODO>`.

---

## 🔗 Dépendances & rayon d'impact (blast radius)

**Aval :**

| Dependency | Purpose | If down → | Degradation |
|---|---|---|---|
| Redis | chemin chaud faisant autorité | les commandes réaction/vue/partage échouent | **Dur** — `503 Unavailable` (backpressure vers les appelants) |
| ScyllaDB | ledger durable + compteurs | le write-behind temporise | **Souple** — Redis reste cohérent ; le ledger rattrape |
| Kafka | write-behind + ingestion de commentaires | persistance + comptes de commentaires retardent | **Souple** — chemin chaud non affecté |
| `post` (gRPC `BatchGetLikeVisibility`, #809) | à qui est le post et si son auteur masque les compteurs de likes | likes retenus pour les non-auteurs | **Mode fermé** pour les likes seulement (vues/partages/commentaires non affectés) ; cache de 60 s |

**Amont (rayon d'impact) :**

| Caller | Uses | Impact si `engagement` est indisponible |
|---|---|---|
| `<TODO: passerelle>` | réaction/vue/partage + `GetPostEngagement` | pas de réactions, pas de comptes d'engagement sur les posts |
| `geo-discovery` | consomme `engagement.score_updated` | les scores de viralité de la carte deviennent périmés |

> **Chemin critique ?** **Oui** pour le chemin d'écriture/lecture de réactions (porté par Redis) ; la
> persistance est asynchrone.

---

## 🔌 Interfaces publiques & contrat d'API

### gRPC — `engagement.v1.EngagementService`

```protobuf
service EngagementService {
  rpc UpsertReaction    (UpsertReactionRequest)    returns (CommandResponse);
  rpc RemoveReaction    (RemoveReactionRequest)    returns (CommandResponse);
  rpc RecordView        (RecordViewRequest)        returns (CommandResponse);
  rpc RecordShare       (RecordShareRequest)       returns (CommandResponse);
  rpc GetPostEngagement (GetPostEngagementRequest) returns (PostEngagementView);
  rpc BatchGetLikes     (BatchGetLikesRequest)     returns (BatchGetLikesResponse); // likes (#665)
  rpc ListReactionsByProfile (ListReactionsByProfileRequest) returns (ListReactionsByProfileResponse); // mesh only
}
```

**Compteurs de likes masqués (#809).** Quand l'auteur d'un post masque ses compteurs de likes (réglages
d'interaction du profil), `GetPostEngagement` ne renvoie aucun `reaction_scores` et un
`total_weighted_score` nul à tout autre que l'auteur (un des profils de l'appelant, d'après le jeton) —
invités compris ; vues, partages et commentaires restent. Le mesh lit tout. À qui est le post et le
réglage de l'auteur viennent de post (`BatchGetLikeVisibility`, en cache 60 s par instance) ; quand post
ne peut pas répondre, les likes sont retenus. Sans `ENGAGEMENT_POST_GRPC_ENDPOINT`, rien n'est retenu
(un avertissement au démarrage).

**Les likes sont des points (#665).** Un like est un point misé dans le wallet ; engagement transforme le
`StakeCommitted` du wallet (`wallet.v1.events`, groupe `engagement-stakes`) en compteur de likes de chaque
post et commentaire. Chaque événement porte le **total** du compte sur la cible, appliqué par un seul
script Lua (le total du compte et la somme de la cible sous le hash tag de la cible,
`engagement:{post:<id>}:…`) : un total qui ne dépasse pas celui détenu ne change rien, si bien que les
relivraisons, le « au moins une fois » de l'outbox du wallet et les événements en désordre sont absorbés
sans marqueur. La copie durable est Scylla `likes_by_target` / `likes_by_account` (migration 0005),
écrite avec l'heure de la mise comme horodatage d'écriture. `GetPostEngagement` ajoute `like_count`,
`my_likes` (un membre : ceux de son compte) et `likes_hidden` ; `BatchGetLikes` (edge `public_read`,
≤ 100 cibles) donne la même chose pour posts et commentaires. Les compteurs masqués (#809) valent pour les
posts : `count` 0 et `hidden`, les likes du lecteur restant affichés. Les réactions cœur
(`UpsertReaction`) disparaissent dans la partie suivante.

### Ports Rust (contrat hexagonal)

```rust
pub trait ScoreStore: Send + Sync + 'static {
    async fn atomic_upsert_reaction(&self, post, profile, kind, weight) -> Result<Option<(ReactionKind, i64)>, EngagementError>;
    async fn atomic_remove_reaction(&self, post, profile) -> Result<Option<(ReactionKind, i64)>, EngagementError>;
    async fn incr_view(&self, post) -> Result<(), EngagementError>;
    async fn incr_share(&self, post) -> Result<(), EngagementError>;
    async fn get_snapshot(&self, post) -> Result<PostEngagementSnapshot, EngagementError>;
}
pub trait ReactionLedger: Send + Sync + 'static { /* upsert/remove/scan_for_recovery/apply_interaction_delta (write-behind only) */ }
```

### Contrat d'erreur (`ENG-xxxx`)

| Range | Category |
|---|---|
| `ENG-1xxx` | reaction state (not found, wrong author) |
| `ENG-2xxx` | reaction kind / weight validation |
| `ENG-3xxx` | Kafka / event publish |
| `ENG-5xxx` | worker / Lua script |
| `ENG-9xxx` | id parsing / domain violation |

---

## 📨 Contrat événementiel & asynchrone

**Publie :**

| Topic | Trigger | Key | Consumers |
|---|---|---|---|
| `engagement.reactions` | every reaction/view/share | `post_id:profile_id` | own `ReactionWriteBehindWorker`; `notification` (reactions) |
| `engagement.score_updated` | virality recompute | `post_id` | `geo-discovery` (map score sync) |

**Consomme :**

| Topic | Consumer group | Purpose | On poison/exhaustion |
|---|---|---|---|
| `comment.created` / `comment.deleted` | `engagement-comment-consumer` | INCR/DECR comment counter (Redis + Scylla) | DLQ `{topic}.dlq` |
| `wallet.v1.events` | `engagement-stakes` | `stake_committed` → likes (#665) : le total du compte sur un post ou un commentaire, idempotent et insensible à l'ordre (Lua Redis + Scylla) | DLQ `{topic}.dlq` |

> **Contrat d'exécution (obligatoire) :** le consommateur de commentaires et le worker write-behind
> s'exécutent sous `run_consumer` — commit manuel après succès, retries bornés avec backoff + jitter, DLQ
> en cas d'épuisement/poison. L'UPSERT du ledger est idempotent, donc la re-livraison est sûre.

---

## 🌩️ Modes de défaillance & dégradation

| Failure | Symptom | Service behavior | Operator action |
|---|---|---|---|
| Redis indisponible | réaction/vue/partage échouent | **Dur** — `503` ; backpressure vers les appelants | vérifier Redis ; le chemin chaud l'exige |
| ScyllaDB indisponible | le write-behind temporise | **Souple** — Redis cohérent ; le ledger rattrape | vérifier la compaction / l'I/O disque de Scylla |
| Crash de worker | partitions réassignées | rejeu at-least-once (`run_consumer`) ; UPSERT idempotent | aucune — auto-réparation |
| Redémarrage Redis **sans AOF** | scores + état de réaction perdus | les compteurs perdent la fenêtre courante ; les réactions nécessitent une récupération cold-start | activer l'AOF ; reconstruire depuis le ledger Scylla |
| Bascule rapide de réaction | — | Lua sérialise ; pas de course | aucune |

**Backpressure & limites.** Le chemin chaud est un round-trip Redis par opération. `CounterFlushWorker`
(défaut 5 s) borne l'amplification d'écriture des compteurs. Les compteurs ScyllaDB sont approximatifs par
conception — ne jamais les considérer comme faisant autorité.

---

## 📦 Intégration & utilisation

```toml
[dependencies]
engagement = { path = "crates/services/engagement" }
```

Bibliothèque uniquement. Implémente [`service_runtime::Service`](../../platform/service-runtime/README.md)
sous le nom `engagement::service::EngagementService` — `build` câble le score store Redis, la config des
poids de réaction, le publisher Kafka et les workers write-behind ; `register` ajoute les services gRPC +
réflexion ; `health_probes` vérifie Redis (le chemin chaud toujours actif). Compilé avec la feature
`i-scripts` de fred pour le Lua.

### Bootstrap (`crates/apps/engagement-server`)

```rust
use std::net::SocketAddr;
use engagement::service::EngagementService;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr: SocketAddr = std::env::var("ENGAGEMENT_GRPC_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:50058".to_owned())
        .parse()?;
    service_runtime::serve::<EngagementService>(addr).await
}
```

---

## ⚙️ Configuration & environnement d'exécution

### Matrice des poids de réaction

| Variable | Default | Description |
|---|---|---|
| `ENGAGEMENT_REACTION_WEIGHT_HEART` | `1` | ❤️ score weight |
| `ENGAGEMENT_REACTION_WEIGHT_FIRE` | `2` | 🔥 score weight |
| `ENGAGEMENT_REACTION_WEIGHT_ROCKET` | `5` | 🚀 score weight |
| `ENGAGEMENT_REACTION_WEIGHT_CLAP` | `1` | 👏 score weight |
| `ENGAGEMENT_REACTION_WEIGHT_SAD` | `1` | 😢 score weight |
| `ENGAGEMENT_BACKFILL_REACTIONS_BY_PROFILE` | non défini | `true` : indexe chaque réaction existante par profil au démarrage (une fois ; idempotent) — #653 |
| `ENGAGEMENT_POST_GRPC_ENDPOINT` | non défini | adresse mesh de post (p. ex. `http://post:50056`) : les compteurs de likes masqués sont retenus (#809). Non défini → retenus pour personne |
| `ENGAGEMENT_POST_RPC_TIMEOUT_MS` · `ENGAGEMENT_POST_CONNECT_TIMEOUT_MS` | `500` · `1000` | délais de cet appel |

### Service + infrastructure héritée

| Variable | Required | Default | Description |
|---|---|---|---|
| `ENGAGEMENT_COUNTER_FLUSH_INTERVAL_SECS` | No | `5` | View/share flush cadence. |
| `REDIS_URL` | **Yes** | — | Redis connection (AOF recommended). |
| `SCYLLA_CONTACT_POINTS` / `SCYLLA_LOCAL_DC` | **Yes** | — | ScyllaDB ledger. |
| `KAFKA_BROKERS` | **Yes** | `localhost:9092` | Kafka brokers. |
| `ENGAGEMENT_GRPC_ADDR` | No | `0.0.0.0:50058` | gRPC bind address. |

> Le réglage complet `SCYLLA_*` / `REDIS_*` / `KAFKA_*` vit dans les crates partagés storage/transport.

### Features de compilation
- `fred` avec `i-scripts` (swap atomique Lua). `build.rs` compile `proto/engagement/v1/*.proto`.

---

## 🚀 Déploiement, migrations & rollback

- **Migrations :** `0001_create_keyspace.cql` → `0002_create_post_reactions_table.cql` →
  `0003_create_post_interaction_counters_table.cql` → `0004_create_reactions_by_profile_table.cql` →
  `0005_create_likes_tables.cql` sur
  `engagement`, appliquées **avant** le premier démarrage. (Le commentaire de table de 0002 contenait un
  `;` ; le lanceur des suites d'intégration coupait dessus jusqu'à ce qu'il respecte les guillemets comme
  `apps/migrator` — la prod n'a jamais été touchée. C'est une virgule désormais — même schéma.)
- **Durabilité Redis :** activer l'AOF (`appendonly yes`, `appendfsync everysec`) — sans cela, un
  redémarrage perd la fenêtre de flush courante et nécessite une récupération cold-start depuis le ledger
  Scylla.
- **Kafka :** pré-créer `engagement.reactions` avec ≥ 12 partitions.
- **Déploiement/Rollback :** `<TODO>` ; la couche gRPC est sans état, mais les workers sont des
  consommateurs at-least-once — sûr à déployer.

---

## 📈 Télémétrie, performance & métriques

- **Runtime :** Tokio multi-thread (requis — `tokio::join!` sur le chemin de lecture).

| Signal | Why it matters | Suggested alert |
|---|---|---|
| `engagement_reaction_upsert_duration_ms` | hot-path latency | p99 > 10 ms ⇒ Redis spike |
| `engagement_counter_flush_lag_posts` | flush worker health | > 10 000 ⇒ behind |
| `engagement_write_behind_consumer_lag` | ledger persistence | > 50 000 ⇒ Kafka consumer lag |
| `engagement_redis_errors_total` | hot-path availability | any spike ⇒ Redis connectivity |
| `engagement_scylla_errors_total` | ledger durability | any spike ⇒ Scylla connectivity |

---

## 🛠️ Développement local

```bash
cargo build -p engagement && cargo clippy -p engagement -- -D warnings
cargo test  -p engagement
docker compose up -d scylla redis kafka       # repo-root compose
for f in crates/services/engagement/migrations/*.cql; do cqlsh -f "$f"; done
```

---

## 🚨 Dépannage & runbook

> Format : **symptôme → cause racine → mitigation.**

**1. Les scores de réaction dérivent après un redémarrage de Redis.**
Cause racine : Redis a été flushé/redémarré sans AOF ; les hashes `engagement:scores:*` et
`engagement:r:*` sont perdus. Mitigation : activer l'AOF pour éviter la récurrence ; exécuter la
récupération cold-start (scanner `engagement.post_reactions`, grouper par `(post_id, kind)`, sommer les
poids, reconstruire par `HSET`) avant de redémarrer le serveur gRPC.

**2. Le lag du consommateur write-behind croît continûment.**
Cause racine : Scylla écrit moins vite que le taux de produce, ou trop peu de membres de consommateur.
Mitigation : vérifier `engagement_write_behind_consumer_lag` ; scaler les instances de
`ReactionWriteBehindWorker` ; vérifier que la compaction de `post_reactions` ne sature pas l'I/O disque.

**3. Erreurs `ENG-5001 ScriptReturnInvalid` dans les logs.**
Cause racine : le swap Lua a renvoyé un type inattendu — généralement une incompatibilité de version Redis
(le comportement de retour null diffère entre 6.x et 7.x) ou une clé de mauvais type. Mitigation : vérifier
Redis ≥ 7.0 ; vérifier que `TYPE engagement:r:{post}:{profile}` est `hash` ; supprimer une clé corrompue et
laisser le prochain upsert la recréer (l'outbox Kafka rejoue quand même vers Scylla).
