---
i18n:
  source: ./README.md
  source_sha256: 88872e9fd1712b89149562b69437f16a8bdc31e88b775823d5d170e3354f1e86
  translated_at: 2026-10-10
  status: complete
---
> 🇫🇷 Traduction française — la version **anglaise** [`README.md`](./README.md) fait foi.
> En cas de divergence, l'anglais prime. Les contrats (codes d'erreur, variables
> d'environnement, topics Kafka, identifiants) sont volontairement laissés en anglais.


# `engagement` — Likes & compteurs d'interaction à fort volume

> **Fiche service**
>
> | | |
> |---|---|
> | **Propriétaire** | `<TODO: équipe>` · `<TODO: #canal-slack>` |
> | **Astreinte / escalade** | `<TODO: rotation-astreinte>` → `<TODO: politique-escalade>` |
> | **Palier (Tier)** | **TIER-1** — colonne vertébrale d'interaction temps réel ; dégradable vers le ledger durable |
> | **Binaire déployable** | `crates/apps/engagement-server` (crate bibliothèque : `crates/services/engagement`) |
> | **Bases de données** | Redis (chemin chaud faisant autorité) · ScyllaDB keyspace `engagement` (copies durables) |
> | **Asynchrone** | ne publie rien · consomme `wallet.v1.events` (likes), `account.v1.events` (effacement), `profile.v1.events` (flag de l'onglet J'aime) et `comment.created` / `comment.deleted` |
> | **Appelants amont** | `<TODO: passerelle>` |
> | **Dépendances aval** | Redis, ScyllaDB, Kafka, `post` (compteurs de likes masqués), `social-graph` (qui peut voir un onglet J'aime) |
> | **SLO** | lecture de snapshot p99 ~0,3 ms (zéro Scylla sur le chemin de lecture) |

---

## 🎯 Vue d'ensemble & rôle du service

`engagement` est la colonne vertébrale d'interaction temps réel. Pour chaque post il possède trois
catégories de données : les **likes** (#665 : un like est un point misé dans le wallet ; engagement
garde le total de chaque compte par post et par commentaire, et leur somme), les **compteurs à fort
volume** (vues/partages, incrémentés dans Redis et flushés vers Scylla) et les **comptes de
commentaires** (ingérés de manière réactive depuis `comment.*`).

Le problème difficile qu'il résout est **compter à l'échelle d'un post viral sans Paxos** : les likes
arrivent de l'outbox du wallet au moins une fois et dans n'importe quel ordre, et les vues arrivent en
rafales. Les likes sont rendus idempotents en portant le **total** du compte, appliqué par un script Lua
qui ne fait qu'avancer ; les compteurs sont de simples `INCR` Redis, flushés vers Scylla par un worker
d'arrière-plan.

**Objectifs clés :** lectures sous la milliseconde sans Scylla sur le chemin chaud ; likes exacts malgré
les re-livraisons et le désordre ; copies durables pour la récupération et l'export RGPD. **Redis fait
autorité ; les compteurs Scylla sont de l'analytique approximative.**

Les réactions pondérées (cœur, feu, fusée, applaudissements, triste : `UpsertReaction` /
`RemoveReaction`, `engagement.reactions`) ont été retirées avec #665 : un like est un point, et il n'y a
pas de retrait.

---

## 📐 Architecture & concepts

```
LIKES (async):  wallet.v1.events StakeCommitted ─► StakeConsumer (group engagement-stakes)
                  ─► RedisLikeStore (one Lua script: the account's total + the target's sum)
                  ─► ScyllaLikeLedger (likes_by_target + likes_by_account, write timestamp = stake time)

COUNTERS (hot, <5ms): gRPC ─► RecordView/Share ─► RedisScoreStore (INCR, one round-trip)
                CounterFlushWorker (every 5s) (DirtyPostTracker → Redis GETSET 0 → Scylla counters)
                CommentEventConsumer (comment.created/deleted → Redis INCR/DECR + Scylla counter)

READ PATH: GetPostEngagement / BatchGetLikes ─► Redis (counters + likes, ~0.3ms p99)
           ListLikesByAccount (mesh, GDPR export) ─► Scylla likes_by_account
```

**Disposition des clés Redis :** `engagement:{post:<id>}:likes` / `engagement:{post:<id>}:likers` (et
`{comment:<id>}` : la somme de la cible et le total de chaque compte, sous le hash tag de la cible ;
les likers expirent 30 jours après le dernier like de la cible, la somme jamais) ;
`engagement:views/shares/comments:{post}` (compteurs). **ScyllaDB :** `engagement.likes_by_target` (qui a
liké une cible, PK `((target_kind, target_id), account_id)`), `engagement.likes_by_account` (ce qu'un
compte a liké, PK `((account_id), target_kind, target_id)`), `engagement.post_interaction_counters`
(table de compteurs approximative). La migration 0006 supprime les tables des réactions,
`post_reactions` et `reactions_by_profile`.

**Positions (#665, le règlement des mises).** La valeur de chaque likeur dans `:likers` est
`total|arrivée` : ses points, et le compteur de la cible juste avant son premier like (une valeur sans
`|` lui est antérieure). Le script d'application garde l'arrivée au premier like du compte et la
renvoie à chaque application ; le consommateur des mises l'enregistre dans `likes_by_target.first_count`
(migration 0008 ; jamais écrasée par un null). `GetLikePositions(account_id, targets)` (**mesh
uniquement**, ≤ 100 cibles) donne la position sur chaque cible — `total`, `count_on_arrival`,
`count_now` — depuis Redis, ou depuis Scylla quand les likers ont expiré. Le règlement du wallet en
déduit la précocité d'un compte.

**L'onglet J'aime d'un profil (#829).** `ListLikesByProfile(profile_id, limit, page_token)` (edge
`public_read`, selon le lecteur) liste les posts que **ce profil** a likés — pas les autres profils de son
compte —, les plus récents d'abord (Scylla `liked_posts_by_profile`, migration 0009, écrite par le
consommateur des mises dans l'unique batch logged de la mise, avec l'heure de la mise ; les ids de post sont des UUIDv7, leur texte se trie donc par
date). Le propriétaire (un des profils du lecteur) et le mesh le voient toujours ; tout autre lecteur
seulement si le propriétaire montre l'onglet (`profile.v1.events` `ProfileTabSettingsChanged` `show_likes`,
groupe `engagement-profile-tabs`, gardé dans `profile_tabs`) **et** que `CheckAccess` de social-graph dit
qu'il peut voir le profil (un profil privé qu'il ne suit pas, un blocage dans un sens ou l'autre) — sinon
vide. Pour lui, chaque page ne garde que les posts dont il peut voir aussi l'auteur (#873 :
`BatchGetLikeVisibility` de post nomme les auteurs, un seul `CheckAccess` les vérifie), si bien que pas même
l'id d'un post qu'il ne pourrait pas ouvrir ne lui parvient ; une page filtrée peut être plus courte que
`limit`, et `next_page_token` suit toujours la page lue. Sans `ENGAGEMENT_SOCIAL_GRAPH_GRPC_ENDPOINT` ou
`ENGAGEMENT_POST_GRPC_ENDPOINT`, un onglet n'est visible que de son propriétaire. `likes_by_account.profile_ids`
nomme chaque profil du compte qui a liké une cible, si bien que l'effacement d'un compte retire aussi la
ligne de l'onglet de chaque profil ; la suppression d'un profil seul (`ProfileDeleted`, #873) retire son
onglet et ses réglages à la date de la suppression.

**Likers expirés.** Un hash de likers qui contient tous les likers porte `_complete` (posé au premier
like de la cible, ou à la fin d'une réhydratation). Une fois expiré, un compte absent d'un nouveau hash
est **inconnu**, pas zéro : le consommateur des mises réhydrate alors tout le hash depuis
`likes_by_target` (`HSETNX`, un total Redis plus récent l'emporte ; likers anonymes compris) avant
d'appliquer, si bien que la mise suivante n'ajoute que la différence ; un compte supprimé entre-temps
est oublié à nouveau après chaque page (sa marque `erased_accounts` est écrite avant le `HDEL` de
l'effaceur). Une lecture se rabat sur la ligne
du lecteur dans `likes_by_target` et lance une réhydratation en arrière-plan (`:rehydrating`, `SET NX`
pendant 60 s).

**Ce qu'un compte a liké (#653, #665).** `ListLikesByAccount(account_id, limit, page_token)` est **mesh
uniquement** (jamais sur l'edge) : l'export de données RGPD lit chaque post et commentaire que le compte
a liké, ses points, le profil qui a liké en dernier et quand, paginé par cible (le jeton est la dernière
cible, `kind:id`). Elle lit Scylla, donc nécessite le chemin Kafka ; sans lui la RPC répond `ENG-5003`
(`UNAVAILABLE`).

**Les likes d'un compte supprimé (RGPD art. 17).** Sur `account.v1.events` `account_deleted` (groupe
`engagement-account-erasure`), qui a liké disparaît et les compteurs restent : les points sont gardés,
anonymement. `LikeEraser` marque d'abord le compte comme effacé (`engagement.erased_accounts`, TTL de
30 jours, migration 0007, avec l'heure de la suppression), puis pour chaque cible likée retire son
entrée `:likers` et, dans un même batch logged, remplace sa ligne `likes_by_target` par une ligne
**anonyme** de même total et supprime sa ligne `likes_by_account` — les compteurs restent ainsi
reconstructibles depuis `likes_by_target`. L'id anonyme est un UUIDv5 du compte, de la cible et de
l'heure de la suppression : un rejeu (re-livraison, batch expiré mais appliqué) réécrit la même ligne,
et il ne collisionne jamais avec un id de compte (v7). Les écritures Scylla portent l'heure de
l'effacement, si bien qu'une mise faite avant lui et arrivée en retard ne peut pas réécrire le compte.
Le consommateur des mises ignore les mises d'un compte marqué, et revérifie après en avoir appliqué une
(l'effacement a pu lister les cibles avant elle).

> **Invariants** (et où ils sont imposés) : le compteur de likes d'une cible est la somme des totaux de
> ses comptes, et le total d'un compte ne fait que croître — tous deux imposés atomiquement par le script
> Lua (un total non supérieur à celui détenu ne change rien) ; la copie Scylla garde le total le plus
> récent quel que soit l'ordre d'arrivée des événements (horodatage d'écriture = heure de la mise).

---

## 📊 Objectifs de niveau de service (SLO)

| SLI | Objectif | Fenêtre | Mesuré par |
|---|---|---|---|
| `GetPostEngagement` p99 | ~0,3 ms (cible < 5 ms) | 1 h | histogramme de lecture de snapshot |
| Lag de flush des compteurs | `< <TODO>` posts | direct | `engagement_counter_flush_lag_posts` |
| Lag du consommateur des mises | `< <TODO>` | direct | lag du groupe `engagement-stakes` |
| Durabilité (likes) | copie Scylla à terme cohérente | — | Kafka at-least-once → totaux monotones |

**Budget d'erreur :** `<TODO>`. **En cas d'épuisement :** `<TODO>`.

---

## 🔗 Dépendances & rayon d'impact (blast radius)

**Aval :**

| Dependency | Purpose | If down → | Degradation |
|---|---|---|---|
| Redis | chemin chaud faisant autorité | vues/partages et lectures échouent ; mises réessayées | **Dur** — `503 Unavailable` (backpressure vers les appelants) |
| ScyllaDB | copies durables + compteurs | mises réessayées, le flush temporise ; `ListLikesByAccount` échoue | **Souple** — lectures Redis non affectées ; les copies rattrapent |
| Kafka | likes + ingestion de commentaires | comptes de likes et de commentaires retardés | **Souple** — lectures non affectées |
| `social-graph` (gRPC `CheckAccess`, #829) | si un lecteur peut voir un profil (et les auteurs d'une page d'onglet J'aime, #873) | les onglets J'aime des autres lecteurs échouent (`ENG-6002`, réessayé) | **Mode fermé** pour les onglets J'aime seulement |
| `post` (gRPC `BatchGetLikeVisibility`, #809) | à qui est le post et si son auteur masque les compteurs de likes | likes retenus pour les non-auteurs | **Mode fermé** pour les likes seulement (vues/partages/commentaires non affectés) ; cache de 60 s |

**Amont (rayon d'impact) :**

| Caller | Uses | Impact si `engagement` est indisponible |
|---|---|---|
| clients (edge) | vue/partage + `GetPostEngagement` / `BatchGetLikes` | pas de compteurs de likes ni d'engagement sur les posts |
| `account` | `ListLikesByAccount` (export RGPD) | exports réessayés au passage suivant |
| `wallet` | `GetLikePositions` (règlement des mises, #665) | les règlements attendent le passage suivant |

> **Chemin critique ?** **Oui** pour le chemin de lecture (porté par Redis) ; les likes et la
> persistance sont asynchrones.

---

## 🔌 Interfaces publiques & contrat d'API

### gRPC — `engagement.v1.EngagementService`

```protobuf
service EngagementService {
  rpc RecordView        (RecordViewRequest)        returns (CommandResponse);
  rpc RecordShare       (RecordShareRequest)       returns (CommandResponse);
  rpc GetPostEngagement (GetPostEngagementRequest) returns (PostEngagementView);
  rpc BatchGetLikes     (BatchGetLikesRequest)     returns (BatchGetLikesResponse); // likes (#665)
  rpc ListLikesByAccount (ListLikesByAccountRequest) returns (ListLikesByAccountResponse); // mesh only
  rpc GetLikePositions  (GetLikePositionsRequest)  returns (GetLikePositionsResponse);  // mesh only
  rpc ListLikesByProfile (ListLikesByProfileRequest) returns (ListLikesByProfileResponse); // a profile's Likes tab
}
```

**Compteurs de likes masqués (#809).** Quand l'auteur d'un post masque ses compteurs de likes (réglages
d'interaction du profil), `GetPostEngagement` renvoie un `like_count` nul et `likes_hidden` à tout autre
que l'auteur (un des profils de l'appelant, d'après le jeton) — invités compris ; vues, partages et
commentaires restent. Le mesh lit tout. À qui est le post et le réglage de l'auteur viennent de post
(`BatchGetLikeVisibility`, en cache 60 s par instance) ; quand post ne peut pas répondre, les likes sont
retenus. Sans `ENGAGEMENT_POST_GRPC_ENDPOINT` rien n'est retenu (un avertissement au démarrage).

**Les likes sont des points (#665).** Un like est un point misé dans le wallet ; engagement transforme
les `StakeCommitted` du wallet (`wallet.v1.events`, groupe `engagement-stakes`) en compteur de likes de
chaque post et commentaire. Chaque événement porte le **total** du compte sur la cible, appliqué par un
script Lua (le total du compte et la somme de la cible sous le hash tag de la cible,
`engagement:{post:<id>}:…`) : un total non supérieur à celui détenu ne change rien, si bien que les
re-livraisons, l'at-least-once de l'outbox du wallet et les événements dans le désordre sont absorbés
sans marqueur. La copie durable est Scylla `likes_by_target` / `likes_by_account` (migration 0005),
écrite avec l'heure de la mise comme horodatage d'écriture. `GetPostEngagement` ajoute `like_count`,
`my_likes` (un membre : ceux de son compte) et `likes_hidden` ; `BatchGetLikes` (edge `public_read`,
≤ 100 cibles) donne la même chose pour les posts et les commentaires. Les compteurs de likes masqués
(#809) s'appliquent aux posts : `count` 0 et `hidden`, les likes du lecteur restant affichés. Les champs
2 et 3 de `PostEngagementView` (`reaction_scores`, `total_weighted_score`) sont réservés.

### Ports Rust (contrat hexagonal)

```rust
pub trait LikeStore: Send + Sync + 'static {       // Redis: the hot copy
    async fn apply_total(&self, target, account, total) -> Result<i64, EngagementError>; // points added
    async fn counts(&self, targets) -> Result<Vec<i64>, EngagementError>;
    async fn mine(&self, account, targets) -> Result<Vec<i64>, EngagementError>;
}
pub trait LikeLedger: Send + Sync + 'static {      // Scylla: the durable copy
    async fn record(&self, target, account, profile_id, total, at_micros) -> Result<(), EngagementError>;
    async fn list_by_account(&self, account, limit, after) -> Result<Vec<AccountLike>, EngagementError>;
}
pub trait ScoreStore: Send + Sync + 'static { /* incr_view/share/comment, decr_comment, get_snapshot */ }
pub trait CounterLedger: Send + Sync + 'static { /* apply_interaction_delta (flush + comment consumer) */ }
```

### Contrat d'erreur (`ENG-xxxx`)

| Range | Category |
|---|---|
| `ENG-5xxx` | worker / script Lua / ledger indisponible (`ENG-5003`) |
| `ENG-6xxx` | pairs (`ENG-6001` : post indisponible, likes retenus ; `ENG-6002` : social-graph indisponible, un onglet J'aime retenu) |
| `ENG-9xxx` | parsing d'id / violation de domaine / cible de like (`ENG-9004`) |

`ENG-1001`, `ENG-2001`/`2002`, `ENG-3001` et `ENG-9002` appartenaient aux réactions pondérées ; ils sont
retirés, jamais réutilisés.

---

## 📨 Contrat événementiel & asynchrone

**Publie :** rien. Le fan-out des likes (notifications, compteurs, centres d'intérêt, classement des
pays) lit directement `wallet.v1.events` du wallet.

**Consomme :**

| Topic | Consumer group | Purpose | On poison/exhaustion |
|---|---|---|---|
| `comment.created` / `comment.deleted` | `engagement-comment-consumer` | INCR/DECR du compteur de commentaires (Redis + Scylla) | DLQ `{topic}.dlq` |
| `wallet.v1.events` | `engagement-stakes` | `stake_committed` → likes (#665) : le total du compte sur un post ou un commentaire, idempotent et insensible à l'ordre (Lua Redis + Scylla) ; les mises d'un compte supprimé sont ignorées | DLQ `{topic}.dlq` |
| `account.v1.events` | `engagement-account-erasure` | `account_deleted` → oublier qui a liké (les compteurs restent) ; autres événements ignorés | DLQ `{topic}.dlq` |
| `profile.v1.events` | `engagement-profile-tabs` | `ProfileTabSettingsChanged` → si le profil montre son onglet J'aime (#829) ; `ProfileDeleted` → son onglet et ses réglages partent, à la date de la suppression (#873) ; autres événements ignorés | DLQ `{topic}.dlq` |

> **Contrat d'exécution (obligatoire) :** les consommateurs des mises, des comptes et des commentaires tournent sous
> `run_consumer` — commit manuel après succès, retry borné avec backoff + jitter, DLQ à l'épuisement /
> sur message empoisonné. Les totaux sont monotones, la re-livraison est donc sûre.

---

## 🌩️ Modes de défaillance & dégradation

| Failure | Symptom | Service behavior | Operator action |
|---|---|---|---|
| Redis indisponible | vues/partages et lectures échouent | **Dur** — `503` ; backpressure vers les appelants | vérifier Redis ; le chemin chaud en dépend |
| ScyllaDB indisponible | mises réessayées, le flush temporise | **Souple** — lectures Redis non affectées ; les copies rattrapent | vérifier la compaction/les I/O disque Scylla |
| Crash d'un worker | partitions réassignées | rejeu at-least-once (`run_consumer`) ; totaux monotones | aucune — auto-réparation |
| Redémarrage Redis **sans AOF** | sommes de likes + compteurs perdus | les compteurs perdent la fenêtre courante ; les likes doivent être reconstruits | activer l'AOF ; reconstruire les likes depuis `likes_by_target` |
| Mise dupliquée / tardive | — | le Lua ignore un total ≤ celui détenu | aucune |

**Backpressure & limites.** Le chemin chaud fait un aller-retour Redis par opération.
`CounterFlushWorker` (5 s par défaut) borne l'amplification d'écriture des compteurs. Les compteurs
ScyllaDB sont approximatifs par conception — ne jamais les traiter comme faisant autorité.

---

## 📦 Intégration & utilisation

```toml
[dependencies]
engagement = { path = "crates/services/engagement" }
```

Bibliothèque uniquement. Implémente [`service_runtime::Service`](../../platform/service-runtime/README.md)
sous le nom `engagement::service::EngagementService` — `build` câble les stores Redis, les copies Scylla
et les workers (mises, flush des compteurs, commentaires) ; `register` ajoute les services gRPC +
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

### Likes

| Variable | Default | Description |
|---|---|---|
| `ENGAGEMENT_POST_GRPC_ENDPOINT` | non défini | adresse mesh de post (p. ex. `http://post:50056`) : les compteurs de likes masqués sont retenus (#809), et à qui est chaque post (les onglets J'aime des autres lecteurs, #873). Non défini → retenus pour personne, un onglet J'aime visible de son seul propriétaire |
| `ENGAGEMENT_POST_RPC_TIMEOUT_MS` · `ENGAGEMENT_POST_CONNECT_TIMEOUT_MS` | `500` · `1000` | délais de cet appel |
| `ENGAGEMENT_SOCIAL_GRAPH_GRPC_ENDPOINT` | non défini | adresse mesh de social-graph (p. ex. `http://social-graph:50053`) : qui peut voir l'onglet J'aime d'un profil (#829). Non défini → l'onglet n'est visible que de son propriétaire |

### Service + infrastructure héritée

| Variable | Required | Default | Description |
|---|---|---|---|
| `ENGAGEMENT_COUNTER_FLUSH_INTERVAL_SECS` | No | `5` | View/share flush cadence. |
| `REDIS_URL` | **Yes** | — | Redis connection (AOF recommended). |
| `SCYLLA_CONTACT_POINTS` / `SCYLLA_LOCAL_DC` | **Yes** | — | ScyllaDB copies. |
| `KAFKA_BROKERS` | **Yes** | `localhost:9092` | Kafka brokers. |
| `ENGAGEMENT_GRPC_ADDR` | No | `0.0.0.0:50058` | gRPC bind address. |

> Le réglage complet `SCYLLA_*` / `REDIS_*` / `KAFKA_*` vit dans les crates partagés storage/transport.

### Features de compilation
- `fred` avec `i-scripts` (le script Lua des likes). `build.rs` compile `proto/engagement/v1/*.proto`.

---

## 🚀 Déploiement, migrations & rollback

- **Migrations :** `0001_create_keyspace.cql` → `0002_create_post_reactions_table.cql` →
  `0003_create_post_interaction_counters_table.cql` → `0004_create_reactions_by_profile_table.cql` →
  `0005_create_likes_tables.cql` → `0006_drop_reaction_tables.cql` →
  `0007_create_erased_accounts_table.cql` → `0008_likes_by_target_first_count.cql` →
  `0009_create_liked_posts_by_profile.cql` sur
  `engagement`, appliquées **avant** le premier démarrage. (Le commentaire de table de 0002 contenait un
  `;` ; le lanceur des suites d'intégration coupait dessus jusqu'à ce qu'il respecte les guillemets comme
  `apps/migrator` — la prod n'a jamais été touchée. C'est une virgule désormais — même schéma.)
- **Durabilité Redis :** activer l'AOF (`appendonly yes`, `appendfsync everysec`) — sans cela, un
  redémarrage perd la fenêtre de flush courante et les likes doivent être reconstruits depuis Scylla.
- **Kafka :** les topics viennent du registre event-topology (le `topic-provisioner` du dépôt infra) ;
  `engagement.reactions` n'est plus ni produit ni provisionné. Les variables
  `ENGAGEMENT_REACTION_WEIGHT_*` et `ENGAGEMENT_BACKFILL_REACTIONS_BY_PROFILE` ont disparu (#665).
- **Déploiement/Rollback :** `<TODO>` ; la couche gRPC est sans état, mais les workers sont des
  consommateurs at-least-once — sûr à déployer.

---

## 📈 Télémétrie, performance & métriques

- **Runtime :** Tokio multi-thread (requis — `tokio::join!` sur le chemin de lecture).

| Signal | Why it matters | Suggested alert |
|---|---|---|
| `engagement_counter_flush_lag_posts` | santé du worker de flush | > 10 000 ⇒ en retard |
| lag du groupe `engagement-stakes` | fraîcheur des likes | > 50 000 ⇒ lag du consommateur Kafka |
| `engagement_redis_errors_total` | disponibilité du chemin chaud | tout pic ⇒ connectivité Redis |
| `engagement_scylla_errors_total` | durabilité des copies | tout pic ⇒ connectivité Scylla |

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

**1. Les compteurs de likes dérivent après un redémarrage de Redis.**
Cause racine : Redis a été flushé/redémarré sans AOF ; les clés `engagement:{post:*}:likes` / `:likers`
sont perdues. Mitigation : activer l'AOF pour éviter la récidive ; reconstruire depuis
`engagement.likes_by_target` (par cible, `HSET` du total de chaque compte dans `:likers` et de leur somme
dans `:likes`) avant de servir les lectures.

**2. Le lag du consommateur des mises croît continuellement.**
Cause racine : les écritures Scylla sont plus lentes que le rythme des mises, ou trop peu de membres
consommateurs. Mitigation : vérifier le lag du groupe `engagement-stakes` ; scaler les réplicas
d'engagement-server (≤ partitions du topic) ; vérifier que la compaction de `likes_by_target` ne sature
pas les I/O disque.

**3. `ENG-5001 ScriptReturnInvalid` dans les logs.**
Cause racine : un script Lua a renvoyé un type inattendu — généralement une clé du mauvais type.
Mitigation : vérifier Redis ≥ 7.0 ; contrôler que `TYPE engagement:{post:<id>}:likers` vaut `hash` ;
supprimer une clé corrompue et la reconstruire depuis `likes_by_target` (les mises sont re-livrées comme
des totaux, la suivante répare donc l'entrée du compte).
