---
i18n:
  source: ./README.md
  source_sha256: 2c8c74708fc61310a6bc08877038acf3e1c8f1790963f5b957b14136e3d43457
  translated_at: 2026-10-06
  status: complete
---
> 🇫🇷 Traduction française — la version **anglaise** [`README.md`](./README.md) fait foi.
> En cas de divergence, l'anglais prime. Les contrats (codes d'erreur, variables
> d'environnement, topics Kafka, identifiants) sont volontairement laissés en anglais.

# `timeline` — Fil d'accueil à fan-out hybride qui garde les célébrités hors du chemin d'écriture

> **Fiche service**
>
> | | |
> |---|---|
> | **Propriétaire** | `<TODO: équipe>` · `<TODO: #canal-slack>` |
> | **Astreinte / escalade** | `<TODO: rotation-astreinte>` → `<TODO: politique-escalade>` |
> | **Palier (Tier)** | **TIER-1** — fil « Following » face utilisateur ; dérivé, cold-start transparent |
> | **Binaire déployable** | `crates/apps/timeline-server` (crate bibliothèque : `crates/services/timeline`) |
> | **Bases de données** | Redis (feeds matérialisés + registres VIP) · ScyllaDB keyspace `timeline` (store froid durable) |
> | **Asynchrone** | ne publie rien · consomme `post.published` / `post.deleted` / `social-graph.followed` / `.unfollowed` |
> | **Appelants amont** | `<TODO: BFF / mobile>` ; appelle `social-graph` (gRPC) |
> | **Dépendances aval** | Redis, ScyllaDB, Kafka, `social-graph` |
> | **SLO** | lecture chaude sub-ms (Redis ZSET) · amplification d'écriture VIP O(1)/post |

---

## 🎯 Vue d'ensemble & rôle du service

`timeline` fournit le fil d'accueil (l'onglet « Following »). Il agrège les posts des comptes suivis en
un flux classé et paginé via une architecture **fan-out-on-write / fan-out-on-read hybride**.

Le problème difficile qu'il résout est le **problème du fan-out des célébrités** : un auteur VIP avec des
millions de followers générerait, en fan-out-on-write pur, des millions d'écritures de feed par post. Il
résout cela par un **routage par palier sur l'auteur** : les auteurs Standard/Premium font du fan-out
vers les ZSETs Redis des followers ; les auteurs VIP ne font jamais de fan-out — leurs posts atterrissent
dans un ZSET par auteur fusionné en mémoire au moment de la requête, bornant l'amplification d'écriture à
O(1) par post quel que soit le nombre de followers.

**Objectifs fondamentaux :** lectures chaudes sub-ms (ZSETs Redis pré-matérialisés, curseurs opaques) ;
isolation d'écriture VIP ; aucun contenu de post stocké (seulement des tokens
`(post_id, author_id, published_at_ms)` — l'hydratation est l'affaire du client/BFF) ; cold-start
transparent (Scylla servi immédiatement, Redis réchauffé en asynchrone, le flag `is_cold` dit au BFF
d'afficher « chargement »).

---

## 📐 Architecture & concepts

La surface gRPC est **en lecture seule** — toutes les écritures arrivent via des workers Kafka.

```
Kafka: post.published │ post.deleted │ social-graph.followed/unfollowed
   ▼                    ▼                ▼
PostPublishedWorker  PostDeletedWorker  Follow{Created,Deleted}Worker
 (Std/Prem → fan-out  (VIP → ZREM;       (Created → add to following set,
  to followers' ZSETs;  Std/Prem → Scylla  backfill Std/Prem posts;
  VIP → ZADD vip:{})    purge)             Deleted → prune)
   └─────────────────────┬──────────────────────┘
                         ▼
   Redis: timeline:feed:{profile}  ZSET (per-follower) · timeline:vip:{author} ZSET
          timeline:following:{id}  SET · timeline:tier:{author} · timeline:warm:{profile}
                         ▼ cold-start
   ScyllaDB: timeline.feed_items_by_profile (TWCS) · timeline.posts_by_author (reverse index)
                         ▼
   gRPC TimelineService.GetFollowingFeed ─► BFF / mobile
```

**Routage du fan-out** (invariant de domaine dur dans `AuthorTier::fan_out_mode()`, **pas** un flag de
config) :

| Tier | Mode | Write | Read |
|---|---|---|---|
| `Standard` (0) | `Write` | push to every follower ZSET + Scylla INSERT | serve `timeline:feed:{profile}` |
| `Premium` (1) | `Write` | same as Standard | same |
| `Vip` (2) | `Read` | ZADD `timeline:vip:{author}` only | merge at query time (`try_join_all`) |

Le palier d'auteur est dénormalisé dans chaque événement `post.published` — **aucun lookup de palier
synchrone sur le chemin d'écriture**. Les membres de ZSET encodent `"{post_id}:{author_id}"` pour que le
BFF identifie l'auteur sans lookup secondaire.

> **Invariants :** les auteurs VIP ne font jamais de fan-out (amplification d'écriture O(1)/post) ; le
> cold-start renvoie les données Scylla avec `is_cold=true` et réchauffe Redis en asynchrone ; la
> reconstruction du following-set sur miss Redis pagine `SocialGraphService.ListFollowing` et route
> conservativement les paliers inconnus vers `Standard`.

---

## 📊 Objectifs de niveau de service (SLO)

| SLI | Objectif | Fenêtre | Mesuré par |
|---|---|---|---|
| `GetFollowingFeed` p99 — chaud (Redis) | `< <TODO> ms` | 1 h | histogramme gRPC |
| Fallback cold-start p99 (Scylla) | `< <TODO> ms` | 1 h | histogramme de lecture Scylla |
| Lag d'ingestion du fan-out (`post.published`) | `< <TODO> s` | direct | lag du consumer-group |
| Amplification d'écriture VIP | O(1) par post | — | invariant (`fan_out_mode`) |

**Budget d'erreur :** `<TODO>`. **En cas d'épuisement :** `<TODO>`.

---

## 🔗 Dépendances & rayon d'impact (blast radius)

**Aval :**

| Dependency | Purpose | If down → | Degradation |
|---|---|---|---|
| Redis | feed chaud + registres VIP | les lectures chaudes échouent | **Souple** — le chemin cold-start sert depuis Scylla |
| ScyllaDB (`timeline`) | store froid durable | cold-start + ingestion échouent | **Dur** pour les lectures froides ; l'ingestion réessaie |
| Kafka | ingestion du fan-out | le feed cesse de se mettre à jour | **Souple** — feed existant servi |
| `social-graph` (gRPC) | reconstruction du following-set | la reconstruction sur miss Redis échoue | **Souple** — boote en lazy ; `TML-3001` réessayable |
| `social-graph` (gRPC) | `CheckAccess` pour le fil de découverte | les lectures de découverte échouent | **Dur** pour la découverte (échec fermé, `TML-8002`) |
| `social-graph` (gRPC) | `ListMutedProfiles` (portée posts) pour les deux fils | les mises en sourdine ne s'appliquent pas à cette requête | **Souple** (échec ouvert : une mise en sourdine est une préférence, pas une règle de sécurité) |
| `geo-discovery` (gRPC) | candidats NEARBY (`QueryTile`) | NEARBY échoue | **Souple** — les autres classements ne sont pas touchés ; `TML-8003` |

**Amont (rayon d'impact) :**

| Caller | Uses | Impact si `timeline` est indisponible |
|---|---|---|
| `<TODO: BFF / mobile>` | `GetFollowingFeed` | le fil d'accueil Following cesse de charger |
| client iOS (edge) | `GetDiscoveryFeed` | For You / Trending / Nearby cessent de charger |

> **Chemin critique ?** Oui pour la surface fil d'accueil ; c'est un read-model dérivé, donc une panne
> dégrade le fil mais pas les actions de publication/sociales.

---

## 🔌 Interfaces publiques & contrat d'API

### gRPC — `timeline.v1.TimelineService`

```protobuf
rpc GetFollowingFeed(GetFollowingFeedRequest) returns (GetFollowingFeedResponse);

message GetFollowingFeedRequest  { string profile_id=1; int32 limit=2; string page_token=3; }
message GetFollowingFeedResponse { repeated FeedItem items=1; string next_page_token=2; bool is_cold=3; }
message FeedItem { string post_id=1; string author_id=2; int64 published_at_ms=3; }

rpc GetDiscoveryFeed(GetDiscoveryFeedRequest) returns (GetDiscoveryFeedResponse);   // edge public_read

message GetDiscoveryFeedRequest  { DiscoveryRanking ranking=1; string region=2; optional double lat=3;
                                   optional double lng=4; ContentLevel content_level=5; string page_token=6; int32 limit=7;
                                   string profile_id=8; bool non_personalized=9; }
message GetDiscoveryFeedResponse { repeated FeedItem items=1; string next_page_token=2; string region_applied=3;
                                   ContentLevel content_level_applied=4; bool personalized=5; }

// Interest tags (#662), edge authenticated + require_profile; each returns the tags left, heaviest first.
rpc ListInterests(ListInterestsRequest)   returns (InterestsResponse);   // { string profile_id=1; }
rpc RemoveInterest(RemoveInterestRequest) returns (InterestsResponse);   // { string profile_id=1; string tag=2; }
rpc ResetInterests(ResetInterestsRequest) returns (InterestsResponse);   // { string profile_id=1; }
message InterestsResponse { repeated Interest interests=1; }             // Interest { string tag=1; double weight=2; }
```

> **Contrat de sérialisation :** le curseur est `base64url("{published_at_ms}:{post_id_hyphenated}")` —
> opaque aux clients, décodé côté serveur uniquement. `limit` est clampé à `TIMELINE_MAX_PAGE_SIZE`.
> `is_cold=true` signifie que la page a été servie depuis ScyllaDB pendant que Redis se réchauffe en
> asynchrone.

### Fil de découverte (`GetDiscoveryFeed`)

Un fil qui n'a besoin d'aucun graphe de suivi : le For You des invités et des membres (#673, B3), classé
pour un membre selon ses centres d'intérêt, sauf s'il l'a désactivé (#662).

- **Pool** (Redis, `timeline:{disc}:recent|fresh|hot` + un hash `timeline:disc:post:<id>` par post) : les posts
  publiés depuis moins de `TIMELINE_DISCOVERY_WINDOW_SECS` (72 h), plafonnés à `TIMELINE_DISCOVERY_POOL_CAP`.
  Alimenté par un seul consumer (`timeline-discovery`) sur `post.v1.events` (publié avec les hashtags de
  la légende / supprimé),
  `counter.v1.popularity` (popularité cumulée) et `moderation.v1.events` (restriction gardée par version,
  enregistrée même avant que la publication soit vue). Comportement de cache : si Redis le perd, il se
  remplit à nouveau en une fenêtre.
- **Classements.** `TRENDING` = le score hot `log10(max(popularity, 1)) + (published_s − epoch) / gravity`
  (`TIMELINE_DISCOVERY_HOT_GRAVITY_SECS`, 12,5 h : un post plus récent d'autant se classe comme un post dix
  fois plus populaire), uniquement les posts ayant de la popularité ; `RECENT` = du plus récent au plus
  ancien ; `FOR_YOU` = trois hot pour un *frais* (sans popularité encore) ; `NEARBY` = les posts de
  geo-discovery autour de `lat`/`lng` (un anneau large au-dessus du seuil de viralité de geo, plus un
  anneau proche sans seuil), classés par score hot ; un invité n'obtient que son pays accordé (accès par
  pays de geo-discovery, transmis en `guest_principal`).
- **Filtres.** Les posts supprimés, retirés ou à visibilité limitée ne sont jamais montrés ; les posts
  soumis à une limite d'âge seulement en `CONTENT_LEVEL_STANDARD`. Un **invité a toujours `RESTRICTED`** ;
  les autres obtiennent ce qu'ils demandent (le client envoie le réglage de contenu sensible du profil,
  #662), `RESTRICTED` par défaut ; un **lecteur de 13 à 17 ans** (tranche `age` du jeton) aussi a toujours
  `RESTRICTED`. Le lecteur vient du jeton (`edge::viewer`) : les auteurs qu'il ne peut pas
  voir (`CheckAccess` ≠ `VISIBLE`) sont exclus, et la lecture **échoue fermée** (`TML-8002`, UNAVAILABLE)
  quand social-graph ne peut pas répondre. Les appelants mesh ne sont pas filtrés par audience.
- **Mises en sourdine (#659).** Les posts des auteurs qu'un des profils du lecteur a mis en sourdine (portée
  posts) sont exclus du fil de découverte, et ceux des auteurs mis en sourdine par le profil du fil sont
  exclus du **fil des abonnements** (un VIP en sourdine n'est même pas lu ; le curseur passe les posts
  écartés). Un appel `ListMutedProfiles` par requête, en échec ouvert.
- **Centres d'intérêt (#662).** Le même consumer lit `engagement.reactions` : la *première* réaction d'un
  profil à un post du pool (`upserted` sans `old_kind` ; un post retiré ou supprimé n'apprend rien) ajoute
  du poids aux hashtags du post dans les centres d'intérêt du profil (Redis, ZSET
  `timeline:int:{<profile>}`, plus `:seen` — un post compte une fois par 30 jours — `:muted`, les tags
  supprimés, et `:off`, la désactivation). Les poids décroissent avec une demi-vie de 30 jours (stockés gonflés par rapport à une époque
  fixe, si bien qu'une écriture ne fait qu'incrémenter), les 100 plus lourds sont gardés, et un stockage
  inactif expire après 180 jours. Une page **FOR_YOU** lue avec `profile_id` (lié aux profils du jeton) est
  reclassée selon l'affinité de ses posts avec les 20 tags les plus lourds — dans la page, si bien que le
  curseur n'est pas touché ; l'ordre des flux est conservé entre égaux — et l'indique (`personalized`). Un
  stockage injoignable sert la page sans reclassement. **Désactivation, appliquée ici :**
  `ProfileFeedSettingsChanged` sur `profile.v1.events` avec `non_personalized` (`FeedSettings` du profil, DSA
  art. 38) efface ce qui a été appris et pose `:off` : plus rien n'est appris et aucune page n'est reclassée
  tant que le titulaire ne la réactive pas, quoi que dise la requête (son `non_personalized` ne coupe le
  classement que pour une lecture). Un profil de **13 à 17 ans** naît désactivé (UK Children's Code) et peut
  l'activer. Un invité n'est jamais personnalisé ; les autres classements et le fil des abonnements ne le
  sont jamais. `ListInterests` / `RemoveInterest` (le tag reste exclu : les réactions
  suivantes ne l'apprennent plus) / `ResetInterests` (tout, tags supprimés compris ; la désactivation reste)
  sont réservés au propriétaire. `ProfileDeleted` sur `profile.v1.events` efface les quatre clés du profil
  (RGPD art. 17).
- **Pagination.** Le curseur porte une position par flux et est lié à son classement. Une page peut être
  courte (voire vide) avec un jeton non vide quand ses candidats ont été filtrés ; un post qui passe de
  frais à hot peut revenir sur une page suivante — les clients dédupliquent par `post_id`.
- **Région.** Acceptée, pas encore appliquée : v1 classe un seul pool global (`region_applied` est vide).

### Ports Rust (contrat hexagonal)

```rust
pub trait FeedStore: Send + Sync { /* Redis hot ZSET: add/cap/prefix-remove/range */ }
pub trait VipRegistry: Send + Sync { /* per-VIP ZSET (ZADD+cap+TTL) */ }
pub trait TierCache: Send + Sync { /* author tier + warm flag */ }
pub trait FollowingStore: Send + Sync { /* following set (SADD/SREM/SMEMBERS) */ }
pub trait FeedRepository / AuthorPostRepository: Send + Sync { /* ScyllaDB cold layer */ }
pub trait SocialGraphClient: Send + Sync { /* paginated gRPC to social-graph */ }
pub trait DiscoveryPool: Send + Sync { /* discovery indices + per-post meta (Redis) */ }
pub trait InterestStore: Send + Sync { /* a profile's interest tags: reinforce / top / remove / reset (Redis) */ }
pub trait NearbyPosts: Send + Sync { /* geo-discovery QueryTile around a point */ }
```

### Contrat d'erreur (`TML-xxxx`)

| Code | Variant | HTTP |
|---|---|---|
| TML-1001 | `FeedNotFound` | 404 |
| TML-2001/2002 | `FanOutFailed` / `VipRegistryWriteFailed` | 500 |
| TML-3001/3002 | `SocialGraphClientError` (retryable) / `SocialGraphInvalidId` | 500 |
| TML-4001 | `ColdStartFailed` | 500 |
| TML-5001/5002 | `ScriptReturnInvalid` / `BackfillFailed` | 500 |
| TML-6001 | `InvalidPageToken` | 422 |
| TML-8001 | `LocationRequired` (NEARBY sans lat/lng) | 422 |
| TML-8002/8003 | `AccessCheckUnavailable` / `NearbyUnavailable` (réessayable) | 503 |
| TML-9001..9004 | invalid ids / domain violation | 422 |

---

## 📨 Contrat événementiel & asynchrone

**Publie :** rien — `timeline` est un matérialiseur read-model pur.

**Consomme :**

| Topic | Consumer group | Worker / action | On poison/exhaustion |
|---|---|---|---|
| `post.published` | `timeline-post-published` | fan-out (Std/Prem) or VIP-register | DLQ `{topic}.dlq` |
| `post.deleted` | `timeline-post-deleted` | VIP ZREM or Scylla purge | DLQ `{topic}.dlq` |
| `social-graph.followed` | `timeline-sg-followed` | backfill recent posts + update following set | DLQ `{topic}.dlq` |
| `social-graph.unfollowed` | `timeline-sg-unfollowed` | prune posts + update following set | DLQ `{topic}.dlq` |
| `post.v1.events` · `counter.v1.popularity` · `moderation.v1.events` · `engagement.reactions` · `profile.v1.events` | `timeline-discovery` | discovery pool (publish / delete, hot score, restriction) ; centres d'intérêt (première réaction, désactivation sur `ProfileFeedSettingsChanged`, effacement sur `ProfileDeleted`) | DLQ `{topic}.dlq` |

> **Contrat d'exécution (obligatoire) :** tous les workers s'exécutent sous `run_consumer` — commit manuel
> après succès, retries bornés avec backoff + jitter, DLQ en cas d'épuisement/poison. Toutes les écritures
> aval sont idempotentes (ZADD idempotent ; upserts Scylla via INSERT).

---

## 🌩️ Modes de défaillance & dégradation

| Failure | Symptom | Service behavior | Operator action |
|---|---|---|---|
| Redis indisponible / froid | les lectures chaudes échouent | **Souple** — le cold-start sert Scylla (`is_cold=true`), réchauffe en asynchrone | vérifier Redis ; auto-réparation |
| ScyllaDB indisponible | cold-start + ingestion échouent | **Dur** pour le chemin froid ; l'ingestion réessaie via `run_consumer` | vérifier Scylla ; drainer la DLQ |
| `social-graph` injoignable au boot | la reconstruction du following échoue | canal connecté en lazy — timeline boote quand même ; `TML-3001` réessayable | vérifier la santé de social-graph |
| Miss du tier cache | palier d'auteur inconnu | route conservativement vers `Standard` (sans bloquer ; corrigé au prochain `post.published`) | aucune — auto-correctif |
| Lag d'ingestion du fan-out | feed périmé | retries dans le budget | scaler le consommateur concerné |

**Backpressure & limites.** `TIMELINE_FEED_CAP` (défaut 500) et `TIMELINE_VIP_REGISTRY_CAP` (200) bornent
la taille des ZSET ; `TIMELINE_MAX_VIP_MERGE_SOURCES` (50) plafonne les fusions VIP par requête ;
`TIMELINE_MAX_PAGE_SIZE` clampe les pages.

---

## 📦 Intégration & utilisation

```toml
[dependencies]
timeline = { path = "crates/services/timeline" }
```

Bibliothèque uniquement. Implémente [`service_runtime::Service`](../../platform/service-runtime/README.md)
sous le nom `timeline::service::TimelineService` — `build` mappe `TimelineConfig → AppConfig`, construit
le client gRPC social-graph sur un canal **connecté en lazy** (timeline boote même si social-graph n'est
pas encore joignable), assemble les adaptateurs cache/persistence + bus CQRS, et lance les cinq workers
d'ingestion ; `register` ajoute les services gRPC + réflexion (surface en lecture seule) ; `health_probes`
vérifie Scylla/Redis.

### Bootstrap (`crates/apps/timeline-server`)

```rust
use std::net::SocketAddr;
use timeline::service::TimelineService;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr: SocketAddr = std::env::var("TIMELINE_GRPC_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:50070".to_owned())
        .parse()?;
    service_runtime::serve::<TimelineService>(addr).await
}
```

---

## ⚙️ Configuration & environnement d'exécution

### Variables propres à `timeline`

| Variable | Default | Description |
|---|---|---|
| `TIMELINE_FEED_CAP` | `500` | Max entries per follower's Redis ZSET. |
| `TIMELINE_VIP_REGISTRY_CAP` | `200` | Max entries per VIP author ZSET. |
| `TIMELINE_BACKFILL_LIMIT` | `100` | Max posts backfilled on follow. |
| `TIMELINE_WARM_TTL_SECS` | `86400` | Warm-flag TTL (24 h). |
| `TIMELINE_TIER_CACHE_TTL_SECS` | `3600` | Author tier cache TTL. |
| `TIMELINE_VIP_REGISTRY_TTL_SECS` | `604800` | VIP ZSET TTL (7 d). |
| `TIMELINE_MAX_PAGE_SIZE` | `50` | Max page size. |
| `TIMELINE_MAX_VIP_MERGE_SOURCES` | `50` | Max VIP ZSETs merged per request. |
| `TIMELINE_SOCIAL_GRAPH_PAGE_SIZE` | `500` | Pagination size for social-graph lists. |
| `TIMELINE_SOCIAL_GRAPH_ENDPOINT` | `http://social-graph:50051` | social-graph gRPC endpoint. |
| `TIMELINE_DISCOVERY_WINDOW_SECS` | `259200` | Durée de présence d'un post dans le pool de découverte (72 h). |
| `TIMELINE_DISCOVERY_POOL_CAP` | `10000` | Nombre max de posts dans le pool de découverte. |
| `TIMELINE_DISCOVERY_HOT_GRAVITY_SECS` | `45000` | Gravité du score hot (12,5 h pour une popularité ×10). |
| `TIMELINE_GEO_DISCOVERY_ENDPOINT` | `http://localhost:50054` | Endpoint gRPC de geo-discovery (NEARBY). |
| `TIMELINE_NEARBY_RADIUS_KM` · `TIMELINE_NEARBY_CLOSE_RADIUS_KM` | `25` · `2` | Rayon des anneaux large / proche de NEARBY. |
| `TIMELINE_NEARBY_CANDIDATES` | `300` | Candidats NEARBY classés par requête. |
| `TIMELINE_KAFKA_GROUP_*` | `timeline-*` | Consumer group IDs (post-published/deleted, sg-followed/unfollowed, discovery). |

> Les variables de connexion ScyllaDB / Redis / Kafka standard des crates de stockage partagés
> s'appliquent. `TIMELINE_GRPC_ADDR` vaut par défaut `0.0.0.0:50070`.

---

## 🚀 Déploiement, migrations & rollback

- **Migrations :** `0001_create_keyspace.cql` → `0002_create_feed_items_by_profile_table.cql` →
  `0003_create_posts_by_author_table.cql` sur `timeline`, appliquées **avant** le premier démarrage.
- **Pièges liés à l'état :** `AuthorTier::fan_out_mode()` est un invariant dur, pas de la config — changer
  la sémantique de palier nécessite une reconstruction du feed. L'encodage des membres de ZSET
  (`{post_id}:{author_id}`) et le format de curseur sont des contrats de lecture.
- **Déploiement/Rollback :** `<TODO>` ; le canal social-graph connecté en lazy rend l'ordre de boot
  tolérant — sûr à déployer.

---

## 📈 Télémétrie, performance & métriques

- **Runtime :** Tokio multi-thread (la fusion VIP utilise `try_join_all`). Subscriber global tracing/OTel
  installé avant `serve`.

| Signal | Why it matters | Suggested alert |
|---|---|---|
| `GetFollowingFeed` p99 (warm) | hot-read SLO | > SLO ⇒ page |
| `is_cold` rate | Redis warm-coverage | sustained high ⇒ check warming / Redis evictions |
| fan-out consumer lag | feed freshness | > threshold ⇒ scale consumers |
| `TML-3001` rate | social-graph dependency health | spike ⇒ check social-graph |
| DLQ produce rate (`{topic}.dlq`) | poison / retry-exhausted | any sustained rate ⇒ page |

---

## 🛠️ Développement local

```bash
docker compose up -d scylladb redis kafka     # repo-root compose
for f in crates/services/timeline/migrations/*.cql; do cqlsh -f "$f"; done
cargo build -p timeline && cargo clippy -p timeline --all-targets
cargo test  -p timeline
```

---

## 🚨 Dépannage & runbook

> Format : **symptôme → cause racine → mitigation.**

**1. Les posts d'un auteur VIP n'apparaissent pas dans les feeds des followers.**
Cause racine : c'est voulu — les posts VIP ne font *pas* de fan-out ; ils vivent dans
`timeline:vip:{author}` et sont fusionnés au moment de la requête. S'ils manquent dans le résultat
fusionné, vérifier `TIMELINE_MAX_VIP_MERGE_SOURCES` (le follower suit peut-être plus de VIP que le cap de
fusion) ou le TTL du ZSET VIP. Mitigation : confirmer `ZCARD timeline:vip:{author}` > 0 ; relever le cap de
fusion si un utilisateur suit beaucoup de VIP.

**2. `GetFollowingFeed` renvoie sans cesse `is_cold=true`.**
Cause racine : le flag warm (`timeline:warm:{profile}`) expire continuellement ou la tâche de réchauffement
asynchrone échoue — souvent une pression d'éviction Redis ou un `ScriptReturnInvalid` (TML-5001) dans le
chemin de réchauffement. Mitigation : vérifier `maxmemory`/l'éviction Redis et les logs de la tâche de
réchauffement ; le chemin froid reste correct (servi depuis Scylla), juste plus lent.

**3. Les posts d'un nouveau follow n'apparaissent pas (pas de backfill).**
Cause racine : l'événement `social-graph.followed` a été consommé mais le backfill a échoué (`TML-5002`),
ou le suivi est VIP (pas de backfill — fusionné en direct à la place). Mitigation : vérifier le lag/la DLQ
de `timeline-sg-followed` ; vérifier le palier du suivi — les follows VIP sautent correctement le backfill.
