---
i18n:
  source: ./README.md
  source_sha256: dd35f4caedfade8273dcce75c25d16eb237e7653445962cbde85c58e9afdacaf
  translated_at: 2026-10-05
  status: complete
---
> 🇫🇷 Traduction française — la version **anglaise** [`README.md`](./README.md) fait foi.
> En cas de divergence, l'anglais prime. Les contrats (codes d'erreur, variables
> d'environnement, topics Kafka, identifiants) sont volontairement laissés en anglais.

# `social-graph` — Arêtes follow/block directionnelles entre profils opaques, filtrées par blocage et résistantes aux célébrités

> **Fiche service**
>
> | | |
> |---|---|
> | **Propriétaire** | `<TODO: équipe>` · `<TODO: #canal-slack>` |
> | **Astreinte / escalade** | `<TODO: rotation-astreinte>` → `<TODO: politique-escalade>` |
> | **Palier (Tier)** | **TIER-1** — feeds, notifications et filtrage par blocage en dépendent |
> | **Binaire déployable** | `crates/apps/social-graph-server` (crate bibliothèque : `crates/services/social-graph`) |
> | **Bases de données** | ScyllaDB keyspace `social_graph` (5 tables) · Redis (sets + compteurs) |
> | **Asynchrone** | publie `social-graph.followed` / `.follow_requested` / `.unfollowed` / `.blocked` / `.author_tier_changed` · consomme `profile.v1.events` (projection d'audience) |
> | **Appelants amont** | `timeline`, `notification`, `<TODO: passerelle>` |
> | **Dépendances aval** | ScyllaDB, Redis, Kafka |
> | **SLO** | `<TODO>` dispo · `GetRelationStatus` p99 `<TODO>` · écriture p99 `<TODO>` |

---

## 🎯 Vue d'ensemble & rôle du service

`social-graph` est le propriétaire strict de **qui suit qui** et **qui bloque qui**, sur des primitives
`ProfileId` (UUIDv7) opaques. Il impose le block-gate, dérive le follow mutuel (amitié) et émet les
événements follow/block qui pilotent le fan-out de la timeline et les notifications.

Le problème difficile qu'il résout est l'**asymétrie de fan-in des célébrités** : les follows sortants
sont bornés (dizaines de milliers) mais les follows entrants sont non bornés (millions pour une
célébrité). Matérialiser l'ensemble entrant complet épuiserait Redis. Il résout cela en stockant les
**follows sortants comme des Sets Redis** (pour une dérivation O(1) du follow mutuel) mais les
**followers entrants comme des compteurs INCR/DECR O(1)**.

**Objectifs fondamentaux :** ne jamais importer `profile` ni `account` (les profils sont des IDs
opaques) ; le blocage gagne toujours (sectionne les follows dans les deux sens, ferme les follows
futurs) ; l'amitié est *dérivée*, jamais dual-write. **Hors périmètre :** métadonnées de profil,
construction de timeline, livraison de notifications.

---

## 📐 Architecture & concepts

Hexagonal / DDD, bus CQRS, tables d'adjacence ScyllaDB, sets + compteurs Redis, événements Kafka.

```
gRPC SocialGraphService ─► CQRS bus ─► Command handlers ─► SocialGraphRepository (ScyllaDB, 4 tables)
                                    └─► Query handlers   ─► SocialGraphCache (Redis sets + counters)
                                    └─► EventPublisher   ─► Kafka (social-graph.*)
```

**Schéma ScyllaDB** (keyspace `social_graph`, NTS RF=3) :

| Table | Partition key | Clustering key | Purpose |
|---|---|---|---|
| `followers` | `followee_id` | `followed_at DESC, follower_id ASC` | fan-in: who follows X |
| `following` | `follower_id` | `followed_at DESC, followee_id ASC` | fan-out: who X follows |
| `follow_status` | `follower_id` | `followee_id ASC` | point-lookup + `followed_at` for DELETE |
| `blocks` | `blocker_id` | `blockee_id ASC` | block point-lookup + list |

`follow_status` existe parce que le DELETE Scylla nécessite la **clé de clustering complète** : il stocke
`followed_at` comme colonne ordinaire afin que l'unfollow/sever ne fasse jamais de read-before-write sur
les listes d'adjacence. Aucun miroir `blocked_by` n'est nécessaire — le gate est composé de deux lookups
O(1) sur la même table `blocks` avec arguments inversés.

**Stratégie Redis :** `sg:following:v1:{id}` (Set) pilote `IsFriend(A,B)` = `SISMEMBER(A,B) AND
SISMEMBER(B,A)` — pas de table `friends`, donc pas de désynchronisation dual-write.
`sg:followers_count:v1:{id}` / `sg:following_count:v1:{id}` (compteurs) satisfont les lectures de compte
en espace O(1).

> **Invariants** (et où ils sont imposés) : pas d'auto-follow/auto-block (pré-vérification du handler) ;
> follow rejeté s'il existe un blocage dans l'un ou l'autre sens (`Relation::follow()`) ;
> re-follow/re-block rejetés ; le blocage sectionne les follows existants dans les deux sens
> (`Relation::block()` → `SeveredFollows`) ; l'unblock ne **restaure pas** les follows sectionnés
> (intentionnel — l'utilisateur doit re-follow).

---

## 📊 Objectifs de niveau de service (SLO)

| SLI | Objectif | Fenêtre | Mesuré par |
|---|---|---|---|
| Disponibilité (non-`UNAVAILABLE`) | `<TODO>` | 30 j | métriques de statut gRPC |
| `GetRelationStatus` p99 (chemin Redis) | `< <TODO> ms` | 1 h | histogramme gRPC |
| Écriture Follow/Block p99 | `< <TODO> ms` | 1 h | histogramme d'écriture Scylla |
| Durabilité | aucune arête acquittée perdue | — | `LocalQuorum` Scylla |

**Budget d'erreur :** `<TODO>`. **En cas d'épuisement :** `<TODO>`.

---

## 🔗 Dépendances & rayon d'impact (blast radius)

**Aval :**

| Dependency | Purpose | If down → | Degradation |
|---|---|---|---|
| ScyllaDB (`social_graph`) | arêtes durables | lectures + écritures échouent | **Dur** — `UNAVAILABLE` |
| Redis | sets + compteurs (lectures statut/ami/compte) | les lectures statut/compte se dégradent | **Souple** — arêtes durables intactes |
| Kafka | émission d'événements | le fan-out aval stagne | **Souple** — arêtes committées quand même |

**Amont (rayon d'impact) :**

| Caller | Uses | Impact visible utilisateur si indisponible |
|---|---|---|
| `timeline` | consomme `social-graph.followed/unfollowed` + appelle `ListFollowing` | les nouveaux follows n'atteignent pas le fil d'accueil |
| `notification` | cache de block-gate (`is_blocked`) | la suppression par blocage s'affaiblit |

> **Chemin critique ?** Partiellement — les écritures sont initiées par l'utilisateur (follow/block) ;
> une grande partie de la consommation est asynchrone.

---

## 🔌 Interfaces publiques & contrat d'API

### gRPC — `social_graph.v1.SocialGraphService`

```protobuf
service SocialGraphService {
  // Commands
  rpc Follow(FollowRequest) returns (CommandResponse);   // une cible privée reçoit une demande en attente (CommandResponse.requested)
  rpc ListFollowRequests(ListFollowRequestsRequest) returns (ListFollowRequestsResponse);   // boîte de réception du propriétaire d'un profil privé ; la première page porte pending_count (plafonné à 1000)
  rpc ApproveFollowRequest(AnswerFollowRequestRequest) returns (CommandResponse);   // propriétaire
  rpc DeclineFollowRequest(AnswerFollowRequestRequest) returns (CommandResponse);   // propriétaire
  rpc CancelFollowRequest(CancelFollowRequestRequest) returns (CommandResponse);   // demandeur
  rpc Unfollow(UnfollowRequest) returns (CommandResponse);
  rpc RemoveFollower(RemoveFollowerRequest) returns (CommandResponse);   // propriétaire : défait l'abonnement de l'abonné
  rpc SetListPrivacy(SetListPrivacyRequest) returns (ListPrivacy);   // propriétaire : qui voit les listes d'abonnés / d'abonnements
  rpc Block(BlockRequest) returns (CommandResponse);
  rpc Unblock(UnblockRequest) returns (CommandResponse);
  rpc Mute(MuteRequest) returns (CommandResponse);   // acteur : posts / stories / messages
  rpc Unmute(UnmuteRequest) returns (CommandResponse);
  rpc Restrict(RestrictRequest) returns (CommandResponse);   // acteur : les commentaires de la cible sur ses posts ne sont vus que de la cible et de l'acteur
  rpc Unrestrict(UnrestrictRequest) returns (CommandResponse);
  // Queries
  rpc GetRelationStatus(GetRelationStatusRequest) returns (RelationStatusView);
  rpc ListFollowers(ListFollowersRequest) returns (ListFollowersResponse);
  rpc ListFollowing(ListFollowingRequest) returns (ListFollowingResponse);
  rpc GetListPrivacy(GetListPrivacyRequest) returns (ListPrivacy);   // propriétaire
  rpc ListBlocks(ListBlocksRequest) returns (ListBlocksResponse);
  rpc ListMutes(ListMutesRequest) returns (ListMutesResponse);   // propriétaire
  rpc ListRestricted(ListRestrictedRequest) returns (ListRestrictedResponse);   // propriétaire
  rpc CheckAccess(CheckAccessRequest) returns (CheckAccessResponse);   // MESH-ONLY : par cible, l'accès au contenu, plus `follows` / `mutual` (un profil lecteur suit la cible / ils se suivent mutuellement — l'audience de localisation d'un auteur, #657)
  rpc CheckInteraction(CheckInteractionRequest) returns (CheckInteractionResponse);   // MESH-ONLY : l'acteur peut-il commenter / mentionner / écrire à la cible (ses réglages d'interaction profile, blocages) ; `held` quand la limite temporaire de la cible (#669) couvre l'acteur — un commentaire / message d'un non-abonné, ou d'un abonné de moins de 7 jours sous `recent_followers` ; en cas de refus, `refusal` dit pourquoi (`BLOCKED`, `NO_ONE`, ou `AUDIENCE` : l'audience abonnés / mutuels exclut l'acteur — chat fait alors de ce message une demande, #656)
  rpc ListMutedProfiles(ListMutedProfilesRequest) returns (ListMutedProfilesResponse);   // MESH-ONLY : les mises en sourdine d'un lecteur pour une portée (timeline)
  rpc ListRestrictedAmong(ListRestrictedAmongRequest) returns (ListRestrictedAmongResponse);   // MESH-ONLY : quels commentateurs le propriétaire du post a restreints (comment)
}
```

**Contrôle d'accès (`CheckAccess`, mesh uniquement).** La règle d'audience qu'applique chaque
lecture selon le lecteur (post, comment, ces listes) : étant donné les profils du lecteur (les
`pids` du jeton ; aucun s'il est anonyme) et jusqu'à 100 profils cibles, chaque cible est `VISIBLE`,
`HEADER_ONLY` (un profil privé qu'aucun profil du lecteur ne suit) ou `HIDDEN` (un blocage dans un
sens ou l'autre, ou un profil masqué par la modération / une suspension de compte / une
suppression). Son propre profil est toujours visible. Quatre requêtes Scylla par appel quelle que
soit la taille (`IN` sur `follow_status`, `blocks` dans les deux sens et `profile_audience`). Les
appelants prennent le lecteur de leur propre requête edge et échouent fermé si cette RPC est
indisponible.

**Listes selon le lecteur.** `ListFollowers` / `ListFollowing` d'un profil qui n'est pas `VISIBLE`
pour le lecteur reviennent vides avec `hidden = true` (le propriétaire et les appelants du mesh les
reçoivent toujours).

**Confidentialité des listes (#659).** Le propriétaire choisit qui d'autre voit chaque liste
(`SetListPrivacy`, les deux liés au `profile_id` du propriétaire sur l'edge) : `EVERYONE` (par
défaut), `FOLLOWERS`, `MUTUALS` (les abonnés auxquels le propriétaire est abonné en retour) ou
`ONLY_ME`. Un lecteur hors de l'audience reçoit une page vide avec `hidden = true`, sur tous les
appareils : le réglage vit dans `profile_audience.lists` (propriété de social-graph, contrairement
aux autres colonnes de cette projection). Un lecteur à plusieurs profils est jugé sur le mieux
placé. **`RemoveFollower`** défait l'abonnement d'un abonné exactement comme son propre `Unfollow`
(compteurs, `social-graph.unfollowed`, élagage de la timeline) ; l'abonné n'est pas notifié et, sur
un profil privé, doit redemander.

**Mises en sourdine (#659).** `Mute` enregistre, par portée (posts, stories, messages), ce que l'acteur
cesse de voir de la cible ; remettre en sourdine remplace les portées, `Unmute` la lève, `ListMutes` pagine
celles du propriétaire et `RelationStatusView.muted` indique à l'acteur comment il met une cible en
sourdine. La cible n'est pas prévenue et rien n'est rompu. Table `social_graph.mutes` (partitionnée par
auteur de la mise en sourdine). Appliqué aujourd'hui aux **posts** : timeline exclut un auteur en sourdine
des fils des abonnements et de découverte du lecteur via `ListMutedProfiles` (mesh uniquement, ≤ 10 profils
lecteurs, ≤ 5 000 mises en sourdine chacun). Stories et messages sont stockés pour le client : il n'existe
encore ni surface de stories ni notification de message.

**Restrictions (#659).** `Restrict` rend les commentaires de la cible sur les posts de l'acteur visibles
de la cible et de l'acteur seulement (le read gate de comment interroge `ListRestrictedAmong`, mesh
uniquement, avec les commentateurs d'une page, ≤ 100 par appel) ; `Unrestrict` la lève, `ListRestricted`
pagine la liste du propriétaire et `RelationStatusView.restricted` l'indique à l'acteur. La cible n'est
pas prévenue et rien n'est rompu. Table `social_graph.restrictions` (partitionnée par propriétaire).

> **Contrat de sérialisation :** `RelationStatus` (du point de vue de l'acteur) : `NONE`, `FOLLOWING`,
> `FOLLOWED_BY`, `MUTUAL` (amitié implicite), `BLOCKING`, `BLOCKED_BY`.

### Contrat d'erreur (`SGR-xxxx`)

| Code | Variant | HTTP |
|---|---|---|
| SGR-1001/1002 | `AlreadyFollowing` / `NotFollowing` | 409 / 422 |
| SGR-1003/1004 | `AlreadyBlocked` / `NotBlocked` | 409 / 422 |
| SGR-2001/2002 | `SelfInteraction` / `BlockGateDenied` | 422 |
| SGR-9001/9002 | `DomainViolation` / `InvalidProfileId` | 422 |
| SDB-* / RDB-* / VAL-* | storage / cache / validation (delegated) | varies |

---

## 📨 Contrat événementiel & asynchrone

**Publie :**

| Topic | Trigger | Key | Consumers |
|---|---|---|---|
| `social-graph.followed` | `Follow` success, or `ApproveFollowRequest` (`via_request: true`) | `{actor}:{target}` | `timeline` (fan-out), `notification` (new follower / request accepted) |
| `social-graph.follow_requested` | `Follow` of a private profile: a pending request (#755); and `FollowRequestWithdrawn` when a pending request goes without becoming a follow — cancelled, declined, cut by a block (either direction), or moot when the actor follows the profile once public. Same topic and key, so a withdrawal is never read before its request | `{actor}:{target}` | `notification` (the owner is told; a withdrawal retracts that notice). `{actor_id, target_id, requested_at}` (+ `withdrawn_at` on a withdrawal) |
| `social-graph.unfollowed` | `Unfollow` success | `{actor}:{target}` | `timeline` (pruning) |
| `social-graph.blocked` | `Block` success | `{actor}:{target}` | content filtering, notification suppression |
| `social-graph.author_tier_changed` | un follow/unfollow franchit un seuil de palier (follower count) | `{profile}` | `profile` (persiste le palier → ré-émet sur `profile.v1.events` pour que `post` le dénormalise → routage de fan-out `timeline`/`geo-discovery`). `{profile_id, new_tier, follower_count, changed_at_ms}` |

`ProfileUnblocked` n'est **pas** publié — aucun fan-out aval n'en a besoin.

**Consomme :**

| Topic | Groupe de consommateurs | Rôle | Sur poison/épuisement |
|---|---|---|---|
| `profile.v1.events` | `social-graph-profile-audience` | projette les faits d'audience dans `profile_audience` : `ProfileVisibilityChanged` → `private` ; `ProfileHidden` / `ProfileDeleted` → `hidden = true` ; `ProfileRestored` → `hidden = false`. Upserts par colonne (idempotents ; le topic a pour clé `profile_id`, donc les faits d'un profil arrivent dans l'ordre). Autres types = commit no-op | DLQ `profile.v1.events.dlq` |

> **Démarrage de la projection :** pas de ligne = public, non masqué. Les profils rendus privés ou
> masqués avant le premier passage de ce consommateur demandent un rejeu ponctuel de
> `profile.v1.events` (aucun environnement actif ne contient de données aujourd'hui).

> **Contrat d'exécution :** les événements sont publiés via un producteur Kafka durable après le commit
> de l'arête. Les consommateurs aval gèrent leur propre traitement at-least-once sous `run_consumer`.

---

## 🌩️ Modes de défaillance & dégradation

| Failure | Symptom | Service behavior | Operator action |
|---|---|---|---|
| ScyllaDB indisponible | follow/block + listes échouent | **Dur** — `UNAVAILABLE` | vérifier le cluster Scylla |
| Redis indisponible | `GetRelationStatus`/comptes se dégradent | **Souple** — dériver depuis Scylla quand possible | vérifier Redis ; les compteurs se resync à la prochaine écriture |
| Kafka indisponible | le fan-out timeline/notification stagne | **Souple** — arêtes committées | vérifier les brokers ; rejeu des consommateurs |
| Dérive de compteur après perte Redis | comptes followers/following erronés | les compteurs sont dérivés, pas source de vérité | reconstruire depuis les tables `followers`/`following` |

**Backpressure & limites.** `ListFollowers/Following/Blocks` sont paginées par curseur. Les écritures
utilisent le profil Scylla **Strict** ; les lectures de statut utilisent **Fast**.

---

## 📦 Intégration & utilisation

```toml
[dependencies]
social-graph = { path = "crates/services/social-graph" }
```

Bibliothèque uniquement. Implémente [`service_runtime::Service`](../../platform/service-runtime/README.md)
sous le nom `social_graph::service::SocialGraphService` — `build` câble le repository ScyllaDB, le cache
Redis et le publisher Kafka durable ; `register` ajoute les services gRPC + réflexion ; `health_probes`
vérifie Scylla/Redis.

### Bootstrap (`crates/apps/social-graph-server`)

```rust
use std::net::SocketAddr;
use social_graph::service::SocialGraphService;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr: SocketAddr = std::env::var("SOCIAL_GRAPH_GRPC_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:50053".to_owned())
        .parse()?;
    service_runtime::serve::<SocialGraphService>(addr).await
}
```

---

## ⚙️ Configuration & environnement d'exécution

### Variables d'infrastructure héritées

| Variable | Required | Default | Description |
|---|---|---|---|
| `SCYLLA_CONTACT_POINTS` / `SCYLLA_LOCAL_DC` | **Yes** | — | ScyllaDB contact points + DC for token-aware routing. |
| `SCYLLA_KEYSPACE` | No | `social_graph` | Keyspace (see migrations). |
| `REDIS_HOSTS` | **Yes** | — | Redis nodes for sets + counters. |
| `KAFKA_BROKERS` | **Yes** | — | Kafka brokers for `social-graph.*`. |
| `SOCIAL_GRAPH_GRPC_ADDR` | No | `0.0.0.0:50053` | gRPC bind address. |

> Le réglage complet `SCYLLA_*` / `REDIS_*` / `KAFKA_*` vit dans les crates partagés storage/transport.

### Features de compilation
- `build.rs` compile `proto/social_graph/v1/*.proto` et émet le descriptor set de réflexion.

---

## 🚀 Déploiement, migrations & rollback

- **Migrations :** `migrations/000{1..6}_*.cql` (keyspace + 5 tables) sur `social_graph`, appliquées
  **avant** le premier démarrage.
- **Déploiement/Rollback :** `<TODO>` ; service sans état, sûr à déployer.
- **Reconstruction des compteurs :** les compteurs followers/following Redis sont dérivés — si Redis est
  perdu, les reconstruire en comptant les tables d'adjacence `followers`/`following` (job hors-ligne).

---

## 📈 Télémétrie, performance & métriques

- **Runtime :** Tokio multi-thread. Subscriber global tracing/OTel installé avant `serve`.

| Signal | Why it matters | Suggested alert |
|---|---|---|
| `GetRelationStatus` p99 | status read-path latency | > SLO ⇒ page |
| `social-graph.*` publish failures | downstream fan-out drift | sustained ⇒ check Kafka |
| `BlockGateDenied` rate | abuse / harassment signal | unusual spike ⇒ investigate |
| Scylla write errors | edge durability | any spike ⇒ check cluster |

---

## 🛠️ Développement local

```bash
cargo build -p social-graph && cargo clippy -p social-graph --all-targets
cargo test  -p social-graph
docker compose up -d scylla redis kafka       # repo-root compose
for f in crates/services/social-graph/migrations/*.cql; do cqlsh -f "$f"; done
```

---

## 🚨 Dépannage & runbook

> Format : **symptôme → cause racine → mitigation.**

**1. `SGR-2002 BlockGateDenied` sur un `Follow` entre deux profils apparemment sans lien.**
Cause racine : un blocage existe dans *l'un ou l'autre* sens (`blocks(A,B)` ou `blocks(B,A)`) ; le gate
est symétrique par conception. Mitigation : vérifier les deux lignes `blocks` ; si le blocage est voulu,
c'est correct — le follow doit rester refusé jusqu'à un `Unblock`.

**2. Les comptes followers/following semblent erronés après un incident Redis.**
Cause racine : les compteurs sont des dérivations Redis O(1), pas la source de vérité ; un flush Redis les
perd. Mitigation : reconstruire en comptant `followers`/`following` pour les profils concernés ; les
compteurs se réparent en avant au prochain follow/unfollow.

**3. Un nouveau follow n'apparaît jamais dans le fil d'accueil de l'utilisateur.**
Cause racine : l'arête a été committée et `social-graph.followed` publié, mais le consommateur de
`timeline` est en retard ou a dead-lettered l'événement. Mitigation : vérifier le lag et la DLQ du
consommateur `social-graph.followed` de timeline ; l'arête elle-même est durable dans Scylla.
