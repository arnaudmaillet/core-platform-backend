---
i18n:
  source: ./README.md
  source_sha256: f17bcc57ac0f7a7fbec4199aebd15dfa3cb9384e5d70bd94358c7a7fcb0781be
  translated_at: 2026-10-05
  status: complete
---
> 🇫🇷 Traduction française — la version **anglaise** [`README.md`](./README.md) fait foi.
> En cas de divergence, l'anglais prime. Les contrats (codes d'erreur, variables
> d'environnement, topics Kafka, identifiants) sont volontairement laissés en anglais.

# `profile` — Couche d'identité publique à haut débit pour le trafic de lecture hyperscale

> **Fiche service**
>
> | | |
> |---|---|
> | **Propriétaire** | `<TODO: équipe>` · `<TODO: #canal-slack>` |
> | **Astreinte / escalade** | `<TODO: rotation-astreinte>` → `<TODO: politique-escalade>` |
> | **Palier (Tier)** | **TIER-0** — chemin de lecture public, résolution d'identité à l'échelle de la flotte |
> | **Binaire déployable** | `crates/apps/profile-server` (crate bibliothèque : `crates/services/profile`) |
> | **Bases de données** | ScyllaDB keyspace `profile` · Redis (cache-aside) |
> | **Asynchrone** | publie `profile.v1.events` (dont `ProfileTierChanged`) · consomme `account.v1.events`, `social-graph.author_tier_changed` |
> | **Appelants amont** | `<TODO: passerelle>`, consommateurs reco/lookup en masse, `geo-discovery` (via événements) |
> | **Dépendances aval** | ScyllaDB, Redis, Kafka |
> | **SLO** | lecture cache-hit p99 **< 1 ms** · cache-miss p99 **< 5 ms** |

---

## 🎯 Vue d'ensemble & rôle du service

`profile` possède toutes les **métadonnées d'identité publiques** : @handles, noms d'affichage, bios,
avatars et classification de profil. C'est le chemin de lecture de référence pour tout consommateur
résolvant une identité publique — des passerelles affichant des cartes utilisateur aux moteurs de
recommandation effectuant des lookups en masse. Il gère une relation 1-à-N entre un `AccountId` privé et
plusieurs agrégats `Profile` indépendants (personnel, professionnel, marque, bot).

Le problème difficile qu'il résout est la **lecture sub-milliseconde à l'échelle hyperscale sans
couplage inter-services** : une couche cache-aside Redis devant ScyllaDB sert les hits de cache en
< 1 ms, et le cycle de vie du compte est ingéré **de façon réactive** via Kafka
(`AccountSuspended/Deactivated/Deleted/Activated` → masquer/restaurer), de sorte qu'il n'y a aucune dépendance
synchrone à `account` sur le chemin de lecture.

**Objectifs fondamentaux :** P99 < 1 ms cache-hit, < 5 ms cache-miss ; @handles globalement uniques via
LWT ScyllaDB (`IF NOT EXISTS`, sans verrou distribué) ; sécurité d'écriture concurrente via `IF version
= ?` optimiste. **SRP strict :** zéro logique de graphe social — abonnements/amis/feeds appartiennent
ailleurs.

---

## 📐 Architecture & concepts

Hexagonal / DDD, bus CQRS, store durable ScyllaDB, cache-aside Redis, masquage réactif Kafka.

```
gRPC ─► ProfileServiceHandler ─► Command bus            Query bus ─► cache.get ─HIT─► return
            │                       │                        │ MISS
            ▼                       ▼                        ▼
   Create/Update/ChangeHandle(LWT)/…              repo.find_by_id ─► cache.set (async)
            │
            ▼
   ScyllaDB: profiles (PK profile_id) · profiles_by_account (PK account_id, CK created_at DESC)
             profile_handles (PK handle — LWT uniqueness index)

   Redis cache-aside: profile:v1:{id} TTL 300s · handle:v1:{handle} TTL 600s · account:profiles:v1:{id} TTL 120s

   Kafka account.v1.events ─► account_suspended / account_deactivated / account_deleted → masquer chaque profil du compte · account_activated → restaurer ceux que la suspension ou la désactivation a masqués
```

**Versionnement des clés de cache.** Toutes les clés portent un préfixe `v1:` — incrémenter le suffixe
effectue une invalidation de cache à l'échelle de la flotte, sans interruption, lors d'une migration de
schéma. **Réservation par tombstone :** un handle supprimé est bloqué 30 jours via `tombstoned_at`,
empêchant le détournement rapide d'identité (`handle_is_available()` l'impose à la couche application).

> **Invariants** (et où ils sont imposés) : unicité du handle via LWT `IF NOT EXISTS` sur
> `profile_handles` ; concurrence optimiste via LWT `IF version = ?` sur `profiles` (→ `PRF-4001`,
> réessayable) ; transitions de statut (`Active⇄Suspended⇄Hidden→Deleted`, `Deleted` terminal) dans
> l'agrégat ; `profile_kind` immuable après création.

---

## 📊 Objectifs de niveau de service (SLO)

| SLI | Objectif | Fenêtre | Mesuré par |
|---|---|---|---|
| Lecture p99 — cache hit | **< 1 ms** | 1 h | `profile.grpc.request.duration` |
| Lecture p99 — cache miss | **< 5 ms** | 1 h | `profile.scylla.query.duration` |
| p99 gRPC (toutes RPC) | < 50 ms (page) | 1 h | `profile.grpc.request.duration` |
| Taux de hit du cache (`by_id`) | > 80 % | 5 min | `profile.cache.hit_ratio` |
| Durabilité | aucune écriture acquittée perdue | — | `LocalQuorum` Scylla sur les écritures |

**Budget d'erreur :** `<TODO>`. **En cas d'épuisement :** `<TODO>`.

---

## 🔗 Dépendances & rayon d'impact (blast radius)

**Aval — ce dont `profile` a besoin pour fonctionner :**

| Dependency | Purpose | If down → | Degradation |
|---|---|---|---|
| ScyllaDB (keyspace `profile`) | store durable | lectures + écritures échouent | **Dur** — `UNAVAILABLE` |
| Redis | cache-aside | cache miss vers Scylla | **Souple** — toutes les lectures retombent ; la latence monte |
| Kafka | masquage réactif + `profile.tier_changed` | le masquage suspend/deactivate/delete stagne | **Souple** — lectures/écritures non affectées |

**Amont — qui dépend de `profile` (rayon d'impact si `profile` tombe) :**

| Caller | Uses | Impact visible utilisateur si `profile` est indisponible |
|---|---|---|
| `<TODO: passerelle>` | `GetProfileById/ByHandle` | l'affichage des cartes / de l'identité échoue |
| `geo-discovery` | consomme `profile.tier_changed` | les badges de palier d'auteur sur la carte deviennent périmés |

> **Chemin critique ?** **Oui** — la résolution d'identité publique est sur le chemin d'affichage
> synchrone de la plupart des surfaces utilisateur.

---

## 🔌 Interfaces publiques & contrat d'API

### gRPC — `profile.v1.ProfileService`

```protobuf
service ProfileService {
  // Lifecycle (all return CommandResponse)
  rpc CreateProfile(CreateProfileRequest) returns (CommandResponse); // privé quand l'`age` du jeton est 13–17
  rpc UpdateProfile(UpdateProfileRequest) returns (CommandResponse);
  rpc ChangeHandle(ChangeHandleRequest) returns (CommandResponse);   // LWT
  rpc UpdateAvatar(UpdateAvatarRequest) returns (CommandResponse);
  rpc UpdateBanner(UpdateBannerRequest) returns (CommandResponse);
  rpc SetVisibility(SetVisibilityRequest) returns (CommandResponse);
  rpc SetInteractionSettings(SetInteractionSettingsRequest) returns (CommandResponse); // qui peut commenter / mentionner / écrire, téléchargements, compteurs de likes ; les ados démarrent abonnés uniquement
  rpc SetLocationSettings(SetLocationSettingsRequest) returns (CommandResponse); // mode fantôme + précis / niveau ville (réservé au propriétaire sur la vue) ; les ados démarrent fantômes ; geo-discovery l'applique
  rpc SetFeedSettings(SetFeedSettingsRequest) returns (CommandResponse); // #662 contenu sensible moins (par défaut) / standard, synchronisé entre appareils ; le client l'envoie comme content_level de timeline ; réservé au propriétaire sur la vue
  rpc SetTabSettings(SetTabSettingsRequest) returns (CommandResponse); // #664 fenêtre de posts (tout / 6 mois / 1 mois / 3 jours ; post masque les posts plus anciens aux visiteurs) + visibilité des onglets j'aime / enregistrés / republications / lieux (stockée ; aucun onglet servi pour l'instant) ; partiel ; réservé au propriétaire sur la vue
  rpc SetCommentFilters(SetCommentFiltersRequest) returns (CommandResponse); // #660 mots masqués (≤ 200, normalisés) + filtre offensant (activé par défaut) ; réservé au propriétaire sur la vue ; comment les applique
  rpc SetDiscoverySettings(SetDiscoverySettingsRequest) returns (CommandResponse); // #661 statut d'activité, accusés de lecture, trouvable par téléphone / e-mail / recherche de handle / QR / suggestions (partiel ; réservé au propriétaire sur la vue) ; les ados démarrent introuvables par téléphone, e-mail, suggestions ; search applique by_handle_search
  rpc SetAccountType(SetAccountTypeRequest) returns (CommandResponse); // #668 personnel / professionnel (créateur) / marque (entreprise) + la fiche de contact publique d'une marque ; un bot reste un bot
  rpc RequestVerification(RequestVerificationRequest) returns (CommandResponse); // #668 propriétaire : catégorie + 1 à 5 documents → file du personnel
  rpc GetVerificationRequest(GetVerificationRequestRequest) returns (GetVerificationRequestResponse); // #668 propriétaire : en attente / approuvée / refusée (+ motif)
  rpc ListPendingVerificationRequests(ListPendingVerificationRequestsRequest) returns (ListPendingVerificationRequestsResponse); // personnel, mesh uniquement
  rpc DecideVerificationRequest(DecideVerificationRequestRequest) returns (CommandResponse); // personnel, mesh uniquement : approuver vérifie le profil ; refuser exige un motif
  rpc VerifyProfile(VerifyProfileRequest) returns (CommandResponse);
  rpc HideProfile(HideProfileRequest) returns (CommandResponse);
  rpc RestoreProfile(RestoreProfileRequest) returns (CommandResponse);
  rpc DeleteProfile(DeleteProfileRequest) returns (CommandResponse);
  // Queries
  rpc GetProfileById(GetProfileByIdRequest) returns (ProfileView);
  rpc GetProfileByHandle(GetProfileByHandleRequest) returns (ProfileView);
  rpc CheckHandleAvailability(CheckHandleAvailabilityRequest) returns (CheckHandleAvailabilityResponse);
  rpc ListProfilesByAccount(ListProfilesByAccountRequest) returns (ListProfilesByAccountResponse);
}
```

**Disponibilité d'un handle.** `CheckHandleAvailability` (edge `public_read` : les invités qui
s'inscrivent, et les membres) applique les règles de handle et la même lecture de disponibilité
que la pré-vérification de `CreateProfile` : `AVAILABLE` (avec le handle normalisé, en minuscules),
`TAKEN` (utilisé, ou libéré depuis moins de 30 jours) ou `INVALID` (avec la raison). Indicatif :
c'est la réservation LWT de `CreateProfile` qui réserve le handle.

**Lectures selon le lecteur.** `GetProfileById/ByHandle` prennent le lecteur du transport
(`edge::viewer`). Le propriétaire (`sub` du jeton = le compte du profil) et les appelants du mesh
reçoivent la vue complète. Tout autre lecteur ne reçoit qu'un profil **actif** (masqué, suspendu
ou supprimé → `NOT_FOUND`), sans les champs réservés au propriétaire : `account_id` (il relierait
entre eux les profils d'un compte), `locale`, `timezone`, `masked_at`, `masking_reason`. Un profil
privé renvoie toujours son en-tête ; ce sont ses posts et ses listes que la confidentialité retient.

### Ports Rust (contrat hexagonal)

```rust
pub trait ProfileRepository: Send + Sync + 'static { /* find_by_id, claim_handle (LWT), save (CAS), … */ }
pub trait ProfileCache:      Send + Sync + 'static { /* get_by_id, set_by_id, invalidate_by_id, … */ }
```

L'agrégat `Profile` porte `version` (verrou optimiste), `handle` (slug `[a-z0-9_.]`, 2–30),
`profile_kind` (immuable), `visibility`, `verified`, `masked_at`/`masking_reason` (positionnés
réactivement).

### Contrat d'erreur (`PRF-xxxx`)

| Code | Variant | HTTP | Retryable |
|---|---|---|---|
| PRF-1001 | `ProfileNotFound` | 404 | No |
| PRF-1002 | `HandleAlreadyTaken` | 409 | No |
| PRF-1003 | `HandleReserved` | 409 | No |
| PRF-2001/2002 | `ProfileNotActive` / `InvalidStatusTransition` | 422 | No |
| PRF-4001 | `ConcurrentModification` | 409 | **Yes** |
| PRF-5001 | `ProfileAlreadyVerified` | 409 | No |
| PRF-5002 | `VerificationPending` (une demande est déjà en cours d'examen) | 409 | No |
| PRF-5003 | `NoPendingVerification` (rien à décider) | 409 | No |
| PRF-9001–9010 | domain / parse / validation | 422 | No |
| SDB-* / RDB-* | storage (delegated) | varies | varies |

---

## 📨 Contrat événementiel & asynchrone

**Publie :**

| Topic | Déclencheur | Clé | Consommateurs |
|---|---|---|---|
| `profile.v1.events` | chaque mutation de cycle de vie — `ProfileCreated` / `ProfileUpdated` / `HandleChanged` / `ProfileVerified` / `ProfileHidden` / `ProfileRestored` / `ProfileDeleted` / `ProfileTierChanged` / `ProfileVisibilityChanged` (`public`/`private` ; `SetVisibility` n'émet que celui-ci) | `profile_id` | `search` (indexation des profils), `post` (dénormalisation du palier auteur), `social-graph` (projection d'audience pour le contrôle d'accès) |

> **Contrat de fil :** un topic versionné unique, tagué en interne sur `type` (convention du service moderation), clé `profile_id` pour l'ordre par-profil. Les événements sont **fins** (ids + horodatages, sans contenu d'affichage) — un consommateur qui a besoin du profil complet l'hydrate via `GetProfileById`. Chaque command handler draine les événements en attente de l'agrégat et les publie **après** l'écriture durable (durable-first ; un publisher no-op couvre la composition sans broker).

**Consomme :**

| Topic | Consumer group | Purpose | On poison/exhaustion |
|---|---|---|---|
| `account.v1.events` | `profile-account-events` | le `DomainEvent` d'account (tagué sur `type`, snake_case) : `account_suspended` / `account_deactivated` / `account_deleted` → masquer **chaque** profil du compte (`HideAccountProfiles`, le type d'événement sert de raison de masquage) ; `account_activated` → restaurer les profils masqués par la suspension ou la désactivation (`RestoreAccountProfiles` ; un masquage pour violation de politique reste). Idempotent par profil : une redélivrance termine un parcours partiel. Autres types = commit no-op | DLQ `account.v1.events.dlq` |
| `social-graph.author_tier_changed` | `profile-author-tier` | dénormalise le palier auteur sur le profil (`SetProfileTier`) → ré-émet sur `profile.v1.events` (`ProfileTierChanged`) ; idempotent si palier inchangé | DLQ `social-graph.author_tier_changed.dlq` |

> **Contrat d'exécution (obligatoire) :** le consommateur d'événements compte s'exécute sous
> `run_consumer` — commit manuel après succès (`enable_auto_commit=false`), retries bornés avec backoff
> + jitter, DLQ en cas d'épuisement/poison. Les erreurs de cache sont toujours traitées comme des miss ;
> les échecs de cache-set sont journalisés, jamais remontés.

---

## 🌩️ Modes de défaillance & dégradation

| Failure | Symptom | Service behavior | Operator action |
|---|---|---|---|
| ScyllaDB indisponible | lectures + écritures échouent | **Dur** — `UNAVAILABLE` | vérifier le cluster Scylla / le DC |
| Redis indisponible / froid | la latence monte | **Souple** — toutes les lectures retombent sur Scylla (profil Fast) | vérifier le taux de hit ; se rétablit en général seul |
| Course LWT sur handle | `PRF-1002` à l'écrivain perdant | sérialisation correcte à la couche store | aucune — remonter à l'utilisateur pour choisir un autre handle |
| Lag du consommateur d'événements compte | profils non masqués à la suspension/suppression | retries dans le budget ; offset non committé | vérifier le lag ; re-dispatcher le masquage au besoin |

**Backpressure & limites.** Les écritures utilisent le profil Scylla **Strict** (`LocalQuorum`) ; les
lectures utilisent **Fast** (`LocalOne` + retry spéculatif déclenchant 1 requête supplémentaire après
20 ms) pour borner la latence de queue.

---

## 📦 Intégration & utilisation

```toml
[dependencies]
profile = { path = "crates/services/profile" }
```

Bibliothèque uniquement. Implémente [`service_runtime::Service`](../../platform/service-runtime/README.md)
sous le nom `profile::service::ProfileService`. `build(infra)` lit ses profils de TTL de cache depuis la
section `[cache]` d'`infrastructure.toml`, assemble repository/cache/bus CQRS, et **lance lui-même le
consommateur supervisé d'événements compte** ; `register` ajoute les services gRPC + réflexion ;
`health_probes` vérifie Scylla/Redis. `profile` est le service *consommateur d'infra* canonique. Le
harnais d'intégration pilote `App::build` directement, donc le graphe câblé testé est celui qui est
livré.

### Bootstrap (`crates/apps/profile-server`)

```rust
use std::net::SocketAddr;
use profile::service::ProfileService;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr: SocketAddr = std::env::var("PROFILE_GRPC_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:50052".to_owned())
        .parse()?;
    service_runtime::serve::<ProfileService>(addr).await
}
```

---

## ⚙️ Configuration & environnement d'exécution

### Variables d'infrastructure héritées (sous-ensemble clé)

| Variable | Required | Default | Description |
|---|---|---|---|
| `SCYLLA_CONTACT_POINTS` | **Yes** | `127.0.0.1:9042` | ScyllaDB contact points. |
| `SCYLLA_LOCAL_DC` | **Yes** | `datacenter1` | DC for token-aware routing. |
| `SCYLLA_KEYSPACE` | No | — | Leave unset if queries fully-qualify table names (recommended). |
| `REDIS_URL` | **Yes** | `redis://127.0.0.1:6379` | Redis connection URL. |
| `KAFKA_BROKERS` | **Yes** | `127.0.0.1:9092` | Kafka brokers. |
| `KAFKA_CONSUMER_GROUP` | No | `profile-service` | account-event consumer group. |
| `PROFILE_GRPC_ADDR` | No | `0.0.0.0:50052` | gRPC bind address. |

> Le réglage complet `SCYLLA_*` / `REDIS_*` / `KAFKA_*` est documenté dans les crates partagés
> storage/transport. Les profils de TTL `[cache]` sont consommés depuis `infrastructure.toml`, pas via env.

### Features de compilation
- `build.rs` compile `proto/profile/v1/*.proto` et émet le descriptor set de réflexion.

---

## 🚀 Déploiement, migrations & rollback

- **Migrations :** `crates/services/profile/migrations/000{1..4}_*.cql` sur le keyspace `profile`,
  appliquées **avant** le premier démarrage.
- **Bump de version de cache :** pour invalider le cache à l'échelle de la flotte lors d'un changement de
  schéma, incrémenter le préfixe de clé `v1:` — sans flush, sans interruption.
- **Déploiement/Rollback :** `<TODO : stratégie>` ; service sans état, sûr à déployer.

---

## 📈 Télémétrie, performance & métriques

- **Runtime :** Tokio multi-thread. Exige un contact point joignable dans `SCYLLA_LOCAL_DC` au démarrage ;
  l'indisponibilité de Redis se dégrade gracieusement.

| Signal | Why it matters | Suggested alert |
|---|---|---|
| `profile.grpc.request.duration` p99 | read-path SLO | > 50 ms for 2m ⇒ page |
| `profile.cache.hit_ratio` (`by_id`) | cache health | < 70% ⇒ investigate TTL/eviction |
| `profile.handle.claim.conflict_total` | LWT contention | > 10/min ⇒ possible hijack attempt |
| `profile.concurrent_modification_total` | write hot-spots | sustained > 0 ⇒ retry-storm risk |
| `profile.kafka.consumer.lag` | masking freshness | > 5 000 ⇒ scale consumers |

---

## 🛠️ Développement local

```bash
docker compose up -d                          # ScyllaDB, Redis, Kafka, OTel collector
cargo build -p profile && cargo clippy -p profile -- -D warnings
cargo test  -p profile                        # add --features integration for live-infra tests
for f in crates/services/profile/migrations/*.cql; do cqlsh -f "$f"; done
```

---

## 🚨 Dépannage & runbook

> Format : **symptôme → cause racine → mitigation.**

**1. `PRF-1002 HandleAlreadyTaken` alors que le handle semble libre.**
Cause racine : un `CreateProfile`/`ChangeHandle` concurrent a gagné la course LWT `IF NOT EXISTS`.
Mitigation : comportement correct — le LWT a sérialisé le conflit au store ; le client doit proposer un
autre handle. Aucune intervention manuelle.

**2. Le cache de profil affiche des données périmées après une mise à jour.**
Cause racine : une lecture parallèle a repeuplé le cache depuis une requête Scylla en vol qui a renvoyé
l'ancienne version avant le quorum. Mitigation : le TTL de 300 s borne la péremption ; pour une cohérence
immédiate, `DEL profile:v1:{id}` et `DEL handle:v1:{handle}`, puis relire pour repeupler.

**3. Les événements compte cessent de masquer les profils après la panne d'un nœud Scylla.**
Cause racine : un handler `HideProfile`/`RestoreProfile` a renvoyé `Storage` ; le message
réessaie/dead-letter. Mitigation : surveiller `profile.kafka.consumer.lag` et `account.v1.events.dlq` ;
pour les profils bloqués, interroger Scylla par `account_id` et re-dispatcher le masquage via l'outillage
admin.
