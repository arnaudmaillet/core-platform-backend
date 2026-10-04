---
i18n:
  source: ./README.md
  source_sha256: 6cf6f02edf29348f6aede548d4384c3305996106dec757291e2856930343fea2
  translated_at: 2026-10-04
  status: complete
---
> 🇫🇷 Traduction française — la version **anglaise** [`README.md`](./README.md) fait foi.
> En cas de divergence, l'anglais prime. Les contrats (codes d'erreur, variables
> d'environnement, topics Kafka, identifiants) sont volontairement laissés en anglais.

# `notification` — Ingestion d'événements sémantiques, fil d'activité durable et push temps réel

> **Fiche service**
>
> | | |
> |---|---|
> | **Propriétaire** | `<TODO: équipe>` · `<TODO: #canal-slack>` |
> | **Astreinte / escalade** | `<TODO: rotation-astreinte>` → `<TODO: politique-escalade>` |
> | **Palier (Tier)** | **TIER-2** — dérivé/best-effort ; le fil est durable, les pushs sont best-effort |
> | **Binaire déployable** | `crates/apps/notification-server` (crate bibliothèque : `crates/services/notification`) |
> | **Bases de données** | ScyllaDB keyspace `notification` (fil TWCS + compteurs) · Redis (collapse + non-lus) |
> | **Asynchrone** | ne publie rien · consomme `engagement.reactions` / `comment.created` / `post.published` |
> | **Appelants amont** | `<TODO: mobile / BFF (stream + lectures de fil)>` |
> | **Dépendances aval** | ScyllaDB, Redis, Kafka |
> | **SLO** | lecture du compte de non-lus sub-ms (Redis) · lecture de fil paginée O(1) · push best-effort |

---

## 🎯 Vue d'ensemble & rôle du service

`notification` boucle la rétroaction utilisateur. Il ingère des événements métier sémantiques depuis
Kafka (`engagement.reactions`, `comment.created`, `post.published`), persiste des enregistrements
d'activité durables par profil dans ScyllaDB, et dispatche des pushs temps réel vers les clients actifs
via un canal gRPC server-streaming.

Le problème difficile qu'il résout est le **fan-out des célébrités** : un post attirant 10k+
réactions/seconde saturerait une seule partition ScyllaDB et spammerait la cible. Il résout cela avec un
**pipeline de write-collapse en couches** — collapse HashMap intra-batch, une fenêtre Redis cross-batch
pour les sujets chauds, et un cap horaire par sujet — de sorte qu'un sujet viral devient une seule ligne
d'activité, flushée périodiquement.

**Objectifs fondamentaux :** fil d'activité paginé par curseur en O(1) (aucun `ALLOW FILTERING`) ; badge
de non-lus sub-ms (Redis L1, fallback compteur Scylla L2) ; protection de fan-out sur les partitions de
célébrités. **SRP :** ne stocke que des IDs de relations sémantiques — aucune chaîne localisée, aucun
handle, aucun contenu ; l'hydratation UI est l'affaire du client.

---

## 📐 Architecture & concepts

```
Kafka: engagement.reactions │ comment.created │ post.published
   │                          │                 │
ReactionNotificationWorker  CommentNotificationWorker  MentionNotificationWorker
 (L1 in-batch collapse,     (cache comment author,    (cache post author, parse
  L2 Redis hot window,       block-gate + self guard)  @mentions from caption)
  L3 hourly cap)
   └──────────────┬──────────────────┬──────────────────┘
                  ▼
       CollapseFlushWorker (polls notification:window_schedule ZSET every 30s,
                            drains settled Redis windows → single Scylla row)
                  ▼
   ScyllaDB notification.notifications_by_profile (TWCS 7d windows, 90d TTL,
       PK target_profile_id, CK created_at DESC, notification_id ASC)
                  ▼
   gRPC NotificationService: List / GetUnreadCount / MarkRead / MarkAllRead
                            + StreamNotifications (tokio::broadcast per profile)
```

> **Invariants :** `NotificationView` ne porte que des UUID + ints d'enum (ni PII ni contenu). `MarkRead`
> exige à la fois `notification_id` ET `created_at_ms` (la clé de clustering Scylla complète pour un
> UPDATE par point). `read_horizon_ms` (positionné par `MarkAllRead`) affiche comme lu tout
> `created_at_ms ≤ horizon` indépendamment du flag par ligne `is_read`. Idempotence : les clés de claim de
> dédup (`notification:dedupe:{profile}:…`) empêchent un événement re-livré de double-incrémenter le
> compteur.

---

## 📊 Objectifs de niveau de service (SLO)

> TIER-2 : le fil durable et le compteur de non-lus portent des objectifs souples ; les pushs temps réel
> sont explicitement best-effort (pas de garantie de livraison — les clients réconcilient via
> `ListNotifications`).

| SLI | Objectif | Fenêtre | Mesuré par |
|---|---|---|---|
| `GetUnreadCount` p99 (Redis L1) | `< <TODO> ms` | 1 h | histogramme gRPC |
| `ListNotifications` p99 (paginée) | `< <TODO> ms` | 1 h | histogramme de lecture Scylla |
| Lag du consommateur d'ingestion | `< <TODO> s` | direct | `kafka_consumer_group_lag{group=~"notification-.*"}` |
| Durabilité du fil | aucune notification acquittée perdue | — | écriture Scylla + commit manuel at-least-once |

**Budget d'erreur :** `<TODO>`. **En cas d'épuisement :** `<TODO>`.

---

## 🔗 Dépendances & rayon d'impact (blast radius)

**Aval :**

| Dependency | Purpose | If down → | Degradation |
|---|---|---|---|
| ScyllaDB (`notification`) | fil + compteurs durables | les écritures/lectures de fil échouent | **Dur** pour le fil ; retries at-least-once |
| Redis | fenêtres de collapse + non-lus L1 + caches block/author | collapse + non-lus se dégradent | **Souple** — le compteur Scylla est l'ancre de durabilité |
| Kafka | ingestion d'événements | les nouvelles notifications s'arrêtent | **Souple** — fil existant servi ; at-least-once à la reprise |

**Amont (rayon d'impact) :**

| Caller | Uses | Impact si `notification` est indisponible |
|---|---|---|
| `<TODO: mobile / BFF>` | lectures de fil + `StreamNotifications` | l'icône cloche / le fil d'activité cesse de se mettre à jour |

> **Chemin critique ?** **Non** — dérivé, asynchrone, best-effort. Une panne dégrade l'engagement mais ne
> bloque pas les actions utilisateur fondamentales.

---

## 🔌 Interfaces publiques & contrat d'API

### gRPC — `notification.v1.NotificationService`

```protobuf
service NotificationService {
  rpc ListNotifications   (ListNotificationsRequest)   returns (ListNotificationsResponse);
  rpc GetUnreadCount      (GetUnreadCountRequest)       returns (GetUnreadCountResponse);
  rpc MarkRead            (MarkReadRequest)             returns (CommandResponse);  // needs notification_id + created_at_ms
  rpc MarkAllRead         (MarkAllReadRequest)          returns (CommandResponse);  // sets read_horizon_ms
  rpc StreamNotifications (StreamNotificationsRequest)  returns (stream StreamNotificationsResponse);
  // Appareils push et préférences (#654)
  rpc RegisterDevice                (RegisterDeviceRequest)                returns (CommandResponse);
  rpc UnregisterDevice              (UnregisterDeviceRequest)              returns (CommandResponse);
  rpc GetNotificationPreferences    (GetNotificationPreferencesRequest)    returns (NotificationPreferences);
  rpc UpdateNotificationPreferences (UpdateNotificationPreferencesRequest) returns (NotificationPreferences);
  rpc ResolvePushTargets            (ResolvePushTargetsRequest)            returns (ResolvePushTargetsResponse);  // MESH-ONLY
}
```

**Appareils push et préférences (#654).** Le contrat et le stockage du push ; **rien n'envoie encore de
push** (pas d'identifiants APNs). Toutes les RPC sauf `ResolvePushTargets` sont `authenticated` sur l'edge
et liées à `profile_id`.
- `RegisterDevice(device_id, token, platform, environment, timezone)` — à appeler à chaque lancement. Un
  jeton enregistré pour un autre **compte** quitte les profils de ce compte (un téléphone transmis ne
  reçoit jamais les push de l'ancien compte) ; le nouveau jeton d'un appareil remplace l'ancien. Tables
  `push_devices` (par profil) et `push_device_tokens` (par jeton).
- Préférences (`notification_preferences`, un document JSON par profil) : push et e-mail par catégorie
  (j'aime, commentaires, mentions, nouveaux abonnés, demandes d'abonnement, messages, posts des comptes
  suivis, lieux à proximité, portefeuille), une pause (≤ 8 h), des heures calmes lues dans le fuseau IANA du
  titulaire. Par défaut : tous les push activés, tous les e-mails désactivés ; **13–17 ans : heures calmes
  22:00–07:00** (d'après l'`age` du jeton en lecture, et écrites au premier `RegisterDevice` d'un adolescent
  pour que l'émetteur les applique). L'e-mail marketing est le consentement `marketing` du compte
  (`account.v1.UpdateConsents`), pas un second interrupteur ici.
- `ResolvePushTargets(profile_id, category)` (mesh) est ce que l'émetteur de push interrogera : les
  appareils, ou `allowed = false` quand la catégorie est désactivée, qu'une pause court ou que ce sont les
  heures calmes.

### Ports Rust (contrat hexagonal)

```rust
pub trait NotificationRepository: Send + Sync + 'static { /* insert, list_paginated, mark_read, *_counter */ }
pub trait UnreadCounter:          Send + Sync + 'static { /* incr/decr/reset/get + read_horizon (Redis L1 + Scylla L2) */ }
pub trait BlockCache:             Send + Sync + 'static { /* is_blocked(sender, target) — social-graph gate */ }
pub trait StreamRegistry:         Send + Sync + 'static { /* subscribe/broadcast (broadcast::Receiver per profile) */ }
pub trait DeviceRegistry:         Send + Sync + 'static { /* register/unregister/devices — push devices per profile */ }
pub trait PreferenceStore:        Send + Sync + 'static { /* get/put — notification preferences per profile */ }
```

### Contrat d'erreur (`NTF-xxxx`)

`NTF-1xxx` lifecycle … `NTF-6001` author-cache miss (reaction notification dropped) … `NTF-9xxx`
identifiers — via le crate partagé `error`.

---

## 📨 Contrat événementiel & asynchrone

**Publie :** rien — `notification` est un consommateur/sink pur.

**Consomme :**

| Topic | Consumer group | Purpose | On poison/exhaustion |
|---|---|---|---|
| `engagement.reactions` | `notification-reaction-consumer` | reaction notifications (collapsed) | DLQ `{topic}.dlq` |
| `comment.created` | `notification-comment-consumer` | comment notifications (block-gated, self-guarded) | DLQ `{topic}.dlq` |
| `post.published` | `notification-mention-consumer` | parse `@mentions`, cache post author | DLQ `{topic}.dlq` |

> **Contrat d'exécution (obligatoire) :** tous les workers s'exécutent sous `run_consumer` — commit manuel
> après succès (`enable_auto_commit=false`, reset earliest), retries bornés avec backoff + jitter, DLQ en
> cas d'épuisement/poison. Scaler les réplicas de consommateur jusqu'au nombre de partitions de chaque
> topic.

---

## 🌩️ Modes de défaillance & dégradation

| Failure | Symptom | Service behavior | Operator action |
|---|---|---|---|
| Fan-out de célébrité (10k/s) | — | L1 intra-batch + L2 fenêtre Redis 30 s (heat > 100) + L3 cap horaire (3/sujet) | aucune — conçu pour ça |
| Redis indisponible | vérifs block/heat ignorées | les workers poursuivent ; écritures Scylla continuent ; les non-lus accumulent une incohérence jusqu'à la reprise | vérifier Redis ; le compteur Scylla réconcilie |
| ScyllaDB indisponible | les écritures de fil échouent | at-least-once : offset non committé → retry → DLQ ; pushs best-effort | vérifier Scylla ; drainer la DLQ |
| Client de stream lent | `RecvError::Lagged` | `tokio::broadcast` abandonne les anciens ; le stream se termine en `Status::DataLoss` | le client se reconnecte + re-`ListNotifications` |
| Crash de CollapseFlushWorker | fenêtre non flushée | le TTL Redis (fenêtre + 10 s de grâce) expire la clé ; le membre de schedule reste pour que le prochain démarrage re-draine (no-op si vide) | redémarrer le worker ; au pire une fenêtre perdue |

**Backpressure & limites.** `NOTIFICATION_MAX_PAGE_SIZE` plafonne les pages de fil ;
`NOTIFICATION_STREAM_BUFFER_SIZE` borne le broadcast par profil ; le cap horaire et la fenêtre de collapse
Redis bornent le volume d'écriture des célébrités.

---

## 📦 Intégration & utilisation

```toml
[dependencies]
notification = { path = "crates/services/notification" }
```

Bibliothèque uniquement. Implémente [`service_runtime::Service`](../../platform/service-runtime/README.md)
sous le nom `notification::service::NotificationService` — `build` câble le repository, le cache, le
broadcast registry, les bus CQRS et les workers Kafka ; `register` ajoute les services gRPC + réflexion ;
`health_probes` vérifie Scylla/Redis.

### Bootstrap (`crates/apps/notification-server`)

```rust
use std::net::SocketAddr;
use notification::service::NotificationService;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr: SocketAddr = std::env::var("NOTIFICATION_GRPC_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:50055".to_owned())
        .parse()?;
    service_runtime::serve::<NotificationService>(addr).await
}
```

---

## ⚙️ Configuration & environnement d'exécution

### Variables propres à `notification` (sous-ensemble clé)

| Variable | Default | Description |
|---|---|---|
| `NOTIFICATION_HOT_SUBJECT_THRESHOLD` | `100` | Reactions / 5-min window to activate L2 Redis cross-batch collapse. |
| `NOTIFICATION_COLLAPSE_WINDOW_SECS` | `30` | Redis collapse window TTL. |
| `NOTIFICATION_COLLAPSE_FLUSH_INTERVAL_SECS` | `30` | CollapseFlushWorker poll cadence. |
| `NOTIFICATION_MAX_PER_SUBJECT_PER_HOUR` | `3` | Hourly cap per `(target, subject, kind)`. |
| `NOTIFICATION_UNREAD_CAP` | `99` | Max unread badge value (shows "99+"). |
| `NOTIFICATION_DEDUPE_TTL_SECS` | `86400` | Idempotency claim TTL — must exceed worst-case redelivery window. |
| `NOTIFICATION_MAX_PAGE_SIZE` | `50` | Feed page cap. |
| `NOTIFICATION_STREAM_BUFFER_SIZE` | `256` | `tokio::broadcast` capacity per streaming profile. |

### Variables d'infrastructure héritées

| Variable | Required | Default | Description |
|---|---|---|---|
| `SCYLLA_HOSTS` | **Yes** | — | ScyllaDB contact points. |
| `SCYLLA_KEYSPACE` | No | `notification` | Keyspace. |
| `REDIS_URL` | **Yes** | — | Redis connection URL. |
| `KAFKA_BROKERS` | **Yes** | — | Kafka brokers. |
| `NOTIFICATION_GRPC_ADDR` | No | `0.0.0.0:50055` | gRPC bind address. |

> Le réglage complet `SCYLLA_*` / `REDIS_*` / `KAFKA_*` vit dans les crates partagés storage/transport.

---

## 🚀 Déploiement, migrations & rollback

- **Migrations :** `001_keyspace.cql` → `002_notifications_by_profile.cql` →
  `003_notification_unread_counters.cql` sur `notification`, appliquées **avant** le premier boot.
- **Kafka :** topics pré-créés — `engagement.reactions` (key `{post}:{profile}`),
  `comment.created`/`comment.deleted` (key `comment_id`), `post.published` (key `post_id`).
- **Déploiement/Rollback :** `<TODO>` ; les workers sont des consommateurs at-least-once, la couche gRPC
  est sans état — sûr à déployer.

---

## 📈 Télémétrie, performance & métriques

- **Runtime :** Tokio multi-thread. Scylla 5.x+ RF=3 ; Redis 7.x+.

| Signal | Why it matters | Suggested alert |
|---|---|---|
| `notification_suppressed_total{reason="write_error"}` | feed durability | rate > 0.01 ⇒ critical |
| `notification_collapse_window_count` (vs `_written_total`) | flush worker health | flush == 0 while writes > 100 ⇒ warning |
| `kafka_consumer_group_lag{group=~"notification-.*"}` | ingest freshness | > 10 000 ⇒ warning |
| `notification_stream_lagged_total` | slow-client churn | spike ⇒ investigate buffer/clients |
| `notification_unread_cache_miss_total` | Redis L1 health | sustained ⇒ check Redis |

---

## 🛠️ Développement local

```bash
docker compose up -d scylla redis kafka       # repo-root compose
for f in crates/services/notification/migrations/*.cql; do cqlsh -f "$f"; done
cargo build -p notification && cargo clippy -p notification -- -D warnings
cargo test  -p notification
# Smoke: grpcurl -plaintext -d '{"profile_id":"018f..."}' 127.0.0.1:50055 notification.v1.NotificationService/ListNotifications
```

---

## 🚨 Dépannage & runbook

> Format : **symptôme → cause racine → mitigation.**

**1. `NTF-6001` : notifications de réaction silencieusement abandonnées pour un post.**
Cause racine : `ReactionNotificationWorker` lit `notification:pa:{post_id}` (peuplé par
`MentionNotificationWorker` sur `post.published`) avant d'écrire ; la clé est absente si le mention worker
a du lag ou si le post est antérieur au déploiement. Mitigation : vérifier le lag de
`notification-mention-consumer` ; rejouer avec `auto.offset.reset=earliest` ; pour une récupération
immédiate `SET notification:pa:{post_id} {author} EX 604800`.

**2. Badge de non-lus désynchronisé après Mark-All-Read.**
Cause racine : Redis évincé (sans persistance) ou `MarkAllRead` a reset Redis mais a échoué avant la ligne
de compteur Scylla. Mitigation : lire le compteur durable
(`SELECT unread_count FROM notification.notification_unread_counters WHERE target_profile_id = <uuid>`),
puis `DEL notification:unread:{profile_id}` — le prochain `GetUnreadCount` repeuple L1 depuis Scylla.

**3. CollapseFlushWorker ne flushe pas les fenêtres de célébrités.**
Cause racine : la tâche Tokio a paniqué, ou `zrangebyscore` échoue sur un problème de connexion Redis.
Mitigation : chercher dans les logs la panique du worker ; `redis-cli ping` ; inspecter
`ZRANGEBYSCORE notification:window_schedule -inf +inf WITHSCORES LIMIT 0 10`. Les fenêtres s'auto-expirent
(`collapse_window_secs + 10`), donc aucune double-écriture ne survient si on patiente.
