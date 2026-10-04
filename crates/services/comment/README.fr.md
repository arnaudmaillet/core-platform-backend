---
i18n:
  source: ./README.md
  source_sha256: eacc89d43357d5e4a942ef46727cf3fc8705d0118854ed3080289f0e69a8591c
  translated_at: 2026-10-04
  status: complete
---
> 🇫🇷 Traduction française — la version **anglaise** [`README.md`](./README.md) fait foi.
> En cas de divergence, l'anglais prime. Les contrats (codes d'erreur, variables
> d'environnement, topics Kafka, identifiants) sont volontairement laissés en anglais.

# `comment` — Moteur de commentaires à fil sur 1 niveau, avec support des GIF

> **Fiche service**
>
> | | |
> |---|---|
> | **Propriétaire** | `<TODO: équipe>` · `<TODO: #canal-slack>` |
> | **Astreinte / escalade** | `<TODO: rotation-astreinte>` → `<TODO: politique-escalade>` |
> | **Palier (Tier)** | **TIER-1** — contenu face utilisateur ; alimente les compteurs de commentaires d'engagement |
> | **Binaire déployable** | `crates/apps/comment-server` (crate bibliothèque : `crates/services/comment`) |
> | **Bases de données** | ScyllaDB keyspace `comment` (2 tables) |
> | **Asynchrone** | publie `comment.created` / `comment.deleted` · ne consomme rien |
> | **Appelants amont** | `<TODO: passerelle>` |
> | **Dépendances aval** | ScyllaDB, Kafka |
> | **SLO** | lecture de feed p99 **< 5 ms** · livraison at-least-once de `comment.*` |

---

## 🎯 Vue d'ensemble & rôle du service

`comment` est le propriétaire exclusif de l'état du cycle de vie des commentaires. Il impose un **fil
strict à 1 niveau** (style TikTok/Instagram), supporte de riches pièces jointes GIF, et émet des
événements Kafka qui pilotent les compteurs atomiques de commentaires du service `engagement` en temps
réel.

Le problème difficile qu'il résout est la **lecture paginée des fils sans aucun `ALLOW FILTERING`** : les
commentaires de premier niveau et les réponses doivent tous deux être des scans valides par préfixe de
clé de clustering sur la même partition. Il résout cela avec une **sentinelle nil-UUID** pour les parents
de premier niveau, de sorte que `WHERE post_id = ? AND parent_id = <nil>` soit un scan de préfixe propre.

**Objectifs fondamentaux :** lectures paginées sub-5 ms P99 (quorum local Scylla, localité TWCS) ; zéro
`ALLOW FILTERING` ; livraison at-least-once de `comment.created`/`comment.deleted`. **Hors périmètre :**
les likes/réactions sur commentaires (appartiennent à `engagement`), les données post/profil.

---

## 📐 Architecture & concepts

Hexagonal / DDD, bus CQRS, store flat-tree ScyllaDB, événements Kafka.

```
gRPC CommentService ─► CommandBus ─► CreateComment ─► Comment::create() ─► repo.insert ─► comment.created
                    │             └─► DeleteComment ─► has_active_replies? ─► Tombstone | Purge ─► comment.deleted
                    └─► QueryBus  ─► GetComment (comments, point read, LCS)
                                  ─► ListTopLevel / ListReplies (comments_by_post)
```

**Flat-tree wide-column ScyllaDB :**

| Table | Partition key | Clustering keys | Purpose |
|---|---|---|---|
| `comment.comments` | `comment_id` | — | source-of-truth point reads & mutations (LCS) |
| `comment.comments_by_post` | `post_id` | `parent_id, created_at DESC, comment_id` | feed pagination, no ALLOW FILTERING (TWCS) |

**Sentinelle nil-UUID :** les commentaires de premier niveau stockent `parent_id = 0000…0000`
(lexicographiquement le plus petit), faisant du scan de premier niveau un préfixe de clustering valide ;
les réponses utilisent leur véritable `comment_id` parent.

**Stratégie de suppression :** `has_active_replies` ? **Tombstone** (body+gif à null, garde la ligne pour
que le fil reste navigable) : **Purge** (DELETE physique des deux tables). Les deux chemins émettent
`comment.deleted`.

> **Invariants** (imposés à la frontière de l'agrégat) : texte ≤ 500 ; doit avoir texte OU gif
> (`EmptyContent`) ; métadonnées GIF complètes (`IncompleteGifMetadata`) ; profondeur de nesting max 1
> niveau (`NestingDepthExceeded`) ; impossible de répondre à un parent supprimé (`ParentDeleted`) ; seul
> l'auteur peut supprimer (`AuthorMismatch`) ; pas de re-suppression (`CommentAlreadyDeleted`).

---

## 📊 Objectifs de niveau de service (SLO)

| SLI | Objectif | Fenêtre | Mesuré par |
|---|---|---|---|
| Lecture de feed p99 (`ListTopLevel`/`ListReplies`) | **< 5 ms** | 1 h | histogramme de lecture Scylla |
| `CreateComment` p99 | `< <TODO> ms` | 1 h | histogramme gRPC |
| Livraison d'événements | at-least-once `comment.*` | — | taux de succès de publication |
| Durabilité | aucun commentaire acquitté perdu | — | `LocalQuorum` Scylla |

**Budget d'erreur :** `<TODO>`. **En cas d'épuisement :** `<TODO>`.

---

## 🔗 Dépendances & rayon d'impact (blast radius)

**Aval :**

| Dependency | Purpose | If down → | Degradation |
|---|---|---|---|
| ScyllaDB (`comment`) | store durable | lectures + écritures échouent | **Dur** — `CMT-…/Storage` |
| Kafka | émission de `comment.*` | les compteurs de commentaires d'engagement retardent | **Souple** — les commentaires persistent quand même |

**Amont (rayon d'impact) :**

| Caller | Uses | Impact si `comment` est indisponible |
|---|---|---|
| `engagement` | consomme `comment.created`/`deleted` | les comptes de commentaires cessent de se mettre à jour |
| `notification` | consomme `comment.created` | les notifications de commentaire s'arrêtent |

> **Chemin critique ?** Partiellement — l'écriture/lecture de commentaires est face utilisateur ; la
> propagation des compteurs/notifications est asynchrone et à terme cohérente.

---

## 🔌 Interfaces publiques & contrat d'API

### gRPC — `comment.v1.CommentService`

```protobuf
service CommentService {
  rpc CreateComment (CreateCommentRequest) returns (CreateCommentResponse);
  rpc DeleteComment (DeleteCommentRequest) returns (CommandResponse);
  rpc GetComment    (GetCommentRequest)    returns (CommentView);
  rpc ListTopLevel  (ListTopLevelRequest)  returns (ListCommentsResponse);
  rpc ListReplies   (ListRepliesRequest)   returns (ListCommentsResponse);
}
```

> **Contrat de sérialisation :** le `parent_id` d'une réponse doit toujours être le `comment_id` de
> **premier niveau** (jamais une autre réponse) — le flat-tree autorise exactement un niveau de nesting.
> Les curseurs de pagination sont `created_at DESC` ; les inserts après le curseur ne sont jamais renvoyés
> (pages stables de façon monotone).

**Lectures selon le lecteur.** `GetComment` / `ListTopLevel` / `ListReplies` prennent le lecteur du
transport (`edge::viewer`). Pour tout appelant hors mesh, un **read gate** décide avec un `GetPost`
(post) et un `CheckAccess` (social-graph) par appel (auteur du post + chaque auteur de commentaire de
la page) :
- le **post** doit être lisible : publié et non retiré par la modération (son auteur le voit quoi
  qu'il arrive), et son auteur `VISIBLE` pour le lecteur (pas un auteur privé qu'il ne suit pas, pas
  de blocage dans un sens ou l'autre, pas masqué). Sinon `GetComment` renvoie `CMT-1001` et les
  listes sont vides ;
- les commentaires d'un auteur **masqué** pour le lecteur (blocage dans un sens ou l'autre, profil
  masqué) sont retirés ; le commentaire d'un profil privé sur un post lisible reste. Une page peut
  revenir plus courte tandis que `next_token` reste valide.

Le gate échoue fermé : une panne renvoie `CMT-5001` (`UNAVAILABLE`), jamais les commentaires.

**Les écritures sont aussi contrôlées** (#656) : `CreateComment` exige un post que le commentateur peut lire (sinon `CMT-1004`, `NOT_FOUND`) et dont l'auteur accepte ses commentaires — `CheckInteraction(COMMENT)` de social-graph sur les réglages d'interaction de l'auteur (tout le monde / abonnés / mutuels / personne) et les blocages (sinon `CMT-1005`, `PERMISSION_DENIED`). Commenter son propre post est toujours permis ; une panne refuse l'écriture (`CMT-5001`).

### Contrat d'erreur (`CMT-xxxx`)

| Code | Error | HTTP |
|---|---|---|
| CMT-1001/1002/1003 | not found / already deleted / author mismatch | 404 / 409 / 403 |
| CMT-2001/2002/2003 | nesting depth / parent not found / parent deleted | 422 / 404 / 422 |
| CMT-3001/3002 | empty content / incomplete GIF metadata | 422 |
| CMT-1004 | `PostNotFound` (absent, ou non lisible par le commentateur) | 404 → `NOT_FOUND` |
| CMT-1005 | `CommentsRestricted` (l'auteur du post n'accepte pas les commentaires de ce profil) | 403 → `PERMISSION_DENIED` |
| CMT-5001 | `AccessCheckUnavailable` (read gate down; retryable) | 503 → `UNAVAILABLE` |
| CMT-4001 | Kafka publish failed | 500 |
| CMT-9001..9004 | invalid ids / domain violation | 422 |

---

## 📨 Contrat événementiel & asynchrone

**Publie :**

| Topic | Trigger | Key | Payload | Consumers |
|---|---|---|---|---|
| `comment.created` | `CreateComment` success | `comment_id` | `comment_id, post_id, author_id, parent_id, created_at_ms` | `engagement` (incr), `notification` |
| `comment.deleted` | `DeleteComment` (either strategy) | `comment_id` | `comment_id, post_id, author_id, deleted_at_ms` | `engagement` (decr) |

**Consomme :** rien.

> **Contrat d'exécution :** les événements sont publiés après l'écriture durable. Les
> `engagement-comment-consumer` et `notification-comment-consumer` aval gèrent leur propre traitement
> at-least-once sous `run_consumer` ; engagement peut reconstruire son compteur depuis sa propre table
> Scylla, donc un échec de publication transitoire est récupérable.

---

## 🌩️ Modes de défaillance & dégradation

| Failure | Symptom | Service behavior | Operator action |
|---|---|---|---|
| Échec d'insert ScyllaDB | `CommentError::Storage` à l'appelant | réessayer avec le même `comment_id` (INSERT est LWW) | vérifier Scylla ; le retry est sûr |
| Échec de publication Kafka | `CMT-4001` ; les compteurs d'engagement retardent | **Souple** — commentaire persisté ; cohérence à terme | vérifier Kafka ; engagement reconstruit depuis son ledger |
| Lecture de feed juste après un soft-delete montre l'ancien contenu | réplica périmé en `LocalOne` | cohérence à terme attendue (convergence sub-ms) | réessayer / passer par `GetComment` |

**Backpressure & limites.** Les listes de feed sont paginées par curseur ; les deux tables sont mises à
jour dans le même handler (fenêtre sub-ms entre le store par point et l'index de feed).

---

## 📦 Intégration & utilisation

```toml
[dependencies]
comment = { path = "crates/services/comment" }
```

Bibliothèque uniquement. Implémente [`service_runtime::Service`](../../platform/service-runtime/README.md)
sous le nom `comment::service::CommentService` — `build` câble le repository ScyllaDB et le publisher
Kafka durable ; `register` ajoute les services gRPC + réflexion ; `health_probes` vérifie Scylla.

### Bootstrap (`crates/apps/comment-server`)

```rust
use std::net::SocketAddr;
use comment::service::CommentService;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr: SocketAddr = std::env::var("COMMENT_GRPC_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:50057".to_owned())
        .parse()?;
    service_runtime::serve::<CommentService>(addr).await
}
```

---

## ⚙️ Configuration & environnement d'exécution

### Variables d'infrastructure héritées

| Variable | Required | Default | Description |
|---|---|---|---|
| `SCYLLA_NODES` | **Yes** | — | ScyllaDB contact points (`host:port`). |
| `SCYLLA_KEYSPACE` | No | `comment` | Keyspace (see migrations). |
| `KAFKA_BOOTSTRAP_SERVERS` | **Yes** | — | Kafka brokers. |
| `KAFKA_SECURITY_PROTOCOL` / `KAFKA_SASL_*` | No | `PLAINTEXT` | Auth for managed Kafka. |
| `COMMENT_GRPC_ADDR` | No | `0.0.0.0:50057` | gRPC bind address. |
| `COMMENT_POST_GRPC_ENDPOINT` / `COMMENT_SOCIAL_GRAPH_GRPC_ENDPOINT` | **Oui** (prod) | `http://localhost:50056` / `:50053` | Endpoints mesh du read gate (`GetPost`, `CheckAccess`). Sans eux, les lectures hors mesh échouent fermé (`CMT-5001`). |
| `COMMENT_GATE_RPC_TIMEOUT_MS` / `COMMENT_GATE_CONNECT_TIMEOUT_MS` | Non | `1000` / `1000` | Délais de ces appels. |

> Le réglage complet `SCYLLA_*` / `KAFKA_*` vit dans les crates partagés storage/transport.

### Features de compilation
- `build.rs` compile `proto/comment/v1/*.proto` et émet le descriptor set de réflexion.

---

## 🚀 Déploiement, migrations & rollback

- **Migrations :** `0001_create_keyspace.cql` → `0002_create_comments_table.cql` →
  `0003_create_comments_by_post_table.cql` sur `comment`, appliquées **avant** le premier démarrage.
- **Déploiement/Rollback :** `<TODO>` ; service sans état, sûr à déployer.
- **Piège de schéma :** la sentinelle nil-UUID et l'ordre de clustering de `comments_by_post` sont un
  contrat de lecture — ne pas les changer une fois que des données existent.

---

## 📈 Télémétrie, performance & métriques

- **Runtime :** Tokio multi-thread. Spans clés : `comment.create`, `comment.delete`, `scylla.*`,
  `kafka.publish`.

| Signal | Why it matters | Suggested alert |
|---|---|---|
| `comment_event_publish_errors_total` | engagement counters diverge | rate > 0 for 5m ⇒ high |
| `scylla_execution_errors_total{service="comment"}` | store health | rate > 0.1 for 2m ⇒ high |
| `CreateComment` p99 | write latency | > 500 ms ⇒ medium |
| `FAILED_PRECONDITION` rate | possible client abuse | spike > baseline ⇒ low |

---

## 🛠️ Développement local

```bash
cargo build -p comment && cargo clippy -p comment -- -D warnings
cargo test  -p comment
docker compose up -d scylla kafka             # repo-root compose
for f in crates/services/comment/migrations/*.cql; do cqlsh -f "$f"; done
```

---

## 🚨 Dépannage & runbook

> Format : **symptôme → cause racine → mitigation.**

**1. Les compteurs de commentaires d'engagement sont périmés après la création d'un commentaire.**
Cause racine : l'événement `comment.created` a été publié, mais `engagement-comment-consumer` est en
retard ou arrêté. Mitigation : vérifier le lag de ce consumer-group ; redémarrer le consommateur de
commentaires d'engagement. Engagement reconstruit son compteur depuis sa propre table Scylla
`post_interaction_counters` au redémarrage — aucune réconciliation manuelle nécessaire.

**2. `CMT-2001 NestingDepthExceeded` pour une réponse qui semble valide.**
Cause racine : le `parent_id` de la requête pointe vers une *réponse* (parent non-nil), c.-à-d. une
réponse-à-réponse. Mitigation : les clients doivent toujours utiliser le `comment_id` de premier niveau
d'origine comme `parent_id` ; vérifier que le `parent_id` de la cible dans `comment.comments` est le nil
UUID.

**3. `comments_by_post` montre du contenu supprimé juste après un soft-delete.**
Cause racine : le read-your-writes n'est pas garanti en `LocalOne` ; le profil Fast peut toucher un
réplica périmé. Mitigation : cohérence à terme attendue (converge en ms). Pour les lectures sensibles à
la cohérence, réessayer ou passer par `GetComment`.
