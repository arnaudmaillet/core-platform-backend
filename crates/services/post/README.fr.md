---
i18n:
  source: ./README.md
  source_sha256: a908d8eb18cbca5a13959683558e5c10d20e0854f561e12f15e78bf0f4f5fb0a
  translated_at: 2026-10-06
  status: complete
---
> 🇫🇷 Traduction française — la version **anglaise** [`README.md`](./README.md) fait foi.
> En cas de divergence, l'anglais prime. Les contrats (codes d'erreur, variables
> d'environnement, topics Kafka, identifiants) sont volontairement laissés en anglais.

# `post` — La source de vérité canonique du contenu créé par les utilisateurs

> **Fiche service**
>
> | | |
> |---|---|
> | **Propriétaire** | `<TODO: équipe>` · `<TODO: #canal-slack>` |
> | **Astreinte / escalade** | `<TODO: rotation-astreinte>` → `<TODO: politique-escalade>` |
> | **Palier (Tier)** | **TIER-0** — le chemin de publication du contenu ; feeds et découverte dérivent de ses événements |
> | **Binaire déployable** | `crates/apps/post-server` (crate bibliothèque : `crates/services/post`) |
> | **Bases de données** | ScyllaDB keyspace `post` (2 tables) |
> | **Asynchrone** | publie `post.v1.events` (unifié) + `post.published` / `post.updated` / `post.deleted` (legacy) · consomme `profile.v1.events` (dénormalisation du palier auteur) + `moderation.v1.events` (restriction de lecture) |
> | **Appelants amont** | `<TODO: passerelle>` |
> | **Dépendances aval** | ScyllaDB, Kafka |
> | **SLO** | `<TODO>` dispo · `GetPost` p99 `<TODO>` · publication p99 `<TODO>` |

---

## 🎯 Vue d'ensemble & rôle du service

`post` est le registre canonique des publications créées par les utilisateurs, sur plusieurs formats
média (Carousel, MainVideo, TextOnly). Il impose les invariants de contenu, gère un cycle de vie
`Draft → Published → Deleted`, et émet un événement Kafka à chaque transition d'état. C'est le
**déclencheur de fan-out** pour le reste de la plateforme — timeline, geo-discovery et notification
construisent tous leurs projections à partir des événements `post.*`.

Le problème difficile qu'il résout est d'**être une source d'événements propre** : chaque post
publié/mis à jour/supprimé doit produire exactement un événement durable et correctement clé auquel les
matérialiseurs aval peuvent se fier, tout en gardant le chemin d'écriture en O(1). Il résout cela avec
un schéma wide-column à deux tables (store par point + index créateur) et une étape de publication
conditionnée à une écriture durable réussie. Il n'a **aucune connaissance** des feeds, timelines ou
graphes sociaux.

**Objectifs fondamentaux :** les invariants de contenu sont non négociables (cardinalité de carousel,
plafonds vidéo, allowlist MIME) ; le cycle de vie est unidirectionnel (`Draft→Published` irréversible,
soft-delete uniquement) ; chaque transition émet son événement.

---

## 📐 Architecture & concepts

Hexagonal / DDD, bus CQRS, store durable ScyllaDB, événements Kafka.

```
gRPC PostService ─► CQRS bus ─► Create/Publish/Update/Delete handlers ─► ScyllaPostRepository (dual-write)
                            └─► Get/ListByProfile handlers
                                            │
                  KafkaEventPublisher ◄─────┘  ─► post.published / post.updated / post.deleted
```

**Conception du stockage — schéma wide-column à deux tables :**
- `post.posts` — store canonique, PK `post_id`, lookups par point O(1).
- `post.posts_by_profile` — index de feed créateur, PK `profile_id`, CK `created_at DESC, post_id ASC`.

Chaque écriture **dual-write les deux tables séquentiellement**. Les pièces jointes sont stockées en JSON
validé (une colonne `text`) pour éviter la complexité de migration des UDT ScyllaDB.

> **Invariants** (et où ils sont imposés, dans la FSM de l'agrégat `Post`) : Carousel 2–10 items, vidéos
> de carousel ≤ 15 s, les items vidéo exigent `thumbnail_url` ; MainVideo = une seule vidéo + thumbnail ;
> TextOnly = zéro pièce jointe ; threading `parent_id`/`root_id` tous deux présents ou tous deux absents ;
> `profile_id` sur Publish/Update/Delete doit correspondre à l'auteur.

---

## 📊 Objectifs de niveau de service (SLO)

| SLI | Objectif | Fenêtre | Mesuré par |
|---|---|---|---|
| Disponibilité (non-`UNAVAILABLE`) | `<TODO>` | 30 j | métriques de statut gRPC |
| `GetPost` p99 (lecture par point) | `< <TODO> ms` | 1 h | histogramme de lecture Scylla |
| `PublishPost` p99 (durable + événement) | `< <TODO> ms` | 1 h | histogramme du handler |
| Complétude d'émission d'événements | 1 événement par transition committée | — | taux de succès de publication |

**Budget d'erreur :** `<TODO>`. **En cas d'épuisement :** `<TODO>`.

---

## 🔗 Dépendances & rayon d'impact (blast radius)

**Aval :**

| Dependency | Purpose | If down → | Degradation |
|---|---|---|---|
| ScyllaDB (`post`) | store durable | lectures + écritures échouent | **Dur** — `UNAVAILABLE` |
| Kafka | émission d'événements | les projections aval stagnent | **Souple** — les écritures committent ; voir note |

**Amont (rayon d'impact — les événements `post.*` alimentent une grande partie de la flotte de lecture) :**

| Caller | Uses | Impact visible utilisateur si `post` est indisponible |
|---|---|---|
| `timeline` | `post.published` / `post.deleted` | aucun nouveau post n'entre dans les fils d'accueil |
| `geo-discovery` | `post.published` | les nouveaux posts n'apparaissent pas sur la carte |
| `notification` | `post.published` (mentions) | les notifications de mention s'arrêtent |

> **Chemin critique ?** **Oui** pour la publication ; le chemin d'écriture est face utilisateur et
> l'événement est le déclencheur amont de toute la flotte côté lecture.

---

## 🔌 Interfaces publiques & contrat d'API

### gRPC — `post.v1.PostService`

```protobuf
service PostService {
  rpc CreatePost (CreatePostRequest) returns (CreatePostResponse);          // draft; PostId pre-generated at boundary
  rpc PublishPost (PublishPostRequest) returns (CommandResponse);           // Draft→Published; emits post.published
  rpc UpdatePost (UpdatePostRequest) returns (CommandResponse);             // emits post.updated
  rpc DeletePost (DeletePostRequest) returns (CommandResponse);             // soft-delete; emits post.deleted
  rpc RestorePost (RestorePostRequest) returns (CommandResponse);           // #663 within 30 days: published again (re-emits post.published at its original time) or a draft
  rpc ListRecentlyDeleted (ListRecentlyDeletedRequest) returns (ListRecentlyDeletedResponse); // #663 the author's restorable posts, newest deletion first
  rpc GetPost (GetPostRequest) returns (PostView);                          // point lookup; viewer-aware
  rpc ListPostsByProfile (ListPostsByProfileRequest) returns (ListPostsByProfileResponse); // cursor-paginated; viewer-aware
  rpc BatchGetLikeVisibility (BatchGetLikeVisibilityRequest) returns (BatchGetLikeVisibilityResponse); // #809 MESH ONLY: author + like counts hidden, ≤ 200 posts
}
// CreatePostRequest / PostView portent une localisation GeoPoint optionnelle :
message GeoPoint { double lat = 1; double lng = 2; }  // WGS-84 ; absent → post non géo-indexé
```

**Permissions de réutilisation (#669).** Un post peut fixer ses propres `allow_remix` /
`allow_sound_reuse` ; sinon il suit le réglage par défaut du profil de son auteur (projeté depuis
`ProfileInteractionSettingsChanged` dans `post.author_reuse_settings` ; désactivé par défaut pour les
ados). Un son original appartient au post qui l'a créé (`post.audio_origins`, premier écrit gagnant).
`CreatePost` avec le son de quelqu'un d'autre — quel que soit le nom que lui donne la requête — est refusé
(`PST-1008`, 403) sauf si ce post, ou à défaut son auteur, autorise la réutilisation ; ses propres sons et les
sons inconnus (pistes de bibliothèque) sont libres. Le remix n'a pas encore de surface serveur : le drapeau
est stocké pour le client.

**Téléchargements et compteurs de likes (#809).** Les réglages `allow_downloads` / `show_like_counts` de
l'auteur sont projetés depuis le même événement dans `post.author_reuse_settings` (migration 0013 ; pas
de ligne ou NULL ⇒ autorisé / affiché ; les ados n'autorisent pas les téléchargements par défaut).
`GetPost` marque le post pour tout autre lecteur que l'auteur — le mesh compris, si bien qu'un service qui
sert des clients retient aussi — avec `downloads_disabled` (l'app ne propose pas d'enregistrer ; les
rendus média nécessaires pour le voir restent servis) et `like_counts_hidden`. Les services qui servent
des compteurs (engagement, counter) retiennent les likes pour tout autre que l'auteur grâce à
`BatchGetLikeVisibility`, réservé au mesh (l'auteur de chaque post et s'il masque ses compteurs de likes ;
posts inconnus absents).

**Mentions (#656).** Une légende mentionne un profil par un lien `[@handle](profile:<uuid>)`. `CreatePost` et
`UpdatePost` interrogent social-graph `CheckInteraction(MENTION)` pour chaque profil mentionné (une fois
chacun, soi-même mis à part) : un profil qui n'accepte pas les mentions de l'auteur (son audience « qui peut
me mentionner », ou un blocage dans un sens ou l'autre) fait refuser l'écriture (`PST-1009`, 403), rien
n'est enregistré. Au plus 20 profils par légende. Une panne fait échouer l'écriture (réessayable) ; une
légende sans mention n'interroge rien.

**Récemment supprimés (#663).** Une suppression est une pierre tombale : le post est indexé dans
`post.deleted_by_profile` (lignes expirées au bout de 30 jours) et `ListRecentlyDeleted` montre à l'auteur ses
posts restaurables. `RestorePost` en ramène un dans les 30 jours tel qu'il était — publié (réannoncé sur
`post.published` à sa date de publication d'origine, pour que fils et recherche le reprennent ; un post
retiré ou limité par la modération n'est restauré que pour son auteur, jamais réannoncé, pour qu'un retrait
survive à supprimer → restaurer) ou brouillon — et le retire de la liste. geo-discovery écarte définitivement
un post supprimé : l'épingle d'un post restauré ne revient pas sur la carte. Les deux sont `authenticated` sur l'edge, liés à `profile_id`.

**Lectures selon le lecteur.** Le lecteur vient du transport (`edge::viewer`), jamais d'un champ de
requête. Un brouillon, un post supprimé ou un post **retiré** par la modération n'est visible que de
son auteur (tout profil des `pids` du jeton) et des appelants du mesh ; tout autre lecteur reçoit
`PST-1001` de `GetPost` et ne le voit pas dans `ListPostsByProfile` (filtré par page : une page peut
revenir plus courte tandis que `next_token` reste valide). `PostView.moderation` /
`PostSummary.moderation` indiquent à l'auteur ce qui est en vigueur ; les posts `LIMITED` restent
lisibles (la découverte l'applique). Un post **`AGE_GATED`** est introuvable pour un lecteur non autorisé au
contenu mature — client anonyme, invité, ou titulaire de 13 à 17 ans (l'`age` du jeton) — et absent de ses
listes ; son auteur et les adultes le lisent. Vient ensuite l'**audience de l'auteur**,
via le `CheckAccess` (mesh uniquement) de social-graph : un auteur privé que le lecteur ne suit pas,
un blocage dans un sens ou l'autre, ou un auteur masqué → `PST-1001` / une liste vide. L'auteur et
les appelants du mesh sautent ce contrôle ; tout autre lecteur (anonyme compris) en dépend, et une
panne échoue fermé avec `PST-5001` (`UNAVAILABLE`), jamais en servant le post.

**Fenêtre d'historique des posts (#664).** Un auteur peut ne montrer aux visiteurs que ses posts récents
(6 mois, 1 mois, 3 jours). Pour tout client autre que l'auteur, `ListPostsByProfile` s'arrête au premier post
plus ancien (la liste va du plus récent au plus ancien ; pas de jeton suivant) et `GetPost` répond `PST-1001`
pour un tel post ; l'auteur et les appelants du mesh voient tous les posts. Un `GetPost` du mesh marque
un tel post `outside_window`, pour qu'un service qui sert des clients le retienne aussi (comment masque ses
commentaires), et donne à tout post sous une fenêtre son `visible_until_ms` (`created_at` + la fenêtre :
search le retire des résultats à partir de là). Rien n'est supprimé. La fenêtre
vient du `ProfileTabSettingsChanged` de profile, projeté dans `post.author_post_windows` par le consommateur
des réglages d'auteur ci-dessous.

**Partage de la localisation (#657). `PostView.location` est ce que l'auteur partage avec le
lecteur : le point du post pour l'auteur ; pour tout autre (mesh compris), le point par défaut, le
centre de sa cellule H3 R5 (~87 km², la bande « ville » de la carte) au niveau ville, et rien en
mode fantôme. Le réglage vient du `ProfileLocationSettingsChanged` de profile, projeté dans
`post.author_location_settings`, et s'applique aux posts créés avant son changement. Une erreur du
magasin fait échouer la lecture plutôt que de montrer le point. Son **audience** (abonnés / mutuels,
#657) ne montre la localisation qu'à un lecteur qui suit l'auteur / lui est mutuel — ce que dit le même
appel `CheckAccess` qui décide de la visibilité du post (`follows` / `mutual`). Tout autre lecteur, un
lecteur anonyme et le mesh (qui ne lit pour personne en particulier) reçoivent le post sans sa
localisation. L'export RGPD lit les posts du titulaire via le mesh avec `GetPostRequest.as_author_id`
égal au titulaire : son propre point, quel que soit son partage — pris en compte depuis le mesh
seulement et pour l'auteur du post seulement (le champ d'un appelant edge est écarté avant la requête).

### Contrat d'erreur (`PST-xxxx`)

| Code | Variant | HTTP |
|---|---|---|
| PST-1001 | `PostNotFound` | 404 |
| PST-1002/1003 | `PostAlreadyPublished` / `PostAlreadyDeleted` | 409 |
| PST-1004 | `NotDraft` | 422 |
| PST-1005 | `AuthorMismatch` | 403 |
| PST-1006 | `PostNotDeleted` (restauration d'un post non supprimé) | 409 |
| PST-1007 | `RestoreWindowExpired` (supprimé il y a plus de 30 jours) | 410 |
| PST-1008 | `SoundReuseNotAllowed` (le créateur du son n'autorise pas sa réutilisation) | 403 |
| PST-1009 | `MentionNotAllowed` (un profil mentionné n'accepte pas les mentions de l'auteur, #656) | 403 |
| PST-2001..2003 | carousel cardinality / video length | 422 |
| PST-3001..3004 | thumbnail / MIME / CDN URL / dimensions | 422 |
| PST-9001/9002 | invalid post/profile ID | 422 |
| PST-9003 | `AttachmentsCorrupted` (JSON deser) | 500 |
| PST-9004 | `DomainViolation` | 422 |
| PST-5001 | `AccessCheckUnavailable` (social-graph `CheckAccess` did not answer; retryable) | 503 → `UNAVAILABLE` |

---

## 📨 Contrat événementiel & asynchrone

> Les topics Kafka sont une API. Les matérialiseurs aval (timeline, geo-discovery, notification) se fient
> à l'`author_tier` et aux coordonnées transportés ici — un changement de schéma les casse comme un
> changement de proto.

**Publie :**

| Topic | Déclencheur | Clé | Consommateurs |
|---|---|---|---|
| `post.v1.events` | chaque événement de cycle de vie (`PostPublished` / `PostUpdated` / `PostDeleted`) | `post_id` | `search` (indexation des posts) |
| `post.published` | `PublishPost` success — porte le `author_tier` dénormalisé, plus `caption` / `thumbnail_url` / `lat`/`lng` optionnels pour la projection geo | `post_id` | `timeline`, `geo-discovery`, `notification` |
| `post.updated` | `UpdatePost` success | `post_id` | `<TODO>` |
| `post.deleted` | `DeletePost` success | `post_id` | `timeline`, `geo-discovery` |

> **Deux styles d'émission, par conception.** `post.v1.events` est le flux unifié et versionné (la convention de la flotte, comme `moderation.v1.events` / `profile.v1.events`) : le `DomainEvent` entier tagué en interne, clé `post_id`. Les topics legacy par-type (`post.published` / `.updated` / `.deleted`, charges utiles brutes) sont conservés pour leurs consommateurs existants (`timeline` / `geo-discovery` / `notification`) ; chaque événement est publié sur **les deux**. Migrer ces consommateurs vers `post.v1.events` et retirer les topics legacy est un nettoyage futur.

**Consomme :**

| Topic | Consumer group | Purpose | On poison/exhaustion |
|---|---|---|---|
| `profile.v1.events` | `post-author-tier` | dénormalise `ProfileTierChanged` dans la projection `author_tiers` (`profile_id → tier`) ; lue sur le chemin de publication pour estampiller `author_tier` sur les posts publiés. Les autres types committent en no-op | DLQ `profile.v1.events.dlq` |
| `profile.v1.events` | `post-author-location` | projette `ProfileLocationSettingsChanged` dans `author_location_settings` (`profile_id → ghost, city`), que `GetPost` applique à la localisation montrée à tout autre que l'auteur, et `ProfileTabSettingsChanged` dans `author_post_windows` (`profile_id → window_days`), que les deux lectures appliquent. Démarre au plus ancien offset (un profil adolescent est créé en mode fantôme). Les autres types committent en no-op | DLQ `profile.v1.events.dlq` |
| `moderation.v1.events` | `post-moderation` | enregistre `enforcement_applied` / `enforcement_reversed` sur un **post** comme sa restriction de modération (`remove_content` → Removed, `visibility_limit` → Limited, `age_gate` → AgeGated ; réversion → None), gardé par l'`EnforcementVersion` par sujet de moderation pour que la redélivrance converge. Les autres entités, les actions au niveau de l'acteur et les autres types committent en no-op | DLQ `moderation.v1.events.dlq` |

> **Contrat d'exécution :** l'événement est publié après le dual-write durable. Les consommateurs aval
> gèrent leur propre traitement at-least-once sous `run_consumer` ; tous traitent `post.*` comme
> idempotent par `post_id`.

---

## 🌩️ Modes de défaillance & dégradation

| Failure | Symptom | Service behavior | Operator action |
|---|---|---|---|
| ScyllaDB indisponible | toutes les RPC échouent | **Dur** — `UNAVAILABLE` ; rien d'acquitté | vérifier le cluster Scylla |
| Dual-write partiel (posts ok, index échoue) | post lisible par id, absent du feed créateur | l'écriture renvoie une erreur ; le client réessaie (idempotent par `post_id`) | réessayer ; réconcilier l'index au besoin |
| Échec de publication Kafka après commit | post durable, projections aval le manquent | **Souple** — le contenu existe mais feeds/carte/notifications retardent | ré-émettre l'événement ou s'appuyer sur le backfill aval |
| `AttachmentsCorrupted` en lecture | `PST-9003` | JSON invalide dans la colonne `text` | inspecter la ligne ; incident de qualité de données |

**Backpressure & limites.** `ListPostsByProfile` est paginée par curseur. Les inserts sont idempotents
sur `post_id` (last-write-wins), donc les retries transitoires sont sûrs.

---

## 📦 Intégration & utilisation

```toml
[dependencies]
post = { path = "crates/services/post" }
```

Bibliothèque uniquement. Implémente [`service_runtime::Service`](../../platform/service-runtime/README.md)
sous le nom `post::service::PostService` — `build` câble le repository ScyllaDB et le publisher Kafka
durable ; `register` ajoute les services gRPC + réflexion ; `health_probes` vérifie Scylla.

### Bootstrap (`crates/apps/post-server`)

```rust
use std::net::SocketAddr;
use post::service::PostService;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr: SocketAddr = std::env::var("POST_GRPC_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:50056".to_owned())
        .parse()?;
    service_runtime::serve::<PostService>(addr).await
}
```

---

## ⚙️ Configuration & environnement d'exécution

### Variables d'infrastructure héritées

| Variable | Required | Default | Description |
|---|---|---|---|
| `SCYLLA_CONTACT_POINTS` / `SCYLLA_LOCAL_DC` | **Yes** | — | ScyllaDB contact points + DC for token-aware routing. |
| `SCYLLA_KEYSPACE` | No | `post` | Keyspace (NTS RF=3, LZ4). |
| `KAFKA_BROKERS` | **Yes** | — | Kafka brokers for `post.*`. |
| `POST_GRPC_ADDR` | No | `0.0.0.0:50056` | gRPC bind address. |
| `POST_SOCIAL_GRAPH_GRPC_ENDPOINT` | **Oui** (prod) | `http://localhost:50053` | Endpoint mesh de social-graph pour le contrôle d'audience (`CheckAccess`). Les lectures par tout autre que l'auteur échouent fermé (`PST-5001`, `UNAVAILABLE`) s'il ne répond pas. |
| `POST_SOCIAL_GRAPH_RPC_TIMEOUT_MS` / `_CONNECT_TIMEOUT_MS` | Non | `1000` / `1000` | Délais de cet appel. |

> Le réglage complet `SCYLLA_*` / `KAFKA_*` vit dans les crates partagés storage/transport.

### Features de compilation
- `build.rs` compile `proto/post/v1/*.proto` et émet le descriptor set de réflexion.

---

## 🚀 Déploiement, migrations & rollback

- **Migrations :** `migrations/0001_create_keyspace.cql` → `0002_create_posts_table.cql` →
  `0003_create_posts_by_profile_table.cql` → `0004`–`0007` (audio, paliers auteur, géo, colonnes de
  modération ; `ALTER` en ligne) sur `post`, appliquées **avant** le premier démarrage.
- **Déploiement/Rollback :** `<TODO>` ; service sans état, sûr à déployer.
- **Piège de schéma :** l'ordre de clustering de l'index créateur (`created_at DESC, post_id ASC`) est un
  contrat de lecture — ne pas le changer une fois que des données existent.

---

## 📈 Télémétrie, performance & métriques

- **Runtime :** Tokio multi-thread. Subscriber global tracing/OTel installé avant `serve`.

| Signal | Why it matters | Suggested alert |
|---|---|---|
| `PublishPost` p99 | publish-path latency | > SLO ⇒ page |
| `post.*` publish failure rate | downstream feed/map drift | sustained ⇒ check Kafka |
| Scylla write errors | content durability | any spike ⇒ check cluster |
| `PST-9003 AttachmentsCorrupted` count | data-quality | > 0 ⇒ investigate |

---

## 🛠️ Développement local

```bash
cargo build -p post && cargo clippy -p post --all-targets
cargo test  -p post
docker compose up -d scylla kafka             # repo-root compose
for f in crates/services/post/migrations/*.cql; do cqlsh -f "$f"; done
```

---

## 🚨 Dépannage & runbook

> Format : **symptôme → cause racine → mitigation.**

**1. `PST-1004 NotDraft` sur `PublishPost`.**
Cause racine : le post est déjà `Published` ou `Deleted` — le cycle de vie est unidirectionnel.
Mitigation : `GetPost` pour confirmer le statut ; la publication est irréversible et à un seul coup par
conception.

**2. Un post publié est absent du feed créateur mais lisible par id.**
Cause racine : le dual-write a partiellement échoué (`posts` ok, `posts_by_profile` non). Mitigation :
ré-émettre l'écriture (idempotente sur `post_id`) ; si cela persiste, réconcilier l'index depuis
`post.posts`.

**3. Un nouveau post n'atteint jamais les timelines/la carte.**
Cause racine : le post a été committé mais l'événement `post.published` n'a pas pu être publié, ou un
consommateur aval est en retard. Mitigation : vérifier la santé de Kafka et les consumer-groups aval ;
ré-émettre l'événement s'il a été abandonné après commit.
