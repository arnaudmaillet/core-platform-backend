---
i18n:
  source: ./README.md
  source_sha256: d40ce0e5fe308fb64333c9858c7c0efd3b108915489f3e6bcd663071550de28c
  translated_at: 2026-10-20
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
> | **Asynchrone** | publie `post.v1.events` (unifié) + `post.deleted` (legacy ; `post.published` / `post.updated` retirés, #487) · consomme `profile.v1.events` (dénormalisation du palier auteur) + `moderation.v1.events` (restriction de lecture) |
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
                  KafkaEventPublisher ◄─────┘  ─► post.v1.events / post.deleted
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
| media (`GetAsset`, `BatchResolveDelivery`, #907) | assets média des pièces jointes | les écritures qui nomment un asset échouent ; les lectures perdent les renditions | **Écriture :** `UNAVAILABLE` (`PST-5002`, rejouable). **Lecture :** fail-open — les pièces jointes gardent leurs URLs |
| wallet (`ListOwnedItems`, #914) | possession des styles de carte | un style ne peut pas être appliqué | `UNAVAILABLE` (`PST-5003`, rejouable) ; la carte par défaut marche toujours ; lectures non touchées |

**Amont (rayon d'impact — les événements `post.*` alimentent une grande partie de la flotte de lecture) :**

| Caller | Uses | Impact visible utilisateur si `post` est indisponible |
|---|---|---|
| `timeline` | `post.v1.events` / `post.deleted` | aucun nouveau post n'entre dans les fils d'accueil |
| `geo-discovery` | `post.v1.events` | les nouveaux posts n'apparaissent pas sur la carte |
| `notification` | `post.v1.events` (mentions) | les notifications de mention s'arrêtent |

> **Chemin critique ?** **Oui** pour la publication ; le chemin d'écriture est face utilisateur et
> l'événement est le déclencheur amont de toute la flotte côté lecture.

---

## 🔌 Interfaces publiques & contrat d'API

### gRPC — `post.v1.PostService`

```protobuf
service PostService {
  rpc CreatePost (CreatePostRequest) returns (CreatePostResponse);          // draft; PostId pre-generated at boundary
  rpc PublishPost (PublishPostRequest) returns (CommandResponse);           // Draft→Published; emits PostPublished
  rpc UpdatePost (UpdatePostRequest) returns (CommandResponse);             // emits PostUpdated
  rpc SetPostCardStyle (SetPostCardStyleRequest) returns (CommandResponse); // #914 le style de carte possédé de l'auteur, ou celui par défaut
  rpc DeletePost (DeletePostRequest) returns (CommandResponse);             // soft-delete; emits post.deleted
  rpc RestorePost (RestorePostRequest) returns (CommandResponse);           // #663 within 30 days: published again (re-emits PostPublished at its original time) or a draft
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
des compteurs retiennent les likes pour tout autre que l'auteur grâce à `BatchGetLikeVisibility`, réservé
au mesh (l'auteur de chaque post et s'il masque ses compteurs de likes ; posts inconnus absents) :
`GetPostEngagement` d'engagement et `BatchGetCounters` / `GetTrending` (`LIKE`) de counter.

**Mentions (#656).** Une légende mentionne un profil par un lien `[@handle](profile:<uuid>)`. `CreatePost` et
`UpdatePost` interrogent social-graph `CheckInteraction(MENTION)` pour chaque profil mentionné (une fois
chacun, soi-même mis à part) : un profil qui n'accepte pas les mentions de l'auteur (son audience « qui peut
me mentionner », ou un blocage dans un sens ou l'autre) fait refuser l'écriture (`PST-1009`, 403), rien
n'est enregistré. Au plus 20 profils par légende. Une panne fait échouer l'écriture (réessayable) ; une
légende sans mention n'interroge rien.

**Récemment supprimés (#663).** Une suppression est une pierre tombale : le post est indexé dans
`post.deleted_by_profile` (lignes expirées au bout de 30 jours) et `ListRecentlyDeleted` montre à l'auteur ses
posts restaurables. `RestorePost` en ramène un dans les 30 jours tel qu'il était — publié (réannoncé sur
comme `PostPublished` à sa date de publication d'origine, pour que fils et recherche le reprennent ; un post
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

**Onglets du profil (#829).** `ListPostsByProfile(tab)` liste tous les posts (non spécifié), les
**Republications** (posts avec un parent) ou les **Lieux** (posts avec un lieu) ; `PostSummary` dit ce qu'est
chaque post (`is_repost`, `has_place`, gardés dans `posts_by_profile` à la création, migration 0014). Un
onglet que le propriétaire masque (`ProfileTabSettingsChanged` `show_reposts` / `show_places`, projetés à
côté de la fenêtre) est **vide** pour tout client autre que l'auteur ; l'auteur et le mesh le voient en
entier. Un onglet Republications masqué n'est pas non plus reconstruit depuis la liste complète : les
republications en sortent pour ces lecteurs. L'onglet Lieux, et `has_place`, suivent le partage de
localisation de l'auteur (#657) comme le lieu lui-même : en mode fantôme, ou pour un lecteur hors de son
audience de localisation, l'onglet est vide et aucun post ne dit avoir un lieu. Les posts antérieurs à la
migration 0014 ne sont lus ni comme l'un ni comme l'autre.

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

**Styles de carte (#914).** Un style de carte est un article du catalogue du wallet (`card.<nom>`) que le
**compte** de l'auteur achète une fois ; `SetPostCardStyle(post_id, profile_id, card_style_id)` (l'auteur
du post) l'applique **gratuitement** à l'un de ses propres posts, ou la carte par défaut (vide). Il
vérifie que le compte le possède (`ListOwnedItems` du wallet sur le mesh, `POST_WALLET_GRPC_ENDPOINT` ;
sinon **`PST-1011`** `PERMISSION_DENIED`, **`PST-5003`** `UNAVAILABLE` quand le wallet ne peut pas
répondre) et l'écrit dans les deux tables (migration 0017). Supprimer le post ne consomme rien.
**Neutre pendant l'expérience :** `PostView.card_style_id` est retenu pour tout lecteur sauf l'auteur (et
l'export RGPD) tant que le post est dans sa fenêtre d'expérience (`POST_CARD_STYLE_NEUTRAL_SECS`, 24 h
après la publication tant que #889 ne la définit pas), afin que les cohortes témoin et test voient toutes
deux la carte par défaut (charte V5.3 §8–9). L'exception convenue avec le propriétaire : un `GetPost`
avec `detail` (l'écran de détail du post) et la liste du profil de l'auteur (`PostSummary.card_style_id`)
le montrent toujours. L'app le dessine depuis des visuels embarqués, au poids visuel de la carte par
défaut.

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
| PST-1010 | `CreateInFlight` (un CreatePost avec cette clé d'idempotence est encore en cours d'écriture, #876 ; à réessayer) | 409 → `ABORTED` |
| PST-2001..2003 | carousel cardinality / video length | 422 |
| PST-3001..3004 | thumbnail / MIME / CDN URL / dimensions | 422 |
| PST-3005 | `MediaAssetNotUsable` (l'`asset_id` d'une pièce jointe n'est pas une image ou vidéo de post utilisable de l'appelant, #907) | 422 |
| PST-3006 | `MediaNotSigned` (la pièce jointe d'un auteur privé est encore en traitement et n'a pas été uploadée SIGNED, #919) | 422 |
| PST-9001/9002 | invalid post/profile ID | 422 |
| PST-9003 | `AttachmentsCorrupted` (JSON deser) | 500 |
| PST-9004 | `DomainViolation` | 422 |
| PST-9006 | `InvalidIdempotencyKey` (pas 8 à 64 lettres, chiffres, `-`, `_`) | 422 |
| PST-9007 | `InvalidMediaAssetId` (pas un UUID) | 422 |
| PST-5001 | `AccessCheckUnavailable` (social-graph `CheckAccess` did not answer; retryable) | 503 → `UNAVAILABLE` |
| PST-5002 | `MediaUnavailable` (media `GetAsset` n'a pas répondu à une écriture qui nomme un asset ; rejouable) | 503 → `UNAVAILABLE` |
| PST-5003 | `WalletUnavailable` (le wallet n'a pas pu dire si l'auteur possède un style de carte, #914 ; rejouable) | 503 → `UNAVAILABLE` |
| PST-1011 | `ItemNotOwned` (un style de carte que le compte de l'auteur ne possède pas, #914) | 403 → `PERMISSION_DENIED` |

**Assets média (#907).** Une pièce jointe peut nommer son asset média (`MediaAttachmentInput.asset_id`,
l'id renvoyé par `media.IssueUploadTicket`). À l'écriture, post interroge media (`GetAsset`) et le refuse
avec `PST-3005`, sauf s'il s'agit d'une image ou vidéo de post qui n'est ni en échec, ni en quarantaine,
ni supprimée et, depuis l'edge, qui appartient au compte de l'appelant. Sinon n'importe qui pourrait
montrer le média SIGNED de quelqu'un via un post. Le refus est identique dans tous les cas, si bien
qu'une sonde n'apprend rien des assets des autres. À la lecture (`GetPost`, `ListRecentlyDeleted`),
chacune de ces pièces jointes porte `asset_id` (une clé de cache stable, contrairement à une URL SIGNED)
et `media`, le `DeliveredMedia` de media :
- le blurhash ;
- pour un asset READY, chaque rendition avec son URL, sa taille, sa visibilité et son expiration ;
- pour un asset encore en traitement, le blurhash seul.

Toutes les pièces jointes d'une réponse sont résolues en un seul `BatchResolveDelivery`, pour les clients
seulement : un lecteur du mesh (l'hydratation de search) reçoit `asset_id` sans `media`. Cette lecture est
**fail-open** : si media est lent ou indisponible, `media` est absent et `cdn_url` / `thumbnail_url`
livrent toujours. Les timelines, le feed découverte et search renvoient des ids de posts et hydratent via
`GetPost`, ils en profitent donc aussi. Sans `POST_MEDIA_GRPC_ENDPOINT`, les ids d'assets sont ignorés à
l'écriture et rien n'est résolu.

**Les images d'un auteur privé sont SIGNED (#919 étape 3).** Post projette quels auteurs sont privés
(`post.author_visibility`, migration 0016) à partir du `ProfileVisibilityChanged` de profile, émis aussi
quand un profil est créé privé.
- **À l'écriture**, pour un auteur privé : un asset public READY est passé en SIGNED (media
  `ChangeDeliveryVisibility`, qui n'accepte que post). Un asset encore en traitement doit avoir été
  uploadé SIGNED (`IssueUploadTicketRequest.delivery_visibility`), car media ne change la visibilité que
  d'un asset READY : sinon `PST-3006`.
- **Quand un auteur bascule**, le consumer des réglages auteur l'enregistre d'abord, puis parcourt tous
  ses posts et livre chaque asset SIGNED, ou de nouveau PUBLIC quand l'auteur redevient public. Le
  parcours est idempotent : une redélivrance le refait ; une panne de media est rejouée. Un asset encore
  en traitement à ce moment garde sa livraison, ce qui est journalisé.
- **Les vidéos** suivent la même règle une fois `POST_SIGNED_VIDEOS_ENABLED` activé (#921 ; désactivé par
  défaut). À activer seulement après que media signe des cookies pour les vidéos SIGNED
  (`MEDIA_CDN_COOKIE_*`). D'ici là, elles restent telles quelles.

Les appels portent le jeton mesh de post (`MESH_TOKEN_FILE`).

**Création idempotente (#876).** `CreatePostRequest.idempotency_key` fait qu'un CreatePost rejoué ne crée
rien et renvoie le `post_id` du premier appel, pendant 24 heures, par auteur. La clé vit dans
`post.create_keys`, écrite uniquement par des LWT : réservée en attente (TTL 60 s) avant toute
vérification, complétée (24 h) dès que le post est stocké, libérée si l'appel échoue avant. Une répétition
pendant que le premier appel est encore en attente reçoit `PST-1010` (`ABORTED`) : il faut réessayer.

---

## 📨 Contrat événementiel & asynchrone

> Les topics Kafka sont une API. Les matérialiseurs aval (timeline, geo-discovery, notification) se fient
> à l'`author_tier` et aux coordonnées transportés ici — un changement de schéma les casse comme un
> changement de proto.

**Publie :**

| Topic | Déclencheur | Clé | Consommateurs |
|---|---|---|---|
| `post.v1.events` | chaque événement de cycle de vie (`PostPublished` / `PostUpdated` / `PostDeleted`) ; `PostPublished` porte le `author_tier` dénormalisé, plus `caption` / `thumbnail_url` / `lat`/`lng` et `cover_asset_id` (l'asset média de la première pièce jointe, #963) optionnels pour la projection geo | `post_id` | `search`, `timeline`, `realtime`, `geo-discovery`, `notification` |
| `post.deleted` | `DeletePost` success | `post_id` | `timeline`, `geo-discovery` |

> **Un flux, un topic legacy.** `post.v1.events` est le flux unifié et versionné (la convention de la flotte, comme `moderation.v1.events` / `profile.v1.events`) : le `DomainEvent` entier tagué en interne, clé `post_id`. Les anciens topics par type `post.published` / `post.updated` sont retirés (#487 : geo-discovery et notification sont passés sur `post.v1.events`) ; `post.deleted` (charge utile brute) reste pour les consommateurs de suppression de timeline et de geo-discovery.

**Consomme :**

| Topic | Consumer group | Purpose | On poison/exhaustion |
|---|---|---|---|
| `profile.v1.events` | `post-author-tier` | dénormalise `ProfileTierChanged` dans la projection `author_tiers` (`profile_id → tier`) ; lue sur le chemin de publication pour estampiller `author_tier` sur les posts publiés. Les autres types committent en no-op | DLQ `profile.v1.events.dlq` |
| `profile.v1.events` | `post-author-location` | projette `ProfileVisibilityChanged` dans `author_visibility` et parcourt les posts de l'auteur pour que leurs médias suivent (SIGNED si privé, #919), `ProfileLocationSettingsChanged` dans `author_location_settings` (`profile_id → ghost, city`), que `GetPost` applique à la localisation montrée à tout autre que l'auteur, et `ProfileTabSettingsChanged` dans `author_post_windows` (`profile_id → window_days, show_reposts, show_places`), que les deux lectures appliquent. Démarre au plus ancien offset (un profil adolescent est créé en mode fantôme). Les autres types committent en no-op | DLQ `profile.v1.events.dlq` |
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
| `POST_MEDIA_GRPC_ENDPOINT` | Non | non défini | endpoint mesh de media (#907) : vérifie les assets des pièces jointes à l'écriture, les livre à la lecture. Non défini : les ids d'assets sont ignorés, les pièces jointes gardent leurs URLs. |
| `POST_WALLET_GRPC_ENDPOINT` | Non | non défini | endpoint mesh du wallet (#914) : si un auteur possède un style de carte (`ListOwnedItems`, jeton mesh de post). Non défini : un style ne peut pas être appliqué (`PST-5003`). Délais `POST_WALLET_RPC_TIMEOUT_MS` / `POST_WALLET_CONNECT_TIMEOUT_MS` (500 / 1000). |
| `POST_CARD_STYLE_NEUTRAL_SECS` | Non | `86400` | combien de temps après la publication les autres lecteurs voient la carte par défaut d'un post stylé (#914). |
| `POST_MEDIA_RPC_TIMEOUT_MS` / `_CONNECT_TIMEOUT_MS` | Non | `300` / `1000` | Délais de ces appels (courts : les lectures sont fail-open). |
| `POST_SIGNED_VIDEOS_ENABLED` | Non | `false` | les vidéos d'un auteur privé passent aussi en SIGNED (#921) ; à activer seulement une fois que media lit les vidéos SIGNED (`MEDIA_CDN_COOKIE_*`) |

> Le réglage complet `SCYLLA_*` / `KAFKA_*` vit dans les crates partagés storage/transport.

### Features de compilation
- `build.rs` compile `proto/post/v1/*.proto` et émet le descriptor set de réflexion.

---

## 🚀 Déploiement, migrations & rollback

- **Migrations :** `migrations/0001_create_keyspace.cql` → `0002_create_posts_table.cql` →
  `0003_create_posts_by_profile_table.cql` → `0004`–`0007` (audio, paliers auteur, géo, colonnes de
  modération ; `ALTER` en ligne) … → `0017_add_card_style.cql` (#914) sur `post`, appliquées **avant** le
  premier démarrage.
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
Cause racine : le post a été committé mais son événement `PostPublished` n'a pas pu être publié, ou un
consommateur aval est en retard. Mitigation : vérifier la santé de Kafka et les consumer-groups aval ;
ré-émettre l'événement s'il a été abandonné après commit.
