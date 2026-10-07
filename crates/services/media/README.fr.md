---
i18n:
  source: ./README.md
  source_sha256: 40f312d5e8da3969b1e8971c09ee5190002aa93bb4e30060bfda7530faf5ed0a
  translated_at: 2026-10-08
  status: complete
---
> 🇫🇷 Traduction française — la version **anglaise** [`README.md`](./README.md) fait foi.
> En cas de divergence, l'anglais prime. Les contrats (codes d'erreur, variables
> d'environnement, topics Kafka, identifiants) sont volontairement laissés en anglais.

# `media` — Déplacer pixels et images à l'échelle hyperscale **sans jamais mettre un octet sur le mesh**

> **Fiche service** &nbsp;·&nbsp; CORE
>
> | | |
> |---|---|
> | **Propriétaire** | `<team>` · `<#slack-channel>` |
> | **Astreinte / escalade** | `<oncall-rotation>` → `<escalation-policy>` |
> | **Tier** | TIER-1 (posture mixte : plan de diffusion fail-**open** · porte de conformité fail-**closed**) |
> | **Déployable** | `crates/apps/media-server` (crate bibliothèque : `crates/services/media`) |
> | **Datastores** | Stockage objet (S3 / MinIO — octets canoniques) · Postgres db `media` (SoR des métadonnées) · Redis Cluster (cache de diffusion + réservations de tickets) |
> | **Async** | publie `media.v1.events` · consomme les notifications de finalisation du stockage objet, `moderation.v1.events`, `account.v1.events`, `post.v1.events`, `profile.v1.events` (Kafka) |
> | **Appelants amont** | gateway/BFF, `post`, `profile` |
> | **Dépendances aval** | Stockage objet, CDN, `moderation` (Screen), Postgres, Redis, Kafka |
> | **SLO** | `<99.9%>` dispo plan de contrôle · `<p99 ResolveDelivery < N ms>` · latence de traitement suivie, non garantie |

---

## 🎯 Vue d'ensemble & rôle du service

`media` est le **plan de contrôle média** de la plateforme : il détient les *assets
média et leur cycle de vie de traitement* — la machine à états de l'asset, le
catalogue des dérivés/renditions, le schéma des clés de stockage, la politique de
diffusion CDN, et les blocages de conformité qui régissent à la fois la diffusion
et la suppression.

Le problème difficile qu'il résout est le **déplacement de binaire à l'échelle
hyperscale sans faire fondre le mesh synchrone** — une conception naïve « POST ta
vidéo à l'API » fait transiter des téraoctets par gRPC et Kafka, couple chaque
upload à un transcodage, et bloque `CreatePost` sur le traitement. Le motif qui
résout cela est une **séparation stricte plan de contrôle / plan de données** : les
octets circulent client ⇄ stockage objet ⇄ CDN sur un chemin pré-signé et direct
que le service *autorise* mais ne *transporte* jamais ; le mesh ne transporte que
des tickets, des métadonnées et des URLs.

**Objectifs essentiels :** (1) **aucun octet sur le mesh** — jamais ; (2) le chemin
de publication n'attend jamais le traitement (upload d'abord, référence et non
octets, résolution progressive) ; (3) le contenu de classe CSAM est filtré
**fail-closed** avant de pouvoir devenir public, et un blocage légal prime sur
l'effacement RGPD.

| Plan | Interface | Sync ? | Posture |
|---|---|---|---|
| **A — Courtage d'upload** | `IssueUploadTicket` → PUT pré-signé (les octets vont direct au stockage) | gRPC, étroit | fail-closed sur la politique |
| **B — Transformation** | Kafka : finalize → validate → screen → derive → publish | async | la latence est un SLO |
| **C — Résolution de diffusion** | `ResolveDelivery` / `BatchResolveDelivery` → URLs CDN / signées | gRPC, lecture chaude | fail-**open** (placeholder) |

---

## 📐 Architecture & concepts

Hexagonal / DDD (`domain` → `application` → `infrastructure`), bus CQRS
commande/requête, **stockage objet** comme magasin canonique des octets,
**Postgres** comme SoR des métadonnées d'asset, **Redis** pour le cache de
diffusion chaud + les réservations de tickets, Kafka pour le pipeline asynchrone et
les événements de cycle de vie.

```
            ┌─────────── plan de contrôle (gRPC, sans octets) ─────┐
 client ───▶│ IssueUploadTicket → asset(Pending) + PUT pré-signé   │
            └──────────────────────────┬──────────────────────────┘
                                        │  (les octets ne passent JAMAIS ici)
   octets ══ PUT ════════════▶ [ Stockage objet ] ◀══ pull origine ══ [ CDN ]
                                        │                                  ▲
              finalize (event S3→Kafka  │  ou RPC CommitUpload)            │ URL signée/immuable
                                        ▼                                  │
   Plan B :  validate → Screen(moderation, fail-closed) → derive ──▶ renditions
                                        │  émet media.v1.events (AssetReady …)
                                        ▼
   Plan C :  ResolveDelivery(asset_id) ─────────────────────────────▶ URL CDN
```

**Immutabilité adressée par contenu (le mécanisme clé).** Les clés de dérivés
publics sont `/{kind}/{content_hash}/{rendition}.{ext}` avec
`Cache-Control: immutable`. Une modification est un *nouvel asset* = un *nouveau
hash* = une *nouvelle URL*, donc **l'invalidation de cache est une opération
réservée aux retraits** — jamais une préoccupation du chemin d'édition. Le média
privé utilise des URLs signées à courte durée de vie, émises par requête *après*
que la périphérie a autorisé le spectateur.

**Les retraits atteignent l'origine.** Une URL publique n'expirant jamais, une
quarantaine ne peut pas compter sur `ResolveDelivery` seul (une URL déjà distribuée
continue de fonctionner). Elle est d'abord enregistrée (plus de nouvelle URL), puis
**tous les objets des arbres de l'asset** (`{kind}/{hash}/…` : rendus, toute la sortie
HLS d'une vidéo) et son upload d'origine **passent sous `quarantine/`** — un préfixe
que l'accès d'origine du CDN n'autorise pas — et le CDN est purgé en dernier. Une
restauration les remet en place *avant* de rétablir l'asset. Chaque étape est
idempotente : un événement rejoué ne refait que ce qui reste. Des octets identiques
partagent leurs clés, donc **un retrait porte sur le contenu** : tout asset aux mêmes
octets est mis en quarantaine avec lui (chacun enregistré et annoncé), et une
annulation les rétablit tous — sauf si l'un d'eux est sous **conservation légale**
(preuve CSAM), auquel cas rien n'est rétabli. Chaque asset enregistre les **mesures de
modération** qui le couvrent ; une annulation ne lève que la sienne, et les octets ne
reviennent que lorsqu'aucune copie n'est plus couverte par une autre mesure (deux
décisions sur deux copies : en annuler une laisse le contenu retiré). Un **ré-upload
d'octets retirés est mis en quarantaine dès son arrivée** (mesures et conservation légale
reprises), avant qu'aucun rendu ne soit réécrit sur les clés publiques partagées. Une
suppression efface les copies publiques comme en quarantaine, sauf les objets qu'un
autre asset aux mêmes octets détient encore (la conservation légale bloque toujours une
suppression d'abord).

> **Invariants** (et où ils sont appliqués) : aucun message ne porte de charge
> utile `bytes` (revue proto + règle de contrat `media-api`) ; les transitions
> d'état d'asset sont gardées dans le domaine ; verrou optimiste sur la ligne
> d'asset (`ConcurrentModification`) ; le stockage objet est canonique pour les
> octets, Postgres pour la vérité-sur-les-octets ; un blocage légal empêche la
> suppression définitive même sur un effacement RGPD du propriétaire.

---

## 📊 Objectifs de niveau de service (SLO) &nbsp;·&nbsp; OPS

| SLI | Objectif | Fenêtre | Mesuré par |
|---|---|---|---|
| Dispo plan de contrôle (non-5xx / non-`UNAVAILABLE`) | `<99.9%>` | 30j glissants | `<metric>` |
| `ResolveDelivery` p99 (Plan C, lecture chaude) | `< <N> ms` | 1h | `<metric>` |
| `IssueUploadTicket` p99 (Plan A) | `< <N> ms` | 1h | `<metric>` |
| Latence de traitement (upload → `AssetReady`) | `< <N> s` p99 | live | lag `<consumer-group>` (SLO, non SLA — la publication n'attend jamais) |
| Durabilité | aucune métadonnée d'asset acquittée perdue | — | commit Postgres ; 11-neuf du stockage objet |

**Budget d'erreur :** `<0.1% / 30j ≈ 43m>`. **À l'épuisement :** `<gel du déploiement | page>`.

---

## 🔗 Dépendances & rayon d'impact &nbsp;·&nbsp; OPS

**Aval — ce dont `media` a besoin pour fonctionner :**

| Dépendance | Rôle | Si indisponible → | Dégradation |
|---|---|---|---|
| Stockage objet | octets canoniques ; cible de pré-signature | uploads + origine de diffusion échouent | **Dure** pour l'upload ; la diffusion s'appuie sur le cache CDN |
| Postgres | SoR des métadonnées d'asset | lectures/écritures de métadonnées échouent | **Dure** — `UNAVAILABLE` sur le plan de contrôle |
| Redis | cache de diffusion, réservations de tickets | chemin cache-miss | **Souple** — résout depuis Postgres |
| CDN | diffusion en périphérie | périphérie froide / pression origine | **Souple** — pull origine, plus lent |
| `moderation` (Screen) | porte CSAM pré-publication | porte indisponible | **Fail-closed** pour la classe CSAM (asset retenu, non publié) |
| Kafka | finalize + événements de cycle de vie | le pipeline cale | **Souple** — les uploads s'accumulent, le traitement prend du retard |

**Amont — qui dépend de `media` (votre rayon d'impact si VOUS tombez) :**

| Appelant | Utilise | Impact visible si `media` est indisponible |
|---|---|---|
| gateway/BFF | `ResolveDelivery` | le média s'affiche en placeholder/blurhash ; le fil charge quand même |
| `post` / `profile` | référence d'asset + `media.v1.events` | les posts se publient (référence Pending) ; media résout plus tard |

> **Chemin critique ?** **Non pour les écritures cœur** — `post`/`profile`
> référencent un `asset_id` et ne bloquent jamais sur media. **Oui pour le rendu
> média** — la résolution de diffusion est sur le chemin de lecture, mais échoue en
> mode ouvert vers un placeholder.

---

## 🔌 Interfaces publiques & contrat d'API &nbsp;·&nbsp; CORE

### gRPC — `media.v1.MediaService` *(Phase 1 — plan de contrôle, zéro champ d'octets)*

```protobuf
service MediaService {
  // Plan A — courtage d'upload (renvoie une URL pré-signée ; les octets vont direct au stockage)
  rpc IssueUploadTicket (IssueUploadTicketRequest) returns (IssueUploadTicketResponse);
  rpc CommitUpload      (CommitUploadRequest)      returns (CommitUploadResponse);
  rpc AbortUpload       (AbortUploadRequest)       returns (AbortUploadResponse);
  // Métadonnées d'asset
  rpc GetAsset          (GetAssetRequest)          returns (GetAssetResponse);
  rpc DeleteAsset       (DeleteAssetRequest)       returns (DeleteAssetResponse);
  rpc ListAssetsByOwner (ListAssetsByOwnerRequest) returns (ListAssetsByOwnerResponse); // mesh uniquement
  // Plan C — résolution de diffusion (lecture chaude ; URLs CDN / signées)
  rpc ResolveDelivery      (ResolveDeliveryRequest)      returns (ResolveDeliveryResponse);
  rpc BatchResolveDelivery (BatchResolveDeliveryRequest) returns (BatchResolveDeliveryResponse);
  // Ops
  rpc Reprocess         (ReprocessRequest)         returns (ReprocessResponse);
}
```

**Les assets d'un compte (#653).** `ListAssetsByOwner(owner_id, limit, page_token, url_ttl_secs)` est
**mesh uniquement** (jamais sur l'edge) : l'export de données RGPD liste chaque asset qu'un compte possède
(non supprimé), par id (index `(owner_id, id)`, migration 0002), chacun avec un téléchargement signé de son
**original**, valable `url_ttl_secs` (60 s – 7 jours, le plafond de S3 ; signé contre l'endpoint public).
Un asset non diffusable (en traitement, en échec, en quarantaine) vient sans URL — un fichier en
quarantaine n'est jamais remis.

> **Règle de contrat / wire :** **aucun message `media.v1` ne porte de charge utile
> `bytes`.** Les uploads sont courtés via des URLs de stockage objet pré-signées ;
> la diffusion est courtée via des URLs CDN / signées. Les enums sont entièrement
> préfixés avec une sentinelle `_UNSPECIFIED = 0`.

**Invariants de frontière :** `CommitUpload` sur un objet absent →
`FAILED_PRECONDITION` (`MED-1005`) ; résolution d'un asset en quarantaine →
`PERMISSION_DENIED` (`MED-7001`, 451) ; suppression sous blocage légal →
`PERMISSION_DENIED` (`MED-7003`).

**Documents privés (#777).** `MEDIA_KIND_PRIVATE_DOCUMENT` contient les pièces d'une demande de
vérification (`RequestVerification` de profile) : JPEG, PNG, HEIC ou PDF, ≤ 10 Mio, possédés par le compte
comme tout asset. **Désactivé par défaut** (`MEDIA_PRIVATE_DOCUMENTS_ENABLED`) : tant que le déploiement ne
tient pas `private/` hors de l'accès d'origine du CDN (core-platform-infra#37), un ticket d'upload pour un
document privé est refusé (`MED-1006`, 503). Un PDF ou un HEIC est vérifié par ses octets magiques (`DocumentMediaProbe` ; jamais
décodé, sans dimensions) ; un JPEG/PNG par la sonde d'images. Le traitement passe le même scan anti-malware,
la même vérification de retrait et le même filtrage CSAM, puis **déplace les octets vers
`private/documents/{asset_id}`** — un préfixe auquel l'accès d'origine du CDN n'est pas accordé (comme
`quarantine/`), indexé par l'asset si bien que des octets identiques venant de deux comptes ne partagent
jamais d'objet — et le marque READY **sans aucun rendu**. Il n'est **jamais diffusé** : `ResolveDelivery`
répond NOT_FOUND et `BatchResolveDelivery` l'omet ; `GetAsset` ne le montre qu'à son propriétaire (un autre
compte : NOT_FOUND ; le mesh le lit) ; la dédup ne s'y applique jamais, dans aucun sens. Le staff les examine
via `GetPrivateDocumentUrl` (un GET signé valide `MEDIA_SIGNED_URL_TTL_SECS`), **mesh uniquement et réservé au staff (#837)** :
l'outil staff envoie le jeton edge du membre du staff (`authorization: Bearer`), que media vérifie
lui-même avec le JWKS d'`auth` (`EDGE_JWKS_URL` / `EDGE_TOKEN_ISSUER` / `EDGE_TOKEN_AUDIENCE`, les
réglages du listener edge) et qui doit porter `verification:review` (modérateurs de contenu, admins). Pas
de jeton ou un jeton invalide : `UNAUTHENTICATED` ; un autre jeton : `PERMISSION_DENIED` ; sans ces
réglages, tout appel est refusé. Chaque lien est enregistré sur le
plan d'audit **avant** d'être rendu : un événement `data_access` sur `audit.v1.events`
(`media.private_document.viewed` : le compte staff, le document, son propriétaire, l'heure, l'expiration du
lien) ; un enregistrement en échec retient le lien (`MED-7004`, 503, retentable). Sans broker, la
consultation est seulement journalisée. L'export
RGPD le liste avec un téléchargement signé. **Conservation :** READY fixe une purge de secours à 90 jours ;
`ProfileVerificationDecided` (`profile.v1.events`, `media-profile-consumer`) la déplace à la décision +
`MEDIA_DOCUMENT_RETENTION_DAYS` (30), uniquement pour les documents du demandeur ; un balayage (toutes les
`MEDIA_DOCUMENT_SWEEP_INTERVAL_SECS`) supprime ce qui est échu, comme une suppression par le propriétaire ;
un gel légal le conserve. Supprimer le document retire toujours son objet privé (jamais partagé).

### Contrat d'erreur

Chaque faute implémente `error::AppError` avec un code `MED-XXXX` stable, mappé vers
`Status` gRPC / HTTP par la crate `error` partagée :

| Plage | Classe |
|---|---|
| `MED-1xxx` | courtage d'upload / ticket (Plan A) |
| `MED-2xxx` | SoR des métadonnées d'asset (+ concurrence) |
| `MED-3xxx` | pipeline de rendition / transformation (Plan B) |
| `MED-4xxx` | adaptateur de stockage objet — le plan de données (infra retryable) |
| `MED-5xxx` | CDN / diffusion / signature (Plan C) |
| `MED-6xxx` | validation / inspection de contenu (magic-byte, decode-bomb, malware) |
| `MED-7xxx` | conformité / Screen (Quarantined / LegalHold = 451 ; ScreenUnavailable = 503 fail-closed) |
| `MED-8xxx` | décodage d'événement entrant / mappage de source |
| `MED-9xxx` | transverse (domaine/parse, E/S d'événements) |

---

## 📨 Événements & contrat asynchrone &nbsp;·&nbsp; CORE

> Les topics Kafka sont une API. Un changement de schéma ici casse les consommateurs exactement comme un changement proto.

**Publie** (`media.v1.events`, structs serde, clé `asset_id`) :

| Topic | Déclencheur | Clé | Consommateurs |
|---|---|---|---|
| `media.v1.events` → `AssetUploaded` | finalize accepté | `asset_id` | `moderation` (screen), pipeline interne |
| `media.v1.events` → `AssetVariantReady` | une rendition se termine | `asset_id` | BFF (rendu progressif) |
| `media.v1.events` → `AssetReady` | toutes les renditions faites | `asset_id` | `post`, `profile`, `search`, `timeline` |
| `media.v1.events` → `AssetFailed` / `AssetQuarantined` / `AssetDeleted` | états terminaux | `asset_id` | appelants, GC |

**Consomme :**

| Topic | Groupe de consommateurs | Rôle | Sur poison/épuisement |
|---|---|---|---|
| finalize du stockage objet (bridgé) | `media-finalize-consumer` | finalisation d'upload (source de vérité au-dessus de `CommitUpload`) | DLQ |
| `moderation.v1.events` | `media-moderation-consumer` | quarantaine / restauration (révoquer / réactiver la diffusion) | DLQ |
| `account.v1.events` | `media-account-consumer` | `account_deleted` → chaque asset du compte est supprimé comme par son propriétaire (octets, rendus, copies en quarantaine, staging, tombstone, `AssetDeleted`, cache, CDN) ; les assets sous gel légal sont gardés et journalisés (RGPD art. 17, #777 ; idempotent). Une preuve dont un dossier de modération a encore besoin (un appel en cours au moment de la suppression) doit être conservée par un gel légal posé par moderation, jamais par ce chemin | DLQ |
| `profile.v1.events` | `media-profile-consumer` | `ProfileVerificationDecided` → les documents privés du demandeur sont purgés à la décision + `MEDIA_DOCUMENT_RETENTION_DAYS` (#777) | DLQ |
| `post.v1.events` / `profile.v1.events` | `media-binding-consumer` | marquer les assets liés ; GC des uploads abandonnés | DLQ |

> **Contrat d'exécution (obligatoire) :** tous les consommateurs tournent sous
> `run_consumer` — commit manuel après une issue terminale, retry borné avec
> backoff + jitter, DLQ sur épuisement/poison. Idempotence : `asset_id`
> déterministe + clés de rendition adressées par contenu (une rendition re-dérivée
> écrit la même clé).

---

## 🌩️ Modes de défaillance & dégradation &nbsp;·&nbsp; OPS

| Défaillance | Symptôme | Comportement du service | Action opérateur |
|---|---|---|---|
| Stockage objet indispo | uploads + origine échouent | l'upload échoue dur ; la diffusion s'appuie sur le cache CDN | vérifier le stockage / IAM ; les uploads réessaient |
| Postgres indispo | erreurs plan de contrôle | `UNAVAILABLE` sur les ops de métadonnées | bascule / restauration |
| Redis froid/évincé | hausse de latence de résolution | reconstruit depuis Postgres, sûr | en général aucune |
| CDN froid / pression origine | latence du premier octet | pull origine depuis le stockage | réchauffer / vérifier la config edge |
| Screen `moderation` indispo | classe CSAM retenue | **fail-closed** : asset non publié | vérifier moderation ; le hard timeout borne l'attente |
| Backlog des workers de transcodage | latence `AssetReady` | publication non impactée ; placeholder affiché | scaler les workers ; vérifier la DLQ |

**Contre-pression & limites :** plafonds de taille par kind + allowlist MIME
appliqués dans la politique d'upload signée ; plafonds decode-bomb / dimensions à la
validation ; hard timeout Screen (Phase 7) pour qu'une panne de moderation ne puisse
pas coincer les uploads ; traitement isolé sur un rôle worker séparé pour que le CPU
de transcodage ne dégrade pas la latence Plan A/C.

---

## 📦 Intégration & usage &nbsp;·&nbsp; CORE

```toml
[dependencies]
media = { path = "crates/services/media" }
```

Bibliothèque uniquement. Implémente
[`service_runtime::Service`](../../platform/service-runtime/README.md) en tant que
`media::service::MediaService` (Phase 5) — `build` câble les adaptateurs stockage
objet / Postgres / Redis et lance les consommateurs finalize, moderation et binding ;
`register` ajoute les services gRPC ; `health_probes` expose la vivacité. La
télémétrie, la config + rechargement à chaud, la limitation de débit en entrée, la
santé et l'arrêt gracieux sont gérés par le runtime.

### Amorçage (`crates/apps/media-server`)

```rust
use media::service::MediaService;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr = std::env::var("MEDIA_GRPC_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:50063".to_owned()).parse()?;
    service_runtime::serve::<MediaService>(addr).await
}
```

> **Statut de build :** complet jusqu'à la Phase 7 — namespace d'erreur, proto
> `media.v1` (8 RPCs de plan de contrôle, aucun champ d'octets), domaine +
> application, adaptateurs S3/Postgres/Redis/CDN/image, serveur + consommateurs
> worker, et une suite d'intégration live MinIO + Postgres + Redis. Tous les RPCs
> du plan de contrôle sont sans octets.
>
> **Autorisation (exigence de déploiement) :** `media` ne s'auto-autorise en rien.
> La périphérie/gateway (via `auth-context`) authentifie l'appelant et fournit
> l'`owner_id` sur `IssueUploadTicket` / `DeleteAsset` / `AbortUpload` ; la
> propriété est vérifiée en défense dans le handler (une suppression par un
> non-propriétaire renvoie `NOT_FOUND`, sans rien divulguer). L'autorisation du
> spectateur pour le média privé se fait à la périphérie **avant** que
> `ResolveDelivery` n'émette une URL signée. N'exposez les RPCs mutants que derrière
> cette porte.

---

## ⚙️ Configuration & environnement d'exécution &nbsp;·&nbsp; CORE

### Variables propres à `media`

| Variable | Requis | Défaut | Description |
|---|---|---|---|
| `MEDIA_GRPC_ADDR` | Non | `0.0.0.0:50063` | adresse de bind du plan de contrôle gRPC |
| `MEDIA_OBJECT_STORE_ENDPOINT` | Non | `http://localhost:9000` | URL de l'endpoint S3/MinIO |
| `MEDIA_OBJECT_STORE_REGION` | Non | `us-east-1` | région S3 |
| `MEDIA_OBJECT_STORE_BUCKET` | Non | `media` | bucket des octets canoniques |
| `MEDIA_S3_ACCESS_KEY` / `MEDIA_S3_SECRET_KEY` | **Oui (prod)** | `minioadmin` | identifiants du stockage objet |
| `MEDIA_PRESIGN_TTL_SECS` | Non | `900` | validité des URLs signées côté serveur |
| `MEDIA_OBJECT_STORE_TIMEOUT_MS` | Non | `10000` | hard timeout sur chaque appel HTTP au stockage objet |
| `MEDIA_CDN_BASE_URL` | Non | `…:9000/media` | origine de diffusion publique adressée par contenu |
| `MEDIA_CLOUDFRONT_DISTRIBUTION_ID` | Non | — | les retraits (quarantaine / suppression) purgent les rendus de l'asset de cette distribution CloudFront (`CreateInvalidation`, SigV4 avec les clés `MEDIA_S3_*`, dont l'utilisateur IAM doit avoir `cloudfront:CreateInvalidation`) ; une purge en échec fait échouer le retrait, qui est rejoué. Non défini : les retraits sont seulement journalisés (exécutions locales) |
| `MEDIA_UPLOAD_TICKET_TTL_SECS` | Non | `900` | fenêtre de validité de l'upload pré-signé |
| `MEDIA_SIGNED_URL_TTL_SECS` | Non | `300` | validité de l'URL de diffusion privée (signée) |
| `MEDIA_PRIVATE_DOCUMENTS_ENABLED` | Non | `false` | autorise l'upload de documents privés (#777) ; à activer seulement une fois `private/` hors de l'accès d'origine du CDN (core-platform-infra#37) |
| `MEDIA_DOCUMENT_RETENTION_DAYS` | Non | `30` | durée de vie d'un document privé après la décision de sa demande de vérification (#777) |
| `MEDIA_DOCUMENT_SWEEP_INTERVAL_SECS` | Non | `3600` | fréquence de purge des documents privés échus |
| `MEDIA_DEDUP_ENABLED` | Non | `false` | dédup par hash de contenu (off tant que le purge-refcount n'est pas durci) |
| `MEDIA_SCREEN_GRPC_ENDPOINT` | Non | `http://localhost:50061` | endpoint de la porte Screen de moderation |
| `MEDIA_SCREEN_TIMEOUT_MS` | Non | `200` | hard timeout fail-closed du Screen |

### Variables d'infrastructure héritées

| Variable | Requis | Défaut | Description |
|---|---|---|---|
| `POSTGRES_* / REDIS_HOSTS / KAFKA_BROKERS` | **Oui** | — | SoR des métadonnées, cache chaud, pipeline asynchrone |

> Le réglage complet connexion/timeout/reconnexion vit dans les crates de stockage/transport partagées concernées.

### Features à la compilation
- `integration-media` — active la suite live MinIO + Postgres + Redis (Phase 6).
- `build.rs` (Phase 1, dans `media-api`) compile `contracts/proto/media/v1/*` et émet le descriptor set de réflexion.

---

## 🚀 Déploiement, migrations & rollback &nbsp;·&nbsp; OPS

- **Migrations :** `crates/services/media/migrations/*.sql` contre la db Postgres
  `media` (Phase 4). Appliquer **avant** de déployer le nouveau binaire.
  Expand-then-contract.
- **Stockage objet / CDN :** le cycle de vie du bucket (dérivés chauds → masters
  froids IA/Glacier) et la politique de cache CDN sont de l'infra déclarée, pas un
  cron applicatif. Bucket object-lock / WORM pour les blocages légaux.
- **Déploiement :** progressif ; les changements de pipeline risqués sont gardés par
  config + `Reprocess`.
- **Rollback :** le binaire est sûr à revenir en arrière (schéma de métadonnées
  compatible amont avec N-1). **Piège à état :** le schéma de clés adressé par
  contenu et les slugs de l'échelle de renditions ne doivent **jamais** changer de
  sens une fois des données existantes — versionnez-les, ne les mutez pas.

**Revue de sécurité (Phase 7, passe manuelle) : propre.** Aucun octet ne traverse
jamais gRPC/Kafka (vérifié — le `media.v1` généré a zéro champ `bytes`) ; aucun
octet brut ni PII dans les logs (seulement des champs opérationnels :
`event_type`, `asset_id`, clés d'objet) ; aucun `unwrap`/`expect`/`panic` sur les
chemins de requête ou de pipeline ; l'inspection de contenu ne fait jamais
confiance au type déclaré par le client ; les URLs signées sont à courte durée ;
un blocage légal empêche la suppression définitive (préservation CSAM/NCMEC primant
sur l'effacement RGPD). Un constat cohérent avec le reste de la flotte, appliqué au
gateway (pas un correctif code) : pas d'autorisation par-RPC de l'appelant — voir la
note Autorisation ci-dessus. Les appels au stockage objet sont bornés par un hard
timeout (`MEDIA_OBJECT_STORE_TIMEOUT_MS`) et la porte Screen par
`MEDIA_SCREEN_TIMEOUT_MS`, donc ni un stockage bloqué ni une porte de modération
bloquée ne peuvent coincer un worker.

**Différé (documenté, non construit) :** transcodage vidéo (v1 images-first — un
port `Transcoder` frère + échelle ABR en fast-follow) ; un vrai sidecar de scan
anti-malware (le port `MalwareScanner` livre un stub passe-tout) ; le consommateur
de GC des orphelins (réclamation des uploads abandonnés non liés après TTL) ;
l'encodage WebP/AVIF (v1 émet du JPEG) ; et le purge RGPD conscient du refcount de
dédup (la dédup est livrée derrière un flag off-par-défaut tant que ce chemin n'a
pas de couverture live ; l'effacement d'un compte garde déjà un objet qu'un autre
asset utilise encore).

---

## 📈 Télémétrie, performance & métriques &nbsp;·&nbsp; CORE

- **Runtime :** Tokio multi-thread. Le serveur d'API (léger, Plan A/C) est déployé
  comme **rôle séparé** des workers de traitement (lourds, Plan B) pour que la
  charge de transcodage ne dégrade pas la latence du plan de contrôle. Contexte de
  trace W3C propagé à travers la frontière Kafka.

| Signal | Pourquoi ça compte | Alerte suggérée |
|---|---|---|
| `ResolveDelivery` p99 | lecture chaude sur le chemin de rendu | `> SLO ⇒ investiguer` |
| latence de traitement (upload→Ready) | UX de rendu progressif | `soutenue > N s ⇒ scaler les workers` |
| taux de fail-closed Screen | santé de la porte CSAM | `pic ⇒ page` |
| taux de production DLQ | poison / retry épuisé | tout taux soutenu ⇒ page |

---

## 🛠️ Développement local &nbsp;·&nbsp; CORE

```bash
cargo build -p media && cargo clippy -p media --all-targets
cargo test  -p media                                   # run unitaire rapide, sans infra
cargo test  -p media --features integration-media      # live MinIO + Postgres + Redis (Phase 6)
```

---

## 🚨 Dépannage & runbook &nbsp;·&nbsp; CORE

> Format : **symptôme → cause racine → mitigation.** Une entrée par classe d'incident réel.

**1. `CommitUpload` renvoie `FAILED_PRECONDITION` (`MED-1005`).**
Cause racine : le client a appelé commit avant la fin du dépôt des octets dans le
stockage objet (ou le PUT a échoué). Mitigation : le client réessaie le PUT direct ;
l'événement de finalisation S3 est le déclencheur faisant autorité et convergera
quoi qu'il arrive.

**2. Le média s'affiche en placeholder indéfiniment.**
Cause racine : l'asset est bloqué en `Processing` — backlog des workers de
transcodage ou message poison dans le pipeline. Mitigation : vérifier le lag de
`media-finalize-consumer` et la DLQ ; scaler les workers ; `Reprocess` l'asset si une
rendition a été perdue.

**3. La suppression renvoie 451 (`MED-7003`).**
Cause racine : un blocage légal est actif (ex. préservation de preuve CSAM) — la
suppression définitive est intentionnellement bloquée, primant sur l'effacement RGPD.
Mitigation : ce comportement est correct ; escalader vers trust & safety / legal, ne
pas forcer la suppression.
