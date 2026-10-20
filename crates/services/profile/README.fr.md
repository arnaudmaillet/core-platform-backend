---
i18n:
  source: ./README.md
  source_sha256: 7e2cc47cf4ad7eaf15267ac5643e4f9059a2ad02814f8b025b65205946a07d2a
  translated_at: 2026-10-20
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
                            account_deleted efface aussi les demandes de vérification du compte (#777)

   gRPC media (mesh) ─► GetAsset : les documents privés d'une demande de vérification sont ceux du demandeur, READY (#777)
```

**Versionnement des clés de cache.** Toutes les clés portent un préfixe `v1:` — incrémenter le suffixe
effectue une invalidation de cache à l'échelle de la flotte, sans interruption, lors d'une migration de
schéma. **Réservation par tombstone :** un handle supprimé est bloqué 30 jours via `tombstoned_at`,
empêchant le détournement rapide d'identité (`handle_is_available()` l'impose à la couche application) ;
un handle libéré par un profil **vérifié** est **réservé définitivement** (`profile_handles.held`) :
personne ne peut reprendre le nom d'une identité vérifiée — sauf ce profil lui-même, qui peut
reprendre son propre handle libéré (un renommage inverse, réservé ou non : `IF profile_id = ?`). Passé les 30 jours, la réservation reprend
un tombstone non réservé par une seconde LWT (`UPDATE … IF tombstoned_at < now − 30 d AND held = null`),
sans tâche de GC. `CreateProfile` réserve le handle **avant** d'écrire le profil : une course perdue
n'écrit rien (pas de ligne orpheline, pas de `pids` parasite), et une écriture en échec rend la
réservation (`DELETE … IF profile_id = ?`). Une réservation dont le profil porte désormais un autre
handle (un renommage interrompu avant le tombstone de l'ancien handle) est réparée à la lecture : les
recherches et les vérifications de disponibilité la passent en tombstone (`IF profile_id = ? AND
tombstoned_at = null AND created_at < now − 5 min`) et elle ne résout plus. Le délai de grâce de
5 minutes protège un renommage en cours, dont la nouvelle réservation précède de quelques
millisecondes l'enregistrement du profil.

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
  rpc ChangeHandle(ChangeHandleRequest) returns (CommandResponse);   // LWT ; payant (#911)
  rpc GetHandleChangePrice(GetHandleChangePriceRequest) returns (HandleChangePrice);   // #911 le prix du prochain changement / le délai du changement gratuit
  rpc UpdateAvatar(UpdateAvatarRequest) returns (CommandResponse);
  rpc UpdateBanner(UpdateBannerRequest) returns (CommandResponse);
  rpc EquipAvatarFrame(EquipAvatarFrameRequest) returns (CommandResponse);   // #913 porter un cadre possédé, ou aucun
  rpc SetVisibility(SetVisibilityRequest) returns (CommandResponse);
  rpc SetInteractionLimit(SetInteractionLimitRequest) returns (CommandResponse); // #669 mettre en attente les commentaires / messages des non-abonnés ou abonnés récents jusqu'à une date (≤ 4 semaines)
  rpc ClearInteractionLimit(ClearInteractionLimitRequest) returns (CommandResponse);
  rpc SetInteractionSettings(SetInteractionSettingsRequest) returns (CommandResponse); // qui peut commenter / mentionner / écrire, téléchargements, compteurs de likes, remix et réutilisation du son original (#669 ; absent d'une requête ⇒ inchangé) ; les ados démarrent abonnés uniquement, sans téléchargement / remix / réutilisation du son
  rpc SetLocationSettings(SetLocationSettingsRequest) returns (CommandResponse); // mode fantôme + précis / niveau ville + audience (tout le monde / abonnés / mutuels ; UNSPECIFIED la garde) + on_new_posts (une préférence stockée ; absent la garde), réservé au propriétaire sur la vue ; les ados démarrent fantômes, mutuels, sans localisation sur les nouveaux posts, et sont plafonnés à mutuels (une audience plus large est refusée, PRF-9001) ; geo-discovery et post l'appliquent (#657)
  rpc SetFeedSettings(SetFeedSettingsRequest) returns (CommandResponse); // #662 contenu sensible moins (par défaut) / standard + non_personalized (For You non classé par centres d'intérêt ; activé par défaut pour un titulaire de 13 à 17 ans, qui peut le couper), synchronisés entre appareils ; annoncés par ProfileFeedSettingsChanged (timeline applique la désactivation) ; réservé au propriétaire sur la vue
  rpc SetTabSettings(SetTabSettingsRequest) returns (CommandResponse); // #664 fenêtre de posts (tout / 6 mois / 1 mois / 3 jours ; post masque les posts plus anciens aux visiteurs) + visibilité des onglets j'aime / enregistrés / republications / lieux (#829 : post sert Republications et Lieux, engagement J'aime, chacun vide pour tout autre que le propriétaire quand il est masqué ; Enregistrés n'a pas encore de liste serveur) ; partiel ; les réglages sont réservés au propriétaire sur la vue, mais chaque lecteur reçoit `ProfileView.visible_tabs` (un onglet masqué est omis, pas affiché vide)
  rpc SetCommentFilters(SetCommentFiltersRequest) returns (CommandResponse); // #660 mots masqués (≤ 200, normalisés) + filtre offensant (activé par défaut) ; réservé au propriétaire sur la vue ; comment les applique
  rpc SetDiscoverySettings(SetDiscoverySettingsRequest) returns (CommandResponse); // #661 statut d'activité, accusés de lecture, trouvable par téléphone / e-mail / recherche de handle / QR / suggestions (partiel ; réservé au propriétaire sur la vue) ; les ados démarrent introuvables par téléphone, e-mail, suggestions ; search applique by_handle_search ; `in_suggestions` ne peut être activé que si le jeton dit 18+ (PRF-9001 : un ado n'est jamais suggéré) ; social-graph le projette pour SuggestProfiles, account lit by_phone / by_email pour FindProfilesByContacts
  rpc SetAccountType(SetAccountTypeRequest) returns (CommandResponse); // #668 personnel / professionnel (créateur) / marque (entreprise) + la fiche de contact publique d'une marque ; un bot reste un bot ; créateur et entreprise réservés aux 18 ans et plus (un jeton 13–17 reçoit FAILED_PRECONDITION)
  rpc RequestVerification(RequestVerificationRequest) returns (CommandResponse); // #668 propriétaire : catégorie + 1 à 5 justificatifs (liens web et/ou documents privés, #777) → file du personnel
  rpc GetVerificationRequest(GetVerificationRequestRequest) returns (GetVerificationRequestResponse); // #668 propriétaire : en attente / approuvée / refusée (+ motif)
  rpc ListPendingVerificationRequests(ListPendingVerificationRequestsRequest) returns (ListPendingVerificationRequestsResponse); // personnel, mesh uniquement : le jeton staff (Bearer, verification:review) est vérifié (#837)
  rpc DecideVerificationRequest(DecideVerificationRequestRequest) returns (CommandResponse); // personnel, mesh uniquement (Bearer, verification:review, #837) : approuver vérifie le profil ; refuser exige un motif
  rpc VerifyProfile(VerifyProfileRequest) returns (CommandResponse);
  rpc HideProfile(HideProfileRequest) returns (CommandResponse);
  rpc RestoreProfile(RestoreProfileRequest) returns (CommandResponse);
  rpc DeleteProfile(DeleteProfileRequest) returns (CommandResponse);
  // Queries
  rpc GetProfileById(GetProfileByIdRequest) returns (ProfileView);
  rpc GetProfileByHandle(GetProfileByHandleRequest) returns (ProfileView);
  rpc CheckHandleAvailability(CheckHandleAvailabilityRequest) returns (CheckHandleAvailabilityResponse);
  rpc ListProfilesByAccount(ListProfilesByAccountRequest) returns (ListProfilesByAccountResponse);
  rpc GetProfileSlots(GetProfileSlotsRequest) returns (ProfileSlots);                    // #916 les emplacements du compte, utilisés, plafond, prix suivant
  rpc BuyProfileSlot(BuyProfileSlotRequest) returns (BuyProfileSlotResponse);            // #916 l'emplacement suivant en gems, issue dans la réponse
  rpc GetShareToken(GetShareTokenRequest) returns (ShareTokenResponse);        // #661 le jeton QR / lien de partage du propriétaire (22 caractères URL-safe), émis à la première demande
  rpc RotateShareToken(RotateShareTokenRequest) returns (ShareTokenResponse);  // #661 le révoque pour un nouveau : l'ancien ne se résout plus, immédiatement
  rpc ResolveShareToken(ResolveShareTokenRequest) returns (ProfileView);       // #661 public_read : le profil tel que le lecteur peut le voir ; NOT_FOUND si révoqué, inactif, ou `by_qr` désactivé (le propriétaire le résout toujours)
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

**Assets média de l'avatar et de la bannière (#907).** `UpdateAvatar` / `UpdateBanner` peuvent nommer
l'asset média que montre l'image (`avatar_asset_id` / `banner_asset_id`, l'id renvoyé par
`media.IssueUploadTicket`). Profile interroge media (`GetAsset`) et le refuse avec `PRF-5007`, sauf
s'il s'agit d'une image (`AVATAR` ou `POST_IMAGE`) du **compte du profil** qui n'est ni en échec, ni en
quarantaine, ni supprimée. Sinon n'importe qui pourrait montrer le média SIGNED de quelqu'un. Le refus
est identique dans tous les cas. Si media ne répond pas, la mise à jour échoue avec `PRF-5005`
(rejouable). L'asset suit chaque mise à jour : une image définie par URL seule, ou effacée, n'en a pas.

Il est stocké à côté de l'URL (`profiles.avatar_asset_id` / `banner_asset_id` et
`profiles_by_account.avatar_asset_id`, migration 0017) ; `UpdateAvatar` rafraîchit désormais aussi la
liste des profils du compte. Les lectures (`GetProfileById/ByHandle`, `ResolveShareToken`,
`ListProfilesByAccount`) renvoient les ids d'assets et, pour les clients seulement, `avatar` / `banner` :
le `DeliveredMedia` de media, avec le blurhash et, une fois READY, chaque rendition avec son URL, sa
taille, sa visibilité et son expiration. Ils sont résolus en un seul `BatchResolveDelivery` par réponse,
en **fail-open** : si media est lent ou indisponible, ils sont absents et les URLs livrent toujours.
Sans `PROFILE_MEDIA_GRPC_ENDPOINT`, les ids d'assets sont ignorés à l'écriture. `CreateProfile` ne prend
toujours que des URLs.

**Emplacements de profil (#916).** Un compte détient un profil par **emplacement** : un est gratuit,
les suivants s'achètent en gems, au plus `PROFILE_MAX_SLOTS` (5), le 2ᵉ à `PROFILE_SLOT_BASE_PRICE` (100)
et chacun des suivants au double du précédent (100 / 200 / 400 / 800). Un emplacement s'achète une fois
et se garde (`profile.profile_slots`, migration 0018) : supprimer un profil libère son emplacement, en
créer un plus tard le réutilise, et le prix dépend des emplacements détenus, jamais des profils vivants.
Un compte qui détenait plus de profils avant les emplacements les garde : ses profils sont enregistrés
comme ses emplacements la première fois qu'ils sont comptés. `CreateProfile` au-delà des emplacements ne
crée rien et répond **dans la réponse** (`success = false`, `no_free_slot = true`). `GetProfileSlots`
donne les emplacements, ceux utilisés, le plafond et le prix suivant. `BuyProfileSlot(expected_price)`
(adultes seulement : sinon **`PRF-6002`**, si bien qu'un mineur garde un profil) répond `BOUGHT`,
`MAX_REACHED`, `INSUFFICIENT_GEMS` ou `PRICE_CHANGED`. Il demande les gems au `SpendGems` du wallet
(réservé au mesh ; `PROFILE_SLOT`, `ref_id` = le numéro de l'emplacement, clé `profile-slot-<n>` :
l'emplacement *n* n'est débité qu'une fois, quels que soient les nouveaux essais ou les courses), en
transmettant le jeton de l'acheteur, puis fait passer le compte de `n − 1` à `n` par LWT. Wallet
injoignable ou non configuré (`PROFILE_WALLET_GRPC_ENDPOINT`) ⇒ **`PRF-6003`** (`UNAVAILABLE`,
fail-closed) ; créer dans la limite des emplacements détenus ne l'appelle jamais. Les gems, réclamations
et plafonds restent par **compte** dans le wallet, qui refuse les mises sur ses propres profils : des
profils en plus n'ajoutent aucun pouvoir de curation. Effacer le compte efface ses emplacements.

**Changements de handle payants (#911).** Une dépense de gems sans effet sur le classement ni la
visibilité. Le **premier** changement de handle d'un compte est gratuit ; les suivants coûtent
`PROFILE_HANDLE_CHANGE_PRICE` (100) gems. Un appelant qui ne peut pas dépenser de gems (moins de 18 ans,
ou âge inconnu) change gratuitement, **une fois tous les 30 jours**. `ChangeHandleRequest.expected_price`
doit être le prix affiché par `GetHandleChangePrice` (qui donne aussi le `next_change_at` d'un appelant
restreint). Les refus sont dans la réponse, sans rien changer ni débiter : `CommandResponse.handle_change`
= `PRICE_CHANGED`, `INSUFFICIENT_GEMS` ou `COOLDOWN` (`CHANGED` en cas de succès). Les gems ne sont
débités **qu'après** la réservation du nouveau handle (un handle pris ou invalide n'est jamais débité ;
une réservation impayée est rendue), via le `SpendGems` du wallet (`HANDLE_CHANGE`, `ref_id` = le
profil, clé `handle-change-<n>` selon le numéro du changement du compte : un nouvel essai paie une fois).
Demander le handle actuel du profil ne change rien. Le compteur (`profile.handle_changes`, migration
0019) augmente une fois le changement enregistré ; une panne entre les deux laisse ce changement non
compté, jamais débité deux fois. Les appelants mesh changent gratuitement, sans être comptés. Wallet
injoignable ⇒ `PRF-6003`. Effacer le compte efface le compteur.

**Cadres d'avatar (#913).** Un cadre est un article du catalogue du wallet (`frame.<nom>`) que le
**compte** achète une fois (`PurchaseItem` du wallet) et que chaque **profil** porte ou non.
`EquipAvatarFrame(frame_id)` (le propriétaire du profil) vérifie que le compte le possède
(`ListOwnedItems` du wallet sur le mesh ; sinon **`PRF-6007`** `PERMISSION_DENIED`, `PRF-6003` quand le
wallet ne peut pas répondre) ; un `frame_id` vide l'enlève. Il est enregistré sur le profil et sur la
liste des profils du compte (migration 0020), et chaque lecture porte `avatar_frame_id` (`ProfileView`,
`ProfileSummaryView`, les résultats profil de search). L'app le dessine depuis des visuels embarqués
indexés par id, et il ne change aucune taille, aucun ordre ni aucun classement (charte V5.3 §35).

**Vitrines de badges (#912).** Un profil montre jusqu'à trois badges obtenus par son **compte** (badges du
wallet, p. ex. `badge.continent_eu`), dans l'ordre : `SetBadgeShowcase(badge_ids)` (le propriétaire)
vérifie chacun auprès de `ListOwnedItems` du wallet (famille `BADGE` ; **`PRF-6007`** pour un badge non
obtenu, `PRF-9001` au-delà de trois ou pour un doublon) et garde sa date d'obtention. Enregistrée en
JSON (`profiles.badge_showcase`, migration 0021) ; chaque lecture porte `ProfileView.badges` (`badge_id`,
`awarded_at`). Une liste vide l'efface.

**Bannières animées (#915, #947).** `UpdateBanner` avec `banner_asset_id` seul (sans `banner_url`) désigne
une **vidéo du compte** (la vidéo de l'un de ses posts) : le `GetAsset` de media doit la dire `VIDEO`
READY détenue par le compte (sinon **`PRF-5007`**, `FAILED_PRECONDITION`), et le compte doit posséder
l'article `banner.animated` du catalogue du wallet (500 gems ; sinon **`PRF-6007`**,
`PERMISSION_DENIED`). La bannière est alors cet asset, sans URL d'image fixe ; les lectures client
portent `ProfileView.banner` avec le `MANIFEST` de la vidéo (la boucle) et son `POSTER`, et `banner_url`
est le poster. Le serveur ne coupe ni ne plafonne la vidéo ; l'app la lit sans le son et montre le poster
avec Réduire les animations, en mode Économie d'énergie et en veille. Les deux champs vides effacent la
bannière ; une bannière fixe par URL fonctionne comme avant.

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
| PRF-5004 | `VerificationDocumentInvalid` (un document privé n'est pas un document prêt du demandeur) | 422 | No |
| PRF-5005 | `MediaUnavailable` (media n'a pas pu vérifier un document privé, ni l'asset d'un avatar / d'une bannière) | 503 | **Yes** |
| PRF-5006 | `SupervisionLocked` (un plancher posé par un superviseur tient ce réglage : seul plus strict est permis, #670) | 422 | No |
| PRF-5007 | `MediaAssetNotUsable` (l'asset d'un avatar / d'une bannière n'est pas une image utilisable du compte du profil, #907) | 422 | No |
| PRF-6001 | `NoFreeProfileSlot` (tous les emplacements sont utilisés ; `CreateProfile` le répond dans la réponse, #916) | 422 | No |
| PRF-6002 | `GemSpendingRestricted` (un emplacement ne s'achète qu'adulte, #916) | 403 | No |
| PRF-6003 | `WalletUnavailable` (le wallet n'a pas pu répondre à un achat d'emplacement ou à un changement de handle payant, #916, #911) | 503 | **Yes** |
| PRF-6004 | `HandleChangePriceChanged` (pas le prix affiché ; `ChangeHandle` le répond dans la réponse, #911) | 422 | No |
| PRF-6005 | `HandleChangeCooldown` (un appelant restreint a changé il y a moins de 30 jours ; dans la réponse, #911) | 422 | No |
| PRF-6006 | `InsufficientGems` (dans la réponse, #911) | 422 | No |
| PRF-6007 | `ItemNotOwned` (un article du catalogue que le compte ne possède pas, #913) | 403 | No |
| PRF-9001–9010 | domain / parse / validation | 422 | No |
| SDB-* / RDB-* | storage (delegated) | varies | varies |

---

## 📨 Contrat événementiel & asynchrone

**Publie :**

| Topic | Déclencheur | Clé | Consommateurs |
|---|---|---|---|
| `profile.v1.events` | chaque mutation de cycle de vie — `ProfileCreated` / `ProfileUpdated` / `HandleChanged` / `ProfileVerified` / `ProfileHidden` / `ProfileRestored` / `ProfileDeleted` / `ProfileTierChanged` / `ProfileVisibilityChanged` (`public`/`private` ; `SetVisibility` n'émet que celui-ci) / `ProfileVerificationDecided` (le personnel a tranché une demande de vérification : `account_id`, `approved`, `document_asset_ids` = ses documents privés, `decided_at_ms` ; #777) | `profile_id` | `search` (indexation des profils), `post` (dénormalisation du palier auteur), `social-graph` (projection d'audience pour le contrôle d'accès), `media` (`ProfileVerificationDecided` : les documents privés sont purgés 30 jours après la décision) |

> **Contrat de fil :** un topic versionné unique, tagué en interne sur `type` (convention du service moderation), clé `profile_id` pour l'ordre par-profil. Les événements sont **fins** (ids + horodatages, sans contenu d'affichage) — un consommateur qui a besoin du profil complet l'hydrate via `GetProfileById`. Chaque command handler draine les événements en attente de l'agrégat et les publie **après** l'écriture durable (durable-first ; un publisher no-op couvre la composition sans broker).

**Consomme :**

| Topic | Consumer group | Purpose | On poison/exhaustion |
|---|---|---|---|
| `account.v1.events` | `profile-account-events` | le `DomainEvent` d'account (tagué sur `type`, snake_case) : `account_suspended` / `account_deactivated` / `account_deleted` → masquer **chaque** profil du compte (`HideAccountProfiles`, le type d'événement sert de raison de masquage) ; `account_activated` → restaurer les profils masqués par la suspension ou la désactivation (`RestoreAccountProfiles` ; un masquage pour violation de politique reste). Idempotent par profil : une redélivrance termine un parcours partiel. `account_deleted` efface ensuite les demandes de vérification du compte (`EraseAccountVerifications`, #777 ; idempotent). **Supervision familiale (#670) :** `supervision_limits_set` garde les planchers du compte (`profile.supervision_floors`, migration 0016) et resserre **chaque** profil du compte à ces planchers tout de suite — privé ; audiences des messages / commentaires au moins au plancher (tout le monde < abonnés < mutuels < personne) ; recherche par pseudo, suggestions et recherche par téléphone / e-mail coupées (un QR code / lien fonctionne toujours) — une modification enregistrée à la fois, annoncée comme d'habitude ; `supervision_limits_cleared` supprime les planchers (les réglages gardent leurs valeurs, déverrouillés). Tant que des planchers tiennent, `SetVisibility` / `SetInteractionSettings` / `SetDiscoverySettings` refusent tout ce qui est plus permissif (`PRF-5006`) ; plus strict, et ce qu'aucun plancher ne couvre, restent au titulaire ; un profil que le compte crée entre-temps naît à ces planchers. Idempotent. Autres types = commit no-op | DLQ `account.v1.events.dlq` |
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
| media injoignable (`PROFILE_MEDIA_GRPC_ENDPOINT`) | une demande de vérification avec documents privés échoue | `PRF-5005` (retryable) ; les demandes avec liens seuls ne sont pas touchées ; non définie → documents privés refusés | vérifier media / la NetworkPolicy profile → media |
| wallet injoignable (`PROFILE_WALLET_GRPC_ENDPOINT`) | les emplacements de profil ne s'achètent pas | `PRF-6003` (retryable) ; créer dans la limite des emplacements détenus n'est pas touché ; non définie → tout achat refusé | vérifier wallet / la NetworkPolicy profile → wallet et `SPEND_GEMS_CALLERS` |

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
| `PROFILE_MEDIA_GRPC_ENDPOINT` | No | — | endpoint mesh de media : vérifie les documents privés d'une demande de vérification (#777) et les assets des avatars / bannières à l'écriture ; les livre à la lecture (#907, fail-open). Non définie : les demandes ne prennent que des liens, les images que leurs URLs. |
| `PROFILE_MEDIA_RPC_TIMEOUT_MS` / `PROFILE_MEDIA_CONNECT_TIMEOUT_MS` | No | `500` / `1000` | délais de ces appels. |
| `PROFILE_WALLET_GRPC_ENDPOINT` | No | — | endpoint mesh du wallet (#916, #911) : les gems des emplacements de profil et des changements de handle payants (`SpendGems`, jeton mesh du pod). Non définie : aucun des deux ne s'achète (`PRF-6003`). |
| `PROFILE_WALLET_RPC_TIMEOUT_MS` / `PROFILE_WALLET_CONNECT_TIMEOUT_MS` | No | `1000` / `500` | délais de cet appel. |
| `PROFILE_HANDLE_CHANGE_PRICE` | No | `100` | le prix d'un changement de handle payant en gems (#911 ; le premier changement est gratuit). |
| `PROFILE_MAX_SLOTS` / `PROFILE_SLOT_BASE_PRICE` | No | `5` / `100` | emplacements de profil (#916) : le plafond, et le prix du 2ᵉ emplacement en gems (chacun des suivants double). |
| `PROFILE_BACKFILL_VISIBILITY` | No | non défini | `1`/`true`/`yes` : au démarrage, ré-annonce une fois la visibilité de chaque profil **privé** (`ProfileVisibilityChanged`, #927). Post en déduit qu'il doit passer en SIGNED les images d'un auteur privé (#919), et les profils déjà privés avant que post ne projette cette visibilité ne l'ont jamais ré-annoncée. Public est la valeur par défaut partout, donc les profils publics sont ignorés. Mêmes règles d'exécution unique que le backfill des onglets ci-dessous : à définir le temps d'un déploiement, avec ou avant post → media (core-platform-infra#88), puis à retirer. |
| `PROFILE_BACKFILL_TAB_SETTINGS` | No | non défini | `1`/`true`/`yes` : au démarrage, ré-annonce une fois les réglages d'onglets non par défaut de chaque profil (`ProfileTabSettingsChanged`, #873), pour les consommateurs démarrés après que ces événements ont quitté la rétention de `profile.v1.events` (engagement, post). Chaque réplica le lance, à chaque démarrage (les répétitions sont sans effet, le dernier écrit l'emporte) : à définir le temps d'un déploiement, au premier déploiement, puis à retirer ; lancé pendant que des propriétaires modifient leurs réglages, une ancienne valeur ré-annoncée peut arriver après une plus récente. |

> Le réglage complet `SCYLLA_*` / `REDIS_*` / `KAFKA_*` est documenté dans les crates partagés
> storage/transport. Les profils de TTL `[cache]` sont consommés depuis `infrastructure.toml`, pas via env.

### Features de compilation
- `build.rs` compile `proto/profile/v1/*.proto` et émet le descriptor set de réflexion.

---

## 🚀 Déploiement, migrations & rollback

- **Migrations :** `crates/services/profile/migrations/00{01..21}_*.cql` sur le keyspace `profile`,
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
