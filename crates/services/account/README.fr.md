---
i18n:
  source: ./README.md
  source_sha256: 581a58cac08db875ae6869feef9f1fcdf21b199f7efd156a00454d1ca3f408ae
  translated_at: 2026-10-08
  status: complete
---
> 🇫🇷 Traduction française — la version **anglaise** [`README.md`](./README.md) fait foi.
> En cas de divergence, l'anglais prime. Les contrats (codes d'erreur, variables
> d'environnement, topics Kafka, identifiants) sont volontairement laissés en anglais.

# `account` — Cycle de vie de l'identité privée : le registre de référence de la plateforme sur *qui* est une personne

> **Fiche service**
>
> | | |
> |---|---|
> | **Propriétaire** | `<TODO: équipe>` · `<TODO: #canal-slack>` |
> | **Astreinte / escalade** | `<TODO: rotation-astreinte>` → `<TODO: politique-escalade>` |
> | **Palier (Tier)** | **TIER-0** — l'identité est sur le chemin critique d'authentification |
> | **Binaire déployable** | `crates/apps/account-server` (crate bibliothèque : `crates/services/account`) |
> | **Bases de données** | PostgreSQL / compatible CockroachDB (db `account`) |
> | **Asynchrone** | publie `account.v1.events` (AccountCreated/Activated/Suspended/Deleted/…) · ne consomme rien |
> | **Appelants amont** | `<TODO: passerelle d'authentification>`, `profile` (via événements) |
> | **Dépendances aval** | PostgreSQL/CockroachDB |
> | **SLO** | `<TODO : 99,95 %>` dispo · lecture de statut p99 `<TODO>` · écriture p99 `<TODO>` |

---

## 🎯 Vue d'ensemble & rôle du service

`account` gère l'intégralité du **cycle de vie privé d'une personne physique** sur la plateforme :
vérification d'identité, identifiants, conformité KYC, droits RGPD et contrôle d'accès par rôles. C'est
le registre de référence de l'existence et du statut d'un compte — le middleware d'authentification de
la passerelle résout chaque requête contre lui.

Le problème difficile qu'il résout est la **correction sous concurrence et contrainte de conformité** :
l'état du compte est une machine à états stricte (cycle de vie + KYC), chaque mutation doit être
sérialisable face à des écrivains concurrents, et le traitement des données personnelles est encadré
légalement (RGPD art. 17 / art. 20). Il résout cela avec un **agrégat à verrouillage optimiste**
(compare-and-swap sur un compteur de version) sur CockroachDB, et une couche de domaine qui rejette
d'emblée les transitions de statut illégales.

**Objectifs fondamentaux :** ne jamais perdre une écriture face à une mise à jour concurrente ; ne
jamais autoriser une transition de cycle de vie illégale ; ne jamais stocker un secret en clair. L'état
financier est explicitement **hors périmètre** — il appartient au service dédié `ledger` (SRP à grande
échelle).

---

## 📐 Architecture & concepts

Architecture Clean / DDD (`domain` → `application` → `infrastructure`) : la couche `domain` est exempte
d'E/S, `application` contient des handlers CQRS purs (17 commandes, 5 requêtes), toutes les E/S vivent
dans `infrastructure` (adaptateur Postgres + gRPC tonic).

```
gRPC (tonic) ─► AccountServiceHandler ─► Command/Query bus ─► AccountRepository (port)
                                                                      │
                                                          PostgreSQL / CockroachDB
                                                          (optimistic lock: version CAS)
                  AccountCreated/… ─► account.v1.events (Kafka) ─► profile, …
```

**Verrouillage optimiste.** Chaque écriture est `UPDATE accounts SET …, version = version + 1 WHERE id
= $1 AND version = $n`. Zéro ligne affectée ⇒ `ConcurrentModification` (réessayable, mappé sur
`ABORTED`). `AccountId` (UUIDv7) implémente `ShardKey` ; toutes les écritures passent par
`run_on_shard(&account_id, …)` pour un routage transactionnel agnostique de la topologie.

> **Invariants** (et où ils sont imposés, dans l'agrégat `Account`) : les transitions de cycle de vie
> (`PendingVerification→Active→Suspended→Active`, `Active→Deactivated→Active` — le titulaire se
> reconnecte, `→Deleted` ; un compte suspendu ne peut pas se désactiver) et les transitions KYC
> (`NotStarted→Submitted→InReview→Approved|Rejected`) sont imposées dans l'agrégat `Account` — une
> transition illégale renvoie `FAILED_PRECONDITION`. L'unicité sur `(identity_id, email)` rend
> `CreateAccount` idempotent.
>
> **Comptes téléphone seul** (inscription par SMS du mode invité) : un compte a un e-mail **ou** un
> numéro de téléphone (ou les deux). L'e-mail est facultatif (`NULL` pour un compte téléphone seul ;
> `AccountView.email` est alors vide, et `account.created` ne porte pas d'`email`), un numéro
> appartient à un compte au plus (`ACC-1004`), et `VerifyPhone` active un compte
> `PendingVerification` comme le fait `VerifyEmail`.

---

## 📊 Objectifs de niveau de service (SLO)

| SLI | Objectif | Fenêtre | Mesuré par |
|---|---|---|---|
| Disponibilité (non-`UNAVAILABLE`) | `<TODO : 99,95 %>` | glissante 30 j | métriques de statut gRPC |
| `GetAccountStatus` p99 (chemin d'auth chaud) | `< <TODO> ms` | 1 h | histogramme gRPC |
| Écriture p99 (commit CAS) | `< <TODO> ms` | 1 h | histogramme d'exécution Postgres |
| Durabilité | aucune écriture acquittée perdue | — | commit sérialisable CockroachDB |

**Budget d'erreur :** `<TODO>`. **En cas d'épuisement :** `<TODO>`. `GetAccountStatus` est le SLI le
plus serré — la passerelle d'authentification l'appelle sur le chemin de requête, donc sa latence est
multipliée sur toute la flotte.

---

## 🔗 Dépendances & rayon d'impact (blast radius)

**Aval — ce dont `account` a besoin pour fonctionner :**

| Dependency | Purpose | If down → | Degradation |
|---|---|---|---|
| PostgreSQL / CockroachDB | registre de référence | toutes les lectures + écritures échouent | **Dur** — `UNAVAILABLE` |
| Kafka | émission d'événements (`account.v1.events`) | les projections aval stagnent | **Souple** — les écritures committent quand même |

**Amont — qui dépend de `account` (rayon d'impact si `account` tombe) :**

| Caller | Uses | Impact visible utilisateur si `account` est indisponible |
|---|---|---|
| `<TODO: passerelle d'auth>` | `GetAccountStatus` | **connexions/autorisations en échec sur toute la plateforme** |
| `profile` | consomme `account.v1.events` | le masquage de profil à la suspension/désactivation/suppression s'arrête |

> **Chemin critique ?** **Oui** — `GetAccountStatus` est sur le chemin d'authentification synchrone ;
> une panne de `account` dégrade chaque requête authentifiée de toute la flotte.

---

## 🔌 Interfaces publiques & contrat d'API

### gRPC — `account.v1.AccountService`

```protobuf
service AccountService {
  // Commands (all return CommandResponse { success, account_id })
  rpc CreateAccount (CreateAccountRequest) returns (CommandResponse);
  rpc VerifyEmail (VerifyEmailRequest) returns (CommandResponse);   // mesh uniquement : auth, après une preuve (id_token vérifié ou code)
  rpc VerifyPhone (VerifyPhoneRequest) returns (CommandResponse);   // mesh uniquement : auth, après un code SMS
  rpc ChangeEmail (ChangeEmailRequest) returns (CommandResponse);   // mesh uniquement : auth, après un code envoyé à la nouvelle adresse (#651)
  rpc ChangePhone (ChangePhoneRequest) returns (CommandResponse);   // mesh uniquement : idem, par SMS — les deux posent et vérifient d'un coup ; ACC-1003/1004 si déjà prise
  rpc ChangePassword (ChangePasswordRequest) returns (CommandResponse);
  rpc EnrollMfa (EnrollMfaRequest) returns (CommandResponse);
  rpc RevokeMfa (RevokeMfaRequest) returns (CommandResponse);
  rpc GetMfaSecret (GetMfaSecretRequest) returns (MfaSecretView);
  rpc ConsumeRecoveryCode (ConsumeRecoveryCodeRequest) returns (CommandResponse);
  rpc ReplaceRecoveryCodes (ReplaceRecoveryCodesRequest) returns (CommandResponse);
  rpc UpdateKycStatus (UpdateKycStatusRequest) returns (CommandResponse);
  rpc SuspendAccount (SuspendAccountRequest) returns (CommandResponse);
  rpc ReactivateAccount (ReactivateAccountRequest) returns (CommandResponse);
  rpc DeactivateAccount (DeactivateAccountRequest) returns (CommandResponse);
  rpc ResumeDeactivatedAccount (ResumeDeactivatedAccountRequest) returns (CommandResponse); // auth, au Login
  rpc RecordLogin (RecordLoginRequest) returns (CommandResponse);
  rpc RecordFailedLogin (RecordFailedLoginRequest) returns (CommandResponse);
  rpc RequestGdprDeletion (RequestGdprDeletionRequest) returns (CommandResponse);
  rpc CancelGdprDeletion (CancelGdprDeletionRequest) returns (CommandResponse);  // pendant le délai de grâce ; se reconnecter annule aussi
  rpc AnonymizeAccount (AnonymizeAccountRequest) returns (CommandResponse);
  rpc RequestDataExport (RequestDataExportRequest) returns (CommandResponse);
  rpc AssignRole (AssignRoleRequest) returns (CommandResponse);
  rpc RevokeRole (RevokeRoleRequest) returns (CommandResponse);
  // Queries
  rpc GetAccountById (GetAccountByIdRequest) returns (AccountView);
  rpc GetAccountByIdentityId (GetAccountByIdentityIdRequest) returns (AccountView);
  rpc GetAccountByEmail      (GetAccountByEmailRequest)      returns (AccountView);   // mesh uniquement (inscription d'auth) : jamais sur l'edge
  rpc GetAccountByPhone      (GetAccountByPhoneRequest)      returns (AccountView);   // mesh uniquement (inscription d'auth par téléphone)
  rpc GetAccountStatus (GetAccountStatusRequest) returns (AccountStatusView); // auth hot path
  rpc SetDateOfBirth (SetDateOfBirthRequest) returns (AccountView);          // une fois, si absente ; âge minimum 13 ans (16 en AU)
  rpc GetGdprRecord (GetGdprRecordRequest) returns (GdprRecordView);          // le sien propre en périphérie
  rpc FindProfilesByContacts (FindProfilesByContactsRequest) returns (FindProfilesByContactsResponse); // #661, le compte de l'appelant en périphérie
  // Supervision familiale (#670), le compte de l'appelant en périphérie
  rpc CreateSupervisionInvite (CreateSupervisionInviteRequest) returns (SupervisionInviteView);
  rpc AcceptSupervisionInvite (AcceptSupervisionInviteRequest) returns (SupervisionView);
  rpc ListSupervisions (ListSupervisionsRequest) returns (ListSupervisionsResponse);
  rpc EndSupervision (EndSupervisionRequest) returns (ListSupervisionsResponse);
  rpc SetSupervisionLimits (SetSupervisionLimitsRequest) returns (SupervisionLimitsView);
  rpc GetSupervisionLimits (GetSupervisionLimitsRequest) returns (SupervisionLimitsView);
  rpc ReportScreenTime (ReportScreenTimeRequest) returns (ScreenTimeView);
  rpc GetSupervisionOverview (GetSupervisionOverviewRequest) returns (SupervisionOverview);
  rpc ListSupervisedConnections (ListSupervisedConnectionsRequest) returns (ListSupervisedConnectionsResponse);
  rpc ListSupervisedReports (ListSupervisedReportsRequest) returns (ListSupervisedReportsResponse);
  rpc UpdateConsents (UpdateConsentsRequest) returns (GdprRecordView);         // GDPR Art. 7 consents + history
  rpc ListAccountsByStatus (ListAccountsByStatusRequest) returns (ListAccountsByStatusResponse);
}
```

> **Contrat de sérialisation / enum :** les enums sont **basés sur 1** (pas de zéro `UNSPECIFIED`).
> `AccountStatus` `PENDING_VERIFICATION=1…DELETED=5` ; `KycStatus` `NOT_STARTED=1…REJECTED=5` ;
> `AccountRole` `USER=1…SUPER_ADMIN=6`. **Valeurs par défaut côté handler** pour les champs absents du
> proto : `RecordFailedLogin.max_attempts=5`, `lockout_duration_secs=900`,
> `RequestGdprDeletion.retention_days=30`.

**Sécurité à la frontière :** mots de passe stockés en Argon2id uniquement (jamais le clair accepté) ;
les champs secrets suppriment `Display`/`Debug` et portent `#[serde(skip)]`.

**Retrouver ses contacts (#661).** `FindProfilesByContacts(account_id, email_sha256[], phone_sha256[])`
reçoit les empreintes SHA-256 des contacts d'un carnet d'adresses — emails en minuscules et sans espaces,
numéros E.164 — 1000 au plus par appel, et **n'en garde aucune**. Une empreinte correspond à l'email /
au téléphone **vérifié** d'un compte **actif** : les empreintes sont des colonnes générées
(`account_contact_sha256`, migration 0007) indexées pour les seuls contacts vérifiés, si bien qu'elles
suivent chaque écriture. Les profils des comptes trouvés sont lus via le mesh (profile, comme le mesh :
statut et réglages de découvrabilité) et gardés s'ils sont actifs et trouvables par ce canal
(`by_email` / `by_phone`), jamais ceux de l'appelant, jamais un profil que les profils de l'appelant
bloquent ou qui les bloque (`CheckAccess` de social-graph ; une réponse absente compte comme masquée).
Chaque résultat nomme l'empreinte dont il vient, pour que l'app affiche le contact. Profile ou
social-graph injoignable ⇒ `ACC-7006` (`UNAVAILABLE`, rejouable). Les empreintes de numéros ne cachent
rien (un plan de numérotation se hache en quelques minutes) : chaque compte peut chercher **5000
empreintes par jour UTC** (`contact_lookup_quota`, migration 0008) ; chaque empreinte envoyée compte,
trouvée ou non, réservée atomiquement avant toute recherche ; au-delà ⇒ `ACC-7007`
(`RESOURCE_EXHAUSTED`, `retry-after-secs` = jusqu'au prochain minuit UTC). Les numéros stockés sont déjà
en E.164 (validés à chaque écriture) : la colonne d'empreinte des téléphones n'a pas besoin de
normalisation.

**Supervision familiale (#670), partie 1 : l'appairage.** Un parent (un **adulte connu** : 18 ans ou
plus selon sa date de naissance) s'appaire avec un ado (13–17 ans). L'un ou l'autre appelle
`CreateSupervisionInvite(account_id, role)` (son propre rôle) et partage le code (10 caractères en base32
de Crockford, ~50 bits ; affiché ou en QR code ; **usage unique, 24 heures** ; la saisie ignore la casse,
les espaces et les tirets, et lit O/I/L comme 0/1/1) ; l'autre appelle
`AcceptSupervisionInvite(account_id, code)` et prend l'autre rôle. Les âges sont lus dans la date de
naissance des deux côtés (un âge inconnu ne convient à aucun rôle : `ACC-3002` ; un ado devenu majeur
entre-temps annule son invitation : `ACC-3001`) ; son propre code donne `ACC-3006` ; un ado a **deux
superviseurs au plus** (`ACC-3003`) ; un parent peut superviser plusieurs ados. Après ces vérifications,
l'invitation est **réservée** à celui qui l'accepte par un compare-and-set (`claimed_by`, migration
0010) : deux comptes en concurrence sur un même code ne s'appairent jamais tous les deux (l'autre reçoit
`ACC-3001`), une nouvelle tentative du même compte aboutit, et un mauvais destinataire (qui échoue à une
vérification) ne la consomme jamais. Un code inconnu ou expiré compte contre le compte : **10 par
heure**, puis `ACC-3007` (`RESOURCE_EXHAUSTED`) — l'énumération est fermée pour une chose aussi sensible
que la supervision d'un mineur. `ListSupervisions`
montre le lien **aux deux côtés** (le compte et les profils actifs de l'autre, depuis quand) — l'ado voit
toujours qui le supervise. `EndSupervision(account_id, other_account_id)` y met fin, de l'un ou l'autre
côté (`ACC-3005` s'il n'y en a pas). Une supervision prend fin d'elle-même quand l'ado a 18 ans (le
passage périodique, `ACCOUNT_SUPERVISION_SWEEP_INTERVAL_SECS`, qui supprime aussi les codes expirés) et
quand l'un des comptes est effacé (le janitor RGPD les termine d'abord ; un échec laisse le compte pour
son passage suivant). Stockage (migration 0009) : `supervision_invites` sur le shard du code ;
`supervisions` sur le shard de **l'ado** (un verrou consultatif garde « deux au plus » atomique ; la
majorité se lit en joignant la ligne `accounts` de l'ado, sur ce même shard), avec
`supervisions_by_supervisor` sur le shard du superviseur — écrit ado d'abord, idempotent, une entrée
d'index périmée supprimée à la lecture. Chaque début et fin est publié (`SupervisionStarted` /
`SupervisionEnded { ended_by: by_teen | by_supervisor | came_of_age | account_deleted }`, `account_id` =
l'ado) avec les ids de profil des deux côtés, pour que chacun puisse être prévenu. Edge : le compte de
l'appelant.

**Limites de supervision (#670 partie 2).** Un jeu **partagé** par ado (`supervision_limits`, migration
0011, sur le shard de l'ado) : l'un ou l'autre superviseur le pose avec
`SetSupervisionLimits(account_id, teen_account_id, limits)` et la dernière modification s'applique
(`set_by`, `set_at` gardés). Chaque limite est un **plancher** que l'ado ne peut que rendre plus strict :
`private_account` ; l'audience la plus large pour `messages` / `comments` (`followers` < `mutuals` <
`no_one`) ; `hidden_from_search` (recherche par pseudo, suggestions, contacts — un QR code ou un lien
partagé fonctionne toujours) ; `daily_minutes` (15 à 1440, sinon `ACC-9001`). Pas son superviseur ⇒
`ACC-3005`. L'ado et chaque superviseur les lisent avec `GetSupervisionLimits`. Chaque changement est
publié (`SupervisionLimitsSet`, avec les ids de profil de l'ado) : profile resserre les réglages de
l'ado jusqu'aux planchers et refuse de les assouplir (partie 2b). Quand la **dernière** supervision de
l'ado prend fin (par l'un ou l'autre côté, à 18 ans, à l'effacement), les limites sont levées
(`SupervisionLimitsCleared`) : les réglages gardent leurs valeurs, déverrouillés. **Temps d'écran :**
l'app déclare son usage avec `ReportScreenTime(account_id, minutes ≤ 15, timezone)` et apprend le total
du jour sur tous les appareils et si la limite est atteinte (elle affiche alors l'écran de pause ; le
serveur ne coupe pas les requêtes). Seul un ado avec une limite quotidienne est compté (`screen_time`,
par compte et jour local, gardé 4 semaines).

**La vue du superviseur (#670 partie 3).** Chaque superviseur et l'ado lui-même voient **la même
vue** (`teen_account_id` vide : celle de l'appelant ; toute autre personne ⇒ `ACC-3005`) :
`GetSupervisionOverview` — les limites, les 7 derniers jours avec du temps compté (le plus récent
d'abord, en jours locaux de l'ado) et les profils actifs de l'ado ;
`ListSupervisedConnections(profile_id, kind)` — une page des abonnements, abonnés ou profils bloqués
d'un profil de l'ado, lus dans social-graph en tant que mesh (quelle que soit la confidentialité des
listes ; un profil qui n'est pas à l'ado ⇒ `ACC-3005` ; un superviseur bloqué par l'ado n'est jamais
listé) ; `ListSupervisedReports` — une page des
signalements faits par l'ado, du plus récent au plus ancien : **qui ou quoi, quand, et la décision**
(`under_review` / `action_taken` / `no_violation`), jamais les mots de l'ado (le
`ListReportsByReporter` de moderation, réservé au mesh, ne les renvoie pas). Un ado doit pouvoir
signaler sans être vu : un signalement `self_harm`, `csam` ou `ncii`, ou visant le contenu d'un
superviseur, n'est jamais listé (moderation les écarte, pagination comprise ; l'ado les voit toujours
dans son propre `ListMyReports`). Pages : 50 par défaut,
100 au plus. social-graph ou moderation injoignable ⇒ `ACC-3008` (`UNAVAILABLE`, rejouable).

**Export de données RGPD (#653, art. 15/20).** `RequestDataExport` marque l'export en attente ; la
**passe d'export** (`ExportDueData`) construit alors, pour chaque compte en attente, un ZIP de fichiers
JSON — le dossier de compte du titulaire (coordonnées, consentements, réglages de connexion ; **ni**
empreinte de mot de passe, ni matériel MFA, ni champ interne), les fichiers des autres services
(`ExportSources` : profils, posts, commentaires, réactions, recherches récentes, graphe social, conversations, liens vers les
médias), un `README.txt` — le stocke en privé (`exports/<compte>/<id>.zip`, `S3ExportStore` : clés
statiques, `ACCOUNT_EXPORT_*`) et inscrit sa **clé d'objet** dans le dossier RGPD — jamais un lien signé,
qui est un secret au porteur : `GetGdprRecord` signe le lien à la lecture pour ce qu'il reste des 7 jours
(`data_export_url` / `data_export_expires_at`, montré au titulaire et à auth ; aucun tant qu'une demande plus
récente est en attente, une fois expiré, ou sans le stockage). `GdprDataExportCompleted` (sans le lien — c'est un secret) permet à auth de
l'envoyer par e-mail. Une source en échec laisse l'export en attente (jamais d'archive partielle) et la
passe le réessaie (`ACC-7005`) ; l'écriture est versionnée, donc une demande faite pendant la
construction d'un export est reconstruite. Les comptes en attente viennent d'un index partiel (migration
0005). Les sources sont les RPC **mesh uniquement** des autres services (`MeshExportPeers`, chaque page,
chaque message converti en JSON via le jeu de descripteurs du service) : par profil son profil, ses posts,
commentaires (`ListCommentsByAuthor`), réactions (`ListReactionsByProfile`), recherches récentes
(`ListRecentSearches` de search, via le mesh, #816), graphe social et conversations
(`ListConversationsByMember`, puis : une conversation `DIRECT` en entier via `GetHistory` ; un groupe ou un
canal via le `GetFormerMemberHistory` mesh de chat, ses propres messages et ceux des autres en placeholders
`{"from": "another member"}` — « directe » est le type de la conversation, jamais la taille de ses membres ;
les groupes quittés (#656, `left_at_ms`) sont inclus, jusqu'au départ, sans leurs membres) ; pour le compte, ses
médias (`ListAssetsByOwner`, liens valables 7 jours). Le serveur lance la passe toutes les
`ACCOUNT_EXPORT_INTERVAL_SECS` quand le stockage est configuré (`ACCOUNT_EXPORT_BUCKET`,
core-platform-infra#28).

**La MFA appartient à auth (#649) ; account ne fait que la conserver.** Toutes les RPC MFA sont **mesh
uniquement** : le titulaire active et désactive la connexion en deux étapes via auth, après un step-up. auth
chiffre la graine TOTP avec sa propre clé (AES-256-GCM ; account stocke le chiffré sans jamais le lire),
hache chaque code de secours et vérifie les codes. `EnrollMfa` prend le chiffré et au moins 6 empreintes de
codes distinctes (`ACC-9001` sinon, `ACC-5001` si la MFA est déjà active). `GetMfaSecret` remet à auth le
chiffré et le nombre de codes restants. `ConsumeRecoveryCode` dépense un code par son empreinte, une seule
fois : l'écriture est versionnée, donc de deux dépenses concurrentes l'une échoue, et un code déjà utilisé ou
inconnu donne `ACC-5003`. `ReplaceRecoveryCodes` prend un jeu régénéré. `AccountView.mfa_enrolled` /
`mfa_recovery_codes_remaining` indiquent au titulaire où il en est.

### Ports Rust (contrat hexagonal)

```rust
pub trait AccountRepository: Send + Sync + 'static { /* save (CAS), find_by_id, find_by_identity_id, … */ }
```

### Contrat d'erreur

| Range / variant | gRPC status |
|---|---|
| `AccountNotFound`, `RoleNotAssigned` | `NOT_FOUND` |
| `IdentityAlreadyRegistered`, `EmailAlreadyRegistered`, `MfaAlreadyEnrolled`, `RoleAlreadyAssigned`, `GdprDeletionAlreadyRequested`, `EmailAlreadyVerified` | `ALREADY_EXISTS` |
| `ConcurrentModification` | `ABORTED` (**retryable**) |
| `AccountNotActive`, `InvalidStatusTransition`, `InvalidKycTransition`, `MfaNotEnrolled`, `RecoveryCodeInvalid`, `AccountAlreadyAnonymized` | `FAILED_PRECONDITION` |
| `Validation`, `InvalidAccountRole/KycStatus/AccountStatus` | `INVALID_ARGUMENT` |
| `Storage` | `UNAVAILABLE` |

Les codes stables vont de `ACC-1xxx` (lifecycle) à `ACC-9xxx` (identifiers), via le crate partagé `error`.

---

## 📨 Contrat événementiel & asynchrone

> Les topics Kafka sont une API. Un changement de schéma ici casse les consommateurs exactement comme un
> changement de proto.

**Publie :**

| Topic | Carries (event kinds) | Key | Consumers |
|---|---|---|---|
| `account.v1.events` | `AccountCreated`, `AccountActivated`, `AccountSuspended`, `AccountDeactivated`, `AccountDeleted`, `EmailChanged`, `EmailVerified`, `PhoneChanged`, `PasswordChanged`, `KycStatusChanged`, `MfaEnrolled`, `MfaRevoked`, `GdprDeletionRequested`, `GdprDataExportRequested`, `GdprDeletionCancelled`, `GdprDataExportCompleted`, `ConsentsUpdated`, `DateOfBirthSet`, `SupervisionStarted`, `SupervisionEnded`, `SupervisionLimitsSet`, `SupervisionLimitsCleared` (#670 ; `account_id` = l'ado) | `account_id` | `profile` (suspend/deactivate/delete → masquer ; activate → restaurer) |

**Consomme :** rien — `account` est un producteur d'événements pur.

> **Contrat d'exécution :** les événements sont publiés en best-effort après le commit durable ; un échec
> Kafka ne fait pas échouer la commande. Les consommateurs (p. ex. `profile`) gèrent leur propre
> traitement at-least-once sous `run_consumer` et dead-letter vers `account.v1.events.dlq`.

---

## 🌩️ Modes de défaillance & dégradation

| Failure | Symptom | Service behavior | Operator action |
|---|---|---|---|
| Postgres/CockroachDB indisponible | toutes les RPC échouent | **Échec dur** — `UNAVAILABLE` ; rien d'acquitté, rien de perdu | vérifier le cluster DB / les ranges |
| Contention d'écriture sur un compte chaud | `ConcurrentModification` (`ABORTED`) | le CAS rejette l'écrivain périmé ; le client réessaie | aucune — comportement correct ; investiguer les tempêtes de retry |
| Kafka indisponible | projections aval périmées | **Souple** — les commits réussissent, événements bufferisés/abandonnés | vérifier les brokers ; rejeu côté aval |

**Backpressure & limites.** `ListAccountsByStatus` est paginée. Le verrouillage après échecs de
connexion (`max_attempts` défaut 5, `lockout_duration_secs` défaut 900) freine le credential-stuffing à
la couche domaine.

---

## 📦 Intégration & utilisation

```toml
[dependencies]
account = { path = "crates/services/account" }
```

Bibliothèque uniquement. Implémente [`service_runtime::Service`](../../platform/service-runtime/README.md)
sous le nom `account::service::AccountService` — `build` construit le pool PostgreSQL via `PgPoolBuilder`
et câble les bus CQRS, `register` ajoute les services gRPC + réflexion, `health_probes` vérifie Postgres
(le pool `Arc`-backed est partagé avec la sonde).

### Bootstrap (`crates/apps/account-server`)

```rust
use std::net::SocketAddr;
use account::service::AccountService;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr: SocketAddr = std::env::var("ACCOUNT_GRPC_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:50059".to_owned())
        .parse()?;
    service_runtime::serve::<AccountService>(addr).await
}
```

---

## ⚙️ Configuration & environnement d'exécution

### Variables d'infrastructure héritées

| Variable | Required | Default | Description |
|---|---|---|---|
| `POSTGRES_*` (URL/pool/timeouts) | **Yes** | — | CockroachDB-compatible connection; see the `postgres-storage` crate. |
| `KAFKA_BROKERS` | **Yes** | — | Kafka bootstrap brokers for `account.v1.events`. |
| `ACCOUNT_GRPC_ADDR` | No | `0.0.0.0:50059` | gRPC bind address. |
| `ACCOUNT_REQUIRE_STEP_UP` | No | `false` | `DeactivateAccount` et `RequestGdprDeletion` en périphérie exigent une preuve d'identifiant de moins de 5 min (l'`auth_time` du jeton, issu de `auth.v1.Login` / `VerifyCredentials`) ; sinon `PERMISSION_DENIED` `step_up_required…`. À activer une fois que les clients font le step-up. |
| `ACCOUNT_GDPR_JANITOR_INTERVAL_SECS` | No | `3600` | Fréquence à laquelle account-server anonymise les comptes dont le délai de grâce d'effacement (30 jours) est écoulé ; `0` désactive le janitor. Sûr sur chaque réplique (CAS optimiste). |
| `ACCOUNT_SUPERVISION_SWEEP_INTERVAL_SECS` | No | `3600` | Fréquence à laquelle les supervisions dont l'ado a eu 18 ans prennent fin et les invitations expirées sont supprimées (#670) ; `0` le désactive. Idempotent, sûr sur chaque réplique. |
| `ACCOUNT_EXPORT_BUCKET` · `ACCOUNT_EXPORT_S3_ENDPOINT` · `ACCOUNT_EXPORT_S3_PUBLIC_ENDPOINT` · `ACCOUNT_EXPORT_S3_REGION` | Non | non défini · `https://s3.amazonaws.com` · = endpoint · `us-east-1` | Le stockage des exports RGPD (#653). Bucket non défini : les exports restent en attente. |
| `ACCOUNT_EXPORT_S3_ACCESS_KEY` · `ACCOUNT_EXPORT_S3_SECRET_KEY` | Non | non défini | Clés statiques de ce bucket (un presign de 7 jours exige des identifiants hors session). |
| `ACCOUNT_EXPORT_INTERVAL_SECS` | Non | `300` | Fréquence de la passe d'export ; `0` la désactive. |
| `ACCOUNT_SEARCH_GRPC_ENDPOINT` | Non | `http://localhost:50062` | Adresse mesh de search : les recherches récentes des profils dans l'export (#816). |
| `ACCOUNT_MODERATION_GRPC_ENDPOINT` | Non | `http://localhost:50061` | Adresse mesh de moderation : les signalements d'un ado supervisé (#670). Injoignable : `ListSupervisedReports` répond `ACC-3008`. |
| `ACCOUNT_{PROFILE,POST,COMMENT,ENGAGEMENT,SOCIAL_GRAPH,CHAT,MEDIA}_GRPC_ENDPOINT` | Non | `http://localhost:<port>` | Les sources mesh de l'export. Une source injoignable laisse l'export en attente. |

> Le réglage complet connexion/timeout/pool vit dans les crates partagés `postgres-storage` et `transport`.

### Features de compilation
- `build.rs` compile `proto/account/v1/*.proto` et émet le descriptor set de réflexion.

---

## 🚀 Déploiement, migrations & rollback

- **Migrations :** `crates/services/account/migrations/*.sql` (sémantique ANSI, compatible CockroachDB,
  PK UUIDv7 pour un clustering favorable aux ranges). Appliquer **avant** de déployer un nouveau binaire.
- **Déploiement :** `<TODO : rolling / canary>`. Service sans état ; sûr à déployer.
- **Rollback :** `<TODO : confirmer que les migrations sont rétro-compatibles avec le binaire N-1>`.
- **Piège conformité :** `AnonymizeAccount` est irréversible (écrasement des PII) — ne jamais l'exécuter
  dans le cadre d'un rollback/rejeu.

---

## 📈 Télémétrie, performance & métriques

- **Runtime :** Tokio multi-thread. Subscriber global tracing/OTel installé avant `serve`.

| Signal | Why it matters | Suggested alert |
|---|---|---|
| `GetAccountStatus` p99 | auth-path latency, fleet-amplified | p99 > SLO ⇒ page |
| `ConcurrentModification` rate | write contention / retry storms | sustained spike ⇒ investigate hot accounts |
| `account.v1.events` publish failures | downstream projection drift | sustained rate ⇒ check Kafka |
| Postgres exec errors | DB health | any spike ⇒ check cluster |

---

## 🛠️ Développement local

```bash
cargo build -p account && cargo clippy -p account --all-targets
cargo test  -p account
docker compose up -d postgres                 # repo-root compose
for f in crates/services/account/migrations/*.sql; do psql -f "$f"; done
```

---

## 🚨 Dépannage & runbook

> Format : **symptôme → cause racine → mitigation.**

**1. `ABORTED: ConcurrentModification` à chaque écriture sur un même compte.**
Cause racine : deux écrivains en course sur le CAS de version, ou un client qui réessaie sans relire la
`version` courante. Mitigation : les clients doivent relire l'agrégat et réessayer avec la version
fraîche ; une tempête persistante pointe vers une boucle de retry boguée, pas vers la DB.

**2. `FAILED_PRECONDITION: InvalidStatusTransition`.**
Cause racine : la transition de cycle de vie/KYC demandée est illégale depuis l'état courant (p. ex.
réactiver un compte `Deleted`). Mitigation : interroger le `status`/`kyc_status` courant via
`GetAccountById` ; la machine à états de la §Architecture définit les arêtes légales.

**3. Profil non masqué après une suspension/suppression.**
Cause racine : l'événement a bien été publié, mais le consommateur `account.v1.events` de `profile` est
en retard ou a dead-lettered l'enregistrement. Mitigation : vérifier le lag du consumer-group et
`account.v1.events.dlq` ; l'écriture du compte elle-même est durable quoi qu'il arrive.
