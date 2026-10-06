---
i18n:
  source: ./README.md
  source_sha256: 55dfce8bdaba1ed7ceaf0bedf4b1a920f4ae210df8bd29976e198f9ea21b365e
  translated_at: 2026-10-06
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
| `account.v1.events` | `AccountCreated`, `AccountActivated`, `AccountSuspended`, `AccountDeactivated`, `AccountDeleted`, `EmailChanged`, `EmailVerified`, `PhoneChanged`, `PasswordChanged`, `KycStatusChanged`, `MfaEnrolled`, `MfaRevoked`, `GdprDeletionRequested`, `GdprDataExportRequested`, `GdprDeletionCancelled`, `GdprDataExportCompleted`, `ConsentsUpdated`, `DateOfBirthSet` | `account_id` | `profile` (suspend/deactivate/delete → masquer ; activate → restaurer) |

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
| `ACCOUNT_EXPORT_BUCKET` · `ACCOUNT_EXPORT_S3_ENDPOINT` · `ACCOUNT_EXPORT_S3_PUBLIC_ENDPOINT` · `ACCOUNT_EXPORT_S3_REGION` | Non | non défini · `https://s3.amazonaws.com` · = endpoint · `us-east-1` | Le stockage des exports RGPD (#653). Bucket non défini : les exports restent en attente. |
| `ACCOUNT_EXPORT_S3_ACCESS_KEY` · `ACCOUNT_EXPORT_S3_SECRET_KEY` | Non | non défini | Clés statiques de ce bucket (un presign de 7 jours exige des identifiants hors session). |
| `ACCOUNT_EXPORT_INTERVAL_SECS` | Non | `300` | Fréquence de la passe d'export ; `0` la désactive. |
| `ACCOUNT_SEARCH_GRPC_ENDPOINT` | Non | `http://localhost:50062` | Adresse mesh de search : les recherches récentes des profils dans l'export (#816). |
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
