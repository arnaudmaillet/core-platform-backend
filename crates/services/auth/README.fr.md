---
i18n:
  source: ./README.md
  source_sha256: 4ce40be949f9fb3e44cd590736bb272c36f011d947fed5dc49fbad08b2d57c52
  translated_at: 2026-10-05
  status: complete
---
> 🇫🇷 Traduction française — la version **anglaise** [`README.md`](./README.md) fait foi.
> En cas de divergence, l'anglais prime. Les contrats (codes d'erreur, variables
> d'environnement, topics Kafka, identifiants) sont volontairement laissés en anglais.

# `auth` — Frontière d'authentification : émettre, suivre et révoquer des sessions sans accès base à chaque requête

> **Fiche service** &nbsp;·&nbsp; CORE
>
> | | |
> |---|---|
> | **Équipe** | `<TODO: team>` · `<TODO: #slack-channel>` |
> | **Astreinte / escalade** | `<TODO: oncall-rotation>` → `<TODO: escalation-policy>` |
> | **Tier** | **TIER-0** — chaque requête authentifiée dépend des jetons émis par ce service |
> | **Déployable** | `crates/apps/auth-server` (crate bibliothèque : `crates/services/auth`) |
> | **Stockage** | PostgreSQL/CockroachDB (db `auth`) · Redis Cluster (sessions/blacklist) |
> | **Asynchrone** | publie `auth.v1.events` (SessionIssued/SessionRevoked/SubjectLinked) · consomme `account.v1.events` (`account_deleted` → effacement RGPD) |
> | **Appelants amont** | gateway / edge, clients utilisateurs (login & refresh) |
> | **Dépendances aval** | Keycloak (IdP), `account` (gRPC, SoR d'identité), PostgreSQL, Redis Cluster |
> | **SLO** | `<TODO: 99.95%>` dispo · login p99 `<TODO>` · refresh p99 `<TODO>` |

> **✅ Statut — toutes les phases (0–7) terminées.** Contrat, domaine, application, infrastructure,
> câblage serveur, suite d'intégration live sur conteneurs, et durcissement ops (**rotation par
> trousseau** de clés de signature ES256 + publication **JWKS**, docs SLO/modes de défaillance/runbook)
> sont en place et verts. Les `<TODO>` restants sont des valeurs propres au déploiement (équipe,
> astreinte, chiffres SLO concrets). Voir [`project_auth_service_blueprint`] pour la conception
> complète et le plan par phases.

---

## 🎯 Vue d'ensemble & rôle du service

`auth` est la frontière **émission / session / courtage IdP** de la plateforme. Il possède l'*acte
d'authentification et son cycle de vie* — courtage de connexion, suivi de session, rotation des
refresh tokens, révocation et émission des jetons d'edge — ainsi que l'unique donnée d'identité qui
relève de l'authentification : le lien sujet-IdP ↔ `account_id`.

Le problème difficile qu'il résout : **authentifier un trafic à l'échelle hyperscale sans lecture en
base à chaque appel**. Une conception naïve consulte une table de sessions à chaque requête et
s'effondre sous la charge. `auth` y répond par un **modèle à jeton scindé** : des **jetons d'edge**
courts et vérifiables localement (vérifiés en pur CPU par la bibliothèque `auth-context` dans chaque
service aval) plus des **refresh tokens** longs, côté serveur, à usage unique, avec rotation
obligatoire et détection de réutilisation. La déconnexion globale instantanée s'appuie sur un
compteur de **génération** par session dans Redis Cluster ; la révocation se compte donc en
millisecondes — jamais une écriture amplifiée sur tous les lecteurs.

**Objectifs fondamentaux :** (1) aucune lecture en base sur le chemin chaud ; (2) réutilisation d'un
refresh token = compromission ⇒ révocation de toute la génération de session ; (3) **100 %
indépendant de l'IdP** — les couches domaine et application ne nomment jamais Keycloak ; migrer vers
Cognito/Okta/custom est un nouvel adaptateur d'infrastructure et zéro changement de domaine.

### Ce que ce service ne possède **pas**
| Préoccupation | Propriétaire |
|---|---|
| Qui est une personne (dossier d'identité, KYC, RGPD, rôles RBAC) | service `account` (SoR d'identité) |
| Identifiants (mots de passe, MFA, récupération) | Keycloak (IdP) — modèle fédéré |
| Vérification entrante des jetons sur le chemin chaud | bibliothèque plateforme `auth-context` |

---

### Sessions invité (mode invité)

`StartGuestSession` (edge **public**) donne à une installation de l'app une session anonyme en
**lecture seule** avant l'inscription. Aucun compte n'est créé ni consulté : l'`account_id` de la
session est un **id invité** neuf, et la ligne est `kind = 'guest'`. Le jeton edge porte
`sub = "guest:<guest_id>"`, `kind = "guest"`, `perms = ["read:public"]`, aucun `pids`, et le `did`
de l'installation ; `Refresh` le fait tourner comme celui d'un membre (sans consulter l'annuaire ni
les profils). `device.device_id` est **obligatoire** ; l'appareil, l'indicateur d'attestation, la
locale et les indices sont enregistrés dans `guest_principals` (le cadeau de bienvenue est crédité
une fois par appareil à l'inscription). L'edge client refuse un jeton invité sur toute méthode
`authenticated` (`PERMISSION_DENIED`) : les invités n'atteignent que les routes
`permission(…, "read:public")` ; realtime refuse les handshakes invité. **Les membres portent aussi
`read:public`** (ajoutée à chaque émission). L'émission d'une session invité n'est pas publiée dans
l'outbox (le plan d'audit enregistre des comptes). La RPC est **désactivée par défaut**
(`AUTH_GUEST_SESSIONS_ENABLED` ; la fleet locale l'active).

**App Attest (B5b).** Les limites par IP seules laissent quiconque dispose de proxys créer des
sessions invité ; l'app prouve donc que chaque installation est une copie authentique de **notre**
app sur un vrai appareil Apple : elle demande à `StartDeviceAttestation` (edge **public**) un défi à
usage unique (Redis `auth:{attest:<sha256>}`, 5 min), atteste pour lui une clé neuve de la Secure
Enclave (`clientDataHash = SHA-256(défi)`), et envoie `attest_key_id` + `attestation` +
`attest_challenge` avec `StartGuestSession`. auth vérifie l'objet `apple-appattest` : la chaîne de
certificats jusqu'à l'Apple App Attestation Root CA (publique, embarquée ; empreinte vérifiée par un
test), le nonce qui la lie au défi, l'id de clé, l'**app id** (`AUTH_APP_ATTEST_APP_IDS`,
`<team id>.<bundle id>` — issu de la configuration, jamais du code), un compteur à zéro et un
**environnement** accepté (`AUTH_APP_ATTEST_ENVIRONMENTS`). Les sessions invité sont ensuite aussi
comptées par clé attestée (`AUTH_APP_ATTEST_GUESTS_PER_DEVICE_PER_DAY`, 5 — par *clé* : une app peut
renouveler sa clé, Apple limitant les attestations par appareil, c'est donc un ralentisseur), et la clé est enregistrée
sur l'invité (`guest_principals.attest_key_id`). Déploiement par `AUTH_APP_ATTEST_MODE` : `off` (par
défaut), `observe` (vérifié et journalisé, jamais refusé), `enforce` (sans attestation →
`PERMISSION_DENIED` `AUT-1006` ; invalide → `AUT-1007` ; au-delà du quota par appareil →
`RESOURCE_EXHAUSTED` `AUT-1008`). Un mode autre que `off` sans app id fait échouer le démarrage.

### Inscription avec Apple / Google (mode invité)

`SignUp` (edge **public**) crée un compte à partir d'un id_token natif **Sign in with Apple /
Google**. auth vérifie lui-même le jeton contre les JWKS du fournisseur (signature, émetteur,
audience = les client ids de l'app `AUTH_APPLE_AUDIENCES` / `AUTH_GOOGLE_AUDIENCES`, expiration,
nonce — le nonce brut ou son SHA-256 hex) ; un fournisseur sans client id est désactivé (`AUT-5009`).
**Le nonce est celui du serveur :** l'app appelle d'abord `StartFederatedSignIn` (edge **public**)
pour un nonce à usage unique (32 octets aléatoires, base64url ; Redis `auth:{fnonce:<sha256>}`,
10 min), le passe au fournisseur (Apple : son SHA-256 hex) et le renvoie brut ; `SignUp` / `Login` le
consomment une fois le jeton vérifié, si bien qu'un id_token volé ne peut pas être rejoué pendant sa
durée de vie. Tant que tous les clients ne le font pas, `AUTH_FEDERATED_NONCE_REQUIRED=false` (par
défaut) se contente de journaliser un nonce que le serveur n'a pas émis ; `true` le refuse
(`AUT-5008`).
La requête porte aussi la **date de naissance** (sous l'âge minimum → `AUT-6005`, rien n'est créé),
le **consentement** (version de la politique, traitement des données obligatoire, marketing,
analytics) et le **pays d'origine**. Le compte est créé via `account` (`CreateAccount` →
`VerifyEmail` quand le fournisseur garantit l'adresse → `UpdateConsents` ; chaque étape est
idempotente, donc une inscription relancée termine une inscription interrompue), l'identité est liée
(`subject_links`, `auth.subject_linked`) et une session membre s'ouvre. **Une personne, un compte :**
si l'identité, ou son e-mail vérifié par le fournisseur, a déjà un compte, la réponse est
`existing_account{method}` (APPLE / GOOGLE / PASSWORD) — seul l'e-mail d'un jeton vérifié est
recherché, et les adresses relais privées d'Apple ne correspondent jamais. Le profil suit
(`profile.CreateProfile`, puis `Refresh` pour que `pids` le porte). `Login` accepte le même
id_token (`IdTokenGrant`) pour les retours ; une identité sans compte reçoit `AUT-6004` (`NOT_FOUND`)
et l'app enchaîne sur `SignUp`. Les deux acceptent le **refresh token invité** de l'appareil : cette
session invitée se termine (`guest_upgraded`) et `guest_principals` enregistre le compte devenu.

### E-mail sans mot de passe (mode invité)

`StartVerification` (edge **public**) envoie un **code à usage unique** à 6 chiffres à une adresse
e-mail ; `SignUp` et `Login` le reprennent (`verification_code{challenge_id, code}`) pour un compte
**sans mot de passe** — son identité est l'adresse sous l'émetteur `urn:core-platform:email`, et il
se reconnecte avec un nouveau code (`SIGN_IN_METHOD_EMAIL_CODE`). Les codes sont stockés hachés dans
Redis (`auth:{otp:<id>}`), vivent `AUTH_VERIFICATION_TTL_SECS` (600), autorisent
`AUTH_VERIFICATION_MAX_ATTEMPTS` (5) essais et sont à usage unique ; un code faux, expiré ou déjà
utilisé donne une seule et même erreur (`AUT-5011`). Les envois sont limités par adresse
(`AUTH_VERIFICATION_PER_HOUR` 5, `AUTH_VERIFICATION_PER_DAY` 20, `AUTH_VERIFICATION_RESEND_SECS` 30
→ `RESOURCE_EXHAUSTED` `AUT-5013` avec `retry-after-secs`) et par IP à l'edge. **Les tentatives sont
bornées par adresse, sans offrir de levier de verrouillage aux attaquants :** les codes faux comptent
par adresse **et IP du client** (l'adresse vue par le transport — l'entrée `X-Forwarded-For` de l'ALB,
`GRPC_TRUSTED_PROXY_HOPS` — jamais un champ de la requête). Après `AUTH_VERIFICATION_MAX_FAILURES_PER_IP`
(15) en 24 h, tous challenges confondus, cette IP ne reçoit plus de code pour l'adresse et même un bon
code est refusé : celui qui devine se verrouille lui-même, tandis que le titulaire, sur un autre
réseau, se connecte toujours. Seuls `AUTH_VERIFICATION_MAX_FAILURES` (50) codes faux venus de partout
(une attaque distribuée) verrouillent l'adresse pour tous jusqu'à la fin de la fenêtre — et son
titulaire reçoit un **e-mail** indiquant que les codes sont en pause pendant 24 heures (une fois par
fenêtre, dans la langue de la dernière demande de code ; jamais par SMS, qui coûterait à chaque
verrouillage). **Rien à énumérer :** la réponse de StartVerification est
la même pour toute adresse ; qu'elle ait un compte (ou un compte Apple / Google) n'est dit qu'à celui
qui saisit le code. L'e-mail part en SMTP vers **Amazon SES** (`AUTH_VERIFICATION_SENDER=smtp`,
`AUTH_SMTP_*`) ; `log` écrit le code dans les logs (exécutions locales uniquement) ; non défini =
désactivé (`AUT-5012`).

**Comptes téléphone seul (SMS).** Les mêmes codes partent par SMS (`channel = SMS`, un numéro
international normalisé en E.164) : identité = le numéro sous `urn:core-platform:phone`, un compte
**sans e-mail** (`account` l'active sur le numéro vérifié), qui se reconnecte avec un nouveau code SMS
(`SIGN_IN_METHOD_PHONE_CODE`). Un numéro déjà détenu par un autre compte répond `existing_account`
avec la méthode de ce compte. Le SMS part via **Amazon SNS** (`Publish`, transactionnel, SigV4 avec
des clés statiques : `AUTH_SMS_SENDER=sns`, `AUTH_SNS_*`) ; avec `AUTH_VERIFICATION_SENDER=log` les
codes SMS sont aussi journalisés. **Garde-fous contre le SMS pumping** (chaque SMS coûte) : le numéro
doit être un numéro **mobile** valide (métadonnées libphonenumber : ni fixe, ni surtaxé, ni à coût
partagé, ni VoIP) d'un pays de `AUTH_SMS_COUNTRIES` — par défaut les marchés de lancement, UE 27 +
IS LI NO + GB CH + GP GF MQ RE YT, **exactement** la liste autorisée de la protect configuration SNS
(core-platform-infra `global/messaging/sms`) ; les territoires qui partagent un indicatif sont résolus
vers leur propre pays (Jersey sous +44 est `JE`) — sinon `FAILED_PRECONDITION` `AUT-5015`, décidé
d'après le seul numéro. Chaque SMS compte ensuite dans deux **budgets quotidiens** (par jour UTC,
Redis `auth:{sms-budget}:<YYYYMMDD>[:<pays>]`, un seul script) : d'abord celui du **pays** de
destination (`AUTH_SMS_COUNTRY_DAILY_BUDGET`, 25), pour qu'un pompage sur un indicatif n'épuise que ce
pays, puis celui du **service** (`AUTH_SMS_DAILY_BUDGET`, 50) ; un SMS que SNS n'a pas pu envoyer est
remboursé. Dès que l'un est épuisé, les codes SMS répondent `UNAVAILABLE` `AUT-5016` jusqu'au jour UTC
suivant (l'e-mail continue de fonctionner) et auth journalise une `error` (à alerter). Dimensionnez-le d'après la limite de
dépense mensuelle SNS (≈ limite / 30 / prix d'un SMS), pour que la limite de SNS ne soit jamais ce
qui coupe les SMS pour le mois.

### Export de données prêt (RGPD art. 15/20, #653)

Quand `account` livre l'export de données d'un titulaire, il publie `gdpr_data_export_completed` — sans le
lien, qui est un secret. Le consommateur d'événements account d'auth (le même groupe `auth-account-events`
que l'effacement) lit alors le dossier RGPD (`GetGdprRecord` : `account` signe le lien à la lecture) et
l'envoie par e-mail à l'adresse du compte (« Ton export de données est prêt », le lien et son dernier jour,
FR/EN ; l'expéditeur de log ne journalise jamais le lien). Plus de lien à remettre (une demande plus
récente, expiré) ou pas d'e-mail enregistré (un compte par téléphone, qui voit le lien dans l'app) : rien
n'est envoyé. Un échec de l'annuaire ou de l'envoi est réessayé par `run_consumer`.

### Effacement de compte (RGPD art. 17)

Quand `account` supprime un compte au terme de son délai de grâce, il publie `account_deleted` ; le
consumer d'auth (`auth-account-events`, sur le `run_consumer` partagé : retry, DLQ, commit manuel)
coupe alors les jetons du compte (nouvelle génération), supprime l'**utilisateur IdP** du compte
(API Admin Keycloak, `DELETE users/{id}` : son e-mail, son nom d'utilisateur et le hash de son mot de
passe — pour chaque lien vers l'IdP de la flotte ; Apple / Google et les identités par code n'en ont
pas) **avant** tout le reste, le lien étant le seul enregistrement de l'id de l'utilisateur IdP (un
échec de l'IdP interrompt sans rien toucher et l'événement est rejoué ; sans client admin configuré,
ces effacements sont rejoués jusqu'à ce qu'il le soit), et supprime définitivement ce qu'auth détient
sur lui : ses sessions et refresh tokens (appareil, IP), ses liens d'identité (pour une identité par
code e-mail ou téléphone, le sujet **est** l'adresse), et l'invité qu'il était avant l'inscription,
avec les sessions de cet invité (sur chaque shard ; index `idx_guest_principals_upgraded`).
Idempotent : un rejeu ne supprime rien de plus. L'identité est ensuite libre : se reconnecter avec elle
ne trouve aucun compte et peut s'inscrire à nouveau.

### Identifiants et step-up

Le mot de passe ne vit que chez l'IdP. `ChangePassword` (edge **authenticated**, membres) prouve le
mot de passe actuel par un password grant sous le nom de connexion du sujet, pose le nouveau via
l'API Admin de Keycloak (`reset-password`, un client confidentiel à service account doté de
`view-users` + `manage-users`), et déconnecte en option toutes les **autres** sessions (révocations
`password_changed` ; celle de l'appelant reste). Nouveaux mots de passe : 8 à 128 caractères et
différents de l'actuel (`AUT-VAL-024/025/026`), puis la politique du realm (`AUT-5006`, sa règle dans
le message). Sans client admin configuré, les RPC d'identifiants répondent `UNAVAILABLE` (`AUT-5005`).

**Alertes de nouvelle connexion (#649).** Une connexion depuis un appareil que le compte n'a jamais
utilisé — son `device_id` (`DeviceContext`) absent de toutes les sessions que le compte a eues, quel que
soit leur statut — envoie un e-mail à l'adresse du compte (« Nouvelle connexion à ton compte », avec
l'user agent de l'appareil — caractères imprimables seulement, 120 au plus, dans un corps en texte brut — et
l'IP vue par le transport). Le `device_id` est écrit par le client : une connexion **sans** `device_id`
compte donc aussi comme un nouvel appareil, sauf si les 5 dernières sessions du compte n'en avaient
aucune (le client du titulaire n'en envoie pas). Pas à la toute première connexion du compte, et
seulement là où les transports de codes sont configurés. L'historique est lu avant l'émission de la
session ; l'e-mail part en arrière-plan, si bien qu'une alerte ne fait jamais échouer ni ralentir une
connexion. Les alertes push attendent APNs.

**Changer son e-mail ou son téléphone (#651).** `ChangeContact(challenge_id, code)` (edge
**authenticated**, membres, derrière le step-up ci-dessous) : le titulaire envoie d'abord un code à la
nouvelle adresse avec `StartVerification`, puis le prouve ici. Tout ce qui le connecte suit, dans cet ordre
— l'e-mail de l'utilisateur IdP d'un compte à mot de passe (API Admin de Keycloak ; son nom de connexion
aussi quand c'était l'e-mail ; l'adresse d'un autre utilisateur IdP donne `AUT-6006`), le compte
(`account.ChangeEmail` / `ChangePhone`, mesh ; l'adresse d'un autre compte donne `AUT-6006` /
`AUT-6007`), et le lien de connexion par code (`urn:core-platform:email|phone`, re-clé dans
`subject_links` : la nouvelle adresse connecte, l'ancienne non). Les liens Apple / Google gardent leur
propre e-mail. L'e-mail enregistré avant est prévenu (un e-mail, jamais un SMS : un changement de
téléphone prévient l'e-mail) ; les codes SMS gardent leur liste de pays et leurs budgets.

**Connexion en deux étapes (#649).** Pour un compte qui l'a activée (`mfa_enrolled` d'`account`),
**toutes** les méthodes de connexion — mot de passe, code e-mail/SMS, Apple, Google — s'arrêtent après
l'identifiant : `Login` n'émet aucune session (ne réactive aucun compte désactivé, ne crée aucun premier
lien) et répond `mfa_required` avec un `mfa_token` opaque à usage unique (5 minutes ; seul son SHA-256
est conservé, `auth:{mfal:…}`). `CompleteLogin(mfa_token, code)` (edge **public** : le jeton fait
preuve) prend ensuite soit six chiffres de l'application d'authentification du titulaire (TOTP RFC 6238,
HMAC-SHA1, pas de 30 s, ±1 pas ; chaque pas ne sert **qu'une fois**, `auth:{mfa:<id>}:step:<n>`), soit
l'un de ses codes de secours (`xxxxx-xxxxx`, dépensé chez `account`). Un mauvais code donne `AUT-5017`
(`UNAUTHENTICATED`). Chaque tentative est comptée **avant** que le code soit examiné (un seul incrément
Redis atomique, `auth:{mfa:<id>}:fail`), si bien que des essais parallèles n'obtiennent pas plus de
tentatives que des essais successifs : au-delà de 5 tentatives en 15 minutes, les codes du compte sont
bloqués (`AUT-5018`, `RESOURCE_EXHAUSTED` + `retry-after-secs`, même le bon ; un bon code remet le compteur
à zéro) ; un `mfa_token` inconnu, expiré ou déjà utilisé
donne `AUT-5021`. La graine TOTP appartient à auth : scellée en AES-256-GCM sous `AUTH_MFA_SEED_KEY`
(l'identifiant de clé y est apposé, les anciennes clés dans `AUTH_MFA_SEED_KEYS_PREVIOUS`) avant
qu'`account` ne la conserve ; un code de secours est conservé sous forme de HMAC-SHA-256 avec une clé
qui en est dérivée. **Sans la clé, la connexion en deux étapes échoue en mode fermé** : un compte qui l'a
activée ne peut pas se connecter (`AUT-5019`, `UNAVAILABLE`) — la clé ne doit jamais être retirée une
fois utilisée.

**Réglages de la connexion en deux étapes (#649).** Toutes edge **authenticated** ; Start, Disable et
Regenerate exigent aussi une preuve d'identifiant récente (le step-up ci-dessous). `StartMfaEnrollment`
crée une nouvelle graine et la renvoie pour l'application d'authentification (l'URI `otpauth://` pour un
QR code — émetteur `AUTH_MFA_ISSUER`, l'e-mail ou le téléphone du titulaire comme libellé — ou le secret
base32 à saisir) ; elle attend, scellée, son premier code (`auth:{mfa:<id>}:enrol`, 10 minutes ;
recommencer la remplace ; `AUT-5022` si déjà active). `ConfirmMfaEnrollment(code)` prend ce premier code
(même limite de tentatives et pas à usage unique qu'à la connexion), active la connexion en deux étapes
chez `account`, renvoie **10 codes de secours, affichés une seule fois** (seuls leurs HMAC sont
conservés), et **déconnecte les autres sessions du compte** (`mfa_changed`, comme un changement de mot de
passe : un intrus déjà connecté ailleurs doit passer le second facteur) ; aucun enrôlement en attente
donne `AUT-5021`. `RegenerateBackupCodes` remplace les codes (les anciens cessent de fonctionner) et
déconnecte aussi les autres sessions ; `DisableMfa` la désactive (`AUT-5020` si elle l'est déjà) sans déconnecter personne : les sessions déjà émises n'y perdent rien. Chaque
changement est envoyé par e-mail à l'adresse du compte (activée, désactivée, nouveaux codes de secours),
si bien qu'une prise de contrôle qui la désactive reste visible.

**Step-up.** Un jeton émis juste après une preuve d'identifiant — `Login` / `CompleteLogin`, ou
`VerifyCredentials` (re-prouver le mot de passe, ou donner un code de deux étapes : le step-up d'un
compte sans mot de passe ; `AUT-5020` si la connexion en deux étapes est désactivée) — porte
`auth_time` ; un jeton rafraîchi non. Les RPC destructives ailleurs appellent
`transport::grpc::edge::require_recent_auth` (`auth_time` de moins de 5 min, mesh exempté) et
répondent sinon `PERMISSION_DENIED` `step_up_required…` ; `account` protège `DeactivateAccount` /
`RequestGdprDeletion` derrière `ACCOUNT_REQUIRE_STEP_UP`. `VerifyCredentials` ré-émet le jeton
d'accès de l'appelant (même session, même refresh token).

**Tranche d'âge.** Chaque émission membre (`Login`, `Refresh`, `VerifyCredentials`) relit la tranche
d'âge du compte et la porte dans le claim `age` (`13-15` / `16-17` / `18+` ; absent sans date de
naissance), donc un anniversaire apparaît en moins d'une durée de vie de jeton d'accès. Les services
orientés client en tirent les protections ados (`EdgePrincipal::is_minor`), p. ex. le nouveau profil
d'un titulaire de 13 à 17 ans démarre privé.

## 📐 Architecture & concepts

Hexagonal / DDD (`domain` → `application` → `infrastructure`), bus CQRS commande/requête, PostgreSQL
pour le registre durable des sessions, Redis Cluster pour la carte de génération / blacklist du
chemin chaud, Kafka pour les événements. L'IdP est masqué derrière un `IdentityProviderPort`
(Port/Adapter), si bien qu'aucun type Keycloak ne fuite au-dessus de `infrastructure`.

```
            ┌──────────────────────── auth-service ───────────────────────┐
 client ──► │ Login/Refresh/Logout ─► bus CQRS ─► ports :                 │
            │   IdentityProviderPort ─┐   SessionRepo/RefreshRepo (PG)     │
            │   AccountDirectoryPort ─┤   SessionCachePort (Redis gen/blk) │
            │   TokenMinterPort ──────┘   SubjectLinkRepo (PG)             │
            └───────┬───────────────────────────┬─────────────────────────┘
       courtage login│                           │émet le jeton d'edge (ES256 ; PASETO en suivi)
                    ▼                            ▼
              Keycloak (IdP)            les services aval vérifient LOCALEMENT via auth-context
                    │                            │  (vérif. signature pur CPU + contrôle O(1)
              résout l'identité ──► account      │   facultatif de `gen` Redis pour logout instantané)
```

**Chemin chaud à jeton scindé.** 99 % des appels API = vérification de signature seule, aucune E/S.
La révocation est un incrément de `generation` écrit dans `auth:sess:{account}:gen` sur Redis ; un
jeton d'edge portant une `gen` périmée est rejeté. Seul `/refresh` (faible QPS) touche PostgreSQL.

> **Invariants** (et où ils sont appliqués) : TTL du jeton d'edge ⊆ TTL de session ⊆ plafond absolu ;
> la rotation du refresh est obligatoire et à usage unique, et toute réutilisation révoque la
> génération entière (appliqué dans l'agrégat `Session`, Phase 2) ; `SubjectLink (iss,sub)→account_id`
> est immuable (Phase 2) ; l'émission de session est conditionnée au statut `account` (couche
> application, Phase 3).

---

## 📊 Objectifs de niveau de service (SLO) &nbsp;·&nbsp; OPS

| SLI | Objectif | Fenêtre | Mesuré par |
|---|---|---|---|
| Disponibilité (non-5xx / non-`UNAVAILABLE`) | `<TODO 99.95%>` | 30j glissants | `<grpc_server_handled_total par code>` |
| Latence `Login` p99 | `< <TODO> ms` | 1h | `<latence rpc par méthode>` (dominée par l'aller-retour IdP) |
| Latence `Refresh` p99 | `< <TODO> ms` | 1h | `<latence rpc>` (une rotation Postgres) |
| Latence `Introspect` p99 | `< <TODO> ms` | 1h | `<latence rpc>` (vérif CPU + ≤1 lecture Redis) |
| Durabilité | aucune écriture session/refresh acquittée perdue | — | Postgres `LocalQuorum`/fsync |

**Budget d'erreur :** `<0,05% / 30j ≈ 21m>`. **En cas de consommation :** gel du rollout, page astreinte.

> **Note — l'edge n'est pas sur le chemin critique d'auth.** Les services aval vérifient les jetons
> d'edge *localement* via `auth-context` ; seuls `Login` / `Refresh` / `Logout` touchent ce service.
> Une panne d'auth empêche les *nouvelles* connexions et refresh mais ne casse **pas** le trafic
> authentifié en vol (les jetons d'edge existants restent vérifiables jusqu'à expiration).

## 🔗 Dépendances & rayon d'impact &nbsp;·&nbsp; OPS

**Aval — ce dont `auth` a besoin :**

| Dépendance | Rôle | Si en panne → | Dégradation |
|---|---|---|---|
| Keycloak (IdP) | vérification des identifiants au `Login` | `Login` échoue (`UNAVAILABLE`) | **Dur** pour les nouvelles connexions ; refresh/introspect intacts |
| `account` (gRPC) | résolution compte + gating actif au `Login` (un compte désactivé par son titulaire est réactivé : `ResumeDeactivatedAccount`, `reactivated = true`) | `Login` échoue | **Dur** pour les nouvelles connexions |
| PostgreSQL | registre sessions + refresh + liens | écritures `Refresh`/`Logout` échouent | **Dur** pour refresh/révocation |
| Redis Cluster | carte de génération + blacklist (chemin chaud) | contrôles de révocation dégradés | **Souple** — la génération se reconstruit depuis Postgres ; une entrée blacklist manquée expire avec le jeton |
| Kafka | émission `auth.v1.events` · consommation `account.v1.events` (groupe `auth-account-events`) | événements non émis · effacements en attente (le consumer reprend à son offset commité) | **Souple** — best-effort ; repli sur le log publisher |

**Amont — rayon d'impact si `auth` tombe :**

| Appelant | Utilise | Impact si `auth` est en panne |
|---|---|---|
| gateway / edge | `Login` / `Refresh` / `Logout` | impossible de se connecter, refresh ou se déconnecter ; **les requêtes déjà authentifiées continuent** jusqu'à expiration |
| UI ops / gestion d'appareils | `ListSessions` / `Introspect` | listing de sessions + introspection côté serveur indisponibles |

## ⚙️ Configuration

| Variable d'env | Rôle | Défaut |
|---|---|---|
| `AUTH_GRPC_ADDR` | Adresse d'écoute gRPC | `0.0.0.0:50060` |
| `AUTH_SIGNING_PRIVATE_PEM` / `AUTH_SIGNING_PUBLIC_PEM` | **Requis.** Paire de clés ES256 du jeton d'edge (PEM) | — |
| `AUTH_SIGNING_KID` · `AUTH_TOKEN_ISSUER` · `AUTH_TOKEN_AUDIENCE` | `kid` / `iss` / `aud` du jeton d'edge | `auth-es256-1` · `https://auth.core-platform` · `core-platform` |
| `AUTH_ACCESS_TTL_SECS` · `AUTH_SESSION_TTL_SECS` · `AUTH_ABSOLUTE_TTL_SECS` · `AUTH_REFRESH_TTL_SECS` | Durées de vie jeton / session | `600` · `1800` · `28800` · `604800` |
| `AUTH_KEYCLOAK_TOKEN_ENDPOINT` · `AUTH_KEYCLOAK_CLIENT_ID` · `AUTH_KEYCLOAK_CLIENT_SECRET` · `AUTH_KEYCLOAK_SCOPE` | Courtier IdP | — · — · — · `openid` |
| `AUTH_KEYCLOAK_ADMIN_URL` · `AUTH_KEYCLOAK_ADMIN_CLIENT_ID` · `AUTH_KEYCLOAK_ADMIN_CLIENT_SECRET` | Gestion des identifiants (`ChangePassword`, `VerifyCredentials`) : la base admin du realm (`…/admin/realms/<realm>`) et un client confidentiel à service account doté de `realm-management` `view-users` + `manage-users`. Absent → ces RPC répondent `UNAVAILABLE` (`AUT-5005`). | — |
| `AUTH_ACCOUNT_GRPC_ENDPOINT` | Endpoint du service `account` | `http://localhost:50059` |
| `AUTH_ACCOUNT_RPC_TIMEOUT_MS` · `AUTH_ACCOUNT_CONNECT_TIMEOUT_MS` | Deadlines par requête / de connexion sur le canal `account` (chemin chaud du login — échouer vite, ne jamais bloquer) | `2000` · `2000` |
| `AUTH_IDP_HTTP_TIMEOUT_MS` · `AUTH_IDP_CONNECT_TIMEOUT_MS` | Deadlines de requête / de connexion des appels HTTP Keycloak (échange de token) | `5000` · `2000` |
| `AUTH_GUEST_SESSIONS_ENABLED` | Interrupteur de `StartGuestSession`. **Désactivé par défaut** : il écrit une session par appel sans identifiant, donc à laisser éteint partout où les contrôles anti-abus (limites par IP / par appareil, App Attest) ne sont pas devant lui. Éteint → `AUT-1005` (`PERMISSION_DENIED`). | `false` |
| `AUTH_APP_ATTEST_MODE` · `AUTH_APP_ATTEST_APP_IDS` · `AUTH_APP_ATTEST_ENVIRONMENTS` · `AUTH_APP_ATTEST_GUESTS_PER_DEVICE_PER_DAY` | App Attest devant `StartGuestSession` : `off` / `observe` / `enforce` ; les `<team id>.<bundle id>` acceptés (séparés par des virgules ; obligatoires sauf `off`) ; `production` et/ou `development` ; sessions invité par appareil attesté et par jour UTC. | `off` · — · `production` · `5` |
| `AUTH_FEDERATED_NONCE_REQUIRED` | Un `SignUp` / `Login` par id_token doit consommer un nonce de `StartFederatedSignIn` (sinon `AUT-5008`). Désactivé : un nonce créé par le client est seulement journalisé — à activer quand tous les clients appellent `StartFederatedSignIn`. | `false` |
| `AUTH_GUEST_RETENTION_DAYS` · `AUTH_GUEST_RETENTION_INTERVAL_SECS` | Conservation des données d'invités : les invités jamais devenus un compte et sans session active (identifiant d'appareil, langue, pays), et les sessions d'invités terminées (appareil, IP, refresh tokens), sont supprimés après ce nombre de jours ; une passe tourne au démarrage puis à cet intervalle sur chaque réplica (par lots, idempotente). Les invités devenus un compte sont conservés (cadeau de bienvenue une fois par appareil). | `90` · `3600` |
| `AUTH_APPLE_AUDIENCES` · `AUTH_GOOGLE_AUDIENCES` | Client ids (séparés par des virgules) pour lesquels un id_token Apple / Google doit être émis (`aud` : bundle / services ids de l'app ; client ids OAuth Google). Vide = l'inscription par ce fournisseur est désactivée (`AUT-5009`). | — |
| `AUTH_FEDERATED_JWKS_TIMEOUT_MS` | Délai de récupération des JWKS d'un fournisseur. | `3000` |
| `AUTH_FEDERATED_JWKS_REFRESH_SECS` | Les clés d'Apple / Google sont récupérées au démarrage puis à cet intervalle en arrière-plan (rotations prises en compte, clés retirées abandonnées ; un échec conserve les dernières clés). Un `kid` inconnu déclenche toujours une récupération, au plus une fois par minute. | `21600` |
| `AUTH_VERIFICATION_SENDER` | Mode d'envoi des codes : `smtp` (Amazon SES), `log` (exécutions locales uniquement — le code est journalisé), non défini = désactivé (`AUT-5012`). | — |
| `AUTH_SMTP_HOST` · `AUTH_SMTP_PORT` · `AUTH_SMTP_USERNAME` · `AUTH_SMTP_PASSWORD` · `AUTH_SMTP_FROM` | Relais SMTP des codes e-mail (SES : `email-smtp.<region>.amazonaws.com`, `587`, STARTTLS, identifiants SMTP SES, un expéditeur vérifié). | — · `587` |
| `AUTH_SMS_SENDER` | Mode d'envoi des codes SMS : `sns`, non défini = désactivé (ou journalisés si `AUTH_VERIFICATION_SENDER=log`). | — |
| `AUTH_SNS_REGION` · `AUTH_SNS_ACCESS_KEY_ID` · `AUTH_SNS_SECRET_ACCESS_KEY` · `AUTH_SNS_SENDER_ID` | Amazon SNS pour les codes SMS (un utilisateur IAM autorisé à `sns:Publish` ; sender id alphanumérique facultatif là où les pays l'autorisent). | — |
| `AUTH_SMS_COUNTRIES` | Pays (ISO 3166-1 alpha-2, séparés par des virgules) vers lesquels les codes SMS peuvent partir ; doit être égal à la liste autorisée de la protect configuration SNS (infra `global/messaging/sms`). Un code inconnu fait échouer le démarrage. | les marchés de lancement (37) |
| `AUTH_SMS_DAILY_BUDGET` | SMS que le service entier peut envoyer par jour UTC (`0` = aucun) ; au-delà `AUT-5016`. | `50` |
| `AUTH_SMS_COUNTRY_DAILY_BUDGET` | SMS qu'un pays de destination peut recevoir par jour UTC, vérifié avant celui du service ; au-delà `AUT-5016`. | `25` |
| `AUTH_MFA_SEED_KEY` · `AUTH_MFA_SEED_KEY_ID` · `AUTH_MFA_SEED_KEYS_PREVIOUS` | Connexion en deux étapes (#649) : la clé AES-256 qui scelle les graines TOTP (32 octets, base64 standard), son identifiant, et les clés retirées (`id:base64,…`) qui ouvrent encore les anciennes graines. Absente → connexion en deux étapes indisponible, en mode fermé (`AUT-5019`). **Ne jamais la retirer une fois utilisée.** Provisionnée par core-platform-infra#27. | — · `k1` · — |
| `AUTH_MFA_ISSUER` | Le nom du service dans l'application d'authentification du titulaire (#649). | `Core Platform` |
| `AUTH_VERIFICATION_TTL_SECS` · `_MAX_ATTEMPTS` · `_PER_HOUR` · `_PER_DAY` · `_RESEND_SECS` · `_MAX_FAILURES_PER_IP` · `_MAX_FAILURES` | Durée de vie d'un code, essais par code, codes par adresse par heure / par jour, délai avant renvoi, codes faux par adresse depuis une IP en 24 h avant verrouillage pour cette IP, et depuis partout avant verrouillage pour tous. | `600` · `5` · `5` · `20` · `30` · `15` · `50` |
| Postgres / Redis / Kafka | via les `from_env()` des crates de stockage partagées | — |

## 🧪 Développement local

```bash
cargo test -p auth                              # rapide, hermétique : units + edge-verify inter-crate
cargo test -p auth --features integration-auth  # live : démarre des conteneurs Postgres + Redis
```

Le run par défaut ne nécessite pas Docker. Il couvre les units domaine/application/handler, le
round-trip mint↔verify ES256, et **`tests/edge_token_verify.rs`** — la preuve inter-crate qu'un
jeton émis ici est accepté par le même décodeur `auth-context` que chaque service aval exécute.

La suite `integration-auth` (`tests/auth_it/`) démarre **PostgreSQL** + **Redis** réels via le
harnais partagé `test-support` et pilote la composition root de production via le handler gRPC. Les
dépendances *externes* d'auth (l'IdP et le service `account`) sont stubbées au niveau de leurs ports.
Scénarios : cycle de vie (login → introspect → logout), rotation refresh + détection de réutilisation
→ révocation de génération, logout global, et allers-retours d'écriture durable. **Keycloak n'est pas
conteneurisé** — l'adaptateur OIDC est testé unitairement, et la suite live se concentre sur la
machinerie session/jeton au-dessus des stores propres à auth.

## 🔥 Modes de défaillance &nbsp;·&nbsp; OPS

| Symptôme | Cause racine probable | Mitigation |
|---|---|---|
| Tous les `Login` → `UNAVAILABLE` | Keycloak ou `account` injoignable | vérifier la santé IdP / `account` ; refresh + introspect fonctionnent toujours |
| Pic de `Refresh` → `UNAUTHENTICATED` | **réutilisation** de refresh token (vol) ou logout global | attendu en cas de réutilisation — la génération de session est révoquée ; investiguer l'IP/appareil source |
| Jetons d'edge acceptés après logout | miss blacklist/génération Redis | les jetons meurent quand même au TTL (≤ `AUTH_ACCESS_TTL_SECS`) ; vérifier Redis et la clé de génération |
| `Introspect` renvoie `active:false` pour un jeton frais | dérive d'horloge, ou un bump de génération (logout global) | vérifier NTP ; confirmer la génération courante du compte dans Redis |
| Les services aval rejettent nos jetons | JWKS non publié / `kid` sorti de rotation | s'assurer que les clés publiques active **et** sortante sont dans le JWKS publié (voir Déploiement) |
| `ConcurrentModification` (AUT-8001) | contention de verrou optimiste sur une ligne session | retryable — l'appelant retente ; persistant ⇒ investiguer des opérations concurrentes dupliquées |

## 🚀 Déploiement &nbsp;·&nbsp; OPS

- **Le throttling / lockout n'est *pas* le rôle de ce service.** La protection brute-force des
  identifiants vit dans Keycloak (modèle fédéré) ; la limitation de débit en ingress est la couche
  `[traffic]` du runtime partagé. Auth n'ajoute aucun throttle redondant.
- **Rotation des clés de signature (sans interruption).** Les jetons d'edge sont ES256, vérifiés par
  un **trousseau de clés** :
  1. Générer une nouvelle paire P-256 ; la définir comme `AUTH_SIGNING_PRIVATE_PEM` /
     `AUTH_SIGNING_PUBLIC_PEM` avec un nouveau `AUTH_SIGNING_KID`.
  2. Déplacer la clé publique *précédente* vers `AUTH_SIGNING_RETIRING_PUBLIC_PEM` /
     `AUTH_SIGNING_RETIRING_KID` pour que les jetons émis sous elle restent vérifiables et dans le JWKS.
  3. Déployer. Les nouveaux jetons sont signés avec le nouveau `kid` ; les anciens valident contre la clé sortante.
  4. Après une fenêtre `AUTH_ABSOLUTE_TTL_SECS` complète, retirer la clé sortante.
- **Publication JWKS.** `Es256TokenMinter::jwks_json()` produit le JWKS de chaque clé du trousseau ;
  le publier à l'URL JWKS well-known du service pour qu'`auth-context` (dans chaque service aval) le
  récupère et le cache. La clé privée ne quitte jamais ce service — seul le matériel public est publié.

## 🛠️ Dépannage

- **`required env var AUTH_SIGNING_PRIVATE_PEM is not set` au démarrage** — la paire de clés de
  signature ES256 est obligatoire ; fournir les deux PEM (voir Configuration).
- **Les jetons se vérifient localement mais `Introspect` dit inactif** — `Introspect` applique en plus
  les contrôles live génération + blacklist ; un jeton peut être cryptographiquement valide mais révoqué.
- **Lancer un scénario :** `cargo test -p auth --features integration-auth <name> -- --nocapture`.

---

## 📋 Codes d'erreur

Espace de noms canonique `AUT-XXXX` — voir [`src/error.rs`](src/error.rs) pour le catalogue faisant
foi (1xxx session · 2xxx refresh/rotation · 3xxx liaison de sujet · 4xxx émission de jeton · 5xxx
courtage IdP · 6xxx annuaire de comptes · 9xxx domaine/parsing). Les codes de stockage (`DB-*`) et de
validation (`VAL-*`) sont délégués de manière transparente.

[`project_auth_service_blueprint`]: ../../../docs/ <!-- TODO : lier le document de conception une fois publié -->
