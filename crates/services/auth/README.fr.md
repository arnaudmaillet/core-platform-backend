---
i18n:
  source: ./README.md
  source_sha256: 41a1bb92c7ee439fe6b8934c45ec927a1fd81ceda1598cd616f14287e24f3422
  translated_at: 2026-10-04
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
> | **Asynchrone** | publie `auth.v1.events` (SessionIssued/SessionRevoked/SubjectLinked) · ne consomme rien |
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
l'outbox (le plan d'audit enregistre des comptes). La vérification App Attest et les limites par IP
/ par appareil sur `StartGuestSession` relèvent de la tranche anti-abus (B5) ; d'ici là, la RPC est
**désactivée par défaut** (`AUTH_GUEST_SESSIONS_ENABLED` ; la fleet locale l'active).

### Inscription avec Apple / Google (mode invité)

`SignUp` (edge **public**) crée un compte à partir d'un id_token natif **Sign in with Apple /
Google**. auth vérifie lui-même le jeton contre les JWKS du fournisseur (signature, émetteur,
audience = les client ids de l'app `AUTH_APPLE_AUDIENCES` / `AUTH_GOOGLE_AUDIENCES`, expiration,
nonce — le nonce brut ou son SHA-256 hex) ; un fournisseur sans client id est désactivé (`AUT-5009`).
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
bornées par adresse :** après `AUTH_VERIFICATION_MAX_FAILURES` (15) codes faux en 24 h, tous
challenges confondus, l'adresse ne reçoit plus de code et même un bon code est refusé jusqu'à la fin
de la fenêtre. **Rien à énumérer :** la réponse de StartVerification est
la même pour toute adresse ; qu'elle ait un compte (ou un compte Apple / Google) n'est dit qu'à celui
qui saisit le code. L'e-mail part en SMTP vers **Amazon SES** (`AUTH_VERIFICATION_SENDER=smtp`,
`AUTH_SMTP_*`) ; `log` écrit le code dans les logs (exécutions locales uniquement) ; non défini =
désactivé (`AUT-5012`).

**Comptes téléphone seul (SMS).** Les mêmes codes partent par SMS (`channel = SMS`, tout numéro
international — normalisé en E.164) : identité = le numéro sous `urn:core-platform:phone`, un compte
**sans e-mail** (`account` l'active sur le numéro vérifié), qui se reconnecte avec un nouveau code SMS
(`SIGN_IN_METHOD_PHONE_CODE`). Un numéro déjà détenu par un autre compte répond `existing_account`
avec la méthode de ce compte. Le SMS part via **Amazon SNS** (`Publish`, transactionnel, SigV4 avec
des clés statiques : `AUTH_SMS_SENDER=sns`, `AUTH_SNS_*`) ; avec `AUTH_VERIFICATION_SENDER=log` les
codes SMS sont aussi journalisés.

### Identifiants et step-up

Le mot de passe ne vit que chez l'IdP. `ChangePassword` (edge **authenticated**, membres) prouve le
mot de passe actuel par un password grant sous le nom de connexion du sujet, pose le nouveau via
l'API Admin de Keycloak (`reset-password`, un client confidentiel à service account doté de
`view-users` + `manage-users`), et déconnecte en option toutes les **autres** sessions (révocations
`password_changed` ; celle de l'appelant reste). Nouveaux mots de passe : 8 à 128 caractères et
différents de l'actuel (`AUT-VAL-024/025/026`), puis la politique du realm (`AUT-5006`, sa règle dans
le message). Sans client admin configuré, les RPC d'identifiants répondent `UNAVAILABLE` (`AUT-5005`).

**Step-up.** Un jeton émis juste après une preuve d'identifiant — `Login`, ou `VerifyCredentials`
(re-prouver le mot de passe ; un code MFA est refusé avec `AUT-5007` tant que l'enrôlement n'existe
pas) — porte `auth_time` ; un jeton rafraîchi non. Les RPC destructives ailleurs appellent
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
| Kafka | émission `auth.v1.events` | événements non émis | **Souple** — best-effort ; repli sur le log publisher |

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
| `AUTH_APPLE_AUDIENCES` · `AUTH_GOOGLE_AUDIENCES` | Client ids (séparés par des virgules) pour lesquels un id_token Apple / Google doit être émis (`aud` : bundle / services ids de l'app ; client ids OAuth Google). Vide = l'inscription par ce fournisseur est désactivée (`AUT-5009`). | — |
| `AUTH_FEDERATED_JWKS_TIMEOUT_MS` | Délai de récupération des JWKS d'un fournisseur. | `3000` |
| `AUTH_VERIFICATION_SENDER` | Mode d'envoi des codes : `smtp` (Amazon SES), `log` (exécutions locales uniquement — le code est journalisé), non défini = désactivé (`AUT-5012`). | — |
| `AUTH_SMTP_HOST` · `AUTH_SMTP_PORT` · `AUTH_SMTP_USERNAME` · `AUTH_SMTP_PASSWORD` · `AUTH_SMTP_FROM` | Relais SMTP des codes e-mail (SES : `email-smtp.<region>.amazonaws.com`, `587`, STARTTLS, identifiants SMTP SES, un expéditeur vérifié). | — · `587` |
| `AUTH_SMS_SENDER` | Mode d'envoi des codes SMS : `sns`, non défini = désactivé (ou journalisés si `AUTH_VERIFICATION_SENDER=log`). | — |
| `AUTH_SNS_REGION` · `AUTH_SNS_ACCESS_KEY_ID` · `AUTH_SNS_SECRET_ACCESS_KEY` · `AUTH_SNS_SENDER_ID` | Amazon SNS pour les codes SMS (un utilisateur IAM autorisé à `sns:Publish` ; sender id alphanumérique facultatif là où les pays l'autorisent). | — |
| `AUTH_VERIFICATION_TTL_SECS` · `_MAX_ATTEMPTS` · `_PER_HOUR` · `_PER_DAY` · `_RESEND_SECS` · `_MAX_FAILURES` | Durée de vie d'un code, essais par code, codes par adresse par heure / par jour, délai avant renvoi, codes faux par adresse en 24 h avant verrouillage. | `600` · `5` · `5` · `20` · `30` · `15` |
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
