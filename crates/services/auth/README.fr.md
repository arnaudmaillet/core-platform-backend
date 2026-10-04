---
i18n:
  source: ./README.md
  source_sha256: 3c638222b7a00428117be154e0c42f92184b8abc0024958200043fc24077740c
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
| `account` (gRPC) | résolution compte + gating actif au `Login` | `Login` échoue | **Dur** pour les nouvelles connexions |
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
| `AUTH_ACCOUNT_GRPC_ENDPOINT` | Endpoint du service `account` | `http://localhost:50059` |
| `AUTH_ACCOUNT_RPC_TIMEOUT_MS` · `AUTH_ACCOUNT_CONNECT_TIMEOUT_MS` | Deadlines par requête / de connexion sur le canal `account` (chemin chaud du login — échouer vite, ne jamais bloquer) | `2000` · `2000` |
| `AUTH_IDP_HTTP_TIMEOUT_MS` · `AUTH_IDP_CONNECT_TIMEOUT_MS` | Deadlines de requête / de connexion des appels HTTP Keycloak (échange de token) | `5000` · `2000` |
| `AUTH_GUEST_SESSIONS_ENABLED` | Interrupteur de `StartGuestSession`. **Désactivé par défaut** : il écrit une session par appel sans identifiant, donc à laisser éteint partout où les contrôles anti-abus (limites par IP / par appareil, App Attest) ne sont pas devant lui. Éteint → `AUT-1005` (`PERMISSION_DENIED`). | `false` |
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
