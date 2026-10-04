---
i18n:
  source: ./DOMAIN.md
  source_sha256: 083bd40ad38a9098f2ba48fd0d2b9f730d16ee2f1cb3673026bf82cc6cb7c07c
  translated_at: 2026-10-04
  status: complete
---
> 🇫🇷 Traduction française — la version **anglaise** [`DOMAIN.md`](./DOMAIN.md) fait foi.
> En cas de divergence, l'anglais prime. Les contrats (codes d'erreur, topics, variables
> d'environnement, noms de types, identifiants d'ADR) restent en anglais.

# `auth` — Contrat de Domaine & Fonctionnel

> **Domain Card**
>
> | | |
> |---|---|
> | **Bounded Context** | Authentication — émission de sessions, refresh et courtage IdP |
> | **Classe de sous-domaine** | **Supporting** — capacité de sécurité nécessaire ; s'appuie en partie sur un IdP fédéré (Keycloak) mais le modèle edge-token / génération de session est sur-mesure |
> | **System of …** | **Record** pour les sessions et refresh tokens (pas pour le compte utilisateur — c'est `account`) |
> | **Racine(s) d'agrégat** | `Session`, `RefreshToken` (`domain`) |
> | **Tier** | **TIER-0** |
> | **Posture de défaillance** | **Fail-closed** — pas de session/token valide, pas d'accès |
> | **Contextes amont** | flux de connexion utilisateur ; IdP fédéré (Keycloak) ; `account` (lien sujet ↔ compte) |
> | **Contextes aval** | chaque service via la bibliothèque de vérification `auth-context` ; `realtime` (vérif edge-token au handshake) ; `audit` (`auth.v1.events`) |
> | **Journal de décisions** | [`ADR-0005`](../../../../docs/adr/0005-auth-federated-idp-with-platform-edge-tokens.md) |

---

## 1. Capacité Métier & Non-Objectifs

**Capacité.** `auth` est l'autorité pour **l'état d'authentification** : il répond à
**« cet appelant est-il bien celui qu'il prétend être en ce moment, et cette session est-elle encore
valide ? »** — en émettant des edge tokens ES256 courts et en gérant le cycle de refresh.

**Le problème difficile.** Fédérer un IdP externe tout en émettant les propres edge tokens rapides
(stateless-à-vérifier) de la plateforme — et pouvoir **révoquer** instantanément malgré l'absence
d'état. Le mécanisme de résolution est une `Generation` monotone par-sujet qui invalide les familles
de tokens.

**Non-objectifs — ce que ce contexte ne fait délibérément PAS :**
- ❌ Posséder le compte utilisateur / la PII → `account` est le SoR.
- ❌ Vérifier les tokens en process pour chaque service → c'est la bibliothèque `auth-context` (une vérification, pas un appel).
- ❌ Autoriser les actions métier → il authentifie ; les services autorisent via permissions/`auth-context`.

---

## 2. Langage Omniprésent

| Terme | Sens dans ce contexte | Symbole de code |
|---|---|---|
| Session | Une session authentifiée de référence | `Session`, `SessionId`, `SessionStatus` |
| Refresh token | L'identifiant longue durée qui frappe des access tokens | `RefreshToken`, `RefreshTokenId`, `RefreshTokenHash` |
| Access token claims | La charge utile de l'edge token ES256 | `AccessTokenClaims` |
| Generation | Compteur monotone par-sujet ; l'incrémenter révoque une famille de tokens | `Generation` |
| IdP subject | L'identifiant de sujet du fournisseur d'identité fédéré | `IdpSubject`, `SubjectLink` |
| Device fingerprint | Liaison par-appareil pour une session | `DeviceFingerprint` |
| Permission | Un octroi d'autorisation porté dans les claims | `Permission` |
| Revocation reason | Pourquoi une session/token a été révoquée | `RevocationReason` |

---

## 3. Modèle de Domaine

| Élément | Type | Frontière d'invariant gardée |
|---|---|---|
| `Session` | racine d'agrégat | Validité de session + révocation via `Generation` |
| `RefreshToken` | racine d'agrégat | Rotation de token ; un refresh token utilisé ne peut être réutilisé |
| `AccessTokenClaims` | VO | Le contrat de l'edge token signé |
| `Generation` | VO | Monotone par-sujet ; le levier de révocation instantanée |
| `SubjectLink` | VO | Liaison sujet IdP ↔ `AccountId` |

**Cycle de vie de session :**

```
issued --(refresh: rotate)--> issued' --(revoke / generation-bump / expiry)--> revoked
```

> **Transitions légales uniquement.** Un refresh token tourne (l'ancien invalidé) ; un bump de
> `Generation` révoque toute la famille ; les sessions expirées/révoquées ne se ré-activent jamais.

---

## 4. Propriété des Données & Frontières

**Ce contexte est la source de vérité pour :**
- Sessions et refresh tokens — **Postgres** (durable) + **Redis** (état hot de session/révocation). Aucun autre service ne les écrit.

**Ce contexte détient des copies qu'il ne possède PAS :**

| Donnée copiée | Possédée par | Maintenue fraîche via | Tolérance d'obsolescence |
|---|---|---|---|
| Lien compte ↔ sujet IdP | `account` / IdP | flux de liaison | au moment de la liaison |

**La liste « ne-pas-écrire » :** auth ne mute jamais l'état du compte et ne stocke aucune donnée de profil/métier.

---

## 5. Invariants & Règles Métier

| # | Invariant | Imposé à | En cas de violation |
|---|---|---|---|
| I1 | Un refresh token est à usage unique (la rotation invalide le précédent) | domaine | `AUT-7xxx` |
| I2 | Un bump de `Generation` révoque tous les tokens de cette famille de sujet | domaine | (révocation) |
| I3 | Les access tokens sont courts et signés ES256 | domaine + infrastructure | la vérif échoue en aval |
| I4 | La révocation est fail-closed et immédiate | application | — |

---

## 6. Workflows & Orchestration

> En ligne jusqu'à ce qu'un C4 corrigé soit régénéré depuis `docs/domain/`.

**Émission.** Connexion (IdP fédéré vérifié) → créer `Session` + `RefreshToken`, frapper un access
token ES256, persister, et émettre `session_issued` sur `auth.v1.events`.

**Refresh (rotation).** Présenter le refresh token → valider + tourner (l'ancien invalidé) → frapper
un nouvel access token. La réutilisation d'un token tourné est un signal de sécurité.

**Révocation.** Déconnexion explicite, événement de sécurité, ou bump de `Generation` → marquer
révoqué, émettre `session_revoked`. La vérification en aval (via `auth-context`) échoue fermée
ensuite.

---

## 7. Relations de Contexte (extrait de Context-Map)

| Contexte voisin | Direction | Pattern | Mécanisme | Ce qui casse s'il change |
|---|---|---|---|---|
| IdP fédéré (Keycloak) | amont | Conformist | OIDC/identifiants | la connexion casse |
| `account` | pair | Customer/Supplier | `SubjectLink` ↔ `AccountId` | résolution du sujet |
| tous les services | aval | Open-Host Service (Published Language) | edge token ES256 vérifié par `auth-context` | tout appel authentifié casse |
| `realtime` | aval | Conformist (verify-only) | vérif edge-token au handshake WS | les nouvelles connexions ne peuvent s'authentifier |
| `audit` | aval | Published Language | `auth.v1.events` | la preuve du cycle de vie des sessions casse |

> **Anti-Corruption Layer :** l'adaptateur IdP-fédéré traduit les claims OIDC externes vers le modèle
> interne `Session` / `AccessTokenClaims`.

---

## 8. Événements de Domaine (sémantique, pas wire)

| Événement (`auth.v1.events`) | Signifie | Émis quand | Qui réagit |
|---|---|---|---|
| `session_issued` | une session authentifiée a été établie | connexion / émission de token | `audit` (Authentication) |
| `session_revoked` | une session a été invalidée | déconnexion / révocation / bump de génération | `audit` (Authentication) |
| `subject_linked` | un sujet IdP a été lié à un compte | flux de liaison de compte | (interne) |

---

## 9. Décisions & Justification

| Décision | ADR | Statut |
|---|---|---|
| Identifiants Keycloak fédérés + edge tokens ES256 sur-mesure ; distinct de `account` (SoR) et `auth-context` (lib de vérif) | [`ADR-0005`](../../../../docs/adr/0005-auth-federated-idp-with-platform-edge-tokens.md) | Accepté |
| Révocation instantanée via `Generation` monotone par-sujet malgré des tokens stateless | [`ADR-0005`](../../../../docs/adr/0005-auth-federated-idp-with-platform-edge-tokens.md) | Accepté |

---

## 10. Classification de Sous-domaine & Évolution

- **Classification :** Supporting — critique pour la sécurité, fédère un IdP générique, sur-mesure seulement à la couche edge-token/session.
- **Volatilité :** faible-à-moyenne — guidée par la posture de sécurité et les changements d'IdP.
- **Dette de modélisation connue :** rien de matériel consigné.
- **Capacités différées :** la MFA comme méthode de step-up (TOTP / codes de secours, #649) et les passkeys ; surfaces de gestion d'appareils/sessions plus riches. Le step-up par mot de passe (`VerifyCredentials` → `auth_time`) est en service.
