---
i18n:
  source: ./DOMAIN.md
  source_sha256: f4cee3c07d9e1238e9fae3b36432231764f95e1a57cdeab3032314a4e3dd7c2f
  translated_at: 2026-10-11
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
> | **Bounded Context** | Authentication — vérification des identifiants, émission de sessions, refresh et révocation |
> | **Classe de sous-domaine** | **Supporting** — capacité de sécurité nécessaire ; s'appuie sur des identifiants standard (id_tokens Apple / Google, codes à usage unique, passkeys WebAuthn) vérifiés en interne ; le modèle edge-token / génération de session est sur-mesure |
> | **System of …** | **Record** pour les sessions et refresh tokens (pas pour le compte utilisateur — c'est `account`) |
> | **Racine(s) d'agrégat** | `Session`, `RefreshToken` (`domain`) |
> | **Tier** | **TIER-0** |
> | **Posture de défaillance** | **Fail-closed** — pas de session/token valide, pas d'accès |
> | **Contextes amont** | flux de connexion utilisateur ; Apple / Google (clés publiées des id_tokens) ; `account` (lien sujet ↔ compte) |
> | **Contextes aval** | chaque service via la bibliothèque de vérification `auth-context` ; `realtime` (vérif edge-token au handshake) ; `audit` (`auth.v1.events`) |
> | **Journal de décisions** | [`ADR-0005`](../../../../docs/adr/0005-auth-federated-idp-with-platform-edge-tokens.md) · [`ADR-0018`](../../../../docs/adr/0018-passwordless-identity-passkeys-for-staff.md) |

---

## 1. Capacité Métier & Non-Objectifs

**Capacité.** `auth` est l'autorité pour **l'état d'authentification** : il répond à
**« cet appelant est-il bien celui qu'il prétend être en ce moment, et cette session est-elle encore
valide ? »** — en émettant des edge tokens ES256 courts et en gérant le cycle de refresh.

**Le problème difficile.** Émettre les propres edge tokens rapides (stateless-à-vérifier) de la
plateforme — et pouvoir **révoquer** instantanément malgré l'absence d'état. Le mécanisme de
résolution est une `Generation` monotone par-sujet qui invalide les familles de tokens.

**Sans mot de passe (#881, ADR-0018).** Personne ne se connecte par mot de passe et il n'y a pas
d'IdP externe : auth vérifie lui-même chaque identifiant — un id_token Apple / Google natif (contre
les clés publiées du fournisseur, avec un nonce émis par le serveur), un code à usage unique envoyé à
une adresse e-mail ou un téléphone, ou une passkey. Les autorisations staff n'existent que dans les
sessions ouvertes avec une passkey enrôlée staff (#880). `Login` ne crée jamais de compte ni de lien
d'identité : une identité sans compte donne `AUT-6004` et part vers `SignUp`.

**Non-objectifs — ce que ce contexte ne fait délibérément PAS :**
- ❌ Posséder le compte utilisateur / la PII → `account` est le SoR.
- ❌ Vérifier les tokens en process pour chaque service → c'est la bibliothèque `auth-context` (une vérification, pas un appel).
- ❌ Autoriser les actions métier → il authentifie ; les services autorisent via permissions/`auth-context`.
- ❌ Stocker ou vérifier des mots de passe → il n'y en a aucun (#881) ; aucun flux de réinitialisation ou de changement de mot de passe n'existe.

---

## 2. Langage Omniprésent

| Terme | Sens dans ce contexte | Symbole de code |
|---|---|---|
| Session | Une session authentifiée de référence | `Session`, `SessionId`, `SessionStatus` |
| Refresh token | L'identifiant longue durée qui frappe des access tokens | `RefreshToken`, `RefreshTokenId`, `RefreshTokenHash` |
| Access token claims | La charge utile de l'edge token ES256 | `AccessTokenClaims` |
| Generation | Compteur monotone par-sujet ; l'incrémenter révoque une famille de tokens | `Generation` |
| Identity subject | Une identité de connexion `(issuer, subject)` : un sujet Apple / Google, ou une adresse e-mail / un numéro de téléphone sous `urn:core-platform:email` / `urn:core-platform:phone` (nom de type historique) | `IdpSubject`, `SubjectLink` |
| Device fingerprint | Liaison par-appareil pour une session | `DeviceFingerprint` |
| Permission | Un octroi d'autorisation porté dans les claims | `Permission` |
| Revocation reason | Pourquoi une session/token a été révoquée | `RevocationReason` |
| Credential | Ce que présente une connexion : un id_token, un code à usage unique ou une assertion de passkey — jamais un mot de passe | `AuthnGrant` |
| Staff passkey | Une passkey enrôlée avec une autorisation d'enrôlement staff ; le seul chemin vers une session portant des autorisations staff | `StaffGrant`, `StaffEnrollmentGrants` (port) |

---

## 3. Modèle de Domaine

| Élément | Type | Frontière d'invariant gardée |
|---|---|---|
| `Session` | racine d'agrégat | Validité de session + révocation via `Generation` |
| `RefreshToken` | racine d'agrégat | Rotation de token ; un refresh token utilisé ne peut être réutilisé |
| `AccessTokenClaims` | VO | Le contrat de l'edge token signé |
| `Generation` | VO | Monotone par-sujet ; le levier de révocation instantanée |
| `SubjectLink` | VO | Liaison sujet d'identité ↔ `AccountId` (créée au `SignUp`, jamais au `Login`) |

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
| Lien compte ↔ sujet d'identité | `account` (le compte) / auth (le lien) | `SignUp`, `ChangeContact` | au moment de la liaison |

**La liste « ne-pas-écrire » :** auth ne mute jamais l'état du compte et ne stocke aucune donnée de profil/métier.

---

## 5. Invariants & Règles Métier

| # | Invariant | Imposé à | En cas de violation |
|---|---|---|---|
| I1 | Un refresh token est à usage unique (la rotation invalide le précédent) | domaine | `AUT-7xxx` |
| I2 | Un bump de `Generation` révoque tous les tokens de cette famille de sujet | domaine | (révocation) |
| I3 | Les access tokens sont courts et signés ES256 | domaine + infrastructure | la vérif échoue en aval |
| I4 | La révocation est fail-closed et immédiate | application | — |
| I5 | Une autorisation staff n'est émise que dans une session ouverte avec une passkey enrôlée staff qui appartient toujours au compte (#880) | application (`Permission::for_session`, relue à chaque émission) | autorisations membres seulement |
| I6 | Une passkey n'est enrôlée staff qu'avec une autorisation à usage unique, non expirée, liée à son compte ; les autorisations viennent d'un admin dans une session par passkey staff (jamais pour lui-même) ou du bootstrap ponctuel tant qu'aucune passkey staff n'existe | application + Postgres (mise à jour conditionnelle) | `AUT-5032` / `AUT-5033` / `AUT-5034` |
| I7 | Une session staff fait son step-up avec sa passkey uniquement | application | `AUT-5031` |

---

## 6. Workflows & Orchestration

> En ligne jusqu'à ce qu'un C4 corrigé soit régénéré depuis `docs/domain/`.

**Émission.** Connexion (identifiant vérifié par auth : id_token, code ou passkey ; l'identité doit
déjà avoir un compte) → créer `Session` + `RefreshToken`, frapper un access
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
| Apple / Google | amont | Conformist | JWKS publiés ; id_tokens natifs | leur connexion casse (codes, passkeys intacts) |
| `account` | pair | Customer/Supplier | `SubjectLink` ↔ `AccountId` | résolution du sujet |
| tous les services | aval | Open-Host Service (Published Language) | edge token ES256 vérifié par `auth-context` | tout appel authentifié casse |
| `realtime` | aval | Conformist (verify-only) | vérif edge-token au handshake WS | les nouvelles connexions ne peuvent s'authentifier |
| `audit` | aval | Published Language | `auth.v1.events` | la preuve du cycle de vie des sessions casse |

> **Anti-Corruption Layer :** l'adaptateur `FederatedTokenVerifier` traduit les claims de l'id_token
> d'un fournisseur en un sujet d'identité interne ; rien de propre au fournisseur n'atteint le modèle
> `Session` / `AccessTokenClaims`.

---

## 8. Événements de Domaine (sémantique, pas wire)

| Événement (`auth.v1.events`) | Signifie | Émis quand | Qui réagit |
|---|---|---|---|
| `session_issued` | une session authentifiée a été établie | connexion / émission de token | `audit` (Authentication) |
| `session_revoked` | une session a été invalidée | déconnexion / révocation / bump de génération | `audit` (Authentication) |
| `subject_linked` | une identité de connexion a été liée à un compte | `SignUp` ; `ChangeContact` qui re-clé une identité par code | (interne) |
| `staff_enrollment_granted` | une autorisation d'enrôlement de passkey staff a été émise (par un admin, ou le bootstrap) | `IssueStaffPasskeyEnrollment` / `auth-server bootstrap-staff` | `audit` (PrivilegedAction) |
| `staff_passkey_enrolled` | une passkey est devenue passkey staff avec une autorisation | `FinishPasskeyRegistration` avec une autorisation | `audit` (PrivilegedAction) |

---

## 9. Décisions & Justification

| Décision | ADR | Statut |
|---|---|---|
| Identifiants par IdP fédéré + edge tokens ES256 sur-mesure ; distinct de `account` (SoR) et `auth-context` (lib de vérif) | [`ADR-0005`](../../../../docs/adr/0005-auth-federated-idp-with-platform-edge-tokens.md) | Accepté — partie IdP fédéré remplacée par l'ADR-0018 |
| Sans mot de passe : auth vérifie lui-même chaque identifiant (Apple / Google, codes, passkeys) ; le staff uniquement par passkeys staff | [`ADR-0018`](../../../../docs/adr/0018-passwordless-identity-passkeys-for-staff.md) | Accepté |
| Révocation instantanée via `Generation` monotone par-sujet malgré des tokens stateless | [`ADR-0005`](../../../../docs/adr/0005-auth-federated-idp-with-platform-edge-tokens.md) | Accepté |

---

## 10. Classification de Sous-domaine & Évolution

- **Classification :** Supporting — critique pour la sécurité ; identifiants standard vérifiés en interne, sur-mesure à la couche edge-token/session.
- **Volatilité :** faible-à-moyenne — guidée par la posture de sécurité et les changements de fournisseurs de connexion.
- **Dette de modélisation connue :** rien de matériel consigné.
- **Capacités différées :** surfaces de gestion d'appareils/sessions plus riches. Le step-up par code de deux étapes ou passkey (`VerifyCredentials` → `auth_time` ; une session staff avec sa passkey uniquement), la connexion en deux étapes (#649), les passkeys (#808 : enregistrement, et connexion comme premier facteur, seconde étape ou step-up) et les passkeys staff (#880) sont en service. Les mots de passe et l'IdP externe ont été retirés (#881).
