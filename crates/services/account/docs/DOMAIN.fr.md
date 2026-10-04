---
i18n:
  source: ./DOMAIN.md
  source_sha256: 41b31b4820853c1d04e1a20c835747a012d5dcffb0d58c423f01dd98d521b6ca
  translated_at: 2026-10-04
  status: complete
---
> 🇫🇷 Traduction française — la version **anglaise** [`DOMAIN.md`](./DOMAIN.md) fait foi.
> En cas de divergence, l'anglais prime. Les contrats (codes d'erreur, topics, variables
> d'environnement, noms de types, identifiants d'ADR) restent en anglais.

# `account` — Contrat de Domaine & Fonctionnel

> **Domain Card**
>
> | | |
> |---|---|
> | **Bounded Context** | Account / Identity — le système de référence du compte utilisateur |
> | **Classe de sous-domaine** | **Supporting** — le cycle de vie de l'identité est une infrastructure nécessaire, pas un différenciateur visible ; sur-mesure car il porte la PII et les obligations RGPD de la plateforme |
> | **System of …** | **Record** pour l'existence du compte, les métadonnées d'identifiants, le KYC, les rôles et l'état RGPD |
> | **Racine(s) d'agrégat** | `Account` (`domain`) |
> | **Tier** | **TIER-0** |
> | **Posture de défaillance** | **Fail-closed** — les écritures de compte font autorité ; une écriture de cycle de vie doit commiter durablement |
> | **Contextes amont** | `auth` (lien de sujet IdP fédéré), flux internes d'ops/inscription |
> | **Contextes aval** | `audit` (conformité), `profile` (persona sur le compte) — via **Open-Host Service / Published Language** (`account.v1.events`) |
> | **Journal de décisions** | [`ADR-0004`](../../../../docs/adr/0004-account-is-the-single-identity-sor.md) |

---

## 1. Capacité Métier & Non-Objectifs

**Capacité.** `account` est l'autorité pour **le compte utilisateur** : il répond à
**« ce compte existe-t-il, quel est son état de cycle de vie, son rôle et son statut KYC, et quel est
l'état licite de ses données personnelles ? »**

**Le problème difficile.** Posséder la PII en tant que système de référence tout en honorant le
RGPD — effacement, export, rectification — sans que d'autres services détiennent des copies fantômes
d'identité que l'effacement ne peut atteindre. Account est le *seul* endroit où vit une identité
réelle ; tous les autres détiennent des références dérivées, non faisant-autorité.

**Non-objectifs — ce que ce contexte ne fait délibérément PAS :**
- ❌ Authentifier / émettre sessions ou tokens → possédé par `auth`.
- ❌ Détenir le persona public (handle, bio, avatar) → possédé par `profile`.
- ❌ Vérifier les tokens → c'est la bibliothèque `auth-context`.

---

## 2. Langage Omniprésent

| Terme | Sens dans ce contexte | Symbole de code |
|---|---|---|
| Account | L'enregistrement de compte utilisateur faisant autorité | `Account`, `AccountId` |
| Identity id | L'identifiant de sujet stable inter-systèmes | `IdentityId` |
| Credential metadata | Hash de mot de passe, enrôlement MFA, codes de récupération — *métadonnées*, pas le flux d'auth | `PasswordHash`, `MfaState`, `RecoveryCodeHash` |
| KYC status | L'état de vérification know-your-customer | `KycStatus`, `KycStatusChanged` |
| Role | Le rôle d'autorisation assigné au compte | `AccountRole`, `RoleAssigned`/`RoleRevoked` |
| GDPR record | L'état de traitement licite des données (demandes export/suppression) | `GdprRecord`, `GdprDeletionRequested`, `GdprDataExportRequested` |
| Encrypted bytes | Les champs PII chiffrés au repos | `EncryptedBytes` |

---

## 3. Modèle de Domaine

| Élément | Type | Frontière d'invariant gardée |
|---|---|---|
| `Account` | racine d'agrégat | Machine à états du cycle de vie + unicité email/identité |
| `EmailAddress` / `PhoneNumber` / `CountryCode` | VO | Validité imposée à la construction |
| `PasswordHash` / `MfaState` / `RecoveryCodeHash` | VO | Intégrité des métadonnées d'identifiants |
| `GdprRecord` | VO | État des demandes d'effacement/export |
| `AccountStatus` / `AccountRole` / `KycStatus` | enum | Vocabulaires fermés cycle de vie / autorisation / vérification |

**Cycle de vie :**

```
created --> activated --(suspend)--> suspended --(reactivate)--> activated
   │            │                                                   │
   │            └--(deactivate)--> deactivated --(le titulaire se reconnecte : Login auth)--> activated
   └────────────────────────── gdpr_deletion_requested ──> deleted (PII erased)
```

> **Transitions légales uniquement.** Les changements d'email/téléphone sont des événements, pas des
> mutations silencieuses ; une suppression RGPD est terminale et déclenche le crypto-shred aval dans
> `audit`.

---

## 4. Propriété des Données & Frontières

**Ce contexte est la source de vérité pour :**
- Les enregistrements de compte, métadonnées d'identifiants, rôles, KYC et état RGPD — **Postgres**
  db `account`. Aucun autre service ne les écrit.

**Ce contexte détient des copies qu'il ne possède PAS :**

| Donnée copiée | Possédée par | Maintenue fraîche via | Tolérance d'obsolescence |
|---|---|---|---|
| Lien sujet IdP | `auth` / IdP fédéré | flux de liaison `auth` | au moment de la liaison |

**La liste « ne-pas-écrire » :** account n'émet jamais de tokens, n'écrit jamais de données de
présentation de profil, et ne stocke la projection d'aucun autre service.

---

## 5. Invariants & Règles Métier

| # | Invariant | Imposé à | En cas de violation |
|---|---|---|---|
| I1 | Unicité email/identité | domaine + contrainte unique Postgres | `ACC-1xxx` |
| I2 | La PII au repos est chiffrée (`EncryptedBytes`) | infrastructure | — |
| I3 | Un changement de cycle de vie émet un événement (pas de changement d'état silencieux) | domaine (publish après-save) | — |
| I4 | La suppression RGPD est terminale et propage l'effacement en aval | application | `ACC-1xxx` |
| I5 | Les rôles sont des octrois/révocations explicites, audités | domaine | `ACC-1xxx` |

---

## 6. Workflows & Orchestration

> En ligne jusqu'à ce qu'un C4 corrigé soit régénéré depuis `docs/domain/`.

**Inscription / cycle de vie.** Une commande create/activate/suspend/deactivate mute l'agrégat
`Account`, persiste vers Postgres, et publie la variante `account.v1.events` correspondante **après**
la sauvegarde (le port EventPublisher → Kafka).

**Effacement RGPD (Art. 17).** `gdpr_deletion_requested` → account marque l'enregistrement supprimé
et émet l'événement ; `audit` le consomme et **crypto-shred le DEK par-sujet**, rendant toute la PII
scellée de ce sujet à travers la flotte définitivement illisible alors que la chaîne se vérifie
toujours — fermant la boucle d'effacement de bout en bout.

**Export RGPD (Art. 15/20).** `gdpr_data_export_requested` → émis pour exécution en aval.

---

## 7. Relations de Contexte (extrait de Context-Map)

| Contexte voisin | Direction | Pattern | Mécanisme | Ce qui casse s'il change |
|---|---|---|---|---|
| `auth` | amont | Customer/Supplier | `SubjectLink` IdP ↔ `AccountId` | résolution du sujet de session |
| `audit` | aval | Published Language | `account.v1.events` (PII scellée ; paire RGPD) | preuve de conformité + boucle Art. 17 |
| `profile` | aval | Published Language | `account.v1.events` | provisionnement du persona |

> **Anti-Corruption Layer :** les consommateurs (`audit`, `profile`) traduisent `account.v1.events`
> vers leurs propres modèles ; account expose un langage publié stable et ne possède le schéma
> d'aucun consommateur.

---

## 8. Événements de Domaine (sémantique, pas wire)

| Événement (`account.v1.events`) | Signifie | Émis quand | Qui réagit |
|---|---|---|---|
| `account_created` / `email_changed` / `email_verified` / `phone_changed` | faits de cycle de vie porteurs de PII | la commande correspondante commite | `audit` (PII scellée), `profile` |
| `password_changed` / `mfa_enrolled` / `mfa_revoked` | faits de sécurité (sans PII) | changement d'identifiant | `audit` (Authentication) |
| `activated`/`deactivated`/`suspended`/`deleted`, `kyc_status_changed` | cycle de vie de l'identité | transition de cycle de vie | `audit` (Identity), `profile` |
| `role_assigned` / `role_revoked` | changement d'autorisation | octroi/révocation de rôle | `audit` (Authorization) |
| `gdpr_deletion_requested` / `gdpr_data_export_requested` | un droit licite sur les données a été invoqué | demande utilisateur/DPO | `audit` (`gdpr_deletion` → crypto-shred du sujet) |

---

## 9. Décisions & Justification

| Décision | ADR | Statut |
|---|---|---|
| Account est le SoR d'identité ; a construit sa voie d'événements sortante (était un producteur fantôme) pour que `audit`/`profile` consomment | [`ADR-0004`](../../../../docs/adr/0004-account-is-the-single-identity-sor.md) | Accepté |

---

## 10. Classification de Sous-domaine & Évolution

- **Classification :** Supporting — investir pour la correction, la sûreté de la PII et la conformité RGPD ; pas un différenciateur.
- **Volatilité :** faible — guidée par le changement réglementaire et l'intégration IdP, pas par le churn de features.
- **Dette de modélisation connue :** rien de matériel consigné.
- **Capacités différées :** workflows KYC plus riches ; pipeline d'exécution d'export en aval de l'événement d'export.
