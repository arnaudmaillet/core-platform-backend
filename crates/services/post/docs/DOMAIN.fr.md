---
i18n:
  source: ./DOMAIN.md
  source_sha256: 75e4499f22103c926b10aad30d9522e3a317b1bd16098a358d0a06710d87c7fb
  translated_at: 2026-10-10
  status: complete
---
> 🇫🇷 Traduction française — la version **anglaise** [`DOMAIN.md`](./DOMAIN.md) fait foi.
> En cas de divergence, l'anglais prime. Les contrats (codes d'erreur, topics, variables
> d'environnement, noms de types, identifiants d'ADR) restent en anglais.

# `post` — Contrat de Domaine & Fonctionnel

> **Domain Card**
>
> | | |
> |---|---|
> | **Bounded Context** | Posts — le cycle de vie du contenu |
> | **Classe de sous-domaine** | **Core** — le contenu publié est la substance primaire de la plateforme |
> | **System of …** | **Record** pour les posts et leur cycle de vie |
> | **Racine(s) d'agrégat** | `Post` (`domain`) |
> | **Tier** | **TIER-0** |
> | **Posture de défaillance** | **Fail-closed en écriture** — un post publié doit persister |
> | **Contextes amont** | clients (auteur) ; `profile` (identité auteur) ; `moderation` (filtrage) ; `media` (pièces jointes) |
> | **Contextes aval** | `timeline`, `geo-discovery`, `search`, `counter`, `realtime` — via **Published Language** (`post.v1.events`) |
> | **Journal de décisions** | [`ADR-0013`](../../../../docs/adr/0013-post-two-table-scylla-with-published-language.md) |

---

## 1. Capacité Métier & Non-Objectifs

**Capacité.** `post` est l'autorité pour **le contenu** : il répond à
**« qu'a été publié par qui, dans quel état, avec quel média — et est-ce encore en ligne ? »**

**Le problème difficile.** Posséder un store de contenu à forte écriture à bas coût et le servir par
auteur, tout en étant la source de fan-out dont dépend tout le côté lecture — un layout ScyllaDB à
deux tables (`post.posts` par id + `post.posts_by_profile` par auteur) avec `post.v1.events` comme
langage publié.

**Non-objectifs — ce que ce contexte ne fait délibérément PAS :**
- ❌ Construire des feeds/timelines → `timeline` consomme `post.v1.events`.
- ❌ Indexer pour la recherche/découverte → `search` / `geo-discovery` consomment les événements.
- ❌ Posséder les octets média → référence les pièces jointes `media` ; les comptes → `counter`.

---

## 2. Langage Omniprésent

| Terme | Sens dans ce contexte | Symbole de code |
|---|---|---|
| Post | Une unité de contenu publié | `Post`, `PostId`, `PostKind`, `PostStatus` |
| Caption | Le texte du post | `Caption` |
| Location | Coordonnées du post optionnelles fournies par le client (WGS-84) | `GeoPoint` |
| Media attachment | Une référence à un asset `media` + son URL CDN | `MediaAttachment`, `CdnUrl` |
| Audio reference | Piste audio attachée | `AudioReference`, `AudioId`, `AudioKind` |

---

## 3. Modèle de Domaine

| Élément | Type | Frontière d'invariant gardée |
|---|---|---|
| `Post` | racine d'agrégat | La machine à états du cycle de vie du contenu |
| `Caption` / `MediaAttachment` / `AudioReference` / `GeoPoint` | VO | Validité du contenu + réf. média/audio + lat/lng valides |
| `PostKind` / `PostStatus` | enum | Vocabulaires fermés kind/status (le proto mappe kind/status +1) |

**Cycle de vie :**

```
published --(update)--> published' --(delete)--> deleted   |   (la porte de modération peut bloquer/retirer)
```

> **Transitions légales uniquement.** Les enums proto mappent le tinyint domaine +1 (pas de
> sentinelle UNSPECIFIED) ; une suppression émet `post.deleted` pour le démantèlement aval.

---

## 4. Propriété des Données & Frontières

**Ce contexte est la source de vérité pour :**
- Les posts — **ScyllaDB** deux tables (`post.posts` par id, `post.posts_by_profile` par auteur). Aucun autre service ne les écrit.

**Ce contexte détient des copies qu'il ne possède PAS :**

| Donnée copiée | Possédée par | Maintenue fraîche via | Tolérance d'obsolescence |
|---|---|---|---|
| Champs d'instantané de profil auteur | `profile` | `profile.v1.events` (DLQ `profile.v1.events.dlq`) | cohérence à terme |
| État de modération | `moderation` | `moderation.v1.events` | cohérence à terme |

**La liste « ne-pas-écrire » :** post ne construit jamais de feeds, index ou comptes — il émet les événements dont ceux-ci dérivent.

---

## 5. Invariants & Règles Métier

| # | Invariant | Imposé à | En cas de violation |
|---|---|---|---|
| I1 | Un post référence un auteur valide | domaine | `PST-1xxx` |
| I2 | Les deux tables restent cohérentes (id + by-profile) | domaine/application | `PST-1xxx` |
| I3 | Un changement de cycle de vie émet le `post.v1.events` correspondant | domaine (après-save) | — |
| I4 | Le mapping proto kind/status est +1 sans UNSPECIFIED | infrastructure (codec) | — |

---

## 6. Workflows & Orchestration

> En ligne jusqu'à ce qu'un C4 corrigé soit régénéré depuis `docs/domain/`.

**Publier / mettre à jour / supprimer.** Commande autorisée → écrire les deux tables Scylla →
publier `PostPublished` / `PostUpdated` / `PostDeleted` sur `post.v1.events` (et `post.deleted`, le seul
topic legacy restant, #487). En aval, `timeline` fan-out, `search`/`geo-discovery` indexent,
`notification` notifie les mentions, `realtime` broadcast.

**Dénormalisation.** Consommer `profile.v1.events` pour garder frais les champs d'instantané auteur ;
consommer `moderation.v1.events` pour refléter l'enforcement.

---

## 7. Relations de Contexte (extrait de Context-Map)

| Contexte voisin | Direction | Pattern | Mécanisme | Ce qui casse s'il change |
|---|---|---|---|---|
| `profile` | amont | ACL | `profile.v1.events` | les instantanés d'auteur deviennent périmés |
| `moderation` | amont | ACL | `moderation.v1.events` | le reflet de l'enforcement casse |
| `media` | amont | Customer/Supplier | références `MediaAttachment` | média cassé dans les posts |
| `timeline`/`search`/`geo-discovery`/`counter`/`realtime` | aval | Published Language (OHS) | `post.v1.events` | tout le côté lecture/découverte casse |

> **Anti-Corruption Layer :** les consumers d'événements `profile`/`moderation` traduisent les formes
> wire étrangères vers les champs dénormalisés de post.

---

## 8. Événements de Domaine (sémantique, pas wire)

| Événement (`post.v1.events`) | Signifie | Émis quand | Qui réagit |
|---|---|---|---|
| `PostPublished` | un nouveau contenu est en ligne (porte `caption`, `thumbnail_url`, `lat`/`lng` optionnels pour `geo`) | la publication commite | `timeline` (fan-out), `search`/`geo` (index), `notification` (mentions), `realtime` |
| `PostUpdated` | le contenu a été édité | l'édition commite | `search` (ré-indexation) |
| `post.deleted` | le contenu a été retiré | la suppression commite | `timeline`/`search`/`geo` (démantèlement) |

---

## 9. Décisions & Justification

| Décision | ADR | Statut |
|---|---|---|
| Layout ScyllaDB deux tables (par id + par auteur) avec `post.v1.events` comme langage publié | [`ADR-0013`](../../../../docs/adr/0013-post-two-table-scylla-with-published-language.md) | Accepté |
| Enrichissement de payload post→geo : `PostPublished` porte caption + miniature + localisation optionnelle (fournie par le client au `CreatePost`) ; les posts sans localisation ne sont pas géo-indexés | _résolu — voir geo-discovery §6_ | Accepté |

---

## 10. Classification de Sous-domaine & Évolution

- **Classification :** Core — le contenu est la substance primaire de la plateforme.
- **Volatilité :** moyenne — les types de post et pièces jointes évoluent.
- **Dette de modélisation connue :** la localisation n'est capturée qu'au `CreatePost` (pas encore de commande « définir la localisation » dédiée) ; les éditions ne la ré-émettent pas.
- **Capacités différées :** média/audio plus riches ; posts programmés ; historique d'édition.
