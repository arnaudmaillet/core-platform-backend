---
i18n:
  source: ./DOMAIN.md
  source_sha256: e929b444419ccd6713e531c0ffc6c28e189b2ecea9efff610531f7c2f2e5d90a
  translated_at: 2026-10-07
  status: complete
---
> 🇫🇷 Traduction française — la version **anglaise** [`DOMAIN.md`](./DOMAIN.md) fait foi.
> En cas de divergence, l'anglais prime. Les contrats (codes d'erreur, topics, variables
> d'environnement, noms de types, identifiants d'ADR) restent en anglais.

# `media` — Contrat de Domaine & Fonctionnel

> **Domain Card**
>
> | | |
> |---|---|
> | **Bounded Context** | Media — plan de contrôle du cycle de vie d'asset + courtage de livraison |
> | **Classe de sous-domaine** | **Supporting** — gestion média nécessaire ; pipeline sur-mesure, mais pas l'origine de la valeur produit |
> | **System of …** | **Record** pour les *métadonnées/cycle de vie* d'asset (les octets vivent dans le store objet, pas ici) |
> | **Racine(s) d'agrégat** | `Asset` (`domain`) |
> | **Tier** | **TIER-1** |
> | **Posture de défaillance** | **Mixte** — *fail-open* sur la résolution de livraison ; *fail-closed* sur le `Screen` CSAM avant qu'un asset ne devienne ready |
> | **Contextes amont** | clients (tickets d'upload) ; `moderation` (Screen + takedown) |
> | **Contextes aval** | `post`, `profile`, `search` — via **Published Language** (`media.v1.events`) |
> | **Journal de décisions** | [`ADR-0011`](../../../../docs/adr/0011-media-byte-free-control-plane.md) |

---

## 1. Capacité Métier & Non-Objectifs

**Capacité.** `media` est l'autorité pour **le cycle de vie de l'asset média** : il répond à
**« cet asset est-il uploadé, screené, transformé et sûr à livrer — et depuis où ? »**

**Le problème difficile.** Gérer de gros binaires sans jamais mettre d'octets sur gRPC/Kafka : un
design à trois plans — (A) un courtier d'upload émettant des tickets **pré-signés direct-vers-store**,
(B) un pipeline de transformation Kafka async, (C) un courtage de résolution-de-livraison/CDN — avec
un **Screen CSAM fail-closed** gardant la mise en ready.

**Non-objectifs — ce que ce contexte ne fait délibérément PAS :**
- ❌ Faire transiter des octets par gRPC/Kafka → les uploads vont direct au store objet via URL pré-signées.
- ❌ Décider la politique de modération → `moderation` décide ; media applique Screen/takedown.
- ❌ Posséder l'endroit où l'asset est *utilisé* → `post` / `profile` le référencent.

---

## 2. Langage Omniprésent

| Terme | Sens dans ce contexte | Symbole de code |
|---|---|---|
| Asset | Un asset média et son cycle de vie | `Asset`, `AssetId`, `AssetState` |
| Upload ticket | Un octroi d'upload pré-signé direct-vers-store | `UploadTicket`, `ReserveParams`, `UploadConstraints` |
| Rendition | Une variante dérivée (taille/format) d'un asset | `Rendition`, `RenditionKind` |
| Content hash | Le hash de dédup/identité des octets | `ContentHash` |
| Storage key | L'emplacement dans le store objet | `StorageKey` |
| Blurhash / dimensions | Placeholder perceptuel + métadonnées de taille | `Blurhash`, `Dimensions` |
| Delivery visibility | Si/comment un asset peut être servi | `DeliveryVisibility` |

---

## 3. Modèle de Domaine

| Élément | Type | Frontière d'invariant gardée |
|---|---|---|
| `Asset` | racine d'agrégat | La machine à états du cycle de vie de l'asset |
| `UploadTicket` / `ReserveParams` / `UploadConstraints` | VO | Un octroi d'upload borné, pré-signé |
| `Rendition` / `RenditionKind` | VO/enum | Variantes dérivées |
| `ContentHash` / `StorageKey` / `MimeType` / `Dimensions` / `Blurhash` | VO | Identité des octets + emplacement + métadonnées |
| `MediaKind` / `AssetState` / `DeliveryVisibility` | enum | Vocabulaires fermés kind/état/livraison |

**Cycle de vie de l'asset :**

```
reserved --(upload+finalize)--> uploaded --(Screen: fail-closed)--> ready --(variant)--> variant_ready
   │                                  │                                │
   │                                  └--(Screen fail)--> quarantined  └--(takedown)--> deleted --(restore)--> ready
   └--(timeout)--> failed
```

> **Transitions légales uniquement.** Un asset ne peut atteindre `ready` sans passer le `Screen`
> CSAM ; un takedown met en quarantaine/supprime ; la restauration est explicite.

---

## 4. Propriété des Données & Frontières

**Ce contexte est la source de vérité pour :**
- Les métadonnées/cycle de vie d'asset — **Postgres** (doc asset, JSONB) + **Redis** (cache). Les
  **octets** vivent dans **S3/MinIO** (store objet), référencés par `StorageKey` — media possède le
  plan de contrôle, pas une copie octets-en-base.

**La liste « ne-pas-écrire » :** media ne décide jamais la politique de modération et ne possède pas
l'endroit où un asset est intégré (`post`/`profile` le référencent).

---

## 5. Invariants & Règles Métier

| # | Invariant | Imposé à | En cas de violation |
|---|---|---|---|
| I1 | Les octets ne traversent jamais gRPC/Kafka — seulement pré-signé direct-vers-store | infrastructure | `MED-1xxx` |
| I2 | Un asset atteint `ready` seulement après un `Screen` CSAM réussi (fail-closed) | application | `MED-7xxx` |
| I3 | Un takedown de modération met en quarantaine/supprime l'asset | application (consumer) | `MED-6xxx` |
| I4 | La résolution de livraison échoue ouverte (dégrade, jamais ne bloque) | application | `MED-5xxx` |

---

## 6. Workflows & Orchestration

> En ligne jusqu'à ce qu'un C4 corrigé soit régénéré depuis `docs/domain/`.

**Plan A — courtier d'upload.** Le client demande un `UploadTicket` → media réserve un `Asset` +
émet une URL pré-signée → le client upload **direct vers le store objet** → finalize.

**Plan B — transformation (async).** Sur `media.v1.events` (uploaded), le processor sonde l'image,
exécute le **Screen CSAM fail-closed** (gRPC vers `moderation`, timeout dur), génère les renditions +
blurhash, et transitionne l'asset vers `ready` (ou `quarantined`).

**Plan C — livraison (fail-open).** Résoudre un asset → URL CDN (courtage CloudFront). Un consumer de
takedown `moderation` met en quarantaine/supprime à la demande.

---

## 7. Relations de Contexte (extrait de Context-Map)

| Contexte voisin | Direction | Pattern | Mécanisme | Ce qui casse s'il change |
|---|---|---|---|---|
| clients | amont | OHS | tickets d'upload pré-signés | les uploads cassent |
| `moderation` | amont | Customer/Supplier (sync) + ACL | gRPC `Screen` + consumer de takedown | filtrage de mise-en-ready / takedowns cassent |
| `post` / `profile` / `search` | aval | Published Language | `media.v1.events` | intégrations/indexation cassent |

> **Anti-Corruption Layer :** les adaptateurs store-objet + CDN isolent le plan octets ; le décodage
> de takedown de modération mappe les événements étrangers vers des transitions d'asset.

---

## 8. Événements de Domaine (sémantique, pas wire)

| Événement (`media.v1.events`) | Signifie | Émis quand | Qui réagit |
|---|---|---|---|
| `asset_uploaded` | les octets ont atterri dans le store | finalize | le pipeline de transformation (Plan B) |
| `asset_ready` / `asset_variant_ready` | l'asset (ou une variante) est sûr à livrer | Screen passe / rendition terminée | `post`, `profile`, `search` |
| `asset_quarantined` / `asset_deleted` / `asset_restored` | transitions sécurité/cycle de vie | échec Screen / takedown / restauration | intégrations, livraison |
| `asset_failed` | le traitement a échoué | timeout/erreur | UX d'upload |

---

## 9. Décisions & Justification

| Décision | ADR | Statut |
|---|---|---|
| Plan de contrôle sans-octets — uploads pré-signés direct-vers-store, octets jamais sur gRPC/Kafka | [`ADR-0011`](../../../../docs/adr/0011-media-byte-free-control-plane.md) | Accepté |
| Split livraison fail-open / Screen CSAM fail-closed | [`ADR-0011`](../../../../docs/adr/0011-media-byte-free-control-plane.md) | Accepté |

---

## 10. Classification de Sous-domaine & Évolution

- **Classification :** Supporting — plomberie média nécessaire ; pipeline sur-mesure.
- **Volatilité :** moyenne — les nouveaux types média/renditions sont additifs.
- **Dette de modélisation connue :** images d'abord ; transcodage vidéo différé.
- **Capacités différées :** sidecar anti-malware réel, consumer orphan-GC, WebP/AVIF, purge RGPD dédup-refcount. Les médias d'un compte sont effacés avec le compte (`account_deleted`, #777 ; gels légaux conservés).
