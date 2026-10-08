---
i18n:
  source: ./DOMAIN.md
  source_sha256: 5e53dc4ef4ea059f2a8475f30ad500b4f87e3bb8a2a7956cc4efe805455d5cc6
  translated_at: 2026-10-08
  status: complete
---
> 🇫🇷 Traduction française — la version **anglaise** [`DOMAIN.md`](./DOMAIN.md) fait foi.
> En cas de divergence, l'anglais prime. Les contrats (codes d'erreur, topics, variables
> d'environnement, noms de types, identifiants d'ADR) restent en anglais.

# `notification` — Contrat de Domaine & Fonctionnel

> **Domain Card**
>
> | | |
> |---|---|
> | **Bounded Context** | Notifications — le fil d'activité utilisateur + fan-out push |
> | **Classe de sous-domaine** | **Supporting** — un plan de livraison/fil dérivé ; ne possède aucun contenu source |
> | **System of …** | **Record** pour le fil d'activité de notifications (dérivé de faits amont) |
> | **Racine(s) d'agrégat** | `Notification` (`domain`) |
> | **Tier** | **TIER-2** — best-effort / dérivé |
> | **Posture de défaillance** | **Fail-open** — une notification manquée est re-dérivable ; rien ne bloque |
> | **Contextes amont** | `comment`, `engagement`, `post`, `social-graph` — via **ACL** sur Kafka |
> | **Contextes aval** | clients (lecture du fil + stream broadcast gRPC) ; push offline (APNs/FCM) ; délégué depuis `realtime` |
> | **Journal de décisions** | [`ADR-0012`](../../../../docs/adr/0012-notification-write-collapse-fanout-claim-gated-counter.md) |

---

## 1. Capacité Métier & Non-Objectifs

**Capacité.** `notification` est l'autorité pour **le fil d'activité** : il répond à
**« qu'est-ce qu'on doit dire à cet utilisateur qui s'est passé, combien de non-lus, et comment le
livrer ? »**

**Le problème difficile.** Réduire un flux d'événements à fort fan-in en un fil par-utilisateur sans
amplification d'écriture — un **fan-out write-collapse Redis** (3 couches + un plafond horaire) sur un
fil d'activité TWCS, avec des ids UUIDv5 déterministes et un compteur de non-lus claim-gated.

**Non-objectifs — ce que ce contexte ne fait délibérément PAS :**
- ❌ Posséder les événements source → il dérive les notifications de faits amont.
- ❌ Être le socket live → `realtime` livre le live ; notification possède le fil durable + le push offline.

---

## 2. Langage Omniprésent

| Terme | Sens dans ce contexte | Symbole de code |
|---|---|---|
| Notification | Une entrée unique du fil d'activité | `Notification`, `NotificationId`, `NotificationKind` |
| Subject | La chose dont parle la notification | `SubjectId`, `SubjectKind` |
| Read / created event | Faits de cycle de vie du fil | `NotificationCreatedEvent`, `NotificationReadEvent` |
| Write-collapse | Coalescer de nombreux déclencheurs en une entrée de fil | (fan-out Redis) |
| Unread counter | Le compteur de badge de non-lus claim-gated | (Redis `SET NX`) |

---

## 3. Modèle de Domaine

| Élément | Type | Frontière d'invariant gardée |
|---|---|---|
| `Notification` | racine d'agrégat | Identité d'entrée de fil + état de lecture |
| `NotificationKind` / `SubjectKind` | enum | Vocabulaires fermés notification/sujet |
| `SubjectId` | VO | Ce que la notification référence |

> **Invariant.** Les ids sont des UUIDv5 déterministes (idempotents au redelivery) ; le compteur de
> non-lus est claim-gated (`SET NX`) pour qu'un événement re-livré ne puisse double-incrémenter ; les
> expéditeurs uniques se coalescent via `SADD`. `created_at` est l'heure d'événement, pas d'ingestion.

---

## 4. Propriété des Données & Frontières

**Ce contexte est la source de vérité pour :**
- Le fil de notifications par-utilisateur + les compteurs de non-lus — **ScyllaDB** (fil d'activité TWCS) + **Redis** (compteurs write-collapse). Dérivé, mais faisant autorité pour la vue de fil.

**La liste « ne-pas-écrire » :** notification ne mute jamais le contenu source ; il réagit aux événements.

---

## 5. Invariants & Règles Métier

| # | Invariant | Imposé à | En cas de violation |
|---|---|---|---|
| I1 | Les ids de notification sont des UUIDv5 déterministes (idempotents) | domaine | `NTF-1xxx` |
| I2 | Le compteur de non-lus est claim-gated (pas de double-compte au redelivery) | application (Redis `SET NX`) | `NTF-1xxx` |
| I3 | `created_at` est l'heure d'événement, pas d'ingestion | domaine | `NTF-9xxx` |

---

## 6. Workflows & Orchestration &nbsp;·&nbsp; DEEP

N/A (TIER-2, réduit) — consomme les événements amont (`comment.created`, `wallet.v1.events` (likes, #665), `post.published`, follows sociaux) sous `run_consumer`, write-collapse vers le fil par-utilisateur, incrémente le compteur de non-lus claim-gated, et pousse via le stream broadcast gRPC (live) ou APNs/FCM (offline, délégué depuis `realtime`).

## 7. Relations de Contexte &nbsp;·&nbsp; DEEP

N/A (TIER-2, réduit) — **amont (ACL) :** streams d'événements `comment`, `engagement`, `post`, `social-graph`. **aval (OHS) :** clients (fil + stream broadcast) ; fournisseurs de push offline.

## 8. Événements de Domaine &nbsp;·&nbsp; DEEP

N/A (TIER-2, réduit) — ne publie **rien de référence** (ses événements `NotificationCreated`/`Read` sont un état de fil interne). Consomme des faits amont dont les sens sont possédés par leurs contextes.

## 9. Décisions & Justification &nbsp;·&nbsp; DEEP

N/A (TIER-2, réduit) — choix clé : fan-out write-collapse Redis + compteur de non-lus claim-gated idempotent (ids UUIDv5 déterministes, `created_at` heure-d'événement, claim de non-lus `SET NX`, coalescence d'expéditeur unique `SADD`) — [`ADR-0012`](../../../../docs/adr/0012-notification-write-collapse-fanout-claim-gated-counter.md) (Accepté).

## 10. Classification de Sous-domaine & Évolution &nbsp;·&nbsp; DEEP

N/A (TIER-2, réduit) — **Supporting**, faible volatilité ; différé : types de notification plus riches, centre de préférences, batching de digests.
