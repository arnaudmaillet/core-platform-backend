---
i18n:
  source: ./DOMAIN.md
  source_sha256: 972d86dff10f559c537be2e0fe5199fbe712c2330cf0dffaa097984916e22441
  translated_at: 2026-10-05
  status: complete
---
> 🇫🇷 Traduction française — la version **anglaise** [`DOMAIN.md`](./DOMAIN.md) fait foi.
> En cas de divergence, l'anglais prime. Les contrats (codes d'erreur, topics, variables
> d'environnement, noms de types, identifiants d'ADR) restent en anglais.

# `chat` — Contrat de Domaine & Fonctionnel

> **Domain Card**
>
> | | |
> |---|---|
> | **Bounded Context** | Conversations & Messagerie |
> | **Classe de sous-domaine** | **Core** — la messagerie directe est une surface produit primaire ; le modèle de shadowing membre/audience est sur-mesure |
> | **System of …** | **Record** pour les conversations, l'appartenance et les messages |
> | **Racine(s) d'agrégat** | `Conversation`, `Message` (`domain`) |
> | **Tier** | **TIER-0** |
> | **Posture de défaillance** | **Fail-closed en écriture** (un message envoyé doit persister) ; la livraison live est best-effort sur son propre plan |
> | **Contextes amont** | clients utilisateur (envoi/lecture) ; `moderation` (filtrage de contenu) |
> | **Contextes aval** | consommateurs des événements `chat.*` ; fait tourner son **propre** plan live (coexiste avec `realtime`) |
> | **Journal de décisions** | [`ADR-0006`](../../../../docs/adr/0006-chat-shadowing-pattern-member-vs-audience-plane.md) |

---

## 1. Capacité Métier & Non-Objectifs

**Capacité.** `chat` est l'autorité pour **les conversations et messages** : il répond à
**« qu'a-t-il été dit, dans quelle conversation, par qui, et qui a le droit de le voir ? »**

**Le problème difficile.** Servir à la fois les *membres* (qui lisent/écrivent) et une *audience* plus
large (qui peut voir une conversation publiée) sans confondre les deux plans de visibilité — le
**Shadowing Pattern** (un plan Membre vs un plan Audience) — et stocker un journal de messages à fort
volume à coût maîtrisé via des partitions bucketées `(conversation_id, bucket)`.

**Non-objectifs — ce que ce contexte ne fait délibérément PAS :**
- ❌ Être le plan générique de livraison live → `realtime` l'est ; chat fait tourner son propre plan live aujourd'hui (coexist-first).
- ❌ Classer/décider sur le contenu → `moderation` décide ; chat applique le filtrage.
- ❌ Stocker les octets média → `media` les possède.

---

## 2. Langage Omniprésent

| Terme | Sens dans ce contexte | Symbole de code |
|---|---|---|
| Conversation | Un fil de messagerie avec une appartenance et une politique | `Conversation`, `ConversationId`, `ConversationKind` |
| Message | Un message unique dans un journal de conversation | `Message`, `MessageId`, `MessageContent` |
| Participant | Un membre d'une conversation, avec un rôle | `Participant`, `Role` |
| Conversation policy | Les règles régissant la conversation | `ConversationPolicy` |
| Visibility | Visibilité plan-membre vs plan-audience (le shadowing) | `Visibility` |
| Content type | Le type de contenu du message | `ContentType` |

---

## 3. Modèle de Domaine

| Élément | Type | Frontière d'invariant gardée |
|---|---|---|
| `Conversation` | racine d'agrégat | Appartenance + politique + état de publication/visibilité |
| `Message` | racine d'agrégat | Intégrité du message dans un bucket de conversation |
| `Participant` / `Role` | VO/enum | Qui peut lire/écrire et à quelle autorité |
| `ConversationPolicy` | VO | Les règles régissant la conversation |
| `Visibility` / `ConversationKind` / `ContentType` | enum | Vocabulaires fermés plan/type/contenu |

**Cycle de vie de conversation :**

```
created --(publish)--> published --(unpublish)--> unpublished (audience plane torn down)
   │  member join/leave                                   │
   └──────────────── message.sent (bucketed log) ─────────┘
```

> **Transitions légales uniquement.** L'appartenance est vérifiée à la frontière gRPC ; une
> dépublication démantèle le plan audience via le `VisibilityWorker` ; seuls les membres écrivent.

---

## 4. Propriété des Données & Frontières

**Ce contexte est la source de vérité pour :**
- Conversations, appartenance et journal de messages — **ScyllaDB** (journal bucketé `(conversation_id, bucket)`). Aucun autre service ne les écrit.
- Routage/présence live — **Redis** pub/sub shardé (`RedisSubscriber`), dérivé/éphémère.

**La liste « ne-pas-écrire » :** chat ne possède pas les octets média (référence `media`), ne décide
pas la modération, et n'écrit l'état d'aucun autre service.

---

## 5. Invariants & Règles Métier

| # | Invariant | Imposé à | En cas de violation |
|---|---|---|---|
| I1 | Seuls les membres peuvent écrire/lire le plan membre | frontière gRPC | `CHT-3xxx`/`PERMISSION_DENIED` |
| I2 | Le plan membre et le plan audience restent distincts (shadowing) | domaine | `CHT-2xxx` |
| I3 | La dépublication démantèle entièrement le plan audience | application (`VisibilityWorker`) | `CHT-4xxx` |
| I4 | Les messages sont ajoutés au bon bucket de conversation | domaine | `CHT-9xxx` |

---

## 6. Workflows & Orchestration

> En ligne jusqu'à ce qu'un C4 corrigé soit régénéré depuis `docs/domain/`.

**Envoyer un message.** Envoi autorisé-membre → ajout au journal Scylla `(conversation_id, bucket)`
→ publier `chat.message.sent` et pousser live via le plan pub/sub shardé Redis propre à chat
(streaming gRPC dual vers les clients connectés).

**Publier / dépublier (shadowing).** Publier une conversation ouvre le plan audience ; dépublier
déclenche le démantèlement par le `VisibilityWorker`, consommant `chat.conversation.unpublished`
(DLQ `chat.conversation.unpublished.dlq`) pour démonter l'état du plan audience.

**Appartenance.** Join/leave émettent `chat.member.joined` / `chat.member.left`. Une conversation
publique est en accès libre ; une conversation privée n'admet qu'un profil détenteur d'une **invitation**
en attente (émise par un owner/admin, consommée par l'adhésion, TTL de 7 jours). Un demandeur non invité
est refusé comme si la conversation n'existait pas (`CHT-1009`, rendu comme `CHT-1001`).

---

## 7. Relations de Contexte (extrait de Context-Map)

| Contexte voisin | Direction | Pattern | Mécanisme | Ce qui casse s'il change |
|---|---|---|---|---|
| clients | amont | OHS | gRPC + dual streaming | la messagerie client casse |
| `moderation` | amont | Customer/Supplier | filtrage de contenu | la voie de contenu filtré casse |
| `realtime` | pair | Separate Ways (coexist) | — (non consommé ; chat possède son plan live) | consolidation future différée |
| consommateurs d'événements | aval | Published Language | topics `chat.*` | les réactions aval cassent |

> **Anti-Corruption Layer :** le subscriber Redis + le visibility worker isolent la mécanique du plan
> live du domaine.

---

## 8. Événements de Domaine (sémantique, pas wire)

| Événement | Signifie | Émis quand | Qui réagit |
|---|---|---|---|
| `chat.conversation.created` / `chat.conversation.published` / `chat.conversation.unpublished` | faits de cycle de vie de conversation | création / publication / dépublication | `VisibilityWorker`, consommateurs aval |
| `chat.member.joined` / `chat.member.left` | changement d'appartenance | join/leave | consommateurs aval |
| `chat.message.sent` | un message a été commité dans le journal | l'envoi commite | (plan live de chat ; **non** consommé par `realtime` pour l'instant) |

---

## 9. Décisions & Justification

| Décision | ADR | Statut |
|---|---|---|
| Shadowing Pattern — plans de visibilité Membre vs Audience distincts | [`ADR-0006`](../../../../docs/adr/0006-chat-shadowing-pattern-member-vs-audience-plane.md) | Accepté |
| Coexist-first avec `realtime` (chat garde son propre plan live pour l'instant) | _voir conséquences ADR-0003_ | Accepté |

---

## 10. Classification de Sous-domaine & Évolution

- **Classification :** Core — la messagerie est une surface produit primaire.
- **Volatilité :** moyenne — les types de conversation et règles de visibilité évoluent avec le produit.
- **Dette de modélisation connue :** le plan live propre de chat duplique `realtime` (la couture de consolidation est délibérément ouverte).
- **Capacités différées :** consolidation sur `realtime` ; média-en-chat plus riche ; réactions.
