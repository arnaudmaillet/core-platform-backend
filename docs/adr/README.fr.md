---
i18n:
  source: ./README.md
  source_sha256: 03e25537b1c6edec5fe5fb7334ca53a39dfa0d7c34dcc6e5233c9b2cee0d4d5b
  translated_at: 2026-10-11
  status: complete
---
> 🇫🇷 Traduction française — la version **anglaise** [`README.md`](./README.md) fait foi.
> En cas de divergence, l'anglais prime. Les identifiants d'ADR, noms de fichiers, statuts et noms
> de contextes restent en anglais.

# Architecture Decision Records (ADR)

> Un **ADR** capture une décision architecturale, les forces qui la sous-tendent et les alternatives
> rejetées — le *pourquoi* que le code et les diagrammes ne peuvent exprimer et qui, sinon, s'évapore
> dans les fils Slack et la mémoire tribale.

## Conventions

- **Format :** MADR allégé — voir [`0000-template.md`](./0000-template.md).
- **Nommage :** `00NN-<kebab-title>.md`, numéro paddé à 4 chiffres, monotone croissant.
- **Immuabilité :** un ADR Accepté n'est jamais édité pour changer sa décision. Pour l'inverser,
  ajouter un **nouvel** ADR qui le remplace et basculer l'ancien en `Superseded by ADR-00MM`. La
  piste préservée des décisions remplacées est l'objectif même.
- **Liaison :** chaque ADR est référencé depuis le `DOMAIN.md §9` du service affecté (et, pour les
  décisions inter-contexte, depuis `docs/domain/CONTEXT_MAP.md`). Ne jamais intégrer la justification
  en ligne dans ces docs — relier l'ADR pour que le *pourquoi* vive en un seul endroit.

## Cycle de vie du statut

```
Proposed --> Accepted --> Superseded by ADR-00MM
                      \--> Deprecated
```

## Index

| ADR | Titre | Statut | Contexte(s) |
|---|---|---|---|
| [0000](./0000-template.md) | _Template (pas une décision)_ | — | — |
| [0001](./0001-audit-is-a-separate-evidence-plane.md) | Audit est un plan de preuve infalsifiable séparé, pas un agrégateur de logs | Accepté | audit |
| [0002](./0002-moderation-decision-enforcement-sor-with-fail-closed-screen.md) | Moderation est le SoR décision/enforcement avec une porte Screen étroite fail-closed | Accepté | moderation |
| [0003](./0003-realtime-is-a-fail-open-system-of-connection.md) | Realtime est un System-of-Connection fail-open, jamais un record store | Accepté | realtime |
| [0004](./0004-account-is-the-single-identity-sor.md) | Account est le SoR d'identité unique et possède une voie d'événements sortante | Accepté | account |
| [0005](./0005-auth-federated-idp-with-platform-edge-tokens.md) | Auth fédère un IdP et émet des edge tokens ES256 plateforme avec révocation par génération | Accepté (remplacé en partie par 0018) | auth |
| [0006](./0006-chat-shadowing-pattern-member-vs-audience-plane.md) | Chat utilise le Shadowing Pattern — plans de visibilité Membre vs Audience séparés | Accepté | chat |
| [0007](./0007-comment-flat-thread-two-table-tombstone-purge.md) | Comment utilise un fil plat nil-UUID, Scylla deux tables, suppression tombstone-vs-purge | Accepté | comment |
| [0008](./0008-counter-magnitudes-are-a-reconcilable-soref.md) | Counter est un SoReference réconciliable pour les magnitudes, distinct de l'état d'arête | Accepté | counter |
| [0009](./0009-engagement-redis-primary-lua-atomic-with-kafka-write-behind.md) | Engagement est Redis-primary (Lua-atomique) avec durabilité Kafka write-behind | Accepté | engagement |
| [0010](./0010-geo-discovery-h3-grid-dual-layer-redis-topk.md) | Geo-discovery est un read-model spatial H3 grid_disk + Redis Top-K double-couche | Accepté | geo-discovery |
| [0011](./0011-media-byte-free-control-plane.md) | Media est un plan de contrôle sans-octets (livraison fail-open / Screen CSAM fail-closed) | Accepté | media |
| [0012](./0012-notification-write-collapse-fanout-claim-gated-counter.md) | Notification utilise un fan-out write-collapse + un compteur de non-lus claim-gated | Accepté | notification |
| [0013](./0013-post-two-table-scylla-with-published-language.md) | Post utilise un layout Scylla deux tables et est la source de fan-out en langage publié | Accepté | post |
| [0014](./0014-profile-public-persona-with-dual-axis-visibility.md) | Profile est le persona public au-dessus du SoR account avec visibilité bi-axiale | Accepté | profile |
| [0015](./0015-search-opensearch-single-store-external-versioning.md) | Search est un read-model OpenSearch avec versioning externe et lecture fail-open | Accepté | search |
| [0016](./0016-social-graph-four-table-scylla-logged-batch.md) | Social-graph utilise un schéma Scylla 4 tables avec double-écritures logged-batch atomiques | Accepté | social-graph |
| [0017](./0017-timeline-hybrid-push-pull-fanout.md) | Timeline utilise un fan-out hybride push/pull | Accepté | timeline |
| [0018](./0018-passwordless-identity-passkeys-for-staff.md) | Identité sans mot de passe : Keycloak retiré, passkeys pour le staff | Accepté | auth ; account |

<!-- Ajouter une ligne par ADR au fur et à mesure. -->

## Backlog

Chaque service a désormais un ADR de keystone (0001–0017). Les prochains candidats sont les **choix
verrouillés plus fins** qui les sous-tendent, à consigner comme leurs propres ADR lors de leur
prochaine révision — ex. le mécanisme crypto-shred RtbF d'audit, son split dual-lane
fail-open/fail-closed, et le schéma d'immuabilité hash-chain + WORM + témoin externe ; la rotation de
session par `Generation` d'auth ; le flux producteur de tier d'auteur profile/social-graph.
