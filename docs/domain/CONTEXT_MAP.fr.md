---
i18n:
  source: ./CONTEXT_MAP.md
  source_sha256: c7694ec870a1f3deaa2a4e8304a9f4e6c8062d650839cbf024a4f927589dcdd6
  translated_at: 2026-10-08
  status: complete
---
> 🇫🇷 Traduction française — la version **anglaise** [`CONTEXT_MAP.md`](./CONTEXT_MAP.md) fait foi.
> En cas de divergence, l'anglais prime. Les contrats (topics, noms de types, codes d'erreur,
> identifiants d'ADR) et les noms de patterns DDD sont volontairement laissés en anglais.

# Context Map

> Rempli depuis les 17 Domain Cards par service (`crates/services/<svc>/docs/DOMAIN.md` §1 + §7),
> ancré dans les crates, leurs protos `*.v1` et la garde de registre de topologie d'événements. Ce
> fichier est **multi-propriétaire** (la guilde d'architecture le possède). Ne **pas** le dériver de
> l'ancien C4 pré-flotte (depuis supprimé).

## Ce qu'est ce document

Une [**context map** DDD](https://martinfowler.com/bliki/BoundedContext.html) : les bounded contexts
et le *type de couplage* sur chaque arête entre eux. Le type de couplage — pas seulement « A parle à
B » — indique au relecteur ce qui casse quand un voisin change.

## Patterns de relation (le vocabulaire)

| Pattern | Sens dans ce codebase |
|---|---|
| **Open-Host Service / Published Language (OHS/PL)** | Un contexte publie un contrat stable pour tous — nos protos `*.v1` et topics Kafka |
| **Anti-Corruption Layer (ACL)** | Un consommateur traduit un schéma étranger vers son propre modèle à la frontière — les mappers `infrastructure/decode.rs` wire→domaine |
| **Conformist** | Un aval accepte le modèle de l'amont tel quel, sans traduction |
| **Customer / Supplier** | Une dépendance synchrone (gRPC) où les besoins de l'aval façonnent l'amont |
| **Separate Ways** | Deux contextes délibérément *non* intégrés (découplés à dessein) |
| **Shared Kernel** | Un modèle partagé dont les deux contextes dépendent (les crates de fondation, ex. `auth-context`) |

## Classification des sous-domaines

Oriente l'investissement. Consolidé depuis la ligne de classe de chaque Domain Card.

| Classe de sous-domaine | Contextes | Justification |
|---|---|---|
| **Core** | `post`, `comment`, `chat`, `social-graph`, `engagement` | Le tissu contenu + social — la substance compétitive de la plateforme |
| **Supporting** | `account`, `auth`, `profile`, `audit`, `moderation`*, `media`, `notification`, `realtime`, `search`, `geo-discovery`, `counter`, `timeline` | Capacités nécessaires et read-models / plans dérivés ; sur-mesure mais pas origine de valeur |

> \* `moderation` est classé **Core** dans sa propre Domain Card (la confiance & sécurité est
> différenciante pour un réseau UGC) ; c'est le choix le plus discutable. Toutes les autres
> classifications suivent « Core = là où la vérité/valeur naît ; Supporting = dérivé,
> infrastructurel ou de conformité ».

## La carte — arêtes asynchrones (Kafka, Published Language)

> Le sens est **producteur → consommateur**. Chaque topic producteur fait partie de l'OHS/PL de ce
> contexte ; chaque consommateur applique un ACL (`decode.rs`) sauf mention Conformist.

| Producteur | Topic (Published Language) | Consommateur | Pattern | Ce qui casse en aval si le producteur change |
|---|---|---|---|---|
| `account` | `account.v1.events` | `audit` | ACL | preuve de conformité + boucle de crypto-shred RGPD Art. 17 |
| `account` | `account.v1.events` | `profile` | ACL | provisionnement du persona |
| `account` | `account.v1.events` (`account_deleted`) | `engagement` | ACL | effacement de qui a liké (#665 ; les compteurs restent) |
| `auth` | `auth.v1.events` | `audit` | ACL | preuve du cycle de vie des sessions |
| `profile` | `profile.v1.events` | `post` | ACL | instantanés d'auteur sur les posts |
| `profile` | `profile.v1.events` | `search` | ACL | indexation des profils |
| `profile` | `profile.v1.events` (`tier_changed`) | `geo-discovery` | ACL | pondération de classement par tier d'auteur |
| `profile` | `profile.v1.events` (`tier_changed`) | `timeline` | ACL | décision de fan-out push/pull |
| `profile` | `profile.v1.events` | `social-graph` | ACL | validité des relations vs existence du profil |
| `post` | `post.v1.events` | `timeline` | ACL | fan-out du fil |
| `post` | `post.v1.events` | `search` | ACL | indexation des posts |
| `post` | `post.v1.events` | `geo-discovery` | ACL | cartes (map cards) |
| `post` | `post.v1.events` | `counter` | ACL | magnitudes des posts |
| `post` | `post.v1.events` | `realtime` | ACL (broadcast) | diffusion live des posts |
| `comment` | `comment.created` / `comment.deleted` | `notification` | ACL | notifications de réponse |
| `comment` | `comment.created` / `comment.deleted` | `engagement` / `counter` | ACL | comptes de commentaires |
| `wallet` | `wallet.v1.events` | `engagement` | ACL | compteurs de likes (#665 : un like est un point) |
| `wallet` | `wallet.v1.events` | `notification` | ACL | notifications de like (le premier lot d'une personne) |
| `wallet` | `wallet.v1.events` | `counter` | ACL | magnitudes de likes |
| `wallet` | `wallet.v1.events` | `timeline` | ACL | centres d'intérêt (un premier like) |
| `wallet` | `wallet.v1.events` | `geo-discovery` | ACL | les likes de chaque pays (le classement des déblocages) |
| `counter` | `counter.v1.popularity` | `search` | ACL | signal de classement par popularité |
| `counter` | `counter.v1.popularity` | `realtime` | ACL (broadcast) | compteurs d'engagement live |
| `moderation` | `moderation.v1.events` (`decision_recorded`) | `audit` | ACL | preuve de conformité (justification DSA) |
| `moderation` | `moderation.v1.events` (Plane B) | `timeline`, `chat`, `account` | ACL | dénormalisation de l'enforcement |
| `moderation` | `moderation.v1.events` | `post`, `search` | ACL | reflet de visibilité/enforcement du contenu |
| `moderation` | `moderation.v1.events` (takedown) | `media` | ACL | retrait d'asset |
| `media` | `media.v1.events` | `post`, `profile`, `search` | ACL | intégrations / indexation |
| `notification` | `notification.v1.events` | `realtime` | ACL (targeted) | livraison live des notifications |
| `chat` | `chat.conversation.unpublished` | `VisibilityWorker` de `chat` | interne | démantèlement du plan audience |
| `social-graph` | événements de relation (`ProfileFollowed`…) | — | OHS (différé) | le producteur Kafka `social-graph.follows` est **différé** ; les consommateurs lisent via gRPC aujourd'hui |

## La carte — arêtes synchrones (gRPC + vérification)

| Appelant | Appelé | Pattern | Mécanisme | Ce qui casse si l'appelé change |
|---|---|---|---|---|
| `media` | `moderation` | Customer/Supplier (sync, fail-closed) | RPC `Screen` | filtrage des uploads en catégories catastrophiques |
| `moderation` | `account` | Customer/Supplier | exécution de suspension/bannissement | application de l'enforcement |
| `timeline` | `social-graph` | Customer/Supplier | lecture de l'ensemble des followers pour le fan-out | fan-out du fil |
| `counter` | `social-graph` | Customer/Supplier | réconciliation du nombre de followers | correction de dérive des magnitudes de followers |
| `post` | `media` | Customer/Supplier | références `MediaAttachment` | média dans les posts |
| `comment` | `post` | Customer/Supplier | références `PostId` | validité des commentaires |
| `auth` | `account` | Customer/Supplier | `SubjectLink` ↔ `AccountId` | résolution du sujet de session |
| `wallet` | `engagement` | Customer/Supplier | `GetLikePositions` (mesh) | règlement des mises (#665) : la précocité de chaque position |
| `realtime` | `auth` | Conformist (verify-only) | vérification du token ES256 via `auth-context` au handshake | authentification des nouvelles connexions |
| **tous les services** | `auth` | Shared Kernel / OHS | token edge vérifié en process via `auth-context` | tout appel authentifié |

## Non-intégrations notables (Separate Ways)

| A | B | Pourquoi découplés à dessein |
|---|---|---|
| `realtime` | `chat` | `chat.message.sent` n'est **pas** consommé par `realtime` ; chat fait tourner son propre plan live (coexist-first — voir [`ADR-0003`](../adr/0003-realtime-is-a-fail-open-system-of-connection.md) / [`ADR-0006`](../adr/0006-chat-shadowing-pattern-member-vs-audience-plane.md)). La consolidation est une décision future. |

## Puits terminaux (consomment, ne produisent rien de référence)

`audit`, `search`, `timeline`, `geo-discovery`, `realtime` ne publient **rien de référence** — ce
sont des read-models ou des puits de preuve. Voir le §8 de chaque Domain Card.

## Diagramme

> Le modèle C4 a été **régénéré à partir de cette carte** (et des Domain Cards) en tant qu'artefact
> dérivé : [`docs/architecture/workspace.dsl`](../architecture/workspace.dsl). Ce document reste la
> source faisant autorité pour les relations ; le diagramme est généré pour y correspondre.
