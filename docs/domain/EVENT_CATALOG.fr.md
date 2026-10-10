---
i18n:
  source: ./EVENT_CATALOG.md
  source_sha256: 3b73b8b76ab69bbbd734450642ec38e10c9e02d24da14a9518755d956eb724b4
  translated_at: 2026-10-10
  status: complete
---
> 🇫🇷 Traduction française — la version **anglaise** [`EVENT_CATALOG.md`](./EVENT_CATALOG.md) fait foi.
> En cas de divergence, l'anglais prime. Les noms d'événements, topics, types et identifiants sont
> volontairement laissés en anglais.

# Catalogue d'événements (sémantique)

> Rempli depuis le §8 de chaque Domain Card producteur (`crates/services/<svc>/docs/DOMAIN.md`).
> Consigne le **sens métier** de chaque événement de domaine — *qu'est-ce que ça signifie que ceci
> soit arrivé, et qui réagit ?* Il ne **reformule pas** le schéma wire/proto (possédé par le contrat
> de chaque producteur).

## Source de vérité & maintenance

Ce catalogue a deux moitiés :

- **Le câblage des topics** (quel service produit/consomme quel topic) est **généré** depuis le
  registre de topologie d'événements (`crates/contracts/event-topology`) dans le bloc ci-dessous —
  il fait autorité sur *quelles* arêtes existent et ne peut dériver (un golden test +
  `tools/event-catalog/sync.sh` l'imposent). Ne pas l'éditer à la main ; changer le registre et
  régénérer.
- **La sémantique des événements** (ce que chaque événement *signifie*, quand il se déclenche, qui
  réagit et *pourquoi*) est rédigée à la main dans les sections par-domaine qui suivent.

Croiser chaque arête dans [`CONTEXT_MAP.md`](./CONTEXT_MAP.md), et le détail par événement dans le
§8 de chaque producteur.

## Câblage des topics (généré)

<!-- BEGIN GENERATED: topic-wiring · source crates/contracts/event-topology · do not edit by hand -->
> ⚙️ Generated from the event-topology registry (`crates/contracts/event-topology`). Do not edit by hand — change the registry and run `cargo run -p event-topology --bin gen-event-catalog` (or `tools/event-catalog/sync.sh --write`). The *meaning* of each event is authored in the semantic sections below.

### Produced topics → consumers

| Topic | Producer | Consumers |
|---|---|---|
| `account.v1.events` | `account` | `audit`, `auth`, `profile`, `media`, `wallet`, `notification`, `engagement` |
| `profile.v1.events` | `profile` | `search`, `post`, `social-graph`, `geo-discovery`, `timeline`, `media`, `engagement`, `comment`, `chat` |
| `wallet.v1.events` | `wallet` | `counter`, `notification`, `timeline`, `geo-discovery`, `engagement` |
| `notification.v1.events` | `notification` | `realtime` |
| `post.published` | `post` | `notification`, `geo-discovery` |
| `post.updated` | `post` | — *(orphan — see below)* |
| `post.deleted` | `post` | `timeline`, `geo-discovery` |
| `post.v1.events` | `post` | `timeline`, `search`, `realtime` |
| `comment.created` | `comment` | `notification`, `engagement` |
| `comment.deleted` | `comment` | `engagement` |
| `social-graph.followed` | `social-graph` | `notification`, `timeline` |
| `social-graph.unfollowed` | `social-graph` | `timeline` |
| `social-graph.blocked` | `social-graph` | — *(orphan — see below)* |
| `social-graph.author_tier_changed` | `social-graph` | `profile` |
| `social-graph.follow_requested` | `social-graph` | `notification` |
| `chat.conversation.created` | `chat` | `chat` |
| `chat.conversation.published` | `chat` | — *(orphan — see below)* |
| `chat.conversation.unpublished` | `chat` | `chat` |
| `chat.member.joined` | `chat` | `chat` |
| `chat.member.left` | `chat` | `chat` |
| `chat.message.sent` | `chat` | `chat` |
| `chat.message.push` | `chat` | `notification` |
| `counter.v1.popularity` | `counter` | `realtime`, `geo-discovery`, `timeline` |
| `moderation.v1.events` | `moderation` | `audit`, `search`, `media`, `post`, `geo-discovery`, `timeline`, `notification` |
| `auth.v1.events` | `auth` | `audit` |
| `media.v1.events` | `media` | `media` |
| `audit.v1.events` | `media` | `audit` |

### Deferred — consumed, producer intentionally not in-repo

| Topic | Consumer(s) | Why |
|---|---|---|
| `moderation.reports` | `moderation` | External user-report intake — produced by the client/edge, not a fleet service. |
| `moderation.signals` | `moderation` | External ML-classifier signals — produced off-fleet. |
| `view.v1.events` | `counter` | Upstream view telemetry producer not yet built (counter-analytics blueprint deferral). |
| `impression.v1.events` | `counter` | Upstream impression telemetry producer not yet built (counter deferral). |
| `click.v1.events` | `counter` | Upstream click telemetry producer not yet built (counter deferral). |
| `social-graph.follows` | `counter` | Counter wants a single combined follow stream; social-graph emits the split past-tense social-graph.followed/.unfollowed instead. Combined producer is deferred — TRACKED NAMING MISMATCH, not just a missing emitter. |

### Orphan producers — produced, no in-repo consumer

| Topic | Producer | Why |
|---|---|---|
| `post.updated` | `post` | No stream consumer — search/timeline/realtime act on post.v1.events PostUpdated; the legacy per-type topic is emitted for completeness. |
| `social-graph.blocked` | `social-graph` | Block is enforced on the gRPC read path; no stream consumer yet. |
| `chat.conversation.published` | `chat` | Chat delivery-plane headroom. |

<!-- END GENERATED: topic-wiring -->

## Identité & Compte — `account.v1.events` (producteur : `account`)

| Événement | Signifie (fait métier au passé) | Émis quand | Consommateurs & pourquoi |
|---|---|---|---|
| `account_created` / `email_changed` / `email_verified` / `phone_changed` | un fait de cycle de vie de compte porteur de PII s'est produit | la commande correspondante commite | `audit` (PII scellée en enveloppe crypto-shred), `profile` (persona) |
| `password_changed` / `mfa_enrolled` / `mfa_revoked` | un fait sécurité/identifiant (sans PII) | changement d'identifiant | `audit` (catégorie Authentication) |
| `activated` / `deactivated` / `suspended` / `deleted` / `kyc_status_changed` | une transition du cycle de vie de l'identité (`deleted` : anonymisé par le janitor RGPD, ou suppression admin) | changement de cycle de vie | `audit` (Identity ; `deleted` → **crypto-shred du sujet**, ferme la boucle Art. 17), `profile`, `wallet` (`deleted` → efface le wallet), `engagement` (`deleted` → oublie qui a liké, les compteurs de likes gardés, #665) |
| `role_assigned` / `role_revoked` | un octroi d'autorisation a changé | octroi/révocation de rôle | `audit` (Authorization) |
| `gdpr_deletion_requested` | le droit à l'effacement (Art. 17) a été invoqué ; effacement programmé à 30 jours (un compte actif est désactivé entre-temps) | demande utilisateur/DPO | `audit` (preuve DataErasure) |
| `gdpr_deletion_cancelled` | un effacement en attente a été retiré pendant son délai de grâce | le titulaire se reconnecte / `CancelGdprDeletion` | `audit` (preuve DataErasure) |
| `gdpr_data_export_requested` | le droit d'accès/portabilité a été invoqué | demande utilisateur/DPO | exécution de l'export (en aval) |
| `date_of_birth_set` | le titulaire a enregistré une date de naissance (≥ l'âge minimum ; la date reste dans account) | `CreateAccount` avec une date / `SetDateOfBirth` | aucun (la tranche d'âge voyage dans le claim `age` du jeton edge) |
| `consents_updated` | le titulaire a donné ou retiré des consentements (art. 7), changements effectifs seulement, avec la version de la politique | `UpdateConsents` change quelque chose | `audit` (Consent — la copie inviolable de `account_consent_history`) |
| `supervision_started` / `supervision_ended` | un parent et un ado se sont appairés / leur supervision a pris fin (#670 ; `account_id` = l'ado, `supervisor_id`, les ids de profil des deux côtés ; `ended_by` : `by_teen`, `by_supervisor`, `came_of_age`, `account_deleted`) | une invitation est acceptée / l'un des côtés y met fin, l'ado a 18 ans (passage périodique), un compte est effacé (janitor) | `notification` (`SupervisionNotificationWorker` : le ou les côtés concernés sont prévenus — les deux au début, l'autre côté quand l'un y met fin, les deux à 18 ans ou à l'effacement) ; les autres consommateurs les ignorent |
| `supervision_limits_set` / `supervision_limits_cleared` | les limites de supervision d'un ado ont été posées (#670 partie 2 : planchers `private_account`, audience de `messages` / `comments`, `hidden_from_search`, `daily_minutes` ; `set_by`) / levées avec sa dernière supervision | `SetSupervisionLimits` / la dernière supervision de l'ado prend fin | `profile` (partie 2b : resserrer les réglages de l'ado jusqu'aux planchers et les verrouiller / les déverrouiller) ; les autres consommateurs les ignorent |

## Authentification — `auth.v1.events` (producteur : `auth`)

| Événement | Signifie | Émis quand | Consommateurs & pourquoi |
|---|---|---|---|
| `session_issued` | une session authentifiée a été établie | connexion / émission de token | `audit` (Authentication) |
| `session_revoked` | une session a été invalidée | déconnexion / révocation / bump de génération | `audit` (Authentication) |
| `subject_linked` | un sujet IdP a été lié à un compte | flux de liaison de compte | interne |

## Profil — `profile.v1.events` (producteur : `profile`)

| Événement | Signifie | Émis quand | Consommateurs & pourquoi |
|---|---|---|---|
| `profile_created` / `profile_updated` | le persona public a été créé/édité | la commande commite | `post`/`search` (instantanés, indexation) |
| `handle_changed` | le @handle a changé | revendication de handle | `search` (ré-indexation), intégrations |
| `profile_verified` | le badge de vérification a changé | vérification | `search`, intégrations |
| `tier_changed` | le tier d'auteur a changé | recalcul de tier (depuis `social-graph`) | `geo-discovery` (pondération), `timeline` (push/pull) |
| `profile_hidden` / `profile_restored` / `profile_deleted` | une transition de visibilité/cycle de vie | action propriétaire ou modération | read-models (démantèlement/restauration) ; `social-graph` (projection d'audience : masqué) ; `engagement` (`deleted` → retire l'onglet J'aime du profil, #873) |
| `profile_visibility_changed` | le propriétaire a rendu le profil privé ou public | `SetVisibility` | `social-graph` (projection d'audience : privé → contenu réservé aux abonnés, via `CheckAccess`) |
| `profile_interaction_settings_changed` | le propriétaire a changé qui peut commenter / mentionner / écrire (tout le monde, abonnés, mutuels, personne), les téléchargements et les compteurs de likes | `SetInteractionSettings`, ou la création du profil d'un titulaire de 13 à 17 ans (défauts ados) | `social-graph` (projection lue par `CheckInteraction`, que comment appelle avant d'écrire) |
| `profile_location_settings_changed` | le propriétaire a activé / désactivé le mode fantôme, ou changé la précision de localisation (précise / ville), son audience (`audience` : tout le monde / abonnés / mutuels, #657) ou la préférence des nouveaux posts (`on_new_posts`) | `SetLocationSettings`, ou la création du profil d'un titulaire de 13 à 17 ans (défaut ado : fantôme, mutuels, pas de localisation sur les nouveaux posts) | `geo-discovery` (projection appliquée à chaque surface de carte : les posts d'un fantôme quittent la carte des autres ; le niveau ville ne les montre qu'à la bande grossière, au centre de la cellule) |

## Contenu — `post.v1.events` (producteur : `post`)

| Événement | Signifie | Émis quand | Consommateurs & pourquoi |
|---|---|---|---|
| `post.published` | un nouveau contenu est en ligne | la publication commite | `timeline` (fan-out), `search`/`geo-discovery` (index), `counter`, `realtime` (broadcast) |
| `post.updated` | le contenu a été édité | l'édition commite | `search`/`geo-discovery` (ré-indexation) |
| `post.deleted` | le contenu a été retiré | la suppression commite | `timeline`/`search`/`geo-discovery` (démantèlement) |

## Commentaires — `comment.created` / `comment.deleted` (producteur : `comment`)

| Événement | Signifie | Émis quand | Consommateurs & pourquoi |
|---|---|---|---|
| `comment.created` | un commentaire a été posté sur un post | la création commite | `notification` (notifier l'auteur), `counter`/`engagement` (compte++) |
| `comment.deleted` | un commentaire a été tombstoné ou purgé | la suppression commite | `counter`/`engagement` (compte--), fils |

## Engagement — aucun événement

`engagement` ne publie rien depuis que les likes sont des points (#665) : `engagement.reactions` a disparu
avec les réactions, et `engagement.score_updated` n'a jamais existé. Le fan-out des likes lit directement
`wallet.v1.events` `stake_committed` du wallet ; engagement le consomme aussi, pour les compteurs de likes.

## Magnitudes — `counter.v1.popularity` (producteur : `counter`)

| Événement | Signifie | Émis quand | Consommateurs & pourquoi |
|---|---|---|---|
| `counter.v1.popularity` | la magnitude de popularité d'une entité a changé | un flush de fenêtre met à jour un score de popularité | `search` (classement), `realtime` (broadcast live) |

## Confiance & Sécurité — `moderation.v1.events` (producteur : `moderation`)

| Événement | Signifie | Émis quand | Consommateurs & pourquoi |
|---|---|---|---|
| `decision_recorded` | une décision d'intégrité faisant autorité a été prise — porte *qui a décidé* + *pourquoi* (SoR DSA) | une décision est enregistrée (auto-screen / revue humaine / réversion d'appel) | `audit` (scelle la justification en enveloppe crypto-shred) |
| `enforcement_applied` / `enforcement_reversed` | une conséquence a été appliquée/levée contre un acteur (versionnée) | l'enforcement commite | `timeline`, `chat`, `account` (dénorm Plane-B) ; `post` (garde la restriction qu'appliquent ses lectures : `remove_content` → auteur seul) ; `search`, `media` (visibilité / retrait) ; `geo-discovery` (suppression de la carte : `remove_content` / `visibility_limit` masquent un post, une réversion le restaure) ; `audit` |
| `case_opened` / `case_resolved` | une unité de revue a été ouverte/fermée | seuil d'ingestion / action du relecteur | consommateurs Plane-B |
| `appeal_resolved` | un appel a été tranché (maintenu / annulé ; `by_reporter` ; `profile_ids` = les profils du compte appelant) | résolution de l'appel | `notification` (informe chacun des `profile_ids` du résultat, #744) |

> Clé `actor_id` pour l'ordonnancement par acteur. `decision_recorded` est la variante preuve de
> conformité (les consommateurs offender-centric l'ignorent ; `audit` consomme celui-ci +
> `enforcement_applied`).

## Conversations — `chat.*` (producteur : `chat`)

| Événement | Signifie | Émis quand | Consommateurs & pourquoi |
|---|---|---|---|
| `chat.conversation.created` / `chat.conversation.published` / `chat.conversation.unpublished` | faits de cycle de vie de conversation | création / publication / dépublication | `VisibilityWorker` (démantèlement du plan audience) ; `InboxWorker` (created : l'entrée de boîte du propriétaire, #656) |
| `chat.member.joined` / `chat.member.left` | l'appartenance a changé | join/leave | `InboxWorker` (l'entrée de boîte du membre, ajoutée / retirée, #656) |
| `chat.message.sent` | un message a été commité dans le journal ; `withheld` (visible de son seul expéditeur) et `request` (le message unique d'une demande) depuis #656 | l'envoi commite | `InboxWorker` (l'entrée de chaque membre remonte en tête — un message retenu ne déplace que celle de son expéditeur) ; plan live propre de chat (**non** consommé par `realtime` — Separate Ways). Les push passent par `chat.message.push` ci-dessous, pas par ce topic. Gardé 2 jours (`RETENTION`) : il porte le corps complet de chaque message ; c'est la fenêtre de reprise de la projection d'inbox |
| `chat.message.push` | le push d'un message (#654) : son expéditeur, un aperçu du texte de 100 caractères (vide pour un média) et les `recipients` — les membres qu'il atteint dans leur inbox (ni son expéditeur, ni le destinataire d'une demande, ni quelqu'un qui bloque l'expéditeur) qui n'ont pas mis la conversation en sourdine. Les messages système ne sont jamais envoyés en push. Gardé 24 h (`RETENTION`) : il porte du texte de message | `InboxWorker` une fois les entrées d'inbox écrites (au moins une fois) | `notification` (`ChatPushWorker` : l'envoie une fois par message — claim Redis sur `message_id` — vers les appareils iOS de chaque destinataire, selon sa préférence `messages`, sa pause et ses heures calmes) |

## Média — `media.v1.events` (producteur : `media`)

| Événement | Signifie | Émis quand | Consommateurs & pourquoi |
|---|---|---|---|
| `asset_uploaded` | les octets ont atterri dans le store objet | finalize | le pipeline de transformation (Plane B) |
| `asset_ready` / `asset_variant_ready` | l'asset (ou une variante) est sûr à livrer | le Screen CSAM passe / rendition terminée | `post`, `profile`, `search` |
| `asset_quarantined` / `asset_deleted` / `asset_restored` | une transition sécurité/cycle de vie | échec Screen / takedown / restauration | intégrations, livraison |
| `asset_failed` | le traitement a échoué | timeout/erreur | UX d'upload |

## Social Graph — événements de relation (producteur : `social-graph`)

> Les topics scindés au passé ci-dessous **sont** produits et consommés (voir le bloc de câblage).
> Seul le stream *combiné* `social-graph.follows` que `counter` préférerait est différé (un
> mismatch de nommage suivi) ; `counter` réconcilie via gRPC en attendant.

| Événement | Signifie | Émis quand | Consommateurs & pourquoi |
|---|---|---|---|
| `ProfileFollowed` / `ProfileUnfollowed` | une arête de follow a été créée/retirée | follow/unfollow commite | `timeline`/`counter` (consomment **via gRPC aujourd'hui** ; le stream `social-graph.follows` est différé) |
| `FollowRequested` / `FollowRequestWithdrawn` | une demande d'abonnement à un profil privé a été faite / a disparu sans devenir un abonnement (annulée, refusée, coupée par un blocage, caduque une fois le profil public) — les deux sur `social-graph.follow_requested`, clé `actor:target`, si bien qu'un retrait n'est jamais lu avant sa demande | demande / annulation / refus / blocage / abonnement à un profil devenu public | `notification` (informe le propriétaire ; un retrait retire cette notification et son compte de non-lus) |
| `ProfileBlocked` / `ProfileUnblocked` | une arête de block a changé (sectionne les follows) | block/unblock commite | fils |
| `AuthorTierChanged` | le tier de l'auteur a changé | le nombre de followers franchit un seuil | `profile` (possède + ré-émet en `tier_changed`) |

## Puits terminaux — ne publient rien de référence

`audit`, `search`, `timeline`, `geo-discovery`, `realtime` consomment ce qui précède et n'affirment
aucun fait métier durable vers l'extérieur. Les `NotificationCreated`/`Read` de `notification` sont
un état de fil interne, pas un stream System-of-Record.
