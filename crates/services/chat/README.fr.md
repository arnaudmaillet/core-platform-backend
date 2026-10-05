---
i18n:
  source: ./README.md
  source_sha256: 3a8aff3593db6a9a5f34196734c610ef8d86293352cc13caac6f7348ea453245
  translated_at: 2026-10-05
  status: complete
---
> 🇫🇷 Traduction française — la version **anglaise** [`README.md`](./README.md) fait foi.
> En cas de divergence, l'anglais prime. Les contrats (codes d'erreur, variables
> d'environnement, topics Kafka, identifiants) sont volontairement laissés en anglais.

# `chat` — Conversations à l'échelle hyperscale, où un groupe privé peut devenir viral sans s'effondrer

> **Fiche service**
>
> | | |
> |---|---|
> | **Propriétaire** | `<TODO: équipe>` · `<TODO: #canal-slack>` |
> | **Astreinte / escalade** | `<TODO: rotation-astreinte>` → `<TODO: politique-escalade>` |
> | **Palier (Tier)** | **TIER-0** — chemin critique temps réel |
> | **Binaire déployable** | `crates/apps/chat-server` (crate bibliothèque : `crates/services/chat`) |
> | **Bases de données** | ScyllaDB keyspace `chat` · Redis Cluster (cache + pub/sub shardé) |
> | **Asynchrone** | publie `chat.conversation.*` / `chat.member.*` / `chat.message.sent` · consomme `chat.conversation.unpublished` |
> | **Appelants amont** | `<TODO: passerelle / clients via gRPC>` |
> | **Dépendances aval** | ScyllaDB, Redis Cluster, Kafka |
> | **SLO** | `<TODO : 99,9 %>` dispo · diffusion temps réel p99 `<TODO>` · lecture historique p99 `<TODO>` |

---

## 🎯 Vue d'ensemble & rôle du service

`chat` est le microservice **Conversation** unifié de la plateforme : il alimente à la fois les
**discussions de groupe** (maillage symétrique `N↔N`, borné, avec présence/saisie/accusés de lecture)
et les **canaux** (diffusion asymétrique `1→N`, lecture passive, audience non bornée) — derrière un
modèle de domaine unique et une seule surface gRPC.

Le problème difficile qu'il résout est le **profil d'E/S hybride** : un administrateur peut basculer un
groupe privé en **Public**, après quoi des millions d'invités passifs peuvent lire l'historique et
s'abonner aux nouveaux messages *pendant que les membres principaux continuent d'interagir en temps
réel*. Réalisé naïvement, ces invités provoquent une **amplification d'écriture** et un
**partitionnement à chaud (hot-partitioning) de ScyllaDB** sur la partition même où les membres
écrivent activement.

Le service résout cela avec le **Shadowing Pattern** : une conversation logique se projette sur deux
plans d'exécution physiquement isolés.

| | **Plan Membre** | **Plan Audience** |
|---|---|---|
| Cardinalité | borné (≤ 500) | non borné (→ millions) |
| Direction | duplex intégral `N↔N` | lecture seule `1→N` |
| Transporte | messages **+** présence + saisie + accusés | **shadow** du message uniquement |
| Écritures par destinataire | accusés seulement, `O(membres)` | **zéro** |

**Objectifs fondamentaux :** les membres ne ressentent jamais l'audience ; les invités ne touchent
jamais les boucles d'écriture/présence des membres ; une écriture durable de message se diffuse aux
*pods* (centaines), jamais aux *abonnés* (millions).

---

## 📐 Architecture & concepts

Découpage hexagonal / DDD (`domain` → `application` → `infrastructure`), bus de commandes/requêtes
CQRS, ScyllaDB pour le journal durable, Redis Cluster pour le cache + le routage temps réel, Kafka pour
les événements.

```
                       ┌─────────────────────── gRPC (tonic) ───────────────────────┐
   member client ─────▶│ StreamConversation (full)        SendMessage / GetHistory… │◀──── guest client
   guest  client ─────▶│ StreamPublic (shadow)            ToggleVisibility / Sub…    │
                       └───────────────┬───────────────────────────┬────────────────┘
                                       │ CQRS                       │ real-time fork
                            ┌──────────▼──────────┐      ┌──────────▼───────────────┐
                            │ Command / Query bus │      │ MessageFanout            │
                            └──────────┬──────────┘      │  ├─ hot-tail cache push  │
                durable write          │                 │  ├─ member channel (all) │
            ┌──────────────────────────▼───┐             │  └─ audience shards(msg) │
            │ ScyllaDB                      │             └──────────┬───────────────┘
            │  messages_by_conversation     │                        │ SPUBLISH
            │   PK (conversation_id, bucket)│             ┌──────────▼───────────────┐
            │  members / subscriptions      │             │ Redis Cluster (sharded   │
            └───────────────────────────────┘            │ pub/sub + cache)         │
                                                          │  {conv:<id>} member slot │
   Kafka ◀── KafkaEventPublisher (chat.*)                │  {aud:<id>:<k>} spread   │
   Kafka ──▶ VisibilityWorker (unpublish → close guests) └──────────┬───────────────┘
                                                                     │ SSUBSCRIBE (refcounted)
                                                          ┌──────────▼───────────────┐
                                                          │ PlaneSubscriber (per pod) │
                                                          │  → ConversationRegistry   │
                                                          │     (member | audience)   │
                                                          │  → local broadcast to     │
                                                          │     gRPC streams          │
                                                          └───────────────────────────┘
```

**Isolation de la partition à chaud.** `messages_by_conversation` utilise une **clé de partition
composite `(conversation_id, bucket)`** (`bucket = floor(created_at_ms / CHAT_MESSAGE_BUCKET_HOURS)`).
Les membres écrivent le bucket *courant* (queue temps réel) ; les invités font défiler les buckets plus
*anciens* (partitions froides, souvent sur d'autres réplicas) — le point chaud d'écriture et le gros de
la charge de lecture sont physiquement séparés par l'âge du bucket. Les lectures de nouveaux messages
pour les invités ne touchent jamais Scylla ; elles arrivent par le plan de diffusion.

> **Invariants** (et où ils sont imposés) : `StreamConversation` exige l'appartenance au roster
> (`PERMISSION_DENIED` sinon) — imposé à la frontière gRPC ; `StreamPublic` exige `visibility == Public` ;
> un tiers d'une conversation **privée** reçoit `NOT_FOUND` de chaque RPC, exactement comme pour une
> conversation inexistante (voir plus bas) ; le flux audience est *structurellement* incapable de transporter
> présence/saisie/accusés ; le plafond de 500 membres par groupe est un invariant de la couche domaine ;
> rejoindre une conversation privée exige une invitation en attente (`NOT_FOUND` sinon, voir plus bas) —
> invariant de la couche domaine.

---

## 📊 Objectifs de niveau de service (SLO)

| SLI | Objectif | Fenêtre | Mesuré par |
|---|---|---|---|
| Disponibilité (écritures/flux non-`UNAVAILABLE`) | `<TODO : 99,9 %>` | glissante 30 j | métriques de statut gRPC |
| Livraison message temps réel → flux membre p99 | `< <TODO> ms` | 1 h | span de fan-out / RTT client |
| Lecture `GetHistory` p99 (profil Fast) | `< <TODO> ms` | 1 h | p99 lecture Scylla par profil |
| `SendMessage` ack durable p99 (profil Strict) | `< <TODO> ms` | 1 h | p99 écriture Scylla |
| Latence de fermeture sur visibilité (`chat-visibility-consumer`) | `< <TODO> s` | direct | lag du consumer-group |
| Durabilité | aucun message acquitté perdu | — | Scylla LocalQuorum sur les écritures membre |

**Budget d'erreur :** `<TODO : 0,1 % / 30 j ≈ 43 min>`. **En cas d'épuisement :**
`<TODO : gel des déploiements / page>`.

> Le fan-out temps réel est **« best-effort » par conception** et se situe *hors* du SLO de durabilité :
> un `SendMessage` est « réussi » dès qu'il est écrit durablement, quel que soit le résultat de la
> diffusion (voir §Modes de défaillance).

---

## 🔗 Dépendances & rayon d'impact (blast radius)

**Aval — ce dont `chat` a besoin pour fonctionner :**

| Dependency | Purpose | If down → | Degradation |
|---|---|---|---|
| ScyllaDB (keyspace `chat`) | journal durable messages/membres/abonnements | les écritures + l'historique froid échouent | **Dur** — `UNAVAILABLE` |
| Redis Cluster | cache hot-tail + routage temps réel shardé + présence | le fan-out temps réel + la présence s'arrêtent ; l'historique reste servi depuis Scylla | **Souple** — chemin durable intact |
| Kafka | événements domaine + visibilité | événements non émis ; les invités ne sont pas fermés à l'unpublish | **Souple** — `SendMessage` réussit toujours |
| `social-graph` (`CheckInteraction`, mesh) | qui peut écrire à qui (#656) | conversations directes et invitations refusées | **Fermé par défaut** — `UNAVAILABLE` (`CHT-5001`, rejouable) ; groupes et canaux non touchés |

**Amont — qui dépend de `chat` (rayon d'impact si `chat` tombe) :**

| Caller | Uses | Impact visible utilisateur si `chat` est indisponible |
|---|---|---|
| `<TODO : passerelle / clients mobile+web>` | gRPC `ChatService` | plus de messagerie, présence ni lecture de canaux |
| `<TODO : notification>` | consomme `chat.message.sent` / `chat.member.*` | plus de notifications issues du chat |

> **Chemin critique ?** **Oui** — `chat` est sur le chemin temps réel synchrone de chaque conversation
> active. Une panne totale est immédiatement visible par l'utilisateur.

---

## 🔌 Interfaces publiques & contrat d'API

### gRPC — `chat.v1.ChatService`

```protobuf
service ChatService {
  // Lifecycle / membership
  rpc CreateConversation (CreateConversationRequest) returns (CreateConversationResponse);
  rpc OpenDirectConversation  (OpenDirectConversationRequest)  returns (OpenDirectConversationResponse); // #656
  rpc RespondToMessageRequest (RespondToMessageRequestRequest) returns (CommandResponse);                // #656
  rpc ToggleVisibility   (ToggleVisibilityRequest)   returns (CommandResponse);
  rpc JoinAsMember       (JoinAsMemberRequest)       returns (CommandResponse); // private ⇒ invitation required
  rpc InviteMember       (InviteMemberRequest)       returns (CommandResponse); // owner/admin only
  rpc Subscribe          (SubscribeRequest)          returns (CommandResponse);
  rpc Unsubscribe        (UnsubscribeRequest)        returns (CommandResponse);
  // Messaging
  rpc SendMessage (SendMessageRequest) returns (SendMessageResponse);
  rpc MarkRead    (MarkReadRequest)    returns (CommandResponse);
  // Member-Plane signals
  rpc SendTyping (SendTypingRequest) returns (CommandResponse);
  rpc Heartbeat  (HeartbeatRequest)  returns (CommandResponse);
  // Queries
  rpc GetHistory        (GetHistoryRequest)        returns (GetHistoryResponse);
  rpc ListMembers       (ListMembersRequest)       returns (ListMembersResponse);
  rpc ListSubscriptions (ListSubscriptionsRequest) returns (ListSubscriptionsResponse);
  rpc ListInbox         (ListInboxRequest)         returns (ListInboxResponse);       // #656
  rpc ListConversationsByMember (ListConversationsByMemberRequest) returns (ListConversationsByMemberResponse); // mesh only
  // Real-time streams
  rpc StreamConversation (StreamConversationRequest) returns (stream StreamConversationResponse); // members
  rpc StreamPublic       (StreamPublicRequest)       returns (stream StreamPublicResponse);       // audience
}
```

**Les conversations d'un profil (#653).** `ListConversationsByMember(member_id, limit, page_token)` est
**mesh uniquement** (jamais sur l'edge) : l'export de données RGPD liste les conversations dont un profil
est membre (id, rôle, joined_at, type ; paginé par id de conversation), puis lit l'historique de chacune en tant
que ce membre (`GetHistory`). Elle lit `chat.conversations_by_member`, l'inverse du roster, écrite et
supprimée avec `members_by_conversation` dans un même LOGGED BATCH. Les appartenances antérieures à cette
table sont indexées par un backfill optionnel et idempotent au démarrage
(`CHAT_BACKFILL_CONVERSATIONS_BY_MEMBER=true`).

> **Contrat de sérialisation / enum :** les valeurs d'enum proto sont **basées sur 0 et égales au
> `tinyint` du domaine** (`CONVERSATION_KIND_GROUP=0`, `…CHANNEL=1`, `…DIRECT=2` ; `VISIBILITY_PRIVATE=0`,
> `…PUBLIC=1` ; `ROLE_OWNER=0…GUEST=4` ; `CONTENT_TYPE_TEXT=0…SYSTEM=2`). Pas de sentinelle
> `UNSPECIFIED` — la couche gRPC effectue un cast direct, sans décalage d'indice.

**Invariants à la frontière :** `StreamConversation` exige l'appartenance au roster ; `StreamPublic`
exige `visibility == Public` ; le flux audience est structurellement incapable de transporter
présence/saisie/accusés. `Heartbeat` et `SendTyping` exigent aussi l'appartenance au roster (un
non-membre ne peut pas injecter de signaux du Member Plane).

**Les conversations privées sont insondables.** Un appelant absent du roster d'une conversation
**privée** reçoit de chaque RPC la même réponse que pour une conversation inexistante — `NOT_FOUND`
avec `CHT-1009`, identique octet pour octet sur le fil à `CHT-1001` — si bien qu'aucune RPC ne permet
de tester l'existence d'une conversation privée. Les membres, et les tiers d'une conversation
**publique** (de toute façon découvrable), conservent l'erreur précise :

| Appelant | RPC réservées aux membres¹ | `ToggleVisibility` | `GetHistory` | `Subscribe` | `StreamPublic` |
|---|---|---|---|---|---|
| membre | autorisé | `PERMISSION_DENIED` sauf owner/admin | historique complet | `FAILED_PRECONDITION` (`CHT-1008`) sur une privée | `FAILED_PRECONDITION` (`CHT-1008`) sur une privée |
| tiers, **publique** | `PERMISSION_DENIED` (`CHT-1007`) | `PERMISSION_DENIED` (`CHT-1007`) | à partir du filigrane public-since | autorisé | autorisé |
| tiers, **privée** | `NOT_FOUND` (`CHT-1009`) | `NOT_FOUND` (`CHT-1009`) | `NOT_FOUND` (`CHT-1009`) | `NOT_FOUND` (`CHT-1009`) | `NOT_FOUND` (`CHT-1009`) |
| tous, conversation inexistante | `NOT_FOUND` (`CHT-1001`) | `NOT_FOUND` (`CHT-1001`) | `NOT_FOUND` (`CHT-1001`) | `NOT_FOUND` (`CHT-1001`) | `NOT_FOUND` (`CHT-1001`) |

¹ `SendMessage`, `MarkRead`, `SendTyping`, `Heartbeat`, `ListMembers`, `StreamConversation`. Elles
autorisent par une seule lecture du roster ; la conversation n'est chargée **que si cette lecture
échoue**, pour choisir la réponse (`application::access::deny_non_member`) : le chemin chaud des membres
est inchangé. `Subscribe` et `StreamPublic` ne vérifient l'appartenance que sur une conversation
privée, pour un `subscriber_id` lié à l'appelant à la frontière (`edge::require_profile` : l'un des
`pids` du jeton ; un invité n'en possède aucun) : personne ne peut s'enquérir de l'appartenance d'autrui.
`JoinAsMember` et `InviteMember` suivent la même règle (paragraphe suivant).

**Rejoindre une conversation privée.** `JoinAsMember` n'est en accès libre que sur une conversation
**publique**. Sur une conversation **privée** (groupe ou canal), il exige une invitation en attente pour
l'appelant, émise par un owner/admin via `InviteMember` (un simple membre reçoit `PERMISSION_DENIED`).
L'invitation est stockée dans `chat.invitations_by_conversation`, expire au bout de **7 jours** (TTL de la
table ; une nouvelle invitation la renouvelle) et est **consommée** par l'adhésion. Sans invitation,
l'adhésion échoue en `NOT_FOUND` avec `CHT-1009`, dont le code gRPC et le message sont identiques à
`CHT-1001` (conversation introuvable) : un tiers ne peut pas distinguer une conversation privée d'une
conversation inexistante. Un tiers qui appelle `InviteMember` sur une conversation privée reçoit la même
réponse. La règle est un invariant du domaine (`Conversation::admit_joiner` / `Conversation::invite`) ;
l'appelant reste lié à son propre profil à la frontière (`edge::require_profile` sur `profile_id` /
`inviter_id`). Aucun événement n'est encore émis pour une invitation : l'invité en est informé hors bande.

**Réglages de présence (#661).** Un membre qui a désactivé son **statut d'activité** (réglages de
découvrabilité du profil) n'annonce aucune présence : ni événement en ligne/hors ligne, ni battement. Celui
qui a désactivé ses **accusés de lecture** ne diffuse aucun `ReceiptEvent`, et `ListMembers` masque son
`last_read` aux autres membres (il voit toujours le sien ; son état non lu ne change pas). Les réglages sont
projetés depuis `profile.v1.events` (`ProfileDiscoverySettingsChanged`, groupe `chat-presence-settings`,
depuis l'offset le plus ancien) dans `chat.presence_settings` ; s'ils ne peuvent pas être lus, présence et
accusés sont retenus (échec fermé).

**Messages directs et demandes de messages (#656).** Une conversation `DIRECT` réunit deux profils,
**unique par paire** (`chat.direct_conversations`, réservée par une LWT : deux ouvertures simultanées
obtiennent la même), jamais publique, jamais rejointe ni ouverte aux invitations, avec les boucles de
présence d'un groupe. On l'ouvre par `OpenDirectConversation(profile_id, peer_id)` — `CreateConversation`
refuse ce type — et les réglages de messages du destinataire décident, via le `CheckInteraction(MESSAGE)`
de social-graph (mesh uniquement) :

| Verdict du destinataire pour l'ouvreur | Ouverture | Messages de l'ouvreur |
|---|---|---|
| autorisé | ouverte (`request = false`) | remis |
| l'audience l'exclut (abonnés / mutuels), ou une limite temporaire le retient (#669) | une **demande** | **un seul**, en attente dans ses demandes ; les suivants échouent en `CHT-1010` |
| messages de personne | refus, `PERMISSION_DENIED` (`CHT-1011`) | refusés |
| un blocage, dans un sens ou l'autre | une demande, jamais traitée | **retenus** : visibles de l'expéditeur seul |

Le destinataire accepte en répondant, en ouvrant lui-même la conversation, ou par
`RespondToMessageRequest(accept = true)`. Un refus est **silencieux** : le demandeur voit toujours sa
demande en attente (`CHT-1010` au second message) et ne peut pas redemander pendant **30 jours**. Un
**expéditeur bloqué voit exactement la même chose** qu'un expéditeur dont la demande est en attente ou a été
refusée — mêmes réponses, mot pour mot — et ses messages sont retenus : stockés avec `withheld = true`,
rendus par `GetHistory` à leur seul expéditeur, écartés du flux de tout autre membre, jamais mis en
cache ni diffusés à une audience, jamais remis, même après un déblocage. Une fois ouverte, une
conversation le reste quels que soient les réglages ensuite ; un blocage retient de nouveau. Ni
présence, ni saisie, ni accusé de lecture ne circulent avant la réponse à une demande (`MarkRead` ne
garde rien). Les transitions d'une demande et son message unique sont des compare-and-set
(`IF request_state = ?`, `IF request_sent = false`) : deux appuis simultanés n'envoient jamais deux
messages. `InviteMember` applique aussi les réglages de l'invité : messages de personne ⇒ `CHT-1011` ;
un blocage ⇒ répondu comme invité, rien n'est stocké ; une audience abonnés / mutuels l'autorise encore
(une invitation est le consentement de l'invité). `chat.message.sent` porte `withheld` et `request`, pour
qu'un futur consommateur de notifications push ne remette ni l'un ni l'autre. social-graph injoignable ⇒
`UNAVAILABLE` (`CHT-5001`) : fermé par défaut. Sans `CHAT_SOCIAL_GRAPH_GRPC_ENDPOINT`, les messages
directs sont coupés et les invitations non vérifiées.

**La boîte de réception (#656).** `ListInbox(profile_id, folder, limit, page_token)` liste les
conversations de l'appelant, de la plus récemment active à la moins active, dans l'un de deux dossiers.
**INBOX** regroupe les conversations directes acceptées, les groupes et les canaux où l'on écrit ; les
canaux auxquels on est seulement abonné restent dans `ListSubscriptions`. **REQUESTS** regroupe les
demandes de messages reçues, sans réponse. Chaque entrée porte :
- le type, et l'autre membre d'une conversation directe ;
- la dernière activité ;
- le dernier message remis (expéditeur, aperçu de 100 caractères) ;
- `unread` : le dernier message est d'un autre et plus récent que la position de lecture de l'appelant.
  Une demande reste non lue jusqu'à sa réponse, puisque son destinataire ne garde aucun accusé ;
- `request` : la demande de l'appelant lui-même, en attente de réponse.

Comment les entrées sont tenues :
- **Stockage.** Les entrées vivent dans `chat.inbox_entries`, plus `chat.inbox_by_activity` pour le
  listage (migration 0011). Un déplacement réécrit les deux dans un même LOGGED BATCH.
- **Écritures.** L'`InboxWorker` les projette depuis les topics de chat : un message remis, un membre qui
  rejoint ou part, la création d'un groupe. Sans broker, la projection se fait en ligne. Une conversation
  directe entre dans une boîte avec son premier message remis.
- **Expéditeurs bloqués.** Un message retenu ne déplace que l'entrée de son expéditeur : un expéditeur
  bloqué n'atteint jamais les demandes de celui qui l'a bloqué. REQUESTS est aussi revérifié auprès du
  gate à chaque lecture, si bien qu'un blocage postérieur à une demande la masque aussi. Gate
  injoignable ⇒ `CHT-5001`.
- **Réponses.** Accepter déplace l'entrée dans INBOX ; refuser la retire pour celui qui refuse seulement.
- **Rejeux.** Une entrée ne recule jamais lors d'un rejeu.

### Ports Rust (contrat hexagonal)

```rust
#[async_trait] pub trait MessageRepository {
    async fn insert(&self, message: &Message) -> Result<(), ChatError>;
    async fn list_history(
        &self, conversation_id: &ConversationId, limit: i32,
        cursor: Option<(i64, Uuid)>,          // (created_at_ms, message_id)
        floor_created_at_ms: Option<i64>,     // audience watermark — pushed into Scylla as `created_at >= ?`
    ) -> Result<(Vec<MessageSummary>, Option<(i64, Uuid)>), ChatError>;
}

#[async_trait] pub trait EventPublisher {            // KafkaEventPublisher in prod
    async fn publish_conversation(&self, event: &DomainEvent) -> Result<(), ChatError>;
    async fn publish_message(&self, event: &MessageEvent) -> Result<(), ChatError>;
}
```

### Contrat d'erreur

Toute défaillance implémente `error::AppError` avec un code stable, mappé vers gRPC `Status` et HTTP par
le crate partagé `error` :

| Range | Class |
|---|---|
| `CHT-1xxx` | lifecycle |
| `CHT-2xxx` | validation |
| `CHT-3xxx` | events |
| `CHT-4xxx` | streaming |
| `CHT-5xxx` | peer services (`CHT-5001`: social-graph's interaction check unavailable — retryable) |
| `CHT-9xxx` | identifiers |

`CHT-1009` (conversation privée que l'appelant ne peut pas voir) est volontairement rendu exactement
comme `CHT-1001` sur le fil (`NOT_FOUND`, même message) ; le code distinct n'existe que dans les journaux
serveur. Chaque RPC portant sur une conversation le renvoie à un tiers d'une conversation privée : ainsi
`CHT-1007` (non-membre) et `CHT-1008` (non publique) n'atteignent jamais que des membres ou des tiers
d'une conversation publique.

Messages directs (#656) : `CHT-1010` (le message unique d'une demande est utilisé — c'est aussi, à
dessein, la réponse à un expéditeur refusé ou bloqué), `CHT-1011` (le destinataire n'accepte de messages
de personne ; jamais la réponse à un blocage), `CHT-1012` (aucune demande d'un autre à traiter).

---

## 📨 Contrat événementiel & asynchrone

> Les topics Kafka sont une API. Un changement de schéma ici casse les consommateurs exactement comme un
> changement de proto.

**Publie :**

| Topic | Trigger | Key | Consumers |
|---|---|---|---|
| `chat.conversation.created` | new conversation created | `conversation_id` | **`chat` lui-même** (InboxWorker) |
| `chat.conversation.published` | visibility → Public | `conversation_id` | `<TODO>` |
| `chat.conversation.unpublished` | visibility → Private | `conversation_id` | **`chat` itself** (VisibilityWorker) |
| `chat.member.joined` | member added to roster | `conversation_id` | **`chat` lui-même** (InboxWorker) |
| `chat.member.left` | member removed from roster | `conversation_id` | **`chat` lui-même** (InboxWorker) |
| `chat.message.sent` | message durably written (`withheld`, `request` depuis #656 : un consommateur push doit ignorer les deux) | `conversation_id` | **`chat` lui-même** (InboxWorker) |

**Consomme :**

| Topic | Consumer group | Purpose | On poison/exhaustion |
|---|---|---|---|
| `chat.conversation.unpublished` | `chat-visibility-consumer` | every pod tears down guest streams cluster-wide when a conversation goes Private | DLQ `chat.conversation.unpublished.dlq` |
| `chat.message.sent`, `chat.conversation.created`, `chat.member.joined`, `chat.member.left` | `chat-inbox` | la boîte de réception de chaque membre (#656), depuis l'offset le plus ancien | DLQ `<topic>.dlq` |

> **Contrat d'exécution (obligatoire) :** le VisibilityWorker s'exécute sous `run_consumer` — commit
> manuel après un résultat terminal, retries bornés avec backoff + jitter, DLQ en cas
> d'épuisement/poison, et reconstruction depuis le dernier offset committé en cas d'erreur broker. Le
> producteur injecte `traceparent`/`tracestate` ; le consommateur rétablit le span parent pour un
> tracing de bout en bout à travers la frontière asynchrone.

---

## 🌩️ Modes de défaillance & dégradation

| Failure | Symptom | Service behavior | Operator action |
|---|---|---|---|
| ScyllaDB indisponible | `SendMessage` / `GetHistory` froid échouent | **Échec dur** — `UNAVAILABLE` ; rien n'est acquitté, donc rien n'est perdu | vérifier la santé du cluster Scylla / du DC |
| Redis indisponible | les messages temps réel s'arrêtent ; présence/saisie disparaissent | **Souple** — `SendMessage` réussit toujours (durable) ; les invités lisent encore l'historique depuis Scylla | vérifier Redis Cluster ; les clients re-`GetHistory` |
| Cache hot-tail Redis froid/évincé | la latence des lecteurs passifs augmente | **Souple & sûr** — les lectures retombent sur Scylla (source de vérité durable) | vérifier le taux de hits / la capacité ; se rétablit en général seul |
| Kafka indisponible | la fermeture à l'unpublish est retardée ; les événements aval s'arrêtent | **Souple** — fan-out non affecté ; la fermeture reprend depuis le dernier offset committé | vérifier les brokers ; surveiller le lag de `chat-visibility-consumer` |
| Consommateur de flux lent (lag) | le client reçoit `Status::data_loss` | abandonné par plan ; **les backpressions membre et audience sont indépendantes** — un invité lent ne bloque jamais un membre | le client se reconnecte + re-`GetHistory` ; scaler les pods / augmenter le buffer |
| Crash de pod | état présence/shard périmé | les sorted sets expirants vieillissent sans nettoyage explicite ; les drop guards libèrent les abonnements à la déconnexion | aucune — auto-réparation |

**Backpressure & limites.** Chaque plan possède son propre canal `tokio::sync::broadcast` par
conversation (`CHAT_MEMBER_STREAM_BUFFER_SIZE` / `CHAT_AUDIENCE_STREAM_BUFFER_SIZE`) ; un débordement ⇒
`Lagged` ⇒ abandon. Coût du fan-out = **pods, pas abonnés** : Redis `SPUBLISH` livre une fois par
*shard*, chaque pod re-diffuse en mémoire ; les abonnements des pods sont **comptés par référence**
(abonnement-au-premier / désabonnement-au-dernier), si bien que le va-et-vient présence/saisie/accusés
ne traverse jamais vers les nœuds purement audience. La taille de page est plafonnée par
`CHAT_MAX_PAGE_SIZE` pour éviter les scans de partition complète. La cohérence est étagée : les
écritures membre utilisent le profil Scylla **Strict** (LocalQuorum) ; l'historique invité utilise
**Fast** (LocalOne + exécution spéculative) ; les scans admin/analytics utilisent **Analytical**
(Quorum). Toutes les clés Redis du Plan Membre par conversation partagent le hash tag `{conv:<id>}`
(un seul slot, pas de `CROSSSLOT`) ; les canaux audience utilisent des tags de dispersion
`{aud:<id>:<k>}` afin qu'une conversation virale ne soit pas épinglée à un seul nœud.

---

## 📦 Intégration & utilisation

```toml
[dependencies]
chat = { path = "crates/services/chat" }
```

Le crate est **bibliothèque uniquement**. Il s'enfiche dans le runtime de flotte partagé en implémentant
[`service_runtime::Service`](../../platform/service-runtime/README.md) sous le nom
`chat::service::ChatService` — `build` câble chaque adaptateur (et lance le plane subscriber par pod, les
reapers de registres et le VisibilityWorker), `register` ajoute les services gRPC, et `health_probes`
expose les sondes de liveness Scylla/Redis. La télémétrie, la config + le hot-reload, le rate-limiting
en entrée, la santé et l'arrêt gracieux sont tous gérés par le runtime.

### Bootstrap (`crates/apps/chat-server`)

```rust
use std::net::SocketAddr;
use chat::service::ChatService;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr: SocketAddr = std::env::var("CHAT_GRPC_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:50051".to_owned())
        .parse()?;

    // Owns telemetry, infrastructure.toml load + hot-reload, the inbound-trace and traffic
    // layers, dynamic gRPC health, and SIGTERM/SIGINT-drained shutdown — then builds `ChatService`
    // and serves until shutdown.
    service_runtime::serve::<ChatService>(addr).await
}
```

---

## ⚙️ Configuration & environnement d'exécution

### Variables propres à Chat

| Variable | Required | Default | Description |
|---|---|---|---|
| `CHAT_MAX_PAGE_SIZE` | No | `50` | Server-enforced cap on `GetHistory`/`ListSubscriptions` page size (prevents full-partition scans). |
| `CHAT_BACKFILL_CONVERSATIONS_BY_MEMBER` | Non | non défini | `true` : indexe chaque appartenance existante par membre au démarrage (une fois ; idempotent) — #653. |
| `CHAT_HOT_TAIL_CACHE_SIZE` | No | `200` | Messages kept in the per-conversation Redis hot-tail cache (read offload). |
| `CHAT_MESSAGE_BUCKET_HOURS` | No | `24` | Time-bucket width for the Scylla message partition key. **Must be identical cluster-wide** and **never changed after data exists** (writer and reader derive the bucket from it). |
| `CHAT_MEMBER_STREAM_BUFFER_SIZE` | No | `256` | `broadcast` capacity per active Member-Plane stream; overflow ⇒ `Lagged`. |
| `CHAT_AUDIENCE_STREAM_BUFFER_SIZE` | No | `1024` | `broadcast` capacity per active Audience-Plane stream (sized larger for fan-out bursts). |
| `CHAT_AUDIENCE_SHARD_COUNT` | No | `16` | Number of Audience-Plane sharded channels a public conversation spreads across. **Must be uniform across the fleet.** |
| `CHAT_PRESENCE_TTL_SECS` | No | `30` | Presence liveness window (also reused as the audience-shard heartbeat TTL). |
| `CHAT_TYPING_TTL_SECS` | No | `6` | Typing-indicator expiry (short by design). |
| `CHAT_SOCIAL_GRAPH_GRPC_ENDPOINT` | No | unset | gRPC mesh de social-graph (`CheckInteraction`, #656). Non défini : messages directs coupés (`CHT-5001`), invitations non vérifiées. |
| `CHAT_SOCIAL_GRAPH_RPC_TIMEOUT_MS` / `CHAT_SOCIAL_GRAPH_CONNECT_TIMEOUT_MS` | No | `1000` / `1000` | Ses délais de requête et de connexion. |

### Variables d'infrastructure héritées

| Variable | Required | Default | Description |
|---|---|---|---|
| `SCYLLA_CONTACT_POINTS` | **Yes** | — | ScyllaDB seed nodes (host:port, comma-separated). |
| `SCYLLA_LOCAL_DC` | **Yes** | — | Local datacenter for token/DC-aware routing. |
| `SCYLLA_KEYSPACE` | No | `chat` | Keyspace (see migrations). |
| `REDIS_HOSTS` | **Yes** | — | Redis Cluster nodes (host:port, comma-separated). |
| `KAFKA_BROKERS` | **Yes** | — | Kafka bootstrap brokers. |
| `KAFKA_SECURITY_PROTOCOL` / `KAFKA_SASL_*` | No | plaintext | Auth for managed Kafka. |

> Le réglage complet connexion/timeout/reconnexion (`SCYLLA_*`, `REDIS_*`, `KAFKA_*`) est documenté dans
> les crates partagés `scylla-storage`, `redis-storage` et `transport`.

### Features de compilation
- `fred` est compilé avec `["partial-tracing", "i-scripts"]` ; le `redis-storage` partagé active
  transitivement la feature `subscriber-client` de fred (requise pour `SSUBSCRIBE`/`SPUBLISH`).
- `build.rs` compile le contrat protobuf (`proto/chat/v1/*.proto`) et émet un descriptor set de réflexion.

---

## 🚀 Déploiement, migrations & rollback

- **Migrations :** appliquer `crates/services/chat/migrations/0001…0011.cql` sur le keyspace `chat`
  **avant** le premier démarrage / avant de déployer un nouveau binaire.
- **Pièges liés à l'état :** `CHAT_MESSAGE_BUCKET_HOURS` et `CHAT_AUDIENCE_SHARD_COUNT` doivent être
  **uniformes sur tout le cluster**, et `CHAT_MESSAGE_BUCKET_HOURS` ne doit **jamais changer une fois que
  des données existent** — un calcul de bucket divergent casse silencieusement la pagination de
  l'historique.
- **Déploiement :** `<TODO : stratégie rolling / canary>`. Les abonnements de plan et la présence
  s'auto-réparent au renouvellement des pods, donc les redémarrages progressifs sont sûrs.
- **Rollback :** `<TODO : confirmer que les migrations sont rétro-compatibles avec le binaire N-1>`.

---

## 📈 Télémétrie, performance & métriques

- **Prérequis d'exécution :** un runtime **Tokio** multi-thread (tâches de streaming longue durée,
  reapers en arrière-plan, consommateurs Kafka, tâches de heartbeat par flux). Le subscriber global
  tracing/OTel doit être installé avant `serve` (les clients attachent le cycle de vie + la propagation
  du contexte de trace W3C à travers la frontière Kafka).

| Signal | Why it matters | Suggested alert |
|---|---|---|
| Broadcast `Lagged` rate (per plane) | clients can't keep up; stream churn | audience-plane lag spike ⇒ raise `CHAT_AUDIENCE_STREAM_BUFFER_SIZE` / add pods |
| Scylla `messages_by_conversation` read p99 by profile | cold-history read pressure / cache miss | p99 > SLO ⇒ verify hot-tail cache hit rate |
| Hot-tail cache hit ratio | read offload health | sustained drop ⇒ Redis pressure / cap too small |
| Kafka consumer lag (`chat-visibility-consumer`) | delayed Audience-Plane teardown | lag > threshold ⇒ broker/Redis investigation |
| DLQ produce rate (`chat.*.dlq`) | poison / retry-exhausted events | any sustained rate ⇒ page |
| Active member vs audience subscriptions per pod | fan-out skew / hotspotting | imbalance ⇒ rebalance shards |

---

## 🛠️ Développement local

```bash
# Build / format / lint (this crate)
cargo build  -p chat
cargo fmt    -p chat
cargo clippy -p chat --all-targets
cargo test   -p chat

# Whole workspace (CI gate)
cargo build  --workspace
cargo clippy --workspace
```

**Services dorsaux locaux** (ScyllaDB + Redis Cluster + Kafka) :

```bash
docker compose up -d scylla redis kafka      # from the repo-root compose file
for f in crates/services/chat/migrations/*.cql; do cqlsh -f "$f"; done
```

> Note disque : le `target/` du workspace est volumineux ; `rm -rf target/debug/incremental` récupère de
> l'espace sans risque (cache reconstruisible) si un build échoue avec `No space left on device`.

---

## 🚨 Dépannage & runbook

> Format : **symptôme → cause racine → mitigation.** Une entrée par classe d'incident réelle.

**1. `StreamPublic` (ou `Subscribe`, `GetHistory`, …) renvoie `NOT_FOUND` pour une conversation qui existe.**
Cause racine : la conversation est `Private` (ou vient d'être dépubliée) et l'appelant n'est pas dans son
roster : le service répond exactement comme pour une conversation inexistante (`CHT-1009` dans les
journaux serveur, voir *Les conversations privées sont insondables*) ; un membre reçoit plutôt
`FAILED_PRECONDITION` (`CHT-1008`). L'accès audience exige `visibility == Public`. Mitigation : confirmer via `GetHistory` en
tant que membre, ou
`ToggleVisibility{make_public:true}` si c'est voulu. Après une dépublication, le `VisibilityWorker`
ferme les flux invités à l'échelle du cluster — les clients doivent cesser de réessayer `StreamPublic`.

**2. Les invités ne voient pas les nouveaux messages, mais les membres oui.**
Cause racine : le Plan Audience ne diffuse pas — en général **aucun shard actif** dans le registre de
routage (le heartbeat de shard de chaque pod audience a expiré) ou un `CHAT_AUDIENCE_SHARD_COUNT`
incohérent entre les pods. Mitigation : vérifier l'activation des shards `chat:{aud:<id>:<k>}` dans Redis
et que `CHAT_AUDIENCE_SHARD_COUNT` est uniforme sur toute la flotte ; confirmer que les pods peuvent
`SPUBLISH`/`SSUBSCRIBE` (pub/sub shardé Redis 7+). Les nouveaux arrivants obtiennent quand même
l'historique depuis le cache hot-tail, donc une lacune de diffusion ressemble à « l'historique
fonctionne, le temps réel non ».

**3. La pagination d'historique saute des messages ou renvoie du vide en plein défilement.**
Cause racine : une valeur de `CHAT_MESSAGE_BUCKET_HOURS` qui diffère entre écrivains et lecteurs (le
calcul de bucket diverge), ou un défilement plus ancien que `MAX_BUCKET_WALK` (90 buckets) par requête.
Mitigation : rendre `CHAT_MESSAGE_BUCKET_HOURS` identique sur tout le cluster et ne jamais le changer une
fois que des données existent ; pour l'historique profond, paginer avec le curseur renvoyé plutôt qu'en
une grosse requête. Pour les lecteurs audience, rappeler que les lectures sont plancher-nées au watermark
« public-since » par conception.
