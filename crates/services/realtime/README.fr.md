---
i18n:
  source: ./README.md
  source_sha256: 94a7d1b7553e37d1e539b0519be3dc39c0252c946cd29ce32e584fd993f77d6d
  translated_at: 2026-10-11
  status: complete
---
> 🇫🇷 Traduction française — la version **anglaise** [`README.md`](./README.md) fait foi.
> En cas de divergence, l'anglais prime. Les contrats (codes d'erreur, variables
> d'environnement, topics Kafka, identifiants) sont volontairement laissés en anglais.

# `realtime` — Maintenir des millions de connexions clientes vivantes, livrer chaque événement à l'appareil exact à l'instant où il se produit, et ne détenir aucune vérité

> **Fiche de service** &nbsp;·&nbsp; CORE
>
> | | |
> |---|---|
> | **Propriétaire** | `<TODO: équipe>` · `<TODO: #canal-slack>` |
> | **Astreinte / escalade** | `<TODO: rotation-astreinte>` → `<TODO: politique-escalade>` |
> | **Palier** | **TIER-1** — la surface temps réel sans laquelle une application à hyper-échelle paraît cassée, mais **dérivée et fail-open** : ni System of Record, ni dans un quelconque chemin d'écriture synchrone ; une panne dégrade vers « se reconnecter et resynchroniser », elle ne perd ni ne bloque jamais un message |
> | **Déployable** | **deux** binaires — `crates/apps/realtime-gateway` (bord avec état, détient les connexions) **et** `crates/apps/realtime-dispatcher` (worker de diffusion sans état). Crate bibliothèque : `crates/services/realtime` |
> | **Écouteurs** | **deux plans** (le premier de la flotte) — un plan client public **WSS** (défaut `:8443`, derrière un LB L4) sur la gateway, et un plan interne **gRPC** de santé/contrôle (`:50066` gateway · `:50067` dispatcher) |
> | **Datastores** | **Redis** uniquement — le registre de connexion/présence (routage `user → node`) + le tissu de saut inter-nœud Pub/Sub. Ne détient aucune entité, ne persiste aucun message |
> | **Async** | consomme `notification.v1.events` (ciblé), `counter.v1.popularity` + `post.v1.events` (broadcast public) (Kafka). Ne publie rien de référence. `chat` reste sur son propre plan live (coexistence) |
> | **Appelants amont** | clients utilisateurs finaux via WSS (mobile / web) — **pas** les services internes |
> | **Dépendances aval** | Redis, Kafka, et `auth` (vérification du jeton de bord via la bibliothèque `auth-context` — une vérification, pas un appel). La durabilité des messages/notifications reste dans `chat` / `notification` |
> | **SLO** | `<TODO>` dispo · événement→appareil p99 `< <TODO ~250> ms` pour les utilisateurs en ligne · établissement de connexion p99 `< <TODO> ms` |

---

## 🎯 Vue d'ensemble & rôle du service

`realtime` est le **plan de livraison temps réel orienté client** de la plateforme : il termine des millions de connexions clientes longue durée et multiplexées, diffuse les événements internes vers l'appareil exact qui doit les voir, et ne détient **aucune** entité. Chaque octet qu'il relaie est déjà durable dans son service propriétaire — `chat` a persisté le message, `notification` a persisté le badge, `counter` détient la magnitude. C'est un System-of-**Connection / Delivery**, jamais un System of Record.

Le problème difficile qu'il résout est double. D'abord, **le piège de charge inversée du polling** : à hyper-échelle, des clients qui interrogent le bord couplent le QPS des services cœur au nombre d'utilisateurs *inactifs* — chaque sondage vide paie un coût complet TLS + auth + diffusion pour ne rien livrer, pointant un DDoS auto-infligé vers le maillage interne. Le push inverse cela pour que le travail interne suive les *événements*, pas les paires d'yeux. Ensuite, **la prolifération des connexions** : `chat` et `notification` ont chacun développé leur propre streaming orienté client, de sorte qu'un seul appareil détient plusieurs sockets avec des heartbeats redondants réveillant la radio indépendamment. Realtime fusionne cela en **une seule socket multiplexée par appareil** et réduit ces services à des *producteurs* d'événements.

**Objectifs cœur :** (1) une connexion persistante et multiplexée par appareil — économe en batterie et compatible pare-feu ; (2) les événements voyagent cœur → appareil sans nouvel appel synchrone dans le maillage (Kafka en entrée, diffusion ciblée en sortie) ; (3) le plan est une **cloison structurelle** — des millions de connexions instables terminent ici et n'atteignent jamais le maillage gRPC, qui ne voit qu'un ensemble borné de pairs gateway stables ; (4) la posture est **fail-open** — un raté de livraison coûte de la latence, jamais des données, car la durabilité vit dans les SoR et les clients resynchronisent à la reconnexion.

| Préoccupation | Chemin | Contrat de latence | Notes |
|---|---|---|---|
| **Transport client** | WSS sur `:443`/`:8443`, enveloppe multiplexée | persistant | une socket par appareil ; canaux logiques (`dm` / `notif` / `presence` / `counter:<id>`) |
| **Ingestion** | consommateurs Kafka async (`run_consumer`) dans `realtime-dispatcher` | aucun (hors chemin d'écriture) | résoudre destinataire → nœud propriétaire → publication ciblée |
| **Dernier saut** | lookup registre → saut inter-nœud (Redis Pub/Sub) → écriture socket | sous-seconde pour les utilisateurs en ligne | best-effort ; un raté est resynchronisé depuis le SoR à la reconnexion |

---

## 📐 Architecture & concepts

Hexagonal / DDD (`domain` → `application` → `infrastructure`), Kafka pour l'ingestion, Redis pour le tissu de routage. Le choix structurel déterminant est **deux déployables** : la gateway de bord avec état et le dispatcher de diffusion sans état partagent une crate de domaine mais aucun processus, déploiement ou domaine de défaillance. Un nœud gateway qui sature la mémoire sur les connexions ne doit jamais pouvoir faire vaciller un service cœur.

```
                       Internet — millions de clients mobile/web
                                     │  WSS :443
                              ┌──────┴───────┐
                              │  LB L4       │   (pas de terminaison WS en L7 — éviter le mur des ports éphémères)
                              └──────┬───────┘
                  ┌──────────────────┼──────────────────┐
            ┌─────┴─────┐      ┌─────┴─────┐      ┌──────┴────┐
            │ gateway 0 │ ···  │ gateway 7 │ ···  │ gateway N │   realtime-gateway (bord avec état)
            └─────┬─────┘      └─────┬─────┘      └──────┬────┘   autoscale sur conns + mém (PAS CPU)
                  │  écritures registre / abo node:{id}       │
                  └────────────────┬─────────────────────────┘
                            ┌──────┴──────┐        ┌────────────────┐
                            │    Redis    │◄───────│   dispatcher   │  realtime-dispatcher
                            │ registre +  │ publie │  run_consumer  │  (diffusion sans état)
                            │  pub/sub    │  vers  └───────┬────────┘
                            └─────────────┘ node:{id}      │ consomme
                                                     ┌─────┴──────┐
                                                     │   Kafka    │  chat · notification ·
                                                     │ (existant) │  counter.v1.popularity · post.v1
                                                     └─────┬──────┘
                                          ┌────────────────┴────────────────┐
                                          │  maillage gRPC cœur (SoR) — protégé│  émet-une-fois ; ne voit jamais un client
                                          └───────────────────────────────────┘
```

**Le pont interne→externe comporte trois étapes.** (A) Les services cœur **émettent une fois** vers Kafka — le plan n'ajoute aucun nouvel appel synchrone vers l'intérieur. (B) Le dispatcher résout le destinataire contre le **registre de connexion** (`presence:{user_id}` → le(s) nœud(s) détenant les sockets vivantes de cet utilisateur) — livraison *ciblée*, jamais diffusion-puis-filtrage. (C) Le dernier saut publie l'événement sur le canal Redis du nœud propriétaire ; la gateway le remet à la boîte aux lettres bornée de la connexion et écrit la socket. Cœur à appareil = un saut Kafka + un saut Redis + une écriture socket.

> **Invariants** (et où ils sont appliqués) :
> - **Le plan ne stocke rien.** Il détient la connexion et le registre de routage éphémère, jamais le contenu. Un raté de registre (destinataire hors ligne) est un no-op — la durabilité et le chemin de push vers téléphone verrouillé appartiennent à `chat` / `notification` — domain + application.
> - **Authentifier une fois, au handshake.** Le jeton de bord est vérifié à l'upgrade WS via `auth-context` ; jamais re-vérifié par trame (cela réintroduirait le coût maillage par-message que le plan existe pour éliminer) — frontière infrastructure. Sa **révocation** est re-vérifiée au heartbeat (#903) : une lecture en cache par socket et par tick, pas par trame.
> - **La seule autorisation est la propriété de portée de canal.** Une connexion ne peut s'abonner qu'aux canaux liés à son identité épinglée (Alice → `dm:alice`, jamais `dm:bob`). La visibilité fine du contenu a été autorisée en amont à l'émission de l'événement — domain.
> - **Un consommateur lent est délesté, pas tamponné.** Chaque connexion a une file d'envoi bornée ; en débordement, le plan jette/déconnecte plutôt que de laisser un mauvais réseau gonfler la mémoire du nœud — infrastructure.
> - **Fail-open, toujours.** Un événement live perdu n'est jamais un message perdu ; le client resynchronise depuis le SoR via son jeton de séquence à la reconnexion — application.

---

## 📊 Objectifs de niveau de service (SLO) &nbsp;·&nbsp; OPS

| SLI | Objectif | Fenêtre | Mesuré par |
|---|---|---|---|
| Disponibilité (handshakes réussis / non-`UNAVAILABLE`) | `<TODO 99.9%>` | glissant 30j | `<metric>` |
| Latence événement→appareil p99 (utilisateurs en ligne) | `< <TODO 250> ms` | 1h | `<metric>` |
| Établissement de connexion p99 | `< <TODO> ms` | 1h | `<metric>` |
| Plafond mémoire par connexion | `< <TODO> KB` | live | RSS / open_connections |

**Budget d'erreur :** `<TODO>`. **À l'épuisement :** `<gel des déploiements | page>`. Comme `realtime` est fail-open, l'objectif de *disponibilité* couvre la dégradation de la livraison live (reconnexion-et-resynchronisation), pas la durabilité — la durabilité appartient aux SoR amont et sort du budget de ce service.

---

## 🔗 Dépendances & rayon d'impact &nbsp;·&nbsp; OPS

**Aval — ce dont `realtime` a besoin pour fonctionner :**

| Dépendance | Rôle | Si en panne → | Dégradation |
|---|---|---|---|
| Redis | registre de connexion/présence + saut inter-nœud Pub/Sub | le dispatcher ne peut résoudre / publier | **Souple** — la livraison live cale ; les clients resynchronisent depuis les SoR à la reconnexion ; aucune perte |
| Kafka | ingestion des événements amont | la diffusion cesse d'avancer | **Souple** — les mises à jour live retardent ; rien de perdu (commit manuel) ; les clients rattrapent via le SoR |
| `auth` (via `auth-context`) | vérification du jeton de bord au handshake | les nouvelles connexions ne peuvent s'authentifier | **Dure pour les nouvelles connexions** — les connexions existantes intactes jusqu'à l'expiration du jeton |

**Amont — qui dépend de `realtime` (votre rayon d'impact si VOUS tombez) :**

| Appelant | Utilise | Impact visible utilisateur si `realtime` est en panne |
|---|---|---|
| clients utilisateurs finaux | le flux WSS live | les DM / notifications / compteurs live cessent d'arriver *instantanément* — ils apparaissent à la prochaine ouverture ou reconnexion (resynchronisés depuis `chat` / `notification`) ; **rien n'est perdu** |

> **Chemin critique ?** **Non** — dérivé, async, fail-open. `realtime` n'est jamais dans le chemin synchrone d'envoi d'un message, de persistance d'une notification, ou d'une quelconque écriture. Il accélère la livraison ; il ne la détient pas.

---

## 🔌 Interfaces publiques & contrat d'API &nbsp;·&nbsp; CORE

### Transport client — enveloppe WSS multiplexée *(Phase 1)*

La surface orientée client n'est **pas** du gRPC. Les clients se connectent via **WebSocket Secure** et échangent une **enveloppe** binaire compacte à préfixe de longueur — `{ stream_seq, channel, ack_required, payload }` — qui impose des canaux logiques (`dm`, `notif`, `presence`, `counter:<id>`, `control`) sur une seule socket physique. Le handshake porte le jeton de bord ; ensuite les trames ne sont pas ré-authentifiées. `WebTransport` sur HTTP/3 est une voie successeur différée conçue d'avance, que le client négocie et depuis laquelle il bascule en repli.

### gRPC interne — santé / contrôle *(Phase 1)*

Le plan interne est uniquement opérationnel : santé + reflection sur `:50066` (gateway) / `:50067` (dispatcher), plus le RPC optionnel de livraison dispatcher↔gateway si le tissu de saut inter-nœud quitte Redis Pub/Sub. **Il n'y a aucun gRPC orienté client et aucun RPC d'écriture de domaine.**

### Ports Rust (contrat hexagonal) *(Phase 3)*

```rust
#[async_trait] pub trait ConnectionRegistry { /* bind · resolve(user → nodes) · evict — le tissu de routage */ }
#[async_trait] pub trait NodeChannel        { /* subscribe(node) · publish(node, event) — le dernier saut */ }
#[async_trait] pub trait TokenVerifier      { /* vérifier le jeton de bord au handshake (auth-context) */ }
#[async_trait] pub trait EventSource        { /* les flux Kafka amont que le dispatcher diffuse */ }
```

### Contrat d'erreur

Chaque défaillance implémente `error::AppError` avec un code `RTM-XXXX` stable, mappé vers gRPC `Status` / HTTP par la crate partagée `error` :

| Plage | Classe |
|---|---|
| `RTM-1xxx` | handshake / authentification de connexion |
| `RTM-2xxx` | transport / framing / protocole |
| `RTM-3xxx` | autorisation d'abonnement (propriété de portée de canal) |
| `RTM-4xxx` | disponibilité du tissu de livraison (cœur fail-open ; retryable) |
| `RTM-5xxx` | cycle de vie de connexion / contre-pression |
| `RTM-8xxx` | décodage / routage d'événement entrant (dispatcher) |
| `RTM-9xxx` | transversal (domaine/parse, consommation d'événement) |

---

## 📨 Événements & contrat async &nbsp;·&nbsp; CORE

> Les topics Kafka sont une API. Un changement de schéma dans un topic consommé casse la livraison exactement comme un changement de proto.

**Publie :** rien de référence. (La présence, si exposée, est de la liveness interne uniquement — voir le blueprint ; ce n'est pas un flux System-of-Record.)

**Consomme :**

| Topic | Groupe de consommateurs | Rôle | En cas de poison/épuisement |
|---|---|---|---|
| `notification.v1.events` | `realtime-notif-fanout` | **ciblé** — livrer notification/badge au canal `notif:<user>` du destinataire | DLQ `notification.v1.events.dlq` |
| `counter.v1.popularity` | `realtime-counter-fanout` | **broadcast** — livrer les pics d'engagement aux spectateurs abonnés à `counter:<entity>` | DLQ `counter.v1.popularity.dlq` |
| `post.v1.events` | `realtime-post-fanout` | **broadcast** — livrer le cycle de vie du post aux abonnés du `feed:<profile_id>` de l'auteur | DLQ `post.v1.events.dlq` |

> **Deux modes de diffusion.** Les événements *ciblés* (notification — et DM/présence) nomment un utilisateur destinataire ; le dispatcher les résout via le registre et saute vers le(s) nœud(s) propriétaire(s). Les événements *broadcast* (counter / feed) nomment une entité, pas un utilisateur ; le dispatcher les publie une fois sur le canal broadcast de la flotte, et chaque nœud livre à ses connexions locales abonnées à ce canal. `chat.message.sent` n'est **pas** consommé — il ne porte aucune liste de membres et chat exécute déjà son propre plan live (la décision coexistence-d'abord) ; le câbler est une consolidation délibérée, pas un décodage.

> **Contrat d'exécution (obligatoire) :** tous les consommateurs du dispatcher tournent sous `run_consumer` — commit manuel après un résultat terminal, retry borné avec backoff + jitter, DLQ à l'épuisement/poison, reconstruction depuis le dernier offset commité sur erreur broker. **Idempotence :** la diffusion est naturellement idempotente — un événement redélivré ré-résout le registre et re-livre ; les trames live dupliquées sont inoffensives (le client déduplique sur `stream_seq`). Un événement non-routable/inconnu (`RTM-8002` / `RTM-8003`) est replié en `Ok` pour que l'offset commite quand même.

---

## 🌩️ Modes de défaillance & dégradation &nbsp;·&nbsp; OPS

| Défaillance | Symptôme | Comportement du service | Action opérateur |
|---|---|---|---|
| Redis registre/pub-sub en panne | la livraison live cale | **fail-open** — les événements patientent/retry ; les clients resynchronisent depuis les SoR à la reconnexion ; aucune perte | restaurer Redis ; la diffusion reprend |
| Kafka indisponible | les mises à jour live retardent | le dispatcher patiente ; offsets non commités → aucune perte | restaurer les brokers ; rattrapage |
| `auth` injoignable | les nouveaux handshakes échouent | connexions existantes intactes ; nouvelles connexions rejetées (`RTM-1001`) jusqu'au rétablissement | restaurer `auth` ; vérifier la config `auth-context` |
| Client lent/bloqué | la file d'une connexion se remplit | **délestage** — jeter le plus ancien / déconnecter (`RTM-5001`) ; mémoire du nœud protégée | aucune (par conception) |
| Connexion semi-ouverte | fuite de FD + slot de registre | le reaper de heartbeat la périme (`RTM-5002`), libère le slot, bascule la présence hors ligne | aucune (par conception) ; surveiller les métriques du reaper |
| Déploiement / drain de nœud | les connexions doivent migrer | stop-accept → trame de contrôle de reconnexion **avec backoff jitter** (`RTM-5003`) → drain | aucune (par conception) ; surveiller les métriques de troupeau de reconnexion |
| Troupeau de reconnexion | pic de handshake auth au déploiement | le backoff jitter client + l'étalement du LB L4 l'absorbent | confirmer la config jitter ; échelonner les déploiements |

**Contre-pression & limites :** file d'envoi bornée par connexion (délestage en débordement) ; plafond d'abonnements par connexion ; plafond de taille de trame entrante ; reaping par échéance de heartbeat ; autoscale sur connexions + mémoire (un nœud inactif-mais-plein est à ~0% CPU — l'autoscaling basé CPU sous-provisionne jusqu'à l'OOM).

---

## 📦 Intégration & utilisation &nbsp;·&nbsp; CORE

```toml
[dependencies]
realtime = { path = "crates/services/realtime" }
```

Bibliothèque uniquement. Implémentera [`service_runtime::Service`](../../platform/service-runtime/README.md) **deux fois** (Phase 5) : `realtime::service::RealtimeGatewayService` (le binaire `realtime-gateway` — la boucle d'acceptation WSS, les écritures de registre, l'abonnement au saut inter-nœud, le hook de drain, la santé gRPC interne) et `realtime::service::RealtimeDispatcherService` (le binaire `realtime-dispatcher` — les boucles de diffusion `run_consumer` supervisées, aucun RPC de domaine). Télémétrie, config + hot-reload, santé et arrêt gracieux sont détenus par le runtime.

> **État de build :** **complet jusqu'à la Phase 7 + le chemin de diffusion publique (broadcast).** 58 tests unitaires plus une suite live `integration-realtime` (Redis réel) couvrent le domaine, le codec proto, la mailbox bornée-à-délestage, la table de routage locale au nœud (livraison ciblée + broadcast, délestage-sous-pression, `broadcast_drain` gracieux), les mappings de décodage amont (notification / counter / post) et les ponts de bout en bout (ciblé : fan-out → registre → saut inter-nœud → socket ; broadcast : fan-out → canal flotte → index de canal → socket). Revu côté sécurité : aucun jeton ni PII dans les logs, le payload opaque n'est jamais journalisé, et aucun panic dans le chemin chaud d'acceptation ou de livraison.
>
> **Différé (explicite, pas des lacunes) :** le test d'intégration de la boucle d'acceptation WebSocket + auth/JWKS (nécessite un IdP live et un client WS de qualité navigateur — le tissu de routage *est* couvert en live) ; l'IT live du dispatcher Kafka (le runner `run_consumer` est couvert par la suite de `transport`) ; la voie de transport WebTransport / HTTP-3 ; la présence comme flux orienté produit ; le routage cross-région / multi-PoP ; la variante de contre-pression `NodeChannel` en gRPC interne ; le câblage `SIGTERM` → `broadcast_drain` ; et la consolidation du streaming client `chat` + `notification` (coexistence d'abord).
>
> **Autorisation (exigence de déploiement) :** `realtime` authentifie le jeton de bord une fois au handshake WS (via `auth-context`) et n'autorise les abonnements que par propriété de portée de canal. Il n'effectue aucune autorisation au niveau du contenu — les événements sont autorisés en amont à l'émission.

---

## ⚙️ Configuration & environnement d'exécution &nbsp;·&nbsp; CORE

### Variables spécifiques à `realtime` *(remplies par phase)*

| Variable | Requise | Défaut | Description |
|---|---|---|---|
| `REALTIME_GATEWAY_WS_ADDR` | Non | `0.0.0.0:8443` | adresse d'écoute WSS client publique (gateway) |
| `REALTIME_GATEWAY_GRPC_ADDR` | Non | `0.0.0.0:50066` | adresse gRPC interne de santé/contrôle de la gateway |
| `REALTIME_DISPATCHER_GRPC_ADDR` | Non | `0.0.0.0:50067` | adresse santé/reflection du dispatcher (aucun RPC de domaine) |
| `REALTIME_HEARTBEAT_INTERVAL_MS` | Non | `<TODO 30000>` | cadence de ping applicatif (maintient le binding NAT, reaper des semi-ouvertes) |
| `REALTIME_HEARTBEAT_TIMEOUT_MS` | Non | `<TODO 90000>` | échéance de pong avant qu'une connexion soit reapée |
| `REALTIME_SEND_QUEUE_CAP` | Non | `<TODO>` | profondeur de file sortante par connexion avant délestage |
| `REALTIME_MAX_SUBSCRIPTIONS` | Non | `<TODO>` | plafond d'abonnements de canal par connexion |
| `REALTIME_MAX_FRAME_BYTES` | Non | `<TODO>` | plafond de taille de trame entrante |
| `REALTIME_NODE_ID` | Non | `<hostname>` | identité de ce nœud gateway pour le registre + le canal de saut inter-nœud |

### Variables d'infrastructure héritées

| Variable | Requise | Défaut | Description |
|---|---|---|---|
| `REDIS_URL` | **Oui** | — | registre de connexion/présence + saut inter-nœud Pub/Sub |
| `KAFKA_BROKERS` | **Oui** (dispatcher) | — | ingestion des événements amont |
| `<auth-context verification config>` | **Oui** | — | vérification du jeton de bord (ES256) au handshake |
| `EDGE_REVOCATION_REDIS_HOSTS` | Non (recommandé) | — | le Redis d'`auth` (voir `service-runtime`, #903) : un jeton qu'`auth` a révoqué est refusé au handshake (`RTM-1001`), et un socket vivant est re-vérifié à chaque heartbeat puis fermé (`ReauthRequired`, `RTM-1001`) dès que son jeton est révoqué — déconnexion, suspension, rôle retiré. Échec ouvert (TIER-1) : un état illisible garde le socket. Non défini ⇒ un jeton révoqué garde son socket |

### Fonctionnalités à la compilation
- `integration-realtime` *(Phase 6)* — active la suite d'intégration live adossée à des conteneurs (Redis + Kafka réels).

---

## 🚀 Déploiement, migrations & rollback &nbsp;·&nbsp; OPS

- **Deux déployables, mis à l'échelle indépendamment.** `realtime-gateway` s'échelonne avec le nombre de connexions concurrentes + la mémoire ; `realtime-dispatcher` s'échelonne avec le débit d'événements. Publiés ensemble (même image/tag), déployés et autoscalés séparément.
- **Aucune migration de schéma.** Le plan ne détient aucun store durable — seulement de l'état de routage Redis éphémère (avec TTL, auto-réparant).
- **Le drain gracieux est une exigence de release.** Au déploiement, la gateway doit stop-accept, émettre une trame de contrôle de reconnexion avec backoff jitter côté client, puis drainer — sans jitter, chaque déploiement déclenche un troupeau de reconnexion vers le chemin de handshake auth.
- **Load balancing L4**, pas L7 : terminer le WS sur un proxy L7 reflète des millions de connexions et heurte le mur des ports éphémères.
- **Rollback :** sûr — les deux binaires sont sans état au-dessus de Redis/Kafka ; le dispatcher reprend depuis les derniers offsets commités, la gateway ré-accepte les connexions (les clients se reconnectent avec backoff).

---

## 📈 Télémétrie, performance & métriques &nbsp;·&nbsp; CORE

- **Runtime :** Tokio multi-thread, I/O async sur epoll/kqueue — une connexion inactive est un future garé, pas un thread. `realtime-gateway` exécute la boucle d'acceptation + les boîtes aux lettres par connexion + le reaper de heartbeat ; `realtime-dispatcher` exécute les consommateurs de diffusion. Souscripteur tracing/OTel global installé avant serve ; trace-context W3C propagé à travers la frontière Kafka.

| Signal | Pourquoi c'est important | Alerte suggérée |
|---|---|---|
| Connexions ouvertes / nœud | signal de capacité + autoscale | proche du plafond du nœud ⇒ scale |
| Mémoire par connexion (RSS / conns) | le plafond de coût C10M | `> SLO` ⇒ investiguer fuite / délestage |
| Latence événement→appareil p99 | réactivité live | `> SLO` ⇒ investiguer registre / saut inter-nœud |
| Taux de délestage de file d'envoi (`RTM-5001`) | pression de consommateur lent | pic soutenu ⇒ investiguer clients / réseau |
| Taux de reap de heartbeat (`RTM-5002`) | churn des semi-ouvertes | pic anormal ⇒ investiguer réseau / LB |
| Taux de reconnexion | troupeau / churn au déploiement | pic hors-déploiement ⇒ investiguer perte de nœud |
| Taux de production DLQ (`*.dlq`) | ingestion empoisonnée / retry épuisé | tout taux soutenu ⇒ page |

---

## 🛠️ Développement local &nbsp;·&nbsp; CORE

```bash
cargo build -p realtime && cargo clippy -p realtime --all-targets
cargo test  -p realtime                                    # run unitaire rapide, sans infra
docker compose up -d redis kafka                           # compose racine du repo (Phase 6)
cargo test  -p realtime --features integration-realtime    # suite live (Phase 6)
```

---

## 🚨 Dépannage & runbook &nbsp;·&nbsp; CORE

> Format : **symptôme → cause racine → mitigation.** Une entrée par classe d'incident réelle.

**1. Les messages n'arrivent qu'à la réouverture de l'app, pas en live.**
Cause racine : Redis registre/pub-sub dégradé, ou retard du dispatcher — diffusion live calée. Mitigation : vérifier la santé Redis et le lag des groupes de consommateurs ; le message est durable dans `chat`/`notification`, donc les clients rattrapent à la reconnexion — aucune perte ; la livraison live se rétablit quand le tissu se rétablit.

**2. La mémoire du nœud grimpe vers l'OOM alors que le CPU est presque inactif.**
Cause racine : accumulation de connexions ou tamponnage de consommateur lent. Mitigation : confirmer que le plafond de file d'envoi par connexion et le délestage sont actifs ; vérifier la métrique de taux de délestage ; autoscale sur connexions + mémoire, pas CPU — un nœud plein et inactif est à ~0% CPU.

**3. Chaque déploiement déclenche un pic auth/handshake.**
Cause racine : troupeau de reconnexion — clients se reconnectant sans jitter au drain. Mitigation : confirmer le backoff jitter dans la trame de contrôle de drain et le SDK client ; échelonner les déploiements de nœuds.

**4. Certains utilisateurs ne reçoivent jamais d'événements live ; les FD/slots de registre fuient.**
Cause racine : connexions semi-ouvertes que le noyau retient encore. Mitigation : confirmer l'intervalle/timeout du reaper de heartbeat ; vérifier la métrique de taux de reap ; le reaping libère le FD + le slot de registre et bascule la présence hors ligne.

**5. Un client peut voir le flux d'un autre utilisateur.**
Cause racine (critique) : une faille d'autorisation de portée de canal. Mitigation : cela doit être impossible — les abonnements sont autorisés contre l'identité épinglée (`RTM-3001`) ; traiter toute occurrence comme un incident de sécurité et auditer le chemin d'abonnement.
