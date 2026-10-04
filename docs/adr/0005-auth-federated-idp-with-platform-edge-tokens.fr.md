---
i18n:
  source: ./0005-auth-federated-idp-with-platform-edge-tokens.md
  source_sha256: 4d67d490cc7bb1286ab101b84a5d34ca691b306d31e7f1646cd82be54038cd42
  translated_at: 2026-10-04
  status: complete
---
> 🇫🇷 Traduction française — la version **anglaise** [`0005-auth-federated-idp-with-platform-edge-tokens.md`](./0005-auth-federated-idp-with-platform-edge-tokens.md) fait foi.
> En cas de divergence, l'anglais prime. Les identifiants, codes, noms de types et statuts restent en anglais.

# ADR-0005 : Auth fédère un IdP et émet des edge tokens ES256 plateforme avec révocation par génération

- **Statut :** Accepted
- **Date :** 2026-06-26
- **Contexte(s) affecté(s) :** auth ; account ; auth-context (lib de vérif) ; realtime ; tous les services
- **Décideurs :** arnaudmaillet (architecture)

## Contexte et problème

L'authentification recouvre trois préoccupations faciles à confondre : le **compte** (qui existe — un
SoR), la **vérification** (ce token est-il valide — une préoccupation par-service) et l'**émission**
(frapper un identifiant, gérer la session). Il faut aussi des tokens rapides et stateless-à-vérifier à
l'edge *et* pouvoir révoquer instantanément — deux objectifs qui normalement s'opposent (les tokens
stateless ne se rappellent pas).

## Décision

`auth` est un contexte distinct qui **fédère un IdP externe (Keycloak) pour les identifiants** et
**émet les propres edge tokens ES256 courts de la plateforme**. Il est séparé d'`account` (le SoR
d'identité) et d'`auth-context` (la bibliothèque de vérification en process que chaque service utilise
— une vérification, pas un appel). La révocation instantanée malgré une vérification stateless est
obtenue avec une **`Generation` monotone par-sujet** : l'incrémenter invalide toute une famille de
tokens ; les refresh tokens sont à usage unique (la rotation invalide le précédent).

## Conséquences

- **Positives :** la vérification est bon marché et décentralisée (`auth-context`, pas d'appel à auth
  par requête) ; les identifiants réutilisent un IdP durci ; la révocation est immédiate via le bump
  de génération.
- **Négatives / compromis accepté :** le compteur de génération doit être vérifié au moment de la
  vérif (un petit lookup) pour que la révocation soit rapide ; le réglage de durée de vie des tokens
  arbitre entre latence de révocation et coût de vérification.
- **Clôt :** la confusion compte/vérif/émission ; la tension stateless-vs-révocable.

## Amendement 2026-09-15 — le claim `pids` et la périphérie client

La surface client-facing est indexée par id de **profil** alors que `sub` est l'id de
**compte**, et un compte possède N profils. Plutôt qu'un lookup par requête (le saut que la
décision ci-dessus exclut) ou un flux de bascule de profil actif, `auth` émet les profils que
possède le compte dans un claim `pids` à chaque login et refresh (lus depuis `profile` via
`ListProfilesByAccount` ; **fail-safe** — une panne émet un claim vide, jamais un login en
échec). Les services lient l'acteur indexé par profil d'une requête avec `require_profile`
(doit figurer dans `pids`) et un acteur indexé par compte avec `require_account` (doit égaler
`sub`), le tout dans le processus (`transport::grpc::edge`). La vérification elle-même est
passée de deux services (realtime, audit) à l'**écouteur de périphérie** (`:9443`) de chaque
serveur client-facing, via `auth-context::spawn_edge_decoder` partagé et l'extracteur
`platform_edge` (qui lit `perms` ; l'extracteur OIDC par défaut l'ignorait silencieusement).
Reste ouvert parmi les conséquences ci-dessus : le **lookup de génération** à la vérification
— la révocation reste bornée par le TTL d'accès de 10 minutes.

## Amendement 2026-10-04 — le claim `did`

La passerelle realtime indexe une socket par appareil et refuse un handshake dont le jeton
n'a pas de claim d'appareil, mais `auth` n'en émettait aucun : aucun jeton réel ne pouvait
ouvrir de socket. `auth` émet désormais `did`, le `DeviceContext.device_id` auquel la session
a été liée au login, à chaque login et refresh (`auth_context::edge::EDGE_DEVICE_CLAIM`, la
valeur par défaut de `REALTIME_DEVICE_CLAIM` côté realtime). Il vient de la session, pas de
la requête de refresh, et reste donc stable pendant toute la vie de la session. Il est omis
quand le client n'a envoyé aucun id d'appareil, et un tel jeton échoue toujours au handshake
realtime.

## Amendement 2026-10-04 — les jetons invité

Le mode invité permet à une installation de l'app de lire le contenu public avant l'inscription.
Plutôt qu'un edge en « authentification optionnelle » (pas de jeton = anonyme), invisible pour la
limitation de débit et l'attribution, `auth` émet des **sessions invité** (`StartGuestSession`,
publique sur l'edge) : une session sans compte derrière, dont le jeton edge porte
`sub = "guest:<guest_id>"`, `kind = "guest"`, `perms = ["read:public"]`, aucun `pids`, et un `did`.
Le préfixe empêche un invité de jamais correspondre à un id de compte dans `require_account`. L'edge
client refuse les invités sur toute règle `authenticated` et les admet sur les règles
`permission(…, "read:public")` ; les membres portent aussi `read:public`, si bien que les lectures
publiques passent à cette permission. Les invités n'ont pas de connexion realtime.

## Alternatives rejetées

| Option | Pourquoi rejetée |
|---|---|
| Tokens opaques vérifiés en appelant auth par requête | Réintroduit un saut synchrone à chaque appel authentifié |
| Tokens longue durée, sans génération | Pas de révocation instantanée ; un token fuité reste valide jusqu'à expiration |
| Construire notre propre store d'identifiants au lieu de fédérer | Re-résout un problème générique et sensible à la sécurité qu'un IdP résout déjà |
