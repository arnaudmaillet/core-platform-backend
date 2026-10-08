---
i18n:
  source: ./0009-engagement-redis-primary-lua-atomic-with-kafka-write-behind.md
  source_sha256: 598067c698f6eb1dfda9273e684311dad52447d7d9d384fcf3eb12f0e4e97046
  translated_at: 2026-10-08
  status: complete
---
> 🇫🇷 Traduction française — la version **anglaise** [`0009-engagement-redis-primary-lua-atomic-with-kafka-write-behind.md`](./0009-engagement-redis-primary-lua-atomic-with-kafka-write-behind.md) fait foi.
> En cas de divergence, l'anglais prime. Les identifiants, codes, noms de types et statuts restent en anglais.

# ADR-0009 : Engagement est Redis-primary avec des arêtes Lua-atomiques et durabilité Kafka write-behind

- **Statut :** Accepted
- **Date :** 2026-06-26
- **Contexte(s) affecté(s) :** engagement ; counter (magnitudes) ; notification, geo-discovery
- **Décideurs :** arnaudmaillet (architecture)

## Contexte et problème

Les réactions sont des bascules extrêmement fréquentes et idempotentes (like/unlike). Un aller-retour
base par bascule ne peut suivre, et un read-modify-write non-atomique court sous concurrence
(double-likes, unlikes perdus). Mais les réactions restent une **arête de référence** (« qui a réagi,
comment ») qui doit survivre à une perte de cache.

## Décision

Engagement est **Redis-primary** : chaque react/unreact est une pose/effacement **Lua-atomique** de
l'arête de réaction plus une mise à jour du score in-Redis (idempotent par construction), avec
**Kafka write-behind** pour l'enregistrement durable et la propagation aval (`engagement.reactions`,
`engagement.score_updated`). Engagement possède l'**arête** ; `counter` possède les **magnitudes**
dérivées (voir ADR-0008). L'arête est la vérité ; le score est dérivé des `ReactionWeight`.

## Conséquences

- **Positives :** les bascules sur le hot path sont atomiques et rapides sans aller-retour base ; la
  durabilité est préservée de façon asynchrone ; les magnitudes sont l'affaire de quelqu'un d'autre.
- **Négatives / compromis accepté :** une fenêtre de lag write-behind où l'enregistrement durable
  traîne derrière Redis ; la réconciliation/le rejeu dépend du stream Kafka.
- **Clôt :** le goulot d'aller-retour base par bascule et les conditions de course sur les réactions.

## Alternatives rejetées

| Option | Pourquoi rejetée |
|---|---|
| Réactions base-primary | L'aller-retour par bascule ne peut soutenir le volume de réactions |
| Read-modify-write Redis non-atomique | Court sous concurrence (réactions doubles/perdues) |
| Garder les magnitudes ici aussi | Le comptage appartient à `counter` (ADR-0008) |

## Amendement 2026-10-08 — les likes sont des points (#665)

Les réactions ont disparu : un like est un point qu'un compte mise dans le `wallet` (qui fait foi), sans
retrait. Engagement garde le chemin chaud Redis-primary atomique en Lua, mais ce qu'il applique a
changé : `wallet.v1.events` `StakeCommitted` porte le **total** du compte sur un post ou un commentaire,
et un script Lua fait avancer le total du compte et la somme de la cible (un total non supérieur à celui
détenu ne change rien). Le flux at-least-once et sans ordre garanti devient ainsi idempotent sans
marqueur. La copie durable est ScyllaDB `likes_by_target` / `likes_by_account`, écrite avec l'heure de
la mise comme horodatage d'écriture. Engagement ne publie plus : `engagement.reactions` et son worker
write-behind sont retirés, et `engagement.score_updated` n'a jamais existé ; les consommateurs lisent
directement le flux du wallet. Le reste de cette décision (Redis-primary, compteurs dans Redis,
`counter` possédant les magnitudes) demeure.
