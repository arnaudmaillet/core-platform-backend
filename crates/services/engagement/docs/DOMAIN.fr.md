---
i18n:
  source: ./DOMAIN.md
  source_sha256: 936b8a0adc33d66e55680cc22aa197b6b6fa451637ccd018090a17e96a1ea57f
  translated_at: 2026-10-08
  status: complete
---
> 🇫🇷 Traduction française — la version **anglaise** [`DOMAIN.md`](./DOMAIN.md) fait foi.
> En cas de divergence, l'anglais prime. Les contrats (codes d'erreur, topics, variables
> d'environnement, noms de types, identifiants d'ADR) restent en anglais.

# `engagement` — Contrat de Domaine & Fonctionnel

> **Fiche de domaine**
>
> | | |
> |---|---|
> | **Bounded Context** | Engagement — likes (points) et compteurs d'interaction |
> | **Classe de sous-domaine** | **Core** — interaction directe de l'utilisateur avec le contenu ; les likes sont le tissu du produit |
> | **System of …** | **Reference** pour les likes (les mises du wallet font foi, #665) ; **Record** pour les compteurs de vues/partages jusqu'à ce que `counter` les remplace |
> | **Racine(s) d'agrégat** | aucune — les likes sont des totaux appliqués (VO `LikeTarget`), les compteurs des incréments |
> | **Tier** | **TIER-1** |
> | **Posture en cas de panne** | **Plutôt fail-open** — Redis-primary avec atomicité Lua, alimenté par Kafka |
> | **Contextes amont** | `wallet` (mises) ; `account` (suppressions) ; clients finaux (vues/partages) ; `comment` (comptes) ; `post` (compteurs de likes masqués) |
> | **Contextes aval** | `account` (export RGPD, `ListLikesByAccount`) — via **Open Host Service** (gRPC mesh) |
> | **Journal de décisions** | [`ADR-0009`](../../../../docs/adr/0009-engagement-redis-primary-lua-atomic-with-kafka-write-behind.md) |

---

## 1. Capacité métier & non-objectifs

**Capacité.** `engagement` répond à **« combien de likes a ce post ou ce commentaire, combien sont les
miens, et combien de fois a-t-il été vu, partagé et commenté ? »**

**Le problème difficile.** Transformer les événements de mise du wallet — livrés au moins une fois et
sans ordre garanti — en compteurs de likes exacts à l'échelle d'un post viral : chaque événement porte le
**total** du compte, appliqué par un script Lua qui ne fait qu'avancer, sans aller-retour base de données
sur le chemin de lecture.

**Non-objectifs — ce que ce contexte ne fait délibérément PAS :**
- ❌ Décider si un like est permis (points, plafonds, visibilité, auto-like) → `wallet` possède les mises.
- ❌ Diffuser les likes (notifications, centres d'intérêt, classement des pays) → ils lisent directement `wallet.v1.events`.
- ❌ Posséder le contenu liké → `post` / `comment`.
- ❌ Les réactions pondérées (cœur, feu, fusée, applaudissements, triste) — retirées avec #665 : un like est un point.

---

## 2. Langage omniprésent

| Terme | Sens dans ce contexte | Symbole de code |
|---|---|---|
| Like | Un point qu'un compte a misé sur un post ou un commentaire (pas de retrait) | `LikeStore`, `LikeLedger` |
| Cible de like | Le post ou le commentaire sur lequel tombe un like | `LikeTarget` |
| Total | Les points d'un compte sur une cible ; ne fait que croître | `apply_total`, `AccountLike::total` |
| Compteur de likes | La somme des totaux sur une cible | `LikeSummary::count` |
| Likes masqués | L'auteur masque ses compteurs de likes (#809) : compteur retenu pour les non-auteurs | `LikeVisibility` |

---

## 3. Modèle de domaine

| Élément | Nature | Frontière d'invariant qu'il garde |
|---|---|---|
| `LikeTarget` | VO | Seulement un post ou un commentaire, avec un id borné |
| `PostId` | VO | Le post auquel appartient un compteur |
| `AccountLike` | modèle de lecture | Ce qu'un compte a liké (export RGPD) |
| `PostEngagementSnapshot` | modèle de lecture | Les compteurs de vues/partages/commentaires d'un post |

**Cycle de vie des likes d'un compte sur une cible :**

```
(none) --(stake, total n)--> n --(stake, total m > n)--> m      (a total ≤ the one held: no change)
```

> **Transitions légales uniquement.** Les totaux ne font que croître ; le compteur de la cible bouge de
> la différence, atomiquement.

---

## 4. Propriété des données & frontières

**Ce contexte est la source de vérité pour :**
- Les compteurs de vues/partages/commentaires — **Redis** (primaire) avec une copie **ScyllaDB** approximative.

**Il détient une copie de référence de :** les likes — les mises du wallet, en totaux par (compte,
cible) : Redis (chemin de lecture) et ScyllaDB `likes_by_target` / `likes_by_account` (récupération,
export RGPD).

**La liste « ne pas écrire » :** engagement ne décide jamais d'un like ; il ne possède pas le contenu.

---

## 5. Invariants & règles métier

| # | Invariant | Imposé à | En cas de violation |
|---|---|---|---|
| I1 | Le compteur de likes d'une cible est la somme des totaux de ses comptes | script Lua (un hash tag par cible) | — (atomique) |
| I2 | Le total d'un compte sur une cible ne fait que croître ; re-livraisons et événements tardifs ne changent rien | script Lua ; horodatage d'écriture Scylla = heure de la mise | — (ignoré) |
| I3 | Les compteurs masqués n'atteignent que l'auteur et le mesh ; quand post ne peut pas répondre, ils sont retenus | application | `ENG-6001` (mode fermé) |
| I4 | Seuls les posts et les commentaires peuvent être likés | `LikeTarget::parse` | `ENG-9004` |
| I5 | Un compte supprimé n'est plus connu comme likeur nulle part ; les points qu'il a donnés restent dans les compteurs | `LikeEraser` ; suppressions Scylla à l'heure de l'effacement ; le consommateur des mises ignore ses mises tardives | — |

---

## 6. Workflows & orchestration

**Like.** Le wallet valide une mise et publie `StakeCommitted` (outbox) ; le `StakeConsumer`
d'engagement applique le total du compte dans Redis, puis écrit la copie durable.

**Lecture.** `GetPostEngagement` / `BatchGetLikes` lisent compteurs et likes dans Redis, en retenant les
compteurs masqués selon la réponse de post (`BatchGetLikeVisibility`, en cache 60 s).

**Export.** L'export RGPD d'account parcourt `ListLikesByAccount` (mesh uniquement) vers `likes.json`.

**Effacement.** Sur `account_deleted`, `LikeEraser` marque le compte comme effacé, l'oublie sur chaque
cible likée (entrée Redis retirée ; ligne Scylla remplacée par une ligne anonyme de même total, les
compteurs restant reconstructibles) et supprime sa liste ; les compteurs restent.

---

## 7. Relations de contexte (tranche de la Context Map)

| Contexte voisin | Direction | Patron | Mécanisme | Ce qui casse s'ils changent |
|---|---|---|---|---|
| `wallet` | amont | Conformist | `wallet.v1.events` (`stake_committed`) | les compteurs de likes cassent |
| `account` | amont | ACL | `account.v1.events` (`account_deleted`) | un compte supprimé reste connu comme likeur |
| `comment` | amont | ACL | `comment.created` / `comment.deleted` | les compteurs de commentaires cassent |
| `post` | amont | Customer/Supplier | gRPC `BatchGetLikeVisibility` | compteurs masqués retenus pour tous sauf le mesh |
| `account` | aval | Open Host Service | gRPC `ListLikesByAccount` | l'export RGPD échoue (réessayé) |

---

## 8. Événements de domaine (sémantique, pas le format filaire)

engagement ne publie aucun événement. Il consomme :

| Événement | Signifie | Effet ici |
|---|---|---|
| `wallet.v1.events` `stake_committed` | les points d'un compte sur une cible ont atteint un nouveau total | le compteur de likes et ceux du compte bougent |
| `comment.created` / `comment.deleted` | un commentaire a été publié / retiré | compteur de commentaires ±1 |
| `account.v1.events` `account_deleted` | un compte a été supprimé | qui a liké est oublié ; les compteurs restent |

---

## 9. Décisions & justification

| Décision | ADR | Statut |
|---|---|---|
| Chemin chaud Redis-primary atomique en Lua, copies durables alimentées par Kafka | [`ADR-0009`](../../../../docs/adr/0009-engagement-redis-primary-lua-atomic-with-kafka-write-behind.md) | Accepted (amendée par #665) |
| Les likes sont des points misés dans le wallet ; réactions retirées | #665 | Accepted |

---

## 10. Classification du sous-domaine & évolution

- **Classification :** Core — interaction directe avec le contenu.
- **Volatilité :** faible — les likes suivent le contrat de mise du wallet.
- **Dette de modélisation connue :** le hash des likers n'a pas de TTL (un plancher de réhydratation
  depuis Scylla est prévu).
- **Capacités différées :** règlement des likes (gems gagnées grâce aux likes, #665).
