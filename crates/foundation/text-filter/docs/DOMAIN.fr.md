---
i18n:
  source: ./DOMAIN.md
  source_sha256: 03925c6bb4480b67a39b7846b53144a61b00f83fca86f7f8648e201138b6afc4
  translated_at: 2026-10-06
  status: complete
---
> 🇫🇷 Traduction française — la version **anglaise** [`DOMAIN.md`](./DOMAIN.md) fait foi.
> En cas de divergence, l'anglais prime. Les contrats (codes d'erreur, variables
> d'environnement, signatures, identifiants) sont volontairement laissés en anglais.

# `text-filter` — Domaine & contrat fonctionnel

> Une seule règle de correspondance pour les filtres de contenu d'un titulaire : *« ce texte
> contient-il quelque chose que son destinataire a choisi de ne pas voir ? »*

> **Fiche de domaine**
>
> | | |
> |---|---|
> | **Capacité partagée** | Confronter un texte aux mots masqués d'un titulaire et à la liste des termes injurieux |
> | **Couche** | `foundation` — pure, sans IO |
> | **Classe de sous-domaine** | **Support** — les réglages de sécurité des commentaires (#660) et des demandes de messages (#810) |
> | **Abstraction(s) principale(s)** | `ContentFilter`, `TermList` |
> | **Empreinte** | pure (sans IO ni état ; `TermList::from_file` lit un fichier une fois, au démarrage) |
> | **Posture d'échec** | N/A — les appelants décident de ce que signifie un filtre illisible (tous deux en mode fermé) |
> | **Dépend de** | `serde` |
> | **Consommé par** | `comment`, `chat` |
> | **Journal des décisions** | extrait de `comment` (#728) quand `chat` a eu besoin de la même règle (#810) |

---

## 1. Capacité technique & hors périmètre

**Capacité.** À partir du `ContentFilter` d'un titulaire et de la `TermList` injurieuse, dire si un texte
est intercepté. Une seule règle pour toutes les surfaces, si bien qu'un mot masqué des commentaires l'est
de la même façon des demandes de messages.

**Hors périmètre :** stocker les filtres, projeter les événements de profile, décider ce que devient un
texte intercepté, et fournir la liste injurieuse — tout cela revient aux services.

## 2. Langage omniprésent

| Terme | Sens |
|---|---|
| **Mots masqués** | Mots, expressions ou emoji qu'un titulaire masque (≤ 200, chacun ≤ 64 caractères, réglés dans profile). |
| **Filtre injurieux** | L'interrupteur du titulaire pour la liste de termes injurieux de l'exploitation ; actif par défaut. |
| **Intercepté** | Un texte que le filtre masque : il contient un mot masqué, ou un terme injurieux avec le filtre actif. |

## 3. Invariants

- La correspondance est insensible à la casse.
- Un terme avec une lettre ou un chiffre correspond à des mots entiers (une expression : ces mots à la
  suite) ; un terme sans aucun des deux (un emoji) correspond n'importe où.
- Un filtre vide et une liste vide n'interceptent rien ; le filtre par défaut garde le filtre injurieux
  actif.
