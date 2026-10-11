---
i18n:
  source: ./0018-passwordless-identity-passkeys-for-staff.md
  source_sha256: 24b3159cca9abaf51bc3531a610537f217a9c3d859e6d1a5a7b62a5c27222ae5
  translated_at: 2026-10-11
  status: complete
---
> 🇫🇷 Traduction française — la version **anglaise** [`0018-passwordless-identity-passkeys-for-staff.md`](./0018-passwordless-identity-passkeys-for-staff.md) fait foi.
> En cas de divergence, l'anglais prime. Les identifiants, codes, noms de types et statuts restent en anglais.

# ADR-0018 : Identité sans mot de passe : Keycloak retiré, passkeys pour le staff

- **Statut :** Accepted
- **Date :** 2026-10-11
- **Contexte(s) affecté(s) :** auth ; account ; auth-context (lib de vérif) ; fleet locale ; infra (déploiement Keycloak)
- **Décideurs :** arnaudmaillet (architecture)

## Contexte et problème

L'[ADR-0005](./0005-auth-federated-idp-with-platform-edge-tokens.fr.md) faisait fédérer à `auth` un
IdP externe (Keycloak) pour les identifiants. Depuis, chaque méthode de connexion des membres est
passée dans `auth` lui-même : Sign in with Apple / Google natif (id_tokens vérifiés contre les clés du
fournisseur, nonces émis par le serveur), codes à usage unique par e-mail ou SMS, et passkeys (#808).
**Le chemin mot de passe était le seul usage restant de Keycloak** — et il apportait un password
grant, un flux de réinitialisation/changement, un client admin, un verrouillage de compte et un second
annuaire d'utilisateurs à effacer au titre du RGPD. Les mots de passe sont aussi l'identifiant le plus
exposé au phishing et à la réutilisation, et ce risque est maximal pour le **staff**, dont les sessions
portent les autorisations privilégiées de la plateforme. Aucun compte n'existe et la production n'a
jamais été lancée : il n'y a rien à migrer.

## Décision

Nous ne connectons personne par mot de passe, et `auth` vérifie lui-même chaque identifiant (epic
#879, #881). Les **membres** se connectent avec Apple / Google (id_token natif), un code à usage unique
envoyé à leur e-mail ou téléphone, ou une passkey ; une identité sans compte donne `AUT-6004` et part
vers `SignUp` (`Login` ne crée jamais de compte ni de lien d'identité). Le **staff** se connecte
uniquement avec une **passkey enrôlée staff** (#880) : les autorisations staff n'existent que dans une
session ouverte avec une passkey enrôlée sous une autorisation à usage unique délivrée par un admin, ou
par la commande ponctuelle `bootstrap-staff` tant qu'aucune passkey staff n'existe. Keycloak est retiré
du code (adaptateurs, ports, `AUTH_KEYCLOAK_*`), le password grant, l'authorization-code grant et
`ChangePassword` sont retirés d'`auth.v1` / `account.v1` (champs réservés), et Keycloak quitte la fleet
locale et l'infrastructure (core-platform-infra#68 ; iOS retire ses écrans de mot de passe,
core-platform-ios#840).

## Conséquences

- **Positives :** aucun mot de passe à hameçonner, réutiliser, bourrer ou fuiter ; les privilèges staff
  exigent un identifiant résistant au phishing et lié à l'appareil ; une dépendance TIER-0 de moins à
  exploiter, patcher et sauvegarder ; l'effacement RGPD n'a plus d'annuaire externe à atteindre ; la
  protection anti-brute-force d'`auth` couvre chaque identifiant qu'il accepte.
- **Négatives / compromis accepté :** plus de flux de réinitialisation de mot de passe — la
  récupération passe par un code à usage unique envoyé à l'e-mail ou au téléphone du compte (ou par
  Apple / Google) ; un membre du staff qui a perdu toutes ses passkeys staff a besoin d'une nouvelle
  autorisation d'un admin. `account` perd le stockage des mots de passe et son verrouillage après échecs
  de connexion (`RecordFailedLogin`, `locked_until`). Les codes d'erreur `AUT-5002`,
  `AUT-5004`…`AUT-5007` sont retirés et jamais réutilisés.
- **Remplace :** la partie IdP fédéré de l'ADR-0005 (identifiants vérifiés par Keycloak). Le modèle de
  jeton d'edge de l'ADR-0005 — jetons ES256, révocation par génération, `pids`, `did`, jetons invité —
  reste en vigueur.
- **Clôt :** les mots de passe comme cible de phishing pour le staff ; le second annuaire
  d'utilisateurs et son chemin d'effacement.

## Alternatives rejetées

| Option | Pourquoi rejetée |
|---|---|
| Garder Keycloak pour les seuls mots de passe | Tout un IdP à exploiter et sécuriser pour le seul identifiant que l'on veut abandonner |
| Garder les mots de passe, vérifiés par `auth` (hachés en propre) | Toujours hameçonnables et réutilisables ; ramène les flux de réinitialisation et le verrouillage |
| Le staff sur les méthodes membres plus un second facteur | Les codes et le TOTP sont hameçonnables ; une passkey est liée à l'origine et à l'appareil |
| Keycloak (ou un autre IdP) pour le SSO du staff | Inutile tant qu'aucun IdP d'entreprise n'existe ; les passkeys offrent la même résistance au phishing en interne |
