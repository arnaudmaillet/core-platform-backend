#!/usr/bin/env bash
# Dev-data seed for the local fleet. Runs INSIDE the compose network.
#
# Creates 3 users (Active account → email-code identity linked in auth →
# profile), mutual follows, and ~5 published posts each. Nothing signs in with
# a password (#881): each user signs in with a code sent to their email, which
# the local auth-server prints to its log (AUTH_VERIFICATION_SENDER=log).
# Idempotent: safe to re-run.
set -uo pipefail

# The email-code identity issuer auth links accounts under; the account's
# identity_id is `${issuer}#${email}`.
EMAIL_ISSUER="urn:core-platform:email"
AUTH_DB="postgres://core:core@postgres:5432/auth"

ACCOUNT="account-server:50059"
PROFILE="profile-server:50052"
SGRAPH="social-graph-server:50053"
POST="post-server:50056"

# username -> handle:display metadata
USERS=("alice" "bob" "carol")
declare -A HANDLE=( [alice]="alice" [bob]="bob" [carol]="carol" )
declare -A DISPLAY=( [alice]="Alice Anderson" [bob]="Bob Brown" [carol]="Carol Clark" )
declare -A EMAIL=( [alice]="alice@dev.local" [bob]="bob@dev.local" [carol]="carol@dev.local" )
declare -A PROFILE_ID   # username -> profile_id (filled below)

log() { printf '\033[36m[seed]\033[0m %s\n' "$*"; }
err() { printf '\033[31m[seed:ERR]\033[0m %s\n' "$*" >&2; }

# Links an email-code identity to an account in auth's database (what a code
# sign-up does), so `Login { verification_code }` finds the account.
link_email() { # <email> <account_id>
  psql "$AUTH_DB" -v ON_ERROR_STOP=1 -qtc \
    "INSERT INTO subject_links (issuer, subject, account_id, linked_at) \
     VALUES ('${EMAIL_ISSUER}', '$1', '$2', now()) ON CONFLICT (issuer, subject) DO NOTHING" >/dev/null
}

# grpcurl wrapper: <host:port> <method> <json> -> prints response (empty on error)
call() { grpcurl -plaintext -max-time 15 -d "$3" "$1" "$2" 2>/dev/null; }

wait_for() { # <host:port> <label>
  for _ in $(seq 1 60); do
    grpcurl -plaintext -max-time 5 "$1" list >/dev/null 2>&1 && { log "$2 reachable"; return 0; }
    sleep 2
  done
  err "$2 ($1) never became reachable"; return 1
}

log "waiting for services…"
for _ in $(seq 1 60); do
  psql "$AUTH_DB" -qtc "SELECT 1 FROM subject_links LIMIT 1" >/dev/null 2>&1 && break
  sleep 2
done
wait_for "$ACCOUNT" account || exit 1
wait_for "$PROFILE" profile || exit 1
wait_for "$SGRAPH"  social-graph || exit 1
wait_for "$POST"    post || exit 1

# ── 1. Accounts + profiles ────────────────────────────────────────────────────
for u in "${USERS[@]}"; do
  identity="${EMAIL_ISSUER}#${EMAIL[$u]}"
  log "$u: identity_id=${identity}"

  aid="$(call "$ACCOUNT" account.v1.AccountService/GetAccountByIdentityId \
        "$(jq -nc --arg i "$identity" '{identity_id:$i}')" | jq -r '.id // empty')"
  if [ -z "$aid" ]; then
    # CreateAccount's response echoes identity_id (not the real account id), so
    # re-fetch by identity to get the server-generated UUID.
    call "$ACCOUNT" account.v1.AccountService/CreateAccount \
      "$(jq -nc --arg i "$identity" --arg e "${EMAIL[$u]}" \
         '{identity_id:$i, email:$e, country_of_residence:"US", date_of_birth:"1990-01-01"}')" >/dev/null
    aid="$(call "$ACCOUNT" account.v1.AccountService/GetAccountByIdentityId \
          "$(jq -nc --arg i "$identity" '{identity_id:$i}')" | jq -r '.id // empty')"
    log "$u: created account ${aid}"
  else
    log "$u: account exists ${aid}"
  fi
  [ -z "$aid" ] && { err "$u: could not create/resolve account"; exit 1; }
  link_email "${EMAIL[$u]}" "$aid" || { err "$u: could not link ${EMAIL[$u]} in auth"; exit 1; }

  # PendingVerification -> Active (ignore if already active)
  call "$ACCOUNT" account.v1.AccountService/VerifyEmail \
    "$(jq -nc --arg a "$aid" '{account_id:$a}')" >/dev/null

  # profile_id is generated server-side and CreateProfile's response echoes
  # account_id (not the real id), so resolve the real profile_id by listing the
  # account's profiles — works whether the profile pre-exists or was just created.
  pid="$(call "$PROFILE" profile.v1.ProfileService/ListProfilesByAccount \
        "$(jq -nc --arg a "$aid" '{account_id:$a, limit:1}')" | jq -r '.profiles[0].profileId // empty')"
  if [ -z "$pid" ]; then
    call "$PROFILE" profile.v1.ProfileService/CreateProfile \
      "$(jq -nc --arg a "$aid" --arg h "${HANDLE[$u]}" --arg d "${DISPLAY[$u]}" \
         '{account_id:$a, handle:$h, display_name:$d, bio:"seeded dev user", profile_kind:1, locale:"en"}')" >/dev/null
    pid="$(call "$PROFILE" profile.v1.ProfileService/ListProfilesByAccount \
          "$(jq -nc --arg a "$aid" '{account_id:$a, limit:1}')" | jq -r '.profiles[0].profileId // empty')"
    log "$u: created profile ${pid}"
  else
    log "$u: profile exists ${pid}"
  fi
  [ -z "$pid" ] && { err "$u: could not create/resolve profile"; exit 1; }
  PROFILE_ID[$u]="$pid"
done

# ── 2. Mutual follows (everyone follows everyone) ─────────────────────────────
for a in "${USERS[@]}"; do
  for b in "${USERS[@]}"; do
    [ "$a" = "$b" ] && continue
    call "$SGRAPH" social_graph.v1.SocialGraphService/Follow \
      "$(jq -nc --arg x "${PROFILE_ID[$a]}" --arg y "${PROFILE_ID[$b]}" '{actor_id:$x, target_id:$y}')" >/dev/null
  done
  log "$a follows the other users"
done

# ── 3. ~5 published posts each ────────────────────────────────────────────────
for u in "${USERS[@]}"; do
  pid="${PROFILE_ID[$u]}"
  have="$(call "$POST" post.v1.PostService/ListPostsByProfile \
         "$(jq -nc --arg p "$pid" '{profile_id:$p}')" | jq '[.posts[]?] | length' 2>/dev/null)"
  have="${have:-0}"
  for n in 1 2 3 4 5; do
    [ "$n" -le "$have" ] && continue
    postid="$(call "$POST" post.v1.PostService/CreatePost \
             "$(jq -nc --arg p "$pid" --arg c "Post #${n} from ${DISPLAY[$u]} — hello from the local fleet!" \
                '{profile_id:$p, kind:1, caption:$c}')" | jq -r '.postId // empty')"
    [ -z "$postid" ] && { err "$u: CreatePost #$n failed"; continue; }
    call "$POST" post.v1.PostService/PublishPost \
      "$(jq -nc --arg id "$postid" --arg p "$pid" '{post_id:$id, profile_id:$p}')" >/dev/null
  done
  log "$u: posts ensured (>=5)"
done

log "✅ seed complete — 3 users (alice/bob/carol @dev.local: sign in with an email code, see auth-server's log), mutual follows, 5 posts each"
