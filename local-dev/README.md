# Local backend fleet (for frontend/client testing)

Run the backend on your laptop so a native/mobile gRPC client can hit it directly
on `localhost` — with **real-mode login** through Keycloak. No Kubernetes, no cloud.

## What comes up

| Layer | Containers |
|---|---|
| Datastores | ScyllaDB, Redis (shared), Redpanda (Kafka API), PostgreSQL, MinIO (S3), OpenSearch, OTLP sink |
| Identity | Keycloak (realm `core-platform`, imported from `keycloak/`) |
| Bootstrap (one-shot) | `db-init` (Postgres DBs) → `migrator` (schema) → `scylla-rf1` (RF=1 downshift) → `topic-provisioner` (Kafka topics) → `minio-init` (bucket) |
| Services | account, auth, profile, social-graph, post, comment, engagement, timeline, notification, chat, geo-discovery, counter (server+worker), moderation, media, search, realtime (gateway+dispatcher) |
| Seed | `seed` one-shot — 3 users, mutual follows, ~5 posts each |

`audit` is intentionally **not** included (compliance plane, not needed for frontend dev).

## gRPC ports (published on `localhost`, 1:1 with the prod registry)

| Service | Port | Service | Port |
|---|---|---|---|
| chat | 50051 | media | 50063 |
| profile | 50052 | counter (server) | 50064 |
| social-graph | 50053 | counter (worker) | 50065 |
| geo-discovery | 50054 | realtime gateway (gRPC) | 50066 |
| notification | 50055 | realtime dispatcher | 50067 |
| post | 50056 | timeline | 50070 |
| comment | 50057 | auth JWKS (HTTP) | 8081 |
| engagement | 50058 | realtime WS | 8443 |
| account | 50059 | Keycloak (host) | 8085 |
| auth (gRPC) | 50060 | MinIO API / console | 9000 / 9001 |
| moderation | 50061 | OpenSearch | 9200 |
| search | 50062 | Kafka (host) | 19092 |

Datastores also exposed: Postgres `:5432`, Redis `:6379`, Scylla `:9042`.

## Usage

```bash
# 0. Generate the local-only ES256 signing key auth needs (once; writes to
#    local-dev/secrets/, which is gitignored — nothing sensitive is committed).
bash local-dev/generate-secrets.sh

# From the repo root. First build compiles ~20 Rust release binaries — build
# SERIALLY (see below); the cargo-chef dep layer is cooked once and reused.
docker compose -f local-dev/docker-compose.fleet.yml up -d

# Seed dev data (idempotent; runs in-network so the Keycloak issuer matches auth)
docker compose -f local-dev/docker-compose.fleet.yml run --rm --no-deps seed

# Tear down (keep data) / wipe volumes
docker compose -f local-dev/docker-compose.fleet.yml down          # -v to wipe
```

### Building (memory-safe)

A parallel build of all images OOM-kills Docker (release `rustc` is heavy). Build
one image at a time; the first image cooks the shared dependency layer, the rest
reuse it:

```bash
for s in $(docker compose -f local-dev/docker-compose.fleet.yml config --services); do
  docker compose -f local-dev/docker-compose.fleet.yml build "$s"
done
```

## Seeded dev users

Three Keycloak users (password **`password`** for all), each with an Active account,
a profile, mutual follows, and 5 published posts:

| username | handle | email |
|---|---|---|
| alice | @alice | alice@dev.local |
| bob | @bob | bob@dev.local |
| carol | @carol | carol@dev.local |

**Real login** (what iOS does): `auth.v1.AuthService/Login` with `grant_type: PASSWORD`
and `{username, password}`. auth brokers the password grant to Keycloak, resolves the
account by `identity_id` (= `iss#sub`), and mints an ES256 edge token + refresh.

```bash
grpcurl -plaintext -d '{"device":{"user_agent":"cli","ip_address":"127.0.0.1","device_id":"d1"},
  "grant_type":"PASSWORD","password":{"username":"alice","password":"password"}}' \
  localhost:50060 auth.v1.AuthService/Login
```

## Edge mode (token-checked, as in staging)

By default every service serves only its **mesh** port, which trusts the network: no
token check and no `EDGE_POLICY` allow-list. A call that staging refuses with
`UNAUTHENTICATED` (no or bad token) or `UNIMPLEMENTED` (RPC not exposed to clients)
then succeeds locally. To test client gating against the real policy, add the edge
overlay:

```bash
docker compose -f local-dev/docker-compose.fleet.yml -f local-dev/docker-compose.edge.yml up -d
```

Every client-facing server then also runs the **client edge** listener on `:9443`
(the port the ALB targets in staging), verifying auth's ES256 token against
`http://auth-server:8081/.well-known/jwks.json`.

- **Inside the fleet network** (the iOS Envoy from `dev/fleet-gateway.sh`): target
  `<service>-server:9443` instead of the mesh port.
- **From the host:** edge port = mesh port + 10000.

| Service | Edge (host) | Service | Edge (host) |
|---|---|---|---|
| chat | 60051 | engagement | 60058 |
| profile | 60052 | account | 60059 |
| social-graph | 60053 | auth | 60060 |
| geo-discovery | 60054 | moderation | 60061 |
| notification | 60055 | search | 60062 |
| post | 60056 | media | 60063 |
| comment | 60057 | counter | 60064 |
| | | timeline | 60070 |

The edge listener does not serve gRPC reflection (prod doesn't either), so point
grpcurl at the protos:

```bash
G="grpcurl -plaintext -import-path crates/contracts/proto -proto auth/v1/service.proto -proto profile/v1/service.proto"
TOKEN=$($G -d '{"device":{"device_id":"d1"},"grant_type":"PASSWORD",
  "password":{"username":"alice","password":"password"}}' \
  localhost:60060 auth.v1.AuthService/Login | jq -r .tokens.accessToken)
$G -H "authorization: Bearer $TOKEN" -d '{"handle":"bob"}' \
  localhost:60052 profile.v1.ProfileService/GetProfileByHandle      # OK
$G -d '{"handle":"bob"}' \
  localhost:60052 profile.v1.ProfileService/GetProfileByHandle      # UNAUTHENTICATED
$G -H "authorization: Bearer $TOKEN" -d '{}' \
  localhost:60052 profile.v1.ProfileService/HideProfile             # UNIMPLEMENTED (mesh-only)
```

(`G` is meant for bash, which splits it into words; in zsh use an array.)

The mesh ports stay published and unauthenticated, as they are in-cluster: only
the edge ports show what a client will get in staging. realtime is not in the
overlay; its WSS gateway (`:8443`) always verifies the token.

## Verified working end-to-end

- ✅ Real login (auth → Keycloak password grant → ES256 token); wrong password rejected.
- ✅ Timeline fan-out — alice's following-feed returns bob's + carol's 10 posts.
- ✅ Search — OpenSearch indexed 3 profiles + 15 posts; queries return hits.
- ✅ Accounts (Active), profiles, follows, published posts.
- ✅ Edge mode (2026-10-04): login on the auth edge; the token carries `did` and
  `pids`; no or bad token → `UNAUTHENTICATED`; mesh-only RPC → `UNIMPLEMENTED`;
  `CreatePost` as an owned profile passes, as another profile → `PERMISSION_DENIED`.
- ✅ Guest mode over the edge (2026-10-04): `StartGuestSession` → token `kind=guest`,
  `read:public`, no `pids`; the guest reads a profile (no `account_id`), a profile's posts and
  followers; `CreatePost` / `Follow` → `PERMISSION_DENIED`; refresh keeps it a guest; members
  carry `read:public` and still read. (`AUTH_GUEST_SESSIONS_ENABLED` is on in this fleet.)

## Known caveats / follow-ups

- **Single-node Scylla → RF=1.** Migrations create keyspaces at RF=3 (prod topology);
  a single node can't satisfy `LOCAL_QUORUM` writes, so `scylla-rf1` downshifts every
  service keyspace to RF=1 after migration. (If you add a Scylla-backed service, add
  its keyspace to that one-shot's list.)
- **Media (images + video).** `media-server` (`:50063`) reaches MinIO in-network
  (`minio:9000`) for byte I/O, but presigns upload/download URLs against
  `localhost:9000` (`MEDIA_OBJECT_STORE_PUBLIC_ENDPOINT`) so a host/browser client can
  resolve them. Flow: `IssueUploadTicket` → PUT the bytes to the presigned URL →
  `CommitUpload` → the asset reaches `READY` (images) or, for video, `media-worker`
  (`:50071`) transcodes it with ffmpeg to a 3-rung HLS ladder + poster and marks it
  `READY`. `ResolveDelivery` then returns the playback URL — for video, an HLS
  `master.m3u8` under `http://localhost:9000/media/post-videos/<hash>/` (MinIO serves
  the bucket with anonymous read, so hls.js can play it directly). Video kinds:
  `MEDIA_KIND_VIDEO`, `video/mp4` + `video/quicktime`, 200 MiB cap.
- **Counters reconcile async.** counter-server/worker are up and respond, but the
  FOLLOWER magnitude reads 0 — follower/following counts are a window onto social-graph's
  set and the reconcile path isn't populating in this stack.
- **realtime notification push.** `realtime-dispatcher` subscribes to `notification.v1.events`,
  which the event-topology registry does **not** provision (no producer). Non-fatal
  (dispatcher stays up, retries) — a genuine registry/consumer inconsistency to resolve
  in code, not here. Other realtime channels + the WSS gateway (`ws://localhost:8443`) are up.
- **`ProfileService/ListProfilesByAccount` is broken** (CQL bug: binds `i64` for a `LIMIT`
  column typed `int`). Use `GetProfileByHandle` / `GetProfileById` instead (the seed does).
- The mesh ports have no JWT enforcement (as in-cluster); use the edge overlay
  above to test token checks and the edge allow-list.

`docker-compose.backends.yml` remains the infra-only smoke stack.
