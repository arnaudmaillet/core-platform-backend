# docs/architecture/workspace.dsl
#
# Corrected C4 model — REGENERATED from docs/domain/CONTEXT_MAP.md + the per-service Domain Cards
# (crates/services/<svc>/docs/DOMAIN.md). This is a DERIVED artifact: the source of truth is the
# code and CONTEXT_MAP.md. If they disagree, the docs are right and this file is stale — regenerate.
#
# Supersedes the pre-fleet model quarantined in docs/_legacy/ (do NOT reference that one).
#
# Conventions:
#   - One container per service (dual-binary services noted in the description; realtime's gateway
#     and dispatcher are modelled separately because the public WS edge is architecturally distinct).
#   - Datastores are modelled as one container per technology; per-service isolation (keyspace / db /
#     namespace) is documented in each Domain Card, not duplicated here.
#   - Async edges route through Kafka (publishes / consumes); sync edges are direct gRPC.
#   - Subdomain class (Core / Supporting) and tier come from CONTEXT_MAP.md.

workspace "Core Platform" "Social platform backend — corrected C4, derived from the domain documentation." {

    model {
        user = person "Client Apps" "End users on mobile / web." "User"

        # --- External systems -------------------------------------------------------------------
        keycloak     = softwareSystem "Keycloak (IdP)" "Federated identity provider for credentials." "External"
        objectStore  = softwareSystem "Object Store (S3 / MinIO)" "Media bytes + audit WORM archive (Object Lock)." "External"
        cdn          = softwareSystem "CDN (CloudFront)" "Media delivery edge." "External"
        push         = softwareSystem "Push (APNs / FCM)" "Offline device push." "External"
        kms          = softwareSystem "KMS / HSM + Witness" "Key custody + external checkpoint witness (RFC3161 / cross-account WORM)." "External"

        backend = softwareSystem "Core Platform" "Rust/tonic microservice fleet (hexagonal; CQRS; Kafka event backbone)." {

            # --- Shared backbone ---------------------------------------------------------------
            kafka     = container "Kafka" "Event backbone — every async edge (Published Language topics) flows through here." "Apache Kafka" "MessageBroker"
            scylla    = container "ScyllaDB" "High-write column store (per-service keyspaces)." "ScyllaDB" "Datastore"
            postgres  = container "PostgreSQL" "Relational store (per-service databases)." "PostgreSQL" "Datastore"
            redis     = container "Redis" "Caches, hot indexes, routing/presence, Lua-atomic counters." "Redis" "Datastore"
            opensearch = container "OpenSearch" "Inverted index (search read-model, single canonical store)." "OpenSearch" "Datastore"

            # --- Identity & Access (Supporting / TIER-0) ---------------------------------------
            group "Identity & Access" {
                account = container "account" "Identity SoR — accounts, credentials metadata, KYC, roles, GDPR state." "Rust/tonic" "Service,Supporting"
                auth    = container "auth" "Authentication — sessions, refresh, federated IdP broker, ES256 edge tokens." "Rust/tonic" "Service,Supporting"
                profile = container "profile" "Public persona over the account SoR — handle, bio, tier, dual-axis visibility." "Rust/tonic" "Service,Supporting"
            }

            # --- Content & Social (Core) -------------------------------------------------------
            group "Content & Social" {
                post        = container "post" "Content SoR — two-table Scylla; post.v1.events is the read-side fan-out source." "Rust/tonic" "Service,Core"
                comment     = container "comment" "Comment threads — flat tree, tombstone/purge." "Rust/tonic" "Service,Core"
                chat        = container "chat" "Conversations & messaging — Shadowing Pattern; runs its own live plane." "Rust/tonic" "Service,Core"
                engagement  = container "engagement" "Likes (the wallet's stakes) and interaction counters — Redis-primary, Lua-atomic." "Rust/tonic" "Service,Core"
                socialGraph = container "social-graph" "Follower/following + block relations; computes author tier." "Rust/tonic" "Service,Core"
            }

            # --- Discovery & Delivery (Supporting) --------------------------------------------
            group "Discovery & Delivery" {
                timeline = container "timeline" "Home-feed read-model — hybrid push/pull fan-out." "Rust/tonic" "Service,Supporting"
                search   = container "search" "Discovery read-model over OpenSearch (+ async indexer worker)." "Rust/tonic" "Service,Supporting"
                geo      = container "geo-discovery" "Spatial discovery — H3 grid + dual-layer Redis Top-K." "Rust/tonic" "Service,Supporting"
                counter  = container "counter" "Magnitudes SoReference — read server + stream worker; publishes counter.v1.popularity." "Rust/tonic" "Service,Supporting"
                realtimeGateway    = container "realtime-gateway" "Stateful WSS edge — millions of multiplexed client connections." "Rust/tonic + axum" "Edge,Supporting"
                realtimeDispatcher = container "realtime-dispatcher" "Stateless fan-out worker — resolves recipients, node-hop delivery." "Rust/Tokio" "Worker,Supporting"
                notification = container "notification" "Activity-feed read-model + push fan-out (write-collapse)." "Rust/tonic" "Service,Supporting"
            }

            # --- Trust, Safety & Compliance (TIER-0) ------------------------------------------
            group "Trust, Safety & Compliance" {
                moderation = container "moderation" "Integrity decision/enforcement SoR — three-plane; fail-closed Screen gate." "Rust/tonic" "Service,Core"
                media      = container "media" "Media control plane — pre-signed uploads; transform; delivery brokerage." "Rust/tonic" "Service,Supporting"
                audit      = container "audit" "Tamper-evident compliance evidence — hash-chained ledger (server + worker)." "Rust/tonic" "Service,Supporting"
            }

            # === Service ↔ datastore =============================================================
            account -> postgres "reads/writes"
            auth -> postgres "reads/writes"
            auth -> redis "session/revocation cache"
            profile -> scylla "reads/writes"
            profile -> redis "L1 entity cache"
            post -> scylla "reads/writes (by id + by author)"
            comment -> scylla "reads/writes (LCS + TWCS)"
            chat -> scylla "message log (bucketed)"
            chat -> redis "sharded pub/sub"
            engagement -> redis "Lua-atomic like totals + counters"
            engagement -> scylla "durable copies (likes, counters)"
            socialGraph -> scylla "4-table relations"
            socialGraph -> redis "hot relation Sets"
            timeline -> redis "feed ZSETs"
            timeline -> scylla "materialized feeds"
            search -> opensearch "index + query"
            geo -> redis "H3 ZSET + cardinality"
            geo -> scylla "map_post_cards"
            counter -> redis "hot counters"
            counter -> postgres "warm SoRef + ledger"
            counter -> scylla "cold TWCS"
            notification -> scylla "TWCS activity feed"
            notification -> redis "write-collapse counters"
            moderation -> postgres "decision/case SoR"
            moderation -> scylla "signal history"
            moderation -> redis "enforcement projection + Screen corpus"
            media -> postgres "asset SoR"
            media -> redis "cache"
            audit -> postgres "append-only ledger"
            realtimeGateway -> redis "connection/presence registry + node-hop pub/sub"
            realtimeDispatcher -> redis "resolve + publish (node-hop)"

            # === Async (Published Language via Kafka) ===========================================
            # producers
            account -> kafka "publishes account.v1.events" "" "Async"
            auth -> kafka "publishes auth.v1.events" "" "Async"
            profile -> kafka "publishes profile.v1.events" "" "Async"
            post -> kafka "publishes post.v1.events" "" "Async"
            comment -> kafka "publishes comment.created/deleted" "" "Async"
            counter -> kafka "publishes counter.v1.popularity" "" "Async"
            moderation -> kafka "publishes moderation.v1.events" "" "Async"
            media -> kafka "publishes media.v1.events" "" "Async"
            chat -> kafka "publishes chat.* (own live plane)" "" "Async"
            notification -> kafka "publishes notification.v1.events" "" "Async"
            socialGraph -> kafka "publishes relation events (follows stream deferred)" "" "Async"
            # consumers
            kafka -> audit "account/auth/moderation .v1.events" "" "Async"
            kafka -> profile "account.v1.events" "" "Async"
            kafka -> post "profile.v1.events, moderation.v1.events" "" "Async"
            kafka -> search "post/profile/moderation .v1.events, counter.v1.popularity" "" "Async"
            kafka -> timeline "post.v1.events" "" "Async"
            kafka -> geo "post.published, profile.tier_changed" "" "Async"
            kafka -> counter "post.v1.events, wallet.v1.events, view/impression/click" "" "Async"
            kafka -> notification "comment.created, wallet.v1.events, post.published" "" "Async"
            kafka -> engagement "comment.created/deleted, wallet.v1.events, account.v1.events" "" "Async"
            kafka -> realtimeDispatcher "notification.v1.events, counter.v1.popularity, post.v1.events" "" "Async"
            kafka -> media "moderation.v1.events (takedown), media.v1.events (transform)" "" "Async"
            kafka -> moderation "post/comment/chat content, moderation.reports/signals" "" "Async"

            # === Sync (gRPC) =====================================================================
            media -> moderation "Screen (sync, fail-closed)" "gRPC" "Sync"
            moderation -> account "suspension/ban execution" "gRPC" "Sync"
            timeline -> socialGraph "follower-set reads (fan-out)" "gRPC" "Sync"
            counter -> socialGraph "follower-count reconciliation" "gRPC" "Sync"
            post -> media "MediaAttachment references" "gRPC" "Sync"
            comment -> post "PostId references" "gRPC" "Sync"
            auth -> account "SubjectLink ↔ AccountId" "gRPC" "Sync"
            realtimeGateway -> auth "edge-token verify at handshake (auth-context)" "in-process verify" "Sync"
            realtimeGateway -> realtimeDispatcher "node-hop delivery (via Redis pub/sub)" "Redis" "Async"

            # === External =======================================================================
            auth -> keycloak "federates credentials (OIDC)"
            media -> objectStore "asset bytes (control plane)"
            media -> cdn "delivery resolution"
            audit -> objectStore "WORM archive (Object Lock)"
            audit -> kms "sign checkpoints + per-subject DEK custody"
            notification -> push "offline device push"
            realtimeDispatcher -> push "delegate offline delivery"

            # === User edges =====================================================================
            user -> realtimeGateway "live updates (WSS :443)"
            user -> auth "login / token refresh"
            user -> objectStore "direct upload (pre-signed URL)"
            user -> cdn "media delivery"
            # Synchronous client read/write API traffic enters the mesh via the platform ingress
            # (see core-platform-infra: docs/infrastructure) to the relevant service; not enumerated here to keep the
            # container view readable.
        }

        # --- Subdomain note (from CONTEXT_MAP.md) -------------------------------------------------
        # Core = post, comment, chat, social-graph, engagement, moderation.
        # Supporting = account, auth, profile, audit, media, notification, realtime, search,
        #              geo-discovery, counter, timeline.
    }

    views {
        systemContext backend "SystemContext" "The platform and the external systems it depends on." {
            include *
            autolayout lr
        }

        container backend "Containers" "Services, datastores, and the Kafka backbone." {
            include *
            autolayout lr
        }

        dynamic backend "PostPublishFanOut" "What happens when a post is published." {
            post -> kafka "publishes post.v1.events"
            kafka -> timeline "consumes → fan-out to follower feeds"
            kafka -> search "consumes → index the post"
            kafka -> geo "consumes → add map card"
            kafka -> counter "consumes → count"
            kafka -> realtimeDispatcher "consumes → broadcast to live viewers"
            autolayout lr
        }

        styles {
            element "User" {
                shape Person
                background #2c3e50
                color #ffffff
            }
            element "External" {
                background #95a5a6
                color #ffffff
            }
            # shape from role, colour from subdomain class (Core vs Supporting)
            element "Service" {
                shape RoundedBox
                color #ffffff
            }
            element "Worker" {
                shape Hexagon
                color #ffffff
            }
            element "Edge" {
                shape RoundedBox
                color #ffffff
            }
            element "Supporting" {
                background #1168bd
                color #ffffff
            }
            element "Core" {
                background #b8341b
                color #ffffff
            }
            element "Datastore" {
                shape Cylinder
                background #6b4f9e
                color #ffffff
            }
            element "MessageBroker" {
                shape Pipe
                background #d98c00
                color #ffffff
            }
            relationship "Async" {
                dashed true
                color #d98c00
            }
            relationship "Sync" {
                dashed false
                color #2c3e50
            }
        }
    }

    configuration {
        scope softwareSystem
    }
}
