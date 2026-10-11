# Tiny seed runner: grpcurl + psql + jq + bash. Runs in the compose network
# (it reaches account, profile, social-graph, post and auth's Postgres).
FROM alpine:3.20
RUN apk add --no-cache bash curl jq postgresql16-client
# grpcurl static binary from the official image.
COPY --from=fullstorydev/grpcurl:v1.9.1 /bin/grpcurl /usr/local/bin/grpcurl
COPY seed.sh /usr/local/bin/seed.sh
RUN chmod +x /usr/local/bin/seed.sh
ENTRYPOINT ["/usr/local/bin/seed.sh"]
