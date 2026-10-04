-- Who may see each profile's follower / following lists (#659), as one JSON
-- text column: {"followers": "...", "following": "..."} with audiences
-- everyone | followers | mutuals | no_one. NULL ⇒ everyone. Unlike the other
-- columns (projections of profile.v1.events), social-graph owns this one:
-- written by SetListPrivacy, read by ListFollowers / ListFollowing.
ALTER TABLE social_graph.profile_audience ADD lists text;
