-- Likes are points (#665): the weighted reactions (heart, fire, rocket, clap,
-- sad) are gone, and with them their tables. The likes live in
-- likes_by_target / likes_by_account (0005); the GDPR export reads those.
DROP TABLE IF EXISTS engagement.post_reactions;

DROP TABLE IF EXISTS engagement.reactions_by_profile;
