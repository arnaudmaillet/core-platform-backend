-- Authors' location sharing (#657), projected from profile.v1.events
-- (ProfileLocationSettingsChanged). No row ⇒ the default (shared precisely).
-- Read in bulk per map query for the authors on screen (IN on the partition key).
CREATE TABLE IF NOT EXISTS geo_discovery.location_settings (
    author_id  uuid,
    ghost      boolean,
    city       boolean,
    PRIMARY KEY (author_id)
);
