-- A member's countries (#665): one partition per account. The home country
-- (free; set once, LWT on the static column) and one row per country
-- unlocked with gems. Read whole by every member map query when the member
-- map filter is on.
CREATE TABLE IF NOT EXISTS geo_discovery.country_unlocks (
    account_id   uuid,
    country      text,
    home_country text STATIC,
    price        bigint,
    unlocked_at  timestamp,
    PRIMARY KEY (account_id, country)
);
