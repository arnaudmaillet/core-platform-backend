-- #910: 24 h country visas. A visa is `visa_until` on the country's row of
-- `country_unlocks` (written with a TTL that ends with it, so the row is read
-- in the same single-partition query as the unlocks); a row with neither
-- `pending` nor an unlock is a visa only. Unlocking the country for good
-- clears it.
ALTER TABLE geo_discovery.country_unlocks ADD visa_until timestamp;

-- A visa purchase per client key: what it agreed (price, the visa's new end)
-- before the gems were spent, so a retry pays once and grants the same end.
-- Kept 7 days.
CREATE TABLE IF NOT EXISTS geo_discovery.country_visa_purchases (
    account_id   uuid,
    purchase_key text,
    country      text,
    price        bigint,
    until        timestamp,
    settled      boolean,
    PRIMARY KEY (account_id, purchase_key)
) WITH default_time_to_live = 604800;
