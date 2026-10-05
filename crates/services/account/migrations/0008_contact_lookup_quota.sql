-- Contact matching's daily budget (#661): how many contact hashes each
-- account looked up per UTC day. Unsalted SHA-256 of phone numbers hides
-- nothing (a numbering plan hashes in minutes), so without a bound one
-- account could map every findable number to its profile. Every submitted
-- hash counts, matched or not, reserved before matching.
CREATE TABLE IF NOT EXISTS contact_lookup_quota (
    account_id UUID    NOT NULL,
    day        DATE    NOT NULL,
    used       INTEGER NOT NULL,
    PRIMARY KEY (account_id, day)
);
