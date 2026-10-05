-- A handle released by a verified profile (deleted or renamed) is held for
-- good: never claimable again, so nobody can take over a verified identity's
-- old name. NULL = an ordinary tombstone (claimable after the reservation).
ALTER TABLE profile.profile_handles ADD held boolean;
