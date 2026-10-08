-- Family supervision floors (#670 part 2): the loosest settings a supervised
-- teen's profiles may have, per account, from account's
-- supervision_limits_set (deleted on supervision_limits_cleared). Audiences:
-- 'followers' | 'mutuals' | 'no_one'; null: no floor.
CREATE TABLE IF NOT EXISTS profile.supervision_floors (
    account_id         uuid PRIMARY KEY,
    private_account    boolean,
    messages           text,
    comments           text,
    hidden_from_search boolean
) WITH compression = {'sstable_compression': 'LZ4Compressor'};
