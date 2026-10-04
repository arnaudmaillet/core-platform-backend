-- Hidden words and the offensive-comment filter (#660), as one JSON column.
-- NULL ⇒ the defaults (no hidden words, offensive filter on).
ALTER TABLE profile.profiles ADD comment_filters text;
