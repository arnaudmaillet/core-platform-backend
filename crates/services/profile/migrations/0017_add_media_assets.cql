-- The media assets an avatar and a banner show (#907): media.v1 asset ids,
-- the stable keys clients cache them by. NULL for images set by URL only.
ALTER TABLE profile.profiles ADD (avatar_asset_id uuid, banner_asset_id uuid);
ALTER TABLE profile.profiles_by_account ADD avatar_asset_id uuid;
