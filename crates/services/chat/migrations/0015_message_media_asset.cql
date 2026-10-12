-- The media asset a MEDIA message shows (#919 step 3): media.v1's asset id,
-- delivered SIGNED to the conversation's readers. NULL for messages with a
-- bare media_ref (sent before, or without one).
ALTER TABLE chat.messages_by_conversation ADD media_asset_id uuid;
