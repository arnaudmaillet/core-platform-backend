-- Whether a profile may appear in others' suggestions (#661), projected from
-- profile's ProfileDiscoverySettingsChanged (in_suggestions). NULL (no event:
-- an adult's default) = suggestible; a teen's profile is announced
-- unsuggestible at creation, and profile keeps it so until 18.
ALTER TABLE social_graph.profile_audience ADD suggestible boolean;
