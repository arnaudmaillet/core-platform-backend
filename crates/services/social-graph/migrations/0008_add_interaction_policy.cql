-- Who may comment / mention / message each profile (#656), projected from
-- profile's ProfileInteractionSettingsChanged, as one JSON text column:
-- {"comments": "...", "mentions": "...", "messages": "..."} with audiences
-- everyone | followers | mutuals | no_one. NULL (no row or no column value) ⇒
-- everyone. Read by CheckInteraction.
ALTER TABLE social_graph.profile_audience ADD interaction text;
