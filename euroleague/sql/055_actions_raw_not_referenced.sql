-- EuroLeague shadow schema: migration 055 -- nothing references actions_raw.
--
-- actions_raw holds the provider's raw PBP events. No function, view or app
-- reader uses it; load_games.py --verify proves, per game, that canonical
-- `actions` reproduces every event and all 22 package fields of raw_event
-- exactly. It is ~15% of the schema, so after a game verifies the loader now
-- deletes that game's raw rows (the provider data stays re-fetchable through
-- the euroleague_api package).
--
-- Two foreign keys pointed INTO actions_raw and blocked that:
--
--   * actions -> actions_raw was ON DELETE CASCADE. Deleting a raw row would
--     have silently deleted the canonical action every statistic is built
--     from. It is dropped, not re-pointed: actions is the canonical table.
--   * qa_incidents -> actions_raw is re-pointed at actions, which carries the
--     same (game_id, source_event_order) primary key, so an incident still
--     has to name a real event. qa_incidents is written after actions and
--     deleted before it (transaction_writer INSERT_ORDER / DELETE_ORDER), so
--     no ON DELETE action is needed.
--
-- Applied by scripts/apply_055_actions_raw_not_referenced.py.

ALTER TABLE euroleague.actions
  DROP CONSTRAINT actions_game_id_source_event_order_fkey;

ALTER TABLE euroleague.qa_incidents
  DROP CONSTRAINT qa_incidents_game_id_source_event_order_fkey;

ALTER TABLE euroleague.qa_incidents
  ADD CONSTRAINT qa_incidents_game_id_source_event_order_fkey
  FOREIGN KEY (game_id, source_event_order)
  REFERENCES euroleague.actions (game_id, source_event_order);
