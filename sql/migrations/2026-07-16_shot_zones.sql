-- shot_zones: persistent per-shot zone flags derived from actions_clean
-- coordinates BEFORE cold-storage purge. actions_clean is truncated after
-- each ETL run, so this table is the only live home for coordinate-derived
-- facts. Rows: 3PT shots with coordinates only (~23k for 2025+2026, ~1-2 MB).
-- Corner rule = committed v2 cut (scripts/analysis/fiba_court_zones.R):
-- corner3_height 2.85 court units = 285 raw provider units.
-- NEVER add this table to etl/cold_storage.R COLD_TABLES.

CREATE TABLE IF NOT EXISTS basketball_test.shot_zones (
  game_id    int  NOT NULL,
  id         int  NOT NULL,
  is_corner3 bool NOT NULL,
  PRIMARY KEY (game_id, id)
);

COMMENT ON TABLE basketball_test.shot_zones IS
  '3PT corner flags from actions_clean coords (y<=285 raw). Populated in ETL Phase 2 + one-time backfill (scripts/backfill_shot_zones.R). Persistent — not cold storage.';
