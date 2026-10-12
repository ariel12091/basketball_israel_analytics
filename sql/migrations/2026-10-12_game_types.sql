-- game_types: the catalogue of provider game_type codes carried by schedule
-- and final_schedule_mv. Until now the meaning of each code existed only in
-- the Shiny constants GAME_TYPE_LABELS / GAME_TYPE_CHOICES_UI (app/R/global.R).
--
-- This table is the source of truth. The Shiny constants stay as a pinned
-- copy, because the filter dropdowns are built into the page before any
-- database connection exists; app/tests/testthat/test-game-types-db.R fails
-- when the two differ. Change a label here AND in global.R.
--
-- Codes that share a label are one choice in the UI: the provider sends the
-- Winner Cup under two codes, 34 (one game in each of 2026 and 2027) and 10
-- (25 games in 2027, as of 2026-10-12).
--
-- After applying, re-run scripts/apply_db_security.R so app_readonly gets its
-- SELECT grant and RLS policy on the new table.

CREATE TABLE IF NOT EXISTS basketball_test.game_types (
  game_type   int  PRIMARY KEY,
  label       text NOT NULL,
  short_label text NOT NULL,
  sort_order  int  NOT NULL UNIQUE
);

COMMENT ON TABLE basketball_test.game_types IS
  'Catalogue of provider game_type codes (schedule.game_type). Source of truth for the labels pinned in app/R/global.R.';
COMMENT ON COLUMN basketball_test.game_types.label IS
  'Filter dropdown label. Codes sharing a label are offered as one choice.';
COMMENT ON COLUMN basketball_test.game_types.short_label IS
  'Compact label for filter chips and the Game Logs table.';
COMMENT ON COLUMN basketball_test.game_types.sort_order IS
  'Dropdown order.';

INSERT INTO basketball_test.game_types (game_type, label, short_label, sort_order)
VALUES
  (5,  'Regular season',           'Regular season', 1),
  (16, 'Playoffs - Quarterfinals', 'PO QF',          2),
  (17, 'Playoffs - Finals',        'PO Finals',      3),
  (26, 'Playoffs - Semifinals',    'PO SF',          4),
  (33, 'Play-in',                  'Play-in',        5),
  (10, 'Winner Cup',               'Winner Cup',     6),
  (34, 'Winner Cup',               'Winner Cup',     7),
  (35, 'State Cup',                'State Cup',      8)
ON CONFLICT (game_type) DO UPDATE
SET label = EXCLUDED.label,
    short_label = EXCLUDED.short_label,
    sort_order = EXCLUDED.sort_order;
