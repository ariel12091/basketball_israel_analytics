-- season_teams: the catalogue of Israeli league teams per season, keyed on the
-- PBP/roster team id every app-facing fact uses. Team ids are recycled between
-- seasons, so the key is (game_year, team_id).
--
-- Until now the display name of a team had no single home: full_rosters,
-- schedule_team_dict and final_schedule_mv each carry their own spelling, and
-- the names the dropdowns show for 2026 and 2027 were typed into
-- STATIC_TEAM_ROSTERS (app/R/global.R). This table holds exactly the names the
-- dropdowns show and is the source of truth for them.
--
-- STATIC_TEAM_ROSTERS stays as a pinned copy, because the Home page is built
-- before any database connection exists; app/tests/testthat/test-season-teams-db.R
-- fails when the two differ. Seasons with no pinned copy (2025) are served
-- from full_rosters, and the same test holds this table to that.
--
-- A new season needs its rows here AND in STATIC_TEAM_ROSTERS.
--
-- After applying, re-run scripts/apply_db_security.R so app_readonly gets its
-- SELECT grant and RLS policy on the new table.

CREATE TABLE IF NOT EXISTS basketball_test.season_teams (
  game_year int  NOT NULL,
  team_id   int  NOT NULL,
  team_name text NOT NULL,
  PRIMARY KEY (game_year, team_id)
);

COMMENT ON TABLE basketball_test.season_teams IS
  'Catalogue of Israeli league teams per season with the display name the app dropdowns use. Source of truth for STATIC_TEAM_ROSTERS in app/R/global.R.';
COMMENT ON COLUMN basketball_test.season_teams.team_id IS
  'PBP/roster team id (full_rosters.team_id), not the schedule-feed id. Recycled between seasons.';

INSERT INTO basketball_test.season_teams (game_year, team_id, team_name)
VALUES
  (2025,   2, 'MACCABI TEL AVIV'),
  (2025,   3, 'HAPOEL TEL AVIV'),
  (2025,   4, 'HAPOEL JERUSALEM'),
  (2025,   7, 'HAPOEL HOLON'),
  (2025,   8, 'ELITZUR NETANYA'),
  (2025,   9, 'BNEI HERZLIYA'),
  (2025,  11, 'GILBOA/GALIL'),
  (2025,  13, 'NESS ZIONA'),
  (2025,  24, 'KIRYAT ATA'),
  (2025,  28, 'HAPOEL AFULA'),
  (2025,  30, 'MACCABI RAMAT GAN'),
  (2025,  44, 'HAPOEL GALIL ELION'),
  (2025, 173, 'BEER SHEVA'),
  (2025, 246, 'HAPOEL HAIFA'),
  (2026,   2, 'MACCABI TEL AVIV'),
  (2026,   3, 'HAPOEL TEL AVIV'),
  (2026,   4, 'HAPOEL JERUSALEM'),
  (2026,   5, 'HAPOEL HOLON'),
  (2026,   6, 'BNEI HERZLIYA'),
  (2026,   7, 'MACCABI RAMAT GAN'),
  (2026,   8, 'HAPOEL HAEMEK'),
  (2026,   9, 'NESS ZIONA'),
  (2026,  10, 'GALIL ELION'),
  (2026,  11, 'BEER SHEVA'),
  (2026,  12, 'KIRYAT ATA'),
  (2026,  13, 'MACCABI RAANANA'),
  (2026,  14, 'RISHON LEZION'),
  (2026,  15, 'ELIZUR NETANYA'),
  (2027,   2, 'MACCABI TEL AVIV'),
  (2027,   3, 'HAPOEL TEL AVIV'),
  (2027,   4, 'HAPOEL JERUSALEM'),
  (2027,   5, 'HAPOEL HOLON'),
  (2027,   6, 'BNEI HERZLIYA'),
  (2027,   7, 'MACCABI RAMAT GAN'),
  (2027,   8, 'HAPOEL HAEMEK'),
  (2027,   9, 'NESS ZIONA'),
  (2027,  10, 'HAPOEL GALIL ELION'),
  (2027,  11, 'BEER SHEVA/DIMONA'),
  (2027,  12, 'IRONI KIRYAT ATA'),
  (2027,  14, 'M. RISHON'),
  (2027,  17, 'HAPOEL EILAT'),
  (2027,  33, 'MACCABI ASHDOD')
ON CONFLICT (game_year, team_id) DO UPDATE
SET team_name = EXCLUDED.team_name;
