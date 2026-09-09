-- The 2026-27 Israeli schedule feed introduced percent-encoded team-logo URLs
-- longer than the historical varchar(130) limit (game 395 is 199 chars).
-- These columns are descriptive source metadata and are not indexed, so text
-- avoids future season rollovers failing on URL length changes.

ALTER TABLE IF EXISTS basketball.schedule
  ALTER COLUMN team_icon_1 TYPE text,
  ALTER COLUMN team_icon_2 TYPE text;

ALTER TABLE IF EXISTS basketball_test.schedule
  ALTER COLUMN team_icon_1 TYPE text,
  ALTER COLUMN team_icon_2 TYPE text;
