-- season_teams carries each team's logo and club colours (2026-11-17).
--
-- Plan: docs/superpowers/plans/2026-11-17-gameflow-phone-focus-layout.md,
-- Task 6. The web app reads them with the team names; Gameflow draws each
-- side in its club's colours (a club is never drawn in another club's colour).
--
--   logo_url        filled from the schedule feed by scripts/team_identity.R
--                   (fill_season_team_logos), which apply_catalogue_migrations.R
--                   and the nightly scripts/sync_catalogues.R call; never
--                   overwrites a value corrected by hand.
--   primary_color,
--   secondary_color entered by hand ('#RRGGBB', upper case); NULL until then,
--                   and the app falls back to its own two colours.
--
-- Idempotent. ALTER TABLE keeps the table's grants; re-run
-- scripts/apply_db_security.R afterwards anyway, as for every catalogue change.

ALTER TABLE basketball_test.season_teams
  ADD COLUMN IF NOT EXISTS logo_url text,
  ADD COLUMN IF NOT EXISTS primary_color text
    CONSTRAINT season_teams_primary_color_hex CHECK (primary_color ~ '^#[0-9A-F]{6}$'),
  ADD COLUMN IF NOT EXISTS secondary_color text
    CONSTRAINT season_teams_secondary_color_hex CHECK (secondary_color ~ '^#[0-9A-F]{6}$');

COMMENT ON COLUMN basketball_test.season_teams.logo_url IS
  'Absolute basket.co.il URL of the team''s logo that season (schedule feed, latest game). Filled by scripts/team_identity.R; a non-NULL value is never overwritten.';
COMMENT ON COLUMN basketball_test.season_teams.primary_color IS
  'Club colour, #RRGGBB upper case, entered by hand. Gameflow draws the team in it.';
COMMENT ON COLUMN basketball_test.season_teams.secondary_color IS
  'Second club colour, #RRGGBB upper case, entered by hand. Used when two clubs'' primaries are too close.';
