-- 065_team_identity.sql
--
-- EuroLeague shadow schema. Each club carries its crest and club colours
-- (2026-11-17; plan docs/superpowers/plans/2026-11-17-gameflow-phone-focus-layout.md,
-- Task 7). The Israeli twin is basketball_test.season_teams (logo_url,
-- primary_color, secondary_color); docs/shared_definitions.md rows "Team logo"
-- and "Team colours".
--
--   crest_url        the provider's crest, images.crest from
--                    https://api-live.euroleague.net/v2/competitions/{E|U}/seasons/{E|U}{season}/clubs
--                    matched on code = provider_team_code, latest season wins.
--                    Filled by scripts/apply_065_team_identity.py (and on each
--                    later run, only where NULL: a hand-corrected URL stays).
--   primary_color,
--   secondary_color  entered by hand ('#RRGGBB', upper case); NULL until then.
--
-- euroleague.teams has one row per competition and club code, no season, so a
-- club that changes crest or colours keeps one value for every season.
-- ALTER TABLE keeps the table's grants.

ALTER TABLE euroleague.teams
  ADD COLUMN IF NOT EXISTS crest_url text,
  ADD COLUMN IF NOT EXISTS primary_color text
    CONSTRAINT teams_primary_color_hex CHECK (primary_color ~ '^#[0-9A-F]{6}$'),
  ADD COLUMN IF NOT EXISTS secondary_color text
    CONSTRAINT teams_secondary_color_hex CHECK (secondary_color ~ '^#[0-9A-F]{6}$');

COMMENT ON COLUMN euroleague.teams.crest_url IS
  'Provider crest URL (clubs endpoint images.crest, latest season). Filled by scripts/apply_065_team_identity.py where NULL.';
COMMENT ON COLUMN euroleague.teams.primary_color IS
  'Club colour, #RRGGBB upper case, entered by hand. Gameflow draws the team in it.';
COMMENT ON COLUMN euroleague.teams.secondary_color IS
  'Second club colour, #RRGGBB upper case, entered by hand. Used when two clubs'' primaries are too close.';
