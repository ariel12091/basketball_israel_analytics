-- 067_player_season_profiles.sql
--
-- EuroLeague shadow schema. Each player's height, position and headshot for a
-- season and club (2026-11-19). The Israeli twin is
-- basketball_test.basket_player_registration (height_m, position_en), read
-- through player_display_names_v; docs/shared_definitions.md rows "Player
-- height and position" and "Player headshot".
--
-- Source: the provider's season roster,
--   https://api-live.euroleague.net/v2/competitions/{E|U}/seasons/{E|U}{season}/people?personType=J
-- one row per player and club (a player who changes club mid-season has one
-- row for each). Filled by euroleague_possessions.roster_collector, run by
-- scripts/apply_067_player_profiles.py (backfill) and
-- scripts/refresh_player_profiles.py (after each load).
--
--   height_m      person.height (cm) / 100, so both leagues carry metres.
--   position_en   positionName mapped to basket.co.il's codes: Guard -> G,
--                 Forward -> F, Center -> C. The provider has no finer
--                 positions (no PG, G-F, F-C).
--   position_name the provider's positionName as sent.
--   headshot_url  the roster row's images.headshot (that club and season),
--                 else person.images.headshot.
--
-- A player the roster lists who has no row in euroleague.players (has not
-- played in a loaded game) gets no row; the next refresh adds them.
-- A refresh never blanks a stored value the provider stops sending.

CREATE TABLE IF NOT EXISTS euroleague.player_season_profiles (
  season smallint NOT NULL CHECK (season BETWEEN 2000 AND 2100),
  player_id bigint NOT NULL REFERENCES euroleague.players(player_id),
  team_id bigint NOT NULL REFERENCES euroleague.teams(team_id),
  height_m numeric(3,2) CHECK (height_m BETWEEN 1.50 AND 2.40),
  position_en text CHECK (position_en IN ('G', 'F', 'C')),
  position_name text,
  headshot_url text CHECK (headshot_url LIKE 'https://%'),
  fetched_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (season, team_id, player_id)
);

CREATE INDEX IF NOT EXISTS euroleague_player_season_profiles_player_idx
  ON euroleague.player_season_profiles (player_id, season);

COMMENT ON TABLE euroleague.player_season_profiles IS
  'Height, position and headshot per season and club, from the provider season roster (people endpoint). Migration 067.';

GRANT SELECT ON euroleague.player_season_profiles TO app_readonly;
ALTER TABLE euroleague.player_season_profiles ENABLE ROW LEVEL SECURITY;
CREATE POLICY app_readonly_select_all ON euroleague.player_season_profiles
  FOR SELECT TO app_readonly USING (true);
