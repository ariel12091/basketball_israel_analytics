-- basket.co.il player registrations: one row per player-season with the
-- player's facts -- names, team, position, height, date of birth,
-- nationality -- and the link to a play-by-play identity.
--
-- Why it is split from basket_player_season again: on 2026-09-23 the shooting
-- and profile tables were merged because they were 1:1 on this key. They are
-- not any more -- basket.co.il's team pages (team.asp) list registered players
-- the season-stat pages do not (2026-11-12: 48 of 2026-27's 227), and those
-- players have facts but no shooting totals. basket_player_season keeps the
-- totals; this table keeps everything else.
--
-- One source per row:
--   source = 'stats'  a player on the season-stat pages; names and team from
--                     the stats row, position/height/nationality/date of birth
--                     from his profile page (scripts/backfill_basket_shooting_history.R)
--   source = 'roster' a player listed only on a team page
--                     (scripts/basket_rosters.R)
-- A stats row always wins: the roster writer never updates one, and the stats
-- writer turns a roster row into a stats row. Neither writer touches the
-- identity columns; etl/basket_identity_match.R owns them.
--
-- Filled here by INSERT ... SELECT from basket_player_season (every row) and
-- basket_player_roster (rows with no stats row). Nothing is dropped:
-- the moved columns leave basket_player_season in a later, separate migration.

CREATE TABLE basketball_test.basket_player_registration (
  game_year integer NOT NULL CHECK (game_year BETWEEN 1990 AND 2100),
  basket_player_id integer NOT NULL CHECK (basket_player_id > 0),
  source text NOT NULL CHECK (source IN ('stats', 'roster')),
  player_name text NOT NULL CHECK (btrim(player_name) <> ''),
  player_name_en text CHECK (player_name_en IS NULL OR btrim(player_name_en) <> ''),
  basket_team_name text CHECK (basket_team_name IS NULL OR btrim(basket_team_name) <> ''),
  -- Team page id and jersey: roster rows only.
  basket_team_id integer CHECK (basket_team_id IS NULL OR basket_team_id > 0),
  jersey text,
  position_name text,
  position_en text,
  height_m numeric CHECK (height_m IS NULL OR height_m BETWEEN 0.5 AND 3.0),
  date_of_birth date CHECK (date_of_birth IS NULL OR date_of_birth BETWEEN DATE '1940-01-01' AND CURRENT_DATE),
  nationality_name text,
  nationality_code text CHECK (nationality_code IS NULL OR nationality_code ~ '^[A-Z]{3}$'),
  -- The row's source page: the stats page for stats rows, the team page for
  -- roster rows.
  source_url text NOT NULL,
  source_content_md5 text NOT NULL CHECK (source_content_md5 ~ '^[0-9a-f]{32}$'),
  fetched_at timestamptz NOT NULL,
  run_id bigint REFERENCES basketball_test.basket_shooting_import_runs (run_id),
  profile_source_url text,
  profile_source_content_md5 text CHECK (profile_source_content_md5 IS NULL OR profile_source_content_md5 ~ '^[0-9a-f]{32}$'),
  profile_fetched_at timestamptz,
  profile_cache_hit boolean,
  identity_id bigint,
  canonical_player_id integer,
  identity_match_status text CHECK (identity_match_status IS NULL OR identity_match_status IN ('verified', 'proposal', 'ambiguous', 'unmatched')),
  identity_matched_on text CHECK (identity_matched_on IS NULL OR identity_matched_on IN ('name_en', 'name_he', 'name_both', 'surname_team', 'manual')),
  identity_matched_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (game_year, basket_player_id),
  CONSTRAINT basket_player_registration_stats_run CHECK (source <> 'stats' OR run_id IS NOT NULL),
  CONSTRAINT basket_player_registration_position_translated CHECK (position_name IS NULL OR position_en IS NOT NULL),
  CONSTRAINT basket_player_registration_profile_not_empty CHECK (
    profile_fetched_at IS NULL OR position_name IS NOT NULL OR height_m IS NOT NULL OR nationality_name IS NOT NULL),
  CONSTRAINT basket_player_registration_identity_consistent CHECK (
    (identity_match_status IN ('verified', 'proposal') AND identity_id IS NOT NULL
       AND canonical_player_id IS NOT NULL AND identity_matched_on IS NOT NULL)
    OR (identity_match_status IN ('ambiguous', 'unmatched') AND identity_id IS NULL AND canonical_player_id IS NULL)
    OR (identity_match_status IS NULL AND identity_id IS NULL AND canonical_player_id IS NULL)
  )
);

INSERT INTO basketball_test.basket_player_registration (
  game_year, basket_player_id, source, player_name, player_name_en, basket_team_name,
  position_name, position_en, height_m, date_of_birth, nationality_name, nationality_code,
  source_url, source_content_md5, fetched_at, run_id,
  profile_source_url, profile_source_content_md5, profile_fetched_at, profile_cache_hit,
  identity_id, canonical_player_id, identity_match_status, identity_matched_on, identity_matched_at,
  created_at, updated_at)
SELECT game_year, basket_player_id, 'stats', player_name, player_name_en, basket_team_name,
  position_name, position_en, height_m, date_of_birth, nationality_name, nationality_code,
  source_url, source_content_md5, fetched_at, run_id,
  profile_source_url, profile_source_content_md5, profile_fetched_at, profile_cache_hit,
  identity_id, canonical_player_id, identity_match_status, identity_matched_on, identity_matched_at,
  created_at, updated_at
FROM basketball_test.basket_player_season;

INSERT INTO basketball_test.basket_player_registration (
  game_year, basket_player_id, source, player_name, player_name_en, basket_team_name,
  basket_team_id, jersey, position_name, position_en, height_m, date_of_birth,
  source_url, source_content_md5, fetched_at,
  identity_id, canonical_player_id, identity_match_status, identity_matched_on, identity_matched_at,
  created_at, updated_at)
SELECT r.game_year, r.basket_player_id, 'roster', r.player_name, r.player_name_en, r.basket_team_name,
  r.basket_team_id, r.jersey, r.position_name, r.position_en, r.height_m, r.date_of_birth,
  r.source_url, r.source_content_md5, r.fetched_at,
  r.identity_id, r.canonical_player_id, r.identity_match_status, r.identity_matched_on, r.identity_matched_at,
  r.created_at, r.updated_at
FROM basketball_test.basket_player_roster r
WHERE NOT EXISTS (
  SELECT 1 FROM basketball_test.basket_player_season s
  WHERE s.game_year = r.game_year AND s.basket_player_id = r.basket_player_id);

CREATE INDEX basket_player_registration_name_idx
  ON basketball_test.basket_player_registration (game_year, lower(player_name));
CREATE INDEX basket_player_registration_dob_idx
  ON basketball_test.basket_player_registration (date_of_birth) WHERE date_of_birth IS NOT NULL;
CREATE INDEX basket_player_registration_identity_idx
  ON basketball_test.basket_player_registration (identity_id, game_year) WHERE identity_match_status = 'verified';
CREATE INDEX basket_player_registration_season_canonical_idx
  ON basketball_test.basket_player_registration (game_year, canonical_player_id) WHERE identity_match_status = 'verified';

COMMENT ON TABLE basketball_test.basket_player_registration IS
  'basket.co.il registrations, one row per player-season: names, team, position, height, date of birth, nationality, identity link. source = stats (season-stat pages + profiles) or roster (team page only); a stats row wins. Shooting totals are in basket_player_season (2026-11-12).';

REVOKE ALL ON TABLE basketball_test.basket_player_registration FROM PUBLIC;
