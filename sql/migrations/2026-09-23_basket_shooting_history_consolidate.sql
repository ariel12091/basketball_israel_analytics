-- Collapse the Basket shooting layer from five relations to three.
--
-- basket_player_season_shooting and basket_player_season_profiles shared a key
-- (game_year, basket_player_id) and held 1160 rows each: a 1:1 split that a
-- view existed only to undo. They become one table. basket_player_identity_map
-- and basket_player_identity_candidates_v are dropped unbuilt -- the map was
-- empty, and Basket PlayerIds do not match resolved_player_identity_v
-- .source_player_id, so identity tables should be designed when that work
-- starts rather than guessed at now.
--
-- The run/page provenance tables are kept unchanged: they are what made the
-- row-level reconciliation of the 2026-09-22 import possible.
--
-- Existing rows are carried across with INSERT ... SELECT. Nothing is re-parsed
-- and the scrape cache is not consulted.

CREATE TABLE IF NOT EXISTS basketball_test.basket_player_season (
  game_year integer NOT NULL CHECK (game_year BETWEEN 1990 AND 2100),
  basket_player_id integer NOT NULL CHECK (basket_player_id > 0),
  player_name text NOT NULL CHECK (btrim(player_name) <> ''),
  player_name_en text CHECK (player_name_en IS NULL OR btrim(player_name_en) <> ''),

  games integer NOT NULL CHECK (games >= 0),
  minutes numeric,
  points numeric,
  fg2_made integer NOT NULL CHECK (fg2_made >= 0),
  fg2_attempted integer NOT NULL CHECK (fg2_attempted >= fg2_made),
  fg3_made integer NOT NULL CHECK (fg3_made >= 0),
  fg3_attempted integer NOT NULL CHECK (fg3_attempted >= fg3_made),
  ft_made integer NOT NULL CHECK (ft_made >= 0),
  ft_attempted integer NOT NULL CHECK (ft_attempted >= ft_made),

  source_url text NOT NULL,
  source_page integer NOT NULL CHECK (source_page > 0),
  source_row integer NOT NULL CHECK (source_row > 0),
  source_content_md5 text NOT NULL CHECK (source_content_md5 ~ '^[0-9a-f]{32}$'),
  fetched_at timestamptz NOT NULL,

  -- Profile columns are NULL until a run is given --with-profiles: they cost one
  -- extra request per season-player, so a run may legitimately omit them.
  position_name text,
  position_en text,
  height_m numeric CHECK (height_m BETWEEN 0.5 AND 3.0),
  nationality_name text,
  nationality_code text CHECK (
    nationality_code IS NULL OR nationality_code ~ '^[A-Z]{3}$'
  ),
  profile_source_url text,
  profile_source_content_md5 text CHECK (
    profile_source_content_md5 IS NULL
      OR profile_source_content_md5 ~ '^[0-9a-f]{32}$'
  ),
  profile_fetched_at timestamptz,
  profile_cache_hit boolean,

  run_id bigint NOT NULL
    REFERENCES basketball_test.basket_shooting_import_runs(run_id),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),

  PRIMARY KEY (game_year, basket_player_id),
  FOREIGN KEY (run_id, game_year, source_page)
    REFERENCES basketball_test.basket_shooting_source_pages
      (run_id, game_year, page_number),
  -- A fetched profile page must have yielded at least one attribute.
  CONSTRAINT basket_player_season_profile_not_empty CHECK (
    profile_fetched_at IS NULL
      OR position_name IS NOT NULL
      OR height_m IS NOT NULL
      OR nationality_name IS NOT NULL
  ),
  -- The invariant the scraper enforces in R, stated in the schema too: a Hebrew
  -- position always carries its English abbreviation.
  CONSTRAINT basket_player_season_position_translated CHECK (
    position_name IS NULL OR position_en IS NOT NULL
  )
);

CREATE INDEX IF NOT EXISTS basket_player_season_name_idx
  ON basketball_test.basket_player_season (game_year, lower(player_name));

INSERT INTO basketball_test.basket_player_season (
  game_year, basket_player_id, player_name, player_name_en,
  games, minutes, points,
  fg2_made, fg2_attempted, fg3_made, fg3_attempted, ft_made, ft_attempted,
  source_url, source_page, source_row, source_content_md5, fetched_at,
  position_name, position_en, height_m, nationality_name, nationality_code,
  profile_source_url, profile_source_content_md5, profile_fetched_at,
  profile_cache_hit,
  run_id, created_at, updated_at
)
SELECT
  s.game_year, s.basket_player_id, s.player_name, s.player_name_en,
  s.games, s.minutes, s.points,
  s.fg2_made, s.fg2_attempted, s.fg3_made, s.fg3_attempted,
  s.ft_made, s.ft_attempted,
  s.source_url, s.source_page, s.source_row, s.source_content_md5, s.fetched_at,
  p.position_name, p.position_en, p.height_m, p.nationality_name,
  p.nationality_code,
  p.source_url, p.source_content_md5, p.fetched_at, p.cache_hit,
  s.run_id, s.created_at, s.updated_at
FROM basketball_test.basket_player_season_shooting s
LEFT JOIN basketball_test.basket_player_season_profiles p
  USING (game_year, basket_player_id)
ON CONFLICT (game_year, basket_player_id) DO NOTHING;

-- Verify before dropping anything. The apply script wraps this in one
-- transaction, so a mismatch here rolls the whole migration back.
DO $$
DECLARE
  old_shooting bigint;
  old_profiles bigint;
  new_rows bigint;
  new_profiles bigint;
  mismatches bigint;
BEGIN
  SELECT count(*) INTO old_shooting
    FROM basketball_test.basket_player_season_shooting;
  SELECT count(*) INTO old_profiles
    FROM basketball_test.basket_player_season_profiles;
  SELECT count(*), count(profile_fetched_at) INTO new_rows, new_profiles
    FROM basketball_test.basket_player_season;

  IF new_rows <> old_shooting THEN
    RAISE EXCEPTION 'row count mismatch: % consolidated vs % shooting',
      new_rows, old_shooting;
  END IF;
  IF new_profiles <> old_profiles THEN
    RAISE EXCEPTION 'profile count mismatch: % consolidated vs % profiles',
      new_profiles, old_profiles;
  END IF;

  -- Value-by-value, not just counts.
  SELECT count(*) INTO mismatches
  FROM basketball_test.basket_player_season n
  JOIN basketball_test.basket_player_season_shooting s
    USING (game_year, basket_player_id)
  LEFT JOIN basketball_test.basket_player_season_profiles p
    USING (game_year, basket_player_id)
  WHERE n.player_name IS DISTINCT FROM s.player_name
     OR n.player_name_en IS DISTINCT FROM s.player_name_en
     OR n.games IS DISTINCT FROM s.games
     OR n.minutes IS DISTINCT FROM s.minutes
     OR n.points IS DISTINCT FROM s.points
     OR n.fg2_made IS DISTINCT FROM s.fg2_made
     OR n.fg2_attempted IS DISTINCT FROM s.fg2_attempted
     OR n.fg3_made IS DISTINCT FROM s.fg3_made
     OR n.fg3_attempted IS DISTINCT FROM s.fg3_attempted
     OR n.ft_made IS DISTINCT FROM s.ft_made
     OR n.ft_attempted IS DISTINCT FROM s.ft_attempted
     OR n.source_content_md5 IS DISTINCT FROM s.source_content_md5
     OR n.position_name IS DISTINCT FROM p.position_name
     OR n.position_en IS DISTINCT FROM p.position_en
     OR n.height_m IS DISTINCT FROM p.height_m
     OR n.nationality_code IS DISTINCT FROM p.nationality_code;

  IF mismatches > 0 THEN
    RAISE EXCEPTION '% consolidated row(s) differ from their source rows',
      mismatches;
  END IF;

  RAISE NOTICE 'consolidated % rows (% with profiles), all values match',
    new_rows, new_profiles;
END
$$;

DROP VIEW IF EXISTS basketball_test.basket_player_season_shooting_resolved_v;
DROP VIEW IF EXISTS basketball_test.basket_player_identity_candidates_v;
DROP TABLE IF EXISTS basketball_test.basket_player_identity_map;
DROP TABLE IF EXISTS basketball_test.basket_player_season_profiles;
DROP TABLE IF EXISTS basketball_test.basket_player_season_shooting;

REVOKE ALL ON TABLE basketball_test.basket_player_season FROM PUBLIC;
