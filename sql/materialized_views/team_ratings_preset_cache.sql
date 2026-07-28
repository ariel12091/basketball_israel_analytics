-- Persisted team-rating results for the fixed Team Hub / Compare presets.
-- The table is rebuilt by season through
-- refresh_team_ratings_preset_cache_for_games(int4[]) during the normal ETL.

DROP TABLE IF EXISTS basketball_test.team_ratings_preset_cache;

CREATE TABLE basketball_test.team_ratings_preset_cache (
  preset_variant text NOT NULL CHECK (
    preset_variant IN (
      'starters_hi',
      'starters_lo',
      'clutch',
      'last10',
      'top4',
      'bottom4'
    )
  ),
  game_year int NOT NULL,
  team_id int NOT NULL,
  team_name text,
  off_ppp numeric,
  def_ppp numeric,
  net_rtg numeric,
  games_played int,
  wins int,
  losses int,
  off_poss int,
  def_poss int,
  rank_net_rtg bigint,
  rank_off_ppp bigint,
  rank_def_ppp bigint,
  off_fga int,
  off_layup_att int,
  off_dunk_att int,
  off_fg3_att int,
  off_c3_att int,
  off_c3_known_att int,
  def_fga int,
  def_layup_att int,
  def_dunk_att int,
  def_fg3_att int,
  def_c3_att int,
  def_c3_known_att int,
  refreshed_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT team_ratings_preset_cache_pkey
    PRIMARY KEY (game_year, preset_variant, team_id)
);

WITH seasons AS (
  SELECT DISTINCT s.game_year
  FROM basketball_test.schedule s
  WHERE s.game_year IS NOT NULL
)
INSERT INTO basketball_test.team_ratings_preset_cache (
  preset_variant,
  game_year,
  team_id,
  team_name,
  off_ppp,
  def_ppp,
  net_rtg,
  games_played,
  wins,
  losses,
  off_poss,
  def_poss,
  rank_net_rtg,
  rank_off_ppp,
  rank_def_ppp,
  off_fga,
  off_layup_att,
  off_dunk_att,
  off_fg3_att,
  off_c3_att,
  off_c3_known_att,
  def_fga,
  def_layup_att,
  def_dunk_att,
  def_fg3_att,
  def_c3_att,
  def_c3_known_att
)
SELECT preset_rows.*
FROM seasons s
CROSS JOIN LATERAL (
  SELECT 'starters_hi'::text AS preset_variant, r.*
  FROM basketball_test.get_team_ratings_dynamic(
    s.game_year,
    p_num_starters_off_min := 3,
    p_num_starters_off_max := 5
  ) r

  UNION ALL

  SELECT 'starters_lo'::text AS preset_variant, r.*
  FROM basketball_test.get_team_ratings_dynamic(
    s.game_year,
    p_num_starters_off_min := 0,
    p_num_starters_off_max := 2
  ) r

  UNION ALL

  SELECT 'clutch'::text AS preset_variant, r.*
  FROM basketball_test.get_team_ratings_dynamic(
    s.game_year,
    p_max_margin := 5,
    p_max_time_remaining := 300
  ) r

  UNION ALL

  SELECT 'last10'::text AS preset_variant, r.*
  FROM basketball_test.get_team_ratings_dynamic(
    s.game_year,
    p_last_n_games := 10
  ) r

  UNION ALL

  SELECT 'top4'::text AS preset_variant, r.*
  FROM basketball_test.get_team_ratings_dynamic(
    s.game_year,
    p_opp_rank_side := 'top',
    p_opp_rank_n := 4,
    p_opp_rank_metric := 'net'
  ) r

  UNION ALL

  SELECT 'bottom4'::text AS preset_variant, r.*
  FROM basketball_test.get_team_ratings_dynamic(
    s.game_year,
    p_opp_rank_side := 'bottom',
    p_opp_rank_n := 4,
    p_opp_rank_metric := 'net'
  ) r
) preset_rows;

CREATE INDEX team_ratings_preset_cache_rank_idx
  ON basketball_test.team_ratings_preset_cache
  (game_year, preset_variant, rank_net_rtg);

ALTER TABLE basketball_test.team_ratings_preset_cache ENABLE ROW LEVEL SECURITY;

CREATE POLICY app_readonly_select_all
  ON basketball_test.team_ratings_preset_cache
  FOR SELECT
  TO app_readonly
  USING (true);

GRANT SELECT ON basketball_test.team_ratings_preset_cache TO app_readonly;
