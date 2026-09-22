-- English player names and positions for the Basket shooting-history layer.
--
-- Additive: both columns are nullable so rows written before this migration
-- stay valid until the next (cached) backfill run fills them. Requires
-- 2026-09-22_basket_shooting_history.sql.

ALTER TABLE basketball_test.basket_player_season_shooting
  ADD COLUMN IF NOT EXISTS player_name_en text
    CHECK (player_name_en IS NULL OR btrim(player_name_en) <> '');

-- English abbreviation as basket.co.il's English pages print it (G, PG, SF,
-- PF, F, G-F, F-C, C), derived from the Hebrew label by a verified lookup.
ALTER TABLE basketball_test.basket_player_season_profiles
  ADD COLUMN IF NOT EXISTS position_en text;

-- Appending a column is allowed by CREATE OR REPLACE VIEW.
CREATE OR REPLACE VIEW basketball_test.basket_player_identity_candidates_v AS
WITH pbp_candidates AS (
  SELECT DISTINCT
    game_year,
    source_player_id AS basket_player_id,
    identity_id,
    display_name
  FROM basketball_test.resolved_player_identity_v
), summarized AS (
  SELECT
    game_year,
    basket_player_id,
    count(*) AS candidate_count,
    min(identity_id) AS candidate_identity_id,
    min(display_name) AS candidate_display_name
  FROM pbp_candidates
  GROUP BY game_year, basket_player_id
)
SELECT
  s.game_year,
  s.basket_player_id,
  s.player_name AS basket_player_name,
  c.candidate_identity_id,
  c.candidate_display_name,
  COALESCE(c.candidate_count, 0) AS candidate_count,
  CASE
    WHEN c.candidate_count = 1 THEN 'same_season_source_id'
    WHEN c.candidate_count > 1 THEN 'ambiguous_source_id'
    ELSE 'no_source_id_match'
  END AS candidate_method,
  m.identity_id AS verified_identity_id,
  s.player_name_en AS basket_player_name_en
FROM basketball_test.basket_player_season_shooting s
LEFT JOIN summarized c
  USING (game_year, basket_player_id)
LEFT JOIN basketball_test.basket_player_identity_map m
  ON m.game_year = s.game_year
 AND m.basket_player_id = s.basket_player_id
 AND m.review_status = 'verified';

-- s.* now expands to include player_name_en in the middle of the column list,
-- which CREATE OR REPLACE rejects, so this view is dropped and recreated.
-- Nothing depends on it yet.
DROP VIEW IF EXISTS basketball_test.basket_player_season_shooting_resolved_v;

CREATE VIEW basketball_test.basket_player_season_shooting_resolved_v AS
SELECT
  s.*,
  p.position_name,
  p.position_en,
  p.height_m,
  p.nationality_name,
  p.nationality_code,
  m.identity_id,
  i.display_name AS identity_display_name,
  (m.identity_id IS NOT NULL) AS identity_verified
FROM basketball_test.basket_player_season_shooting s
LEFT JOIN basketball_test.basket_player_season_profiles p
  USING (game_year, basket_player_id)
LEFT JOIN basketball_test.basket_player_identity_map m
  ON m.game_year = s.game_year
 AND m.basket_player_id = s.basket_player_id
 AND m.review_status = 'verified'
LEFT JOIN basketball_test.player_identities i
  ON i.identity_id = m.identity_id;

REVOKE ALL ON TABLE basketball_test.basket_player_identity_candidates_v FROM PUBLIC;
REVOKE ALL ON TABLE basketball_test.basket_player_season_shooting_resolved_v FROM PUBLIC;
