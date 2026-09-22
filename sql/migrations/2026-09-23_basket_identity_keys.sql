-- Correct the access path recorded by
-- 2026-09-23_basket_player_season_identity.sql, whose index comment claimed
-- canonical_player_id identifies "one segev player". It does not, across
-- seasons: canonical ids are recycled. 18 of 712 canonical ids in
-- resolved_player_identity_v belong to two different people in different
-- seasons -- 1119 is AMIT GERSHON in 2025 and MICHAEL FOSTER JR. in 2026,
-- 1027 is ANTONIO BLAKENEY and ROMAN SORKIN, 1091 TAMIR BLATT and
-- YONATAN MALUL.
--
-- The two keys mean different things and both are needed:
--
--   (game_year, canonical_player_id)  join to play-by-play facts IN a season
--   identity_id                       the person, across seasons
--
-- Verified by date of birth, which basket.co.il supplies and the play-by-play
-- side does not: grouped by identity_id, zero Basket rows disagree on it;
-- grouped by canonical_player_id, all ten multi-row groups disagree.

-- The person, across seasons. This is the index a "career" question needs.
CREATE INDEX IF NOT EXISTS basket_player_season_identity_idx
  ON basketball_test.basket_player_season (identity_id, game_year)
  WHERE identity_match_status = 'verified';

-- Kept, but for what it can actually answer: a within-season join to
-- play-by-play facts. The leading game_year says the season is not optional.
DROP INDEX IF EXISTS basketball_test.basket_player_season_canonical_idx;
CREATE INDEX IF NOT EXISTS basket_player_season_season_canonical_idx
  ON basketball_test.basket_player_season (game_year, canonical_player_id)
  WHERE identity_match_status = 'verified';
