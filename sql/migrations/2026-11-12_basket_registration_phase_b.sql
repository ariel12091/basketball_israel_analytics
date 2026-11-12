-- Phase B of the Basket registration split
-- (docs/superpowers/plans/2026-11-12-basket-player-registration-split.md).
--
-- basket_player_registration has owned the players' facts since 2026-11-12,
-- and the Basket refresh has written it on main. basket_player_season keeps
-- only the shooting totals and their page provenance; the copies of the moved
-- columns go, together with their CHECKs and indexes. basket_player_roster
-- (2026-11-12, superseded by the registration table's roster rows the same
-- day) goes. Every season row now references its registration.
--
-- Run by scripts/apply_basket_registration_phase_b.R, which backs both tables
-- up to Parquet and checks the copies equal the registration table first.

ALTER TABLE basketball_test.basket_player_season
  DROP COLUMN player_name,
  DROP COLUMN player_name_en,
  DROP COLUMN position_name,
  DROP COLUMN position_en,
  DROP COLUMN height_m,
  DROP COLUMN nationality_name,
  DROP COLUMN nationality_code,
  DROP COLUMN profile_source_url,
  DROP COLUMN profile_source_content_md5,
  DROP COLUMN profile_fetched_at,
  DROP COLUMN profile_cache_hit,
  DROP COLUMN basket_team_name,
  DROP COLUMN date_of_birth,
  DROP COLUMN identity_id,
  DROP COLUMN canonical_player_id,
  DROP COLUMN identity_match_status,
  DROP COLUMN identity_matched_on,
  DROP COLUMN identity_matched_at;

ALTER TABLE basketball_test.basket_player_season
  ADD CONSTRAINT basket_player_season_registration_fkey
  FOREIGN KEY (game_year, basket_player_id)
  REFERENCES basketball_test.basket_player_registration (game_year, basket_player_id);

COMMENT ON TABLE basketball_test.basket_player_season IS
  'basket.co.il season-stat pages: shooting totals per player-season and their page provenance. Player facts, names and the identity link are in basket_player_registration (2026-11-12).';

DROP TABLE basketball_test.basket_player_roster;
