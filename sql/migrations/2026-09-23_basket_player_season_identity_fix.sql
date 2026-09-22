-- Drop the unique index added earlier the same day by
-- 2026-09-23_basket_player_season_identity.sql. It asserted one verified
-- registration per (game_year, canonical_player_id), which is false.
--
-- basket.co.il issues a registration per player-TEAM-season, not per
-- player-season, and its shooting totals are per team stint. D.J. Burns holds
-- two 2026 registrations: 21790 at Rishon Lezion (24 games, 55 3PA) and 24882
-- at Bnei Herzliya (13 games, 1 3PA), under two spellings of his name. Both
-- resolve to canonical player 1143, and both are correct.
--
-- A consumer asking what Basket knows about one player in one season may
-- therefore get several rows and must aggregate them, rather than expecting at
-- most one. The primary key (game_year, basket_player_id) remains the real
-- uniqueness claim.

DROP INDEX IF EXISTS basketball_test.basket_player_season_identity_unique_idx;
