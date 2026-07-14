-- 2026-07-15: post-audit cleanup (applied via scratch R scripts; recorded here).
--
-- 1) Drop analysis leftovers (~43 MB): pbp_shot_clock_marks (+ dependent view
--    pbp_shot_clock_faults_v) and the twelve debug_178_* tables. All were
--    created by ad-hoc analysis (scripts/mark_shot_clock.R, game-178 debug);
--    pbp_shot_clock_marks archived to output/archive/*.parquet before drop
--    and is recomputable by scripts/mark_shot_clock.R.
DROP VIEW IF EXISTS basketball_test.pbp_shot_clock_faults_v;
DROP TABLE IF EXISTS basketball_test.pbp_shot_clock_marks;
DROP TABLE IF EXISTS basketball_test.debug_178_actions_clean;
DROP TABLE IF EXISTS basketball_test.debug_178_bad_stint_rows;
DROP TABLE IF EXISTS basketball_test.debug_178_full_rosters;
DROP TABLE IF EXISTS basketball_test.debug_178_lineups_lookup;
DROP TABLE IF EXISTS basketball_test.debug_178_possessions;
DROP TABLE IF EXISTS basketball_test.debug_178_pws_current_id_join;
DROP TABLE IF EXISTS basketball_test.debug_178_pws_time_join;
DROP TABLE IF EXISTS basketball_test.debug_178_pws_time_unmatched;
DROP TABLE IF EXISTS basketball_test.debug_178_schedule;
DROP TABLE IF EXISTS basketball_test.debug_178_stints_current;
DROP TABLE IF EXISTS basketball_test.debug_178_stints_time_valid;
DROP TABLE IF EXISTS basketball_test.debug_178_subs;

-- 2) Drop the dead usg_pct column from player_four_factors_by_game.
--    No app or SQL consumer reads it (Tab 5 / Tab 7 USG% comes from
--    player_traditional_stats_mv / get_player_traditional_dynamic, which
--    compute usage independently), and the 2026-07-14 rebuild showed its
--    stored values had drifted from the repo formula (probable old deploy
--    drift). The refresh function and table definition no longer write it —
--    deploy sql/functions/refresh_player_four_factors_by_game_for_games.sql
--    BEFORE running this ALTER (the new function omits the column and works
--    against both schemas).
ALTER TABLE basketball_test.player_four_factors_by_game DROP COLUMN usg_pct;
