# DB Tables: Usage, Relations, Purpose

Generated on 2026-03-16 12:37:32.

This document defines each `basketball_test` relation with:
- usage (app/etl/sql footprint)
- upstream/downstream dependencies
- purpose

## basketball_test.actions_clean

- Kind: `table`
- Size: `32.01 MB`
- Purpose: Canonical cleaned actions feed (event-level).
- Usage class: No direct repo references found
- Usage refs: app=0, etl=0, sql=0
- Upstream relations: None captured
- Downstream relations: None captured

## basketball_test.app_meta

- Kind: `table`
- Size: `0.03 MB`
- Purpose: App metadata / operational key-value store (e.g., ETL last success).
- Usage class: App-facing
- Usage refs: app=1, etl=0, sql=0
- Upstream relations: None captured
- Downstream relations: None captured

## basketball_test.df_pts_poss_lineups_longer_mv

- Kind: `table`
- Size: `98.20 MB`
- Purpose: Core possession-points-lineup fact table used by lineup/four-factors logic.
- Usage class: App-facing
- Usage refs: app=5, etl=0, sql=43
- Upstream relations: None captured
- Downstream relations: lineup_four_factors_by_game, mv_lineup_totals_by_day, player_onoff_by_game, player_traditional_stats_mv, team_ppp_ratings_mv

## basketball_test.final_schedule_mv

- Kind: `materialized_view`
- Size: `0.16 MB`
- Purpose: Game schedule with context (home/away, outcome, GN, dates).
- Usage class: App-facing
- Usage refs: app=17, etl=0, sql=31
- Upstream relations: sched_long
- Downstream relations: player_onoff_by_game, player_traditional_stats_mv, team_ppp_ratings_mv

## basketball_test.full_rosters

- Kind: `table`
- Size: `1.96 MB`
- Purpose: Season roster dimension (player-team-year names/ids).
- Usage class: App-facing
- Usage refs: app=20, etl=0, sql=12
- Upstream relations: None captured
- Downstream relations: player_traditional_stats_mv, team_four_factors_mv, team_ppp_ratings_mv

## basketball_test.games_ingest_status

- Kind: `table`
- Size: `0.05 MB`
- Purpose: Ingest pipeline status tracking by game.
- Usage class: No direct repo references found
- Usage refs: app=0, etl=0, sql=0
- Upstream relations: None captured
- Downstream relations: None captured

## basketball_test.lineup_four_factors_by_game

- Kind: `materialized_view`
- Size: `9.12 MB`
- Purpose: Per-game lineup four-factors aggregates for lineup modal/details.
- Usage class: App-facing
- Usage refs: app=4, etl=0, sql=10
- Upstream relations: df_pts_poss_lineups_longer_mv, schedule
- Downstream relations: team_four_factors_mv

## basketball_test.lineups_lookup

- Kind: `table`
- Size: `72.80 MB`
- Purpose: Lineup hash identity map (5-man lineup composition).
- Usage class: SQL object only
- Usage refs: app=0, etl=0, sql=16
- Upstream relations: None captured
- Downstream relations: player_onoff_by_game, player_traditional_stats_mv

## basketball_test.lineups_lookup_on

- Kind: `table`
- Size: `4.70 MB`
- Purpose: Lookup for on-court lineup combinations used by on/off workflows.
- Usage class: SQL object only
- Usage refs: app=0, etl=0, sql=7
- Upstream relations: None captured
- Downstream relations: None captured

## basketball_test.mv_lineup_totals_by_day

- Kind: `materialized_view`
- Size: `9.38 MB`
- Purpose: Daily lineup totals rollup for incremental refresh and checks.
- Usage class: App-facing
- Usage refs: app=2, etl=0, sql=5
- Upstream relations: df_pts_poss_lineups_longer_mv, schedule
- Downstream relations: None captured

## basketball_test.onoff_default_mv

- Kind: `table`
- Size: `0.28 MB`
- Purpose: Precomputed default on/off output surface.
- Usage class: App-facing
- Usage refs: app=1, etl=0, sql=9
- Upstream relations: None captured
- Downstream relations: None captured

## basketball_test.player_advanced_stats_mv

- Kind: `table`
- Size: `0.30 MB`
- Purpose: Precomputed advanced player metrics surface.
- Usage class: App-facing
- Usage refs: app=1, etl=0, sql=9
- Upstream relations: None captured
- Downstream relations: None captured

## basketball_test.player_four_factors_by_game

- Kind: `table`
- Size: `106.82 MB`
- Purpose: Per-game player four-factor aggregates.
- Usage class: SQL object only
- Usage refs: app=0, etl=0, sql=10
- Upstream relations: None captured
- Downstream relations: None captured

## basketball_test.player_onoff_by_game

- Kind: `materialized_view`
- Size: `87.19 MB`
- Purpose: Per-game player on/off aggregates (materialized for speed).
- Usage class: SQL object only
- Usage refs: app=0, etl=0, sql=6
- Upstream relations: df_pts_poss_lineups_longer_mv, final_schedule_mv, lineups_lookup
- Downstream relations: None captured

## basketball_test.player_traditional_stats_mv

- Kind: `materialized_view`
- Size: `0.14 MB`
- Purpose: Precomputed traditional player stat surface.
- Usage class: App-facing
- Usage refs: app=1, etl=0, sql=4
- Upstream relations: df_pts_poss_lineups_longer_mv, final_schedule_mv, full_rosters, lineups_lookup
- Downstream relations: None captured

## basketball_test.possessions

- Kind: `table`
- Size: `35.58 MB`
- Purpose: Possession-level fact table from ETL parsing.
- Usage class: SQL object only
- Usage refs: app=0, etl=0, sql=1
- Upstream relations: None captured
- Downstream relations: None captured

## basketball_test.pws

- Kind: `table`
- Size: `57.62 MB`
- Purpose: Player-with-starters / participation intermediate used in SQL refresh chain.
- Usage class: SQL object only
- Usage refs: app=0, etl=0, sql=2
- Upstream relations: None captured
- Downstream relations: None captured

## basketball_test.sched_long

- Kind: `view`
- Size: `0.00 MB`
- Purpose: Long schedule view used by SQL downstream transforms.
- Usage class: SQL object only
- Usage refs: app=0, etl=0, sql=2
- Upstream relations: schedule, schedule_team_dict
- Downstream relations: final_schedule_mv

## basketball_test.schedule

- Kind: `table`
- Size: `0.17 MB`
- Purpose: Raw/normalized schedule table from ingest.
- Usage class: SQL object only
- Usage refs: app=0, etl=0, sql=13
- Upstream relations: None captured
- Downstream relations: lineup_four_factors_by_game, mv_lineup_totals_by_day, sched_long, team_ppp_ratings_mv

## basketball_test.schedule_team_dict

- Kind: `table`
- Size: `0.03 MB`
- Purpose: Schedule team id/name mapping dictionary.
- Usage class: No direct repo references found
- Usage refs: app=0, etl=0, sql=0
- Upstream relations: None captured
- Downstream relations: sched_long

## basketball_test.stints

- Kind: `table`
- Size: `5.77 MB`
- Purpose: Lineup stint-level intervals from substitution logic.
- Usage class: No direct repo references found
- Usage refs: app=0, etl=0, sql=0
- Upstream relations: None captured
- Downstream relations: None captured

## basketball_test.sub_lineups

- Kind: `table`
- Size: `57.42 MB`
- Purpose: Derived sub-lineups generated from full lineups.
- Usage class: App + ETL
- Usage refs: app=2, etl=1, sql=17
- Upstream relations: None captured
- Downstream relations: None captured

## basketball_test.sub_lineups_stats

- Kind: `table`
- Size: `12.60 MB`
- Purpose: Aggregated stats by sub-lineup hash.
- Usage class: App-facing
- Usage refs: app=1, etl=0, sql=8
- Upstream relations: None captured
- Downstream relations: None captured

## basketball_test.subs

- Kind: `table`
- Size: `8.62 MB`
- Purpose: Substitution event table from ingest/ETL.
- Usage class: No direct repo references found
- Usage refs: app=0, etl=0, sql=0
- Upstream relations: None captured
- Downstream relations: None captured

## basketball_test.team_four_factors_mv

- Kind: `materialized_view`
- Size: `0.05 MB`
- Purpose: Team-level four-factors summary surface.
- Usage class: App-facing
- Usage refs: app=1, etl=0, sql=3
- Upstream relations: full_rosters, lineup_four_factors_by_game
- Downstream relations: None captured

## basketball_test.team_metrics_by_game_mv

- Kind: `table`
- Size: `0.34 MB`
- Purpose: Team metrics by game fact summary.
- Usage class: App-facing
- Usage refs: app=6, etl=0, sql=9
- Upstream relations: None captured
- Downstream relations: team_metrics_rolling_mv

## basketball_test.team_metrics_rolling_mv

- Kind: `materialized_view`
- Size: `0.45 MB`
- Purpose: Rolling team metrics summary by window.
- Usage class: SQL object only
- Usage refs: app=0, etl=0, sql=3
- Upstream relations: team_metrics_by_game_mv
- Downstream relations: None captured

## basketball_test.team_ppp_ratings_mv

- Kind: `materialized_view`
- Size: `0.03 MB`
- Purpose: Team PPP ratings summary surface.
- Usage class: App-facing
- Usage refs: app=5, etl=0, sql=12
- Upstream relations: df_pts_poss_lineups_longer_mv, final_schedule_mv, full_rosters, schedule
- Downstream relations: None captured

## Method notes

- Usage/purpose values are sourced from `docs/db_relation_usage_map.md`.
- Dependency edges are sourced from `tmp_db_view_deps.csv` (`pg_depend` extraction).
- Function-level dynamic SQL dependencies were not available in `tmp_db_func_deps.csv` (header only).
