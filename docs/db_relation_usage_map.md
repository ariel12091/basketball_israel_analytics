# DB Relation Usage Map

Generated on 2026-03-15 from live catalog (`basketball_test`, `basketball`) and repository references.

| Relation | Kind | Size MB | Purpose (Inferred) | App refs | ETL refs | SQL refs | Usage Class |
|---|---:|---:|---|---:|---:|---:|---|
| basketball_test.actions_clean | table | 32.01 | Canonical cleaned actions feed (event-level). | 0 | 0 | 0 | No direct repo references found |
| basketball_test.app_meta | table | 0.03 | App metadata / operational key-value store (e.g., ETL last success). | 1 | 0 | 0 | App-facing |
| basketball_test.df_pts_poss_lineups_longer_mv | table | 98.2 | Core possession-points-lineup fact table used by lineup/four-factors logic. | 5 | 0 | 43 | App-facing |
| basketball_test.final_schedule_mv | materialized_view | 0.16 | Game schedule with context (home/away, outcome, GN, dates). | 17 | 0 | 31 | App-facing |
| basketball_test.full_rosters | table | 1.96 | Season roster dimension (player-team-year names/ids). | 20 | 0 | 12 | App-facing |
| basketball_test.games_ingest_status | table | 0.05 | Ingest pipeline status tracking by game. | 0 | 0 | 0 | No direct repo references found |
| basketball_test.lineup_four_factors_by_game | materialized_view | 9.12 | Per-game lineup four-factors aggregates for lineup modal/details. | 4 | 0 | 10 | App-facing |
| basketball_test.lineups_lookup | table | 72.8 | Lineup hash identity map (5-man lineup composition). | 0 | 0 | 16 | SQL object only |
| basketball_test.lineups_lookup_on | table | 4.7 | Lookup for on-court lineup combinations used by on/off workflows. | 0 | 0 | 7 | SQL object only |
| basketball_test.mv_lineup_totals_by_day | materialized_view | 9.38 | Daily lineup totals rollup for incremental refresh and checks. | 2 | 0 | 5 | App-facing |
| basketball_test.onoff_default_mv | table | 0.28 | Precomputed default on/off output surface. | 1 | 0 | 9 | App-facing |
| basketball_test.player_advanced_stats_mv | table | 0.3 | Precomputed advanced player metrics surface. | 1 | 0 | 9 | App-facing |
| basketball_test.player_four_factors_by_game | table | 106.82 | Per-game player four-factor aggregates. | 0 | 0 | 10 | SQL object only |
| basketball_test.player_onoff_by_game | materialized_view | 87.19 | Per-game player on/off aggregates (materialized for speed). | 0 | 0 | 6 | SQL object only |
| basketball_test.player_traditional_stats_mv | materialized_view | 0.14 | Precomputed traditional player stat surface. | 1 | 0 | 4 | App-facing |
| basketball_test.possessions | table | 35.58 | Possession-level fact table from ETL parsing. | 0 | 0 | 1 | SQL object only |
| basketball_test.pws | table | 57.62 | Player-with-starters / participation intermediate used in SQL refresh chain. | 0 | 0 | 2 | SQL object only |
| basketball_test.sched_long | view | 0 | Long schedule view used by SQL downstream transforms. | 0 | 0 | 2 | SQL object only |
| basketball_test.schedule | table | 0.17 | Raw/normalized schedule table from ingest. | 0 | 0 | 13 | SQL object only |
| basketball_test.schedule_team_dict | table | 0.03 | Schedule team id/name mapping dictionary. | 0 | 0 | 0 | No direct repo references found |
| basketball_test.stints | table | 5.77 | Lineup stint-level intervals from substitution logic. | 0 | 0 | 0 | No direct repo references found |
| basketball_test.sub_lineups | table | 57.42 | Derived sub-lineups generated from full lineups. | 2 | 1 | 17 | App + ETL |
| basketball_test.sub_lineups_stats | table | 12.6 | Aggregated stats by sub-lineup hash. | 1 | 0 | 8 | App-facing |
| basketball_test.subs | table | 8.62 | Substitution event table from ingest/ETL. | 0 | 0 | 0 | No direct repo references found |
| basketball_test.team_four_factors_mv | materialized_view | 0.05 | Team-level four-factors summary surface. | 1 | 0 | 3 | App-facing |
| basketball_test.team_metrics_by_game_mv | table | 0.34 | Team metrics by game fact summary. | 6 | 0 | 9 | App-facing |
| basketball_test.team_metrics_rolling_mv | materialized_view | 0.45 | Rolling team metrics summary by window. | 0 | 0 | 3 | SQL object only |
| basketball_test.team_ppp_ratings_mv | materialized_view | 0.03 | Team PPP ratings summary surface. | 5 | 0 | 12 | App-facing |

## Method
- Catalog source: live `pg_class` / `pg_namespace` for table/materialized view/view objects in `basketball_test`, `basketball`.
- Usage scan: `ripgrep` across `app/`, `etl/`, `sql/`, `scripts/`, `docs/`.
- Reference counts are static code hits, not runtime query frequency.
- Purpose is inferred from relation names + SQL object files and should be validated where marked ambiguous.
