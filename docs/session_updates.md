# Session Updates

## 2026-03-03
- Fixed Game Logs tab date-range mismatch in `app/R/server_tab4.R`.
- Root cause: tab used static defaults (`DEFAULT_START`/`DEFAULT_END`) while active season default is `2026`.
- Change: `gl_dates` now syncs to `shared$season_date_bounds(input$game_year)` on season change.
- Change: reset action now restores season bounds (including min/max) instead of setting dates to `NA`.
- Impact: Game Logs loads correctly on default season without manual date adjustment.

- Fixed Traditional rank delta rendering in `app/R/server_tab3.R`.
- Change: `show_delta` now follows `tr_delta_enabled()` instead of hard-disabled flag.
- Impact: rank delta arrows now appear in Traditional mode (team/opponent) when baseline rules allow deltas.

## 2026-03-07
- Aligned clutch margin semantics in `sql/functions/get_player_traditional_dynamic.sql` with other clutch-enabled SQL functions.
- Change: clutch margin/status checks now use pre-possession margin (subtracting `team_score` where relevant), matching the logic already used in:
  - `sql/functions/fetch_lineups_all.sql`
  - `sql/functions/fetch_lineups_four_factors.sql`
  - `sql/functions/get_team_ratings_dynamic.sql`
  - `sql/functions/get_team_four_factors_dynamic.sql`
- Impact: clutch filtering behavior is now consistent across Tab 2/3 and Tab 5 Traditional outputs for boundary possessions.
