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
