# EuroLeague Team Ratings minutes and pace

Last updated: 2026-08-12

## Application pattern

The EuroLeague Team Ratings tab follows the Israeli Team Ratings pattern:

1. Fetch filtered team rating and possession aggregates.
2. Fetch team minutes independently under the same schedule and starter-context
   filters.
3. Join minutes to ratings by `team_id` in R.
4. Calculate pace only after aggregation:

   - `off_pace = off_poss / minutes * 40`
   - `def_pace = def_poss / minutes * 40`

The Israeli unfiltered path can infer duration from regulation and overtime
periods. EuroLeague does not need that inference: its canonical
`matchup_segments_actions` fact stores `segment_seconds` once per consecutive
lineup segment. Migration 018 exposes those durations through
`euroleague.get_team_minutes_dynamic(...)`.

The function first sums duration at `(game_id, team_id)` grain, then sums the
selected games by team. This preserves overtime and prevents segment duration
from being multiplied by event rows. Starter filters apply to
`own_starters`/`opp_starters` before the game totals are rolled up.

## Security boundary

The Shiny application connects as the restricted PostgreSQL role
`app_readonly`. That role intentionally has no direct `SELECT` privilege on
`euroleague.matchup_segments_actions`.

The first migration-018 deployment used PostgreSQL's default
`SECURITY INVOKER`. The function therefore ran with `app_readonly` permissions
and failed with:

```text
permission denied for table matchup_segments_actions
```

The app converted that database error to `NULL`, which appeared as empty
Minutes and Pace cells.

The corrected function is a narrowly scoped `SECURITY DEFINER` function with a
fixed search path:

```sql
SECURITY DEFINER
SET search_path = pg_catalog, euroleague, public
```

All underlying relations are schema-qualified, the function contains no
dynamic SQL, and `app_readonly` receives only `team_id` plus aggregated
`minutes`. Direct access to the canonical segment table remains denied. This is
preferable to granting the application role broad access to internal lineup
segments.

The server now logs and displays a notification for future minutes-query
errors instead of silently rendering blank values.

## Verification

Migration 018 was applied to the live isolated `euroleague` schema. The exact
connection used by the local app (`app/.Renviron`, role `app_readonly`) returned:

- 20 teams;
- zero null or non-positive minute totals;
- season totals ranging from 1,050 to 1,300 minutes.

The role still reports no direct `SELECT` privilege on
`matchup_segments_actions`, confirming that the security boundary remains in
place.

Relevant files:

- `sql/018_team_minutes_read_layer.sql`
- `scripts/apply_018_team_minutes_read_layer.py`
- `scripts/verify_app_team_minutes.R`
- `../../app/R/server_tab9_euro_team.R`
- `../../app/R/helpers.R`
