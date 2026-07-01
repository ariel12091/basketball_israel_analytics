# Trends Tab — Design Spec

**Date:** 2026-07-01
**Status:** Approved for planning
**Area:** Shiny app (`app/`) + SQL materialized views (`sql/`) + ETL refresh pipeline

## 1. Purpose

A new Shiny tab — **"Trends"** — that answers one question at a glance:
**who is rising and who is falling right now.**

It surfaces **direction** (momentum over recent games), deliberately complementary to
Tab 3 (Team Ratings), which shows **level** (how good a team is overall). There is no
metric overlap in intent: Tab 3 = current standing, Trends = recent change.

**Design ethos: conclusion-first.** The verdict is on screen with zero investigation
required. Drill-in detail is optional, never mandatory.

## 2. Scope

- **Subjects:** Teams **and** Players (subject toggle in the sidebar).
- **Platform:** Shiny only for now (consistent with Tab 5). No React/Plumber work.
- **Charting:** Inline SVG built with `htmltools` — **no new charting dependency.**
  Both the row sparklines and the drill-in line chart are hand-built SVG. Keeps the
  shinyapps.io bundle light and matches the app's existing custom-HTML-cell approach.

Out of scope: React port, player metrics beyond those listed in §5, per-tab season
selector (uses the global navbar `input$game_year`).

## 3. The Main View — Momentum Board

Two ranked groups stacked in the 9-col main area:

```
 HEATING UP                              (rolling window vs. prior window)
 1  Maccabi TA   +14.3   ▁▂▄▅▆█   ▲ +6.2   [row click → chart]
 2  Hapoel J     +6.7    ▃▄▄▅▆▇   ▲ +4.1
 ...
 COOLING DOWN
 1  Eilat        -4.4    █▆▅▃▂▁   ▼ -7.9
 2  Holon        +1.2    ▇▆▅▄▃▂   ▼ -5.3
```

Each row: **rank · name · latest rolling value · inline-SVG sparkline (last N games) · ▲/▼ Δ**.

- **Heating Up** = rows with positive Δ, sorted by Δ descending.
- **Cooling Down** = rows with negative Δ, sorted by Δ ascending (biggest drop first).
- Δ ("momentum") = current rolling value − rolling value one full window earlier,
  computed in R from the per-game rolling series.
- **Sign/color polarity follows the metric.** For Def PPP, lower is better, so a
  *decrease* is "heating up" — the ▲/▼ and grouping must respect the metric's polarity
  (reuse the offense/defense polarity convention already documented in CLAUDE.md).

## 4. Sidebar Controls (3-col)

- **Subject toggle:** Team / Player.
- **Metric dropdown:** default **Net Rating**; also Off PPP, Def PPP, TS%, eFG%
  (team). Player metric set per §5.
- **Rolling window:** R3 / **R5** (default) / R10.
- **Min games:** default = window size (a team/player needs at least a full window to
  appear). Lowering it allows shorter histories but Δ still requires a prior window.
- Season: from the global navbar selector (`input$game_year`) — not repeated here.

## 5. Data Layer

### 5.1 Teams — reuse existing MV (with a bug fix, see §6)

`team_metrics_rolling_mv` already exists, is refreshed by the ETL pipeline, and is
**current** (verified 2026-07-01: 446 games through 2026-06-23, matching
`final_schedule_mv`). It provides, per team per game:

- `net_rtg_r3/r5/r10`, `off_ppp_r*`, `def_ppp_r*`, `off_ts_r*`, `off_efg_r*`,
  `def_ts_r*`, `def_efg_r*`
- `team_game_seq` (per team, per season game ordinal)
- `delta_net_rtg_r5`, `prev_net_rtg_r5`

The board computes Δ for any metric/window in R from the rolling series, so **no schema
change is needed for teams** (the §6 `net_rtg` fix is already applied).

### 5.2 Players — new MVs (mirror the team pattern)

Build two new objects analogous to the team ones:

- **`player_metrics_by_game_mv`** — physical table, player-per-team-per-game grain.
  Sources:
  - `player_onoff_by_game` (filtered `is_on_key = 1`, pivoted `type_lineup`
    offense/defense) → on-court points/poss → **on-court net** = `(off_pts/off_poss −
    def_pts/def_poss) × 100`. Also on-court Off PPP / Def PPP.
  - `player_four_factors_by_game` → player TS% / eFG% (has `total_points`, `total_poss`,
    `ts_poss_count`, `total_fga`, `total_fgm`, `total_fg3_made`, `minutes`, `usg_pct`).
  - Store the same `*_raw` count columns the team MV stores, so rates can be re-derived
    and net rating is computed with correct precedence from the start (avoid the §6 bug).
- **`player_metrics_rolling_mv`** — rolling 3/5/10 windows over
  `player_metrics_by_game_mv`, partitioned by `(game_year, player_id, team_id)`, ordered
  by `game_date, game_id`, mirroring `team_metrics_rolling_mv`.

**Player headline metric:** on-court net momentum. Dropdown also offers on-court Off PPP,
Def PPP, TS%, eFG%.

**Player identity:** respect the existing identity/alias handling used elsewhere (a
player can change `team_id` across the season). Partition rolling by `(game_year,
player_id, team_id)` so a mid-season transfer starts a fresh streak per team; revisit if
cross-team continuity is desired (defer — matches how Tab 5 combines cross-team only).

**Min-minutes guard:** players with trivial floor time in a game distort on-court rates.
Apply a per-game minimum-minutes threshold when building the by-game MV (exact value a
plan-time decision; start with a small floor, e.g. ≥ 3 minutes, and document it).

### 5.3 ETL / rebuild wiring

- Add an incremental `refresh_player_metrics_by_game_for_games(int4[])` mirroring
  `refresh_team_metrics_by_game_for_games`.
- Register both new MVs in `sql/rebuild_all_mvs.R` at the correct level
  (`player_metrics_by_game_mv` at L3 as a `type = "table"`, `player_metrics_rolling_mv`
  at L4, after its by-game source), and add them to the `etl_full.R` refresh order.

## 6. `net_rtg` precedence bug — ALREADY FIXED (prerequisite, done)

Fixed separately before this feature and merged to `main` (commit `faf8941`).

`net_rtg` in `team_metrics_by_game_mv.sql` **and**
`refresh_team_metrics_by_game_for_games.sql` had an operator-precedence bug:

```sql
( off_frac ) - ( def_frac ) * 100   -- ×100 bound only to the def term
```

It computed `off_frac − (def_frac × 100)` ≈ −104 instead of `(off_frac − def_frac) ×
100`, propagating into `net_rtg_r*`, `prev_net_rtg_r5`, `delta_net_rtg_r5`. It was
invisible because Tab 3 recomputes net rating from the `*_raw` columns and never reads
the `net_rtg` column; the Trends tab is the first direct consumer.

**Resolution:** parenthesization corrected in both files, `team_metrics_by_game_mv`
rebuilt in place via `refresh_team_metrics_by_game_for_games(NULL)`, and
`team_metrics_rolling_mv` refreshed. Verified: `net_rtg` now equals `off_ppp − def_ppp`
(0 mismatch rows), range ±76, mean 0.0. **No remaining work for this feature** — team
rolling data is correct and current.

## 7. Shiny Implementation Shape

Follow the existing modular pattern:

- `app/R/ui_tab6_trends.R` — sidebar controls + main momentum-board container.
- `app/R/server_tab6_trends.R` — `server_tab6(input, output, session, shared)`:
  - Reads global season, subject, metric, window, min-games.
  - Fetches the relevant rolling MV rows for the season (team or player).
  - Computes latest rolling value + Δ per subject in R; splits into Heating/Cooling with
    metric-aware polarity; sorts.
  - Renders each group as a table/HTML list with an inline-SVG sparkline column.
  - Registers a row-click handler (`Shiny.setInputValue`, per Tab 2's lineup→modal
    pattern) that opens a modal with a larger inline-SVG line chart of the rolling series
    (optionally R3/R5/R10 overlaid + faint per-game actual dots), annotated with latest
    value + Δ.
- Wire `server_tab6(...)` into `app.R` and add the tab to the navbar with an icon +
  active amber underline, matching the other tabs.

**Reusable helpers to add** (in `global.R` or a small `R/trends_helpers.R`):

- `svg_sparkline(values, polarity)` → inline SVG string for a table cell.
- `svg_trend_chart(series_list, ...)` → inline SVG string for the modal.
- `momentum_split(df, metric, window, polarity)` → latest value, Δ, group assignment.

## 8. Edge Cases

- Fewer than `window` games → excluded by the min-games filter (default = window).
- No prior full window (early season) → Δ is null → row shown neutral / not ranked into
  either group (or a small "settling in" section — plan-time choice; default: omit).
- Rolling already partitioned by `game_year` (team) / `(game_year, player_id, team_id)`
  (player) → no cross-season bleed.
- Ties in Δ → stable secondary sort by latest rolling value.
- Metric with null rolling value for a subject (e.g. no qualifying possessions) → omit
  that subject.

## 9. Success Criteria

- Opening the Trends tab immediately shows a ranked Heating Up / Cooling Down board for
  the current season, teams by default, keyed on Net Rating momentum — no clicks needed.
- Switching Subject, Metric, and Window re-ranks the board correctly, with correct
  offense/defense polarity.
- `net_rtg` and derived rolling/delta values are sane (§6 fix already applied).
- Clicking a row opens a readable line chart of that subject's rolling series.
- New player MVs are wired into `rebuild_all_mvs.R` and `etl_full.R` and refresh
  incrementally by `game_id`.
- No new R package dependency is introduced.

## 10. Open Decisions Deferred to Plan-Time

- Exact per-game min-minutes floor for players (start ≈ 3 min).
- Whether to show an "insufficient history" section vs. omit such subjects.
- Whether the modal chart overlays all three windows or just the selected one.
