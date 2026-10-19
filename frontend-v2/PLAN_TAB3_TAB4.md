# Plan: React Frontend — Tab 3 (Team Ratings) + Tab 4 (Game Logs)

## Context
Tabs 1-2 are complete in the React frontend-v2. Tabs 3-4 are placeholder stubs. The Shiny app has full implementations for both. This plan ports those two tabs to React + Plumber, following the same patterns established in Tabs 1-2.

---

## Scope Overview

| Work Item | Tab 3 | Tab 4 |
|-----------|-------|-------|
| Plumber endpoints | 2 (`/api/teams/{summary,four-factors}`) | 2 (`/api/gamelogs/{summary,four-factors}`) |
| Rename functions | `rename_team_summary()`, `rename_team_ff()` | `rename_gamelog_summary()`, `rename_gamelog_ff()` |
| TypeScript types | `TeamRating`, `TeamFourFactors` | `GameLogSummary`, `GameLogFourFactors` |
| React page | `TeamRatingsPage.tsx` | `GameLogsPage.tsx` |
| New shared filter: Starters | Added to FilterDrawer + FilterState (used by all tabs) |
| Tab-local: Clutch | Yes (same pattern as Tab 2) | No |
| Tab-local: Team dropdown | No (uses drawer Teams) | Yes (single-select "All teams") |

---

## Step 1: Add Starters Filter to Shared State

Currently the React frontend has **no starters UI** (grep confirms 0 hits in `frontend-v2/src/`). Shiny has starters on all 4 tabs with the same pattern: mode (ALL/>=/<= ) + value (0-5) for own + opponent lineups.

**Files:**
- `frontend-v2/src/types/index.ts` — add to `FilterState`:
  ```
  startersOffMode: '' | 'gte' | 'lte'
  startersOffValue: number | null   // 0-5
  startersDefMode: '' | 'gte' | 'lte'
  startersDefValue: number | null
  ```
- `frontend-v2/src/features/filters/store.ts` — add defaults, include in `buildApiParams()`, include in `needsFilteredPath()`, reset on RESET
- `frontend-v2/src/features/filters/FilterDrawer.tsx` — add "Lineup Starters" section with 2 rows (own + opp, each with mode select + value select)
- `frontend-v2/src/features/filters/FilterChips.tsx` — render starters chip when active

---

## Step 2: Tab 3 Plumber Endpoints

**File:** `frontend-v2/server/plumber.R` — add 2 endpoints + 2 rename functions

### `/api/teams/summary`
- Params: `game_year, start_date, end_date, game_type, opp_ids, home_away, outcome, gn_min, gn_max, last_n, opp_rank_side, opp_rank_n, opp_rank_metric, num_starters_off_mode, num_starters_off, num_starters_def_mode, num_starters_def, clutch_margin, clutch_status, clutch_minutes, clutch_ot_margin`
- Fast path: `SELECT ... FROM team_ppp_ratings_mv WHERE game_year = $1` (when no filters)
- Filtered path: `get_team_ratings_dynamic(23 params)` (same as `server_tab3.R:run_team_ratings_dynamic`)
- **Pace columns:** Port `fetch_team_game_minutes()` from `server_tab3.R` — runs the same query to compute `game_minutes` per team, then computes `off_pace = (off_poss / game_minutes) * 40`, `def_pace` same
- **PR ranking (summary):** Discrete rank colors are handled client-side (rank columns come from MV/SQL function). No additional server-side PR computation needed for summary.
- Rename: `rename_team_summary(df)` maps `team_name→teamName, off_ppp→offPpp, def_ppp→defPpp, net_rtg→netRtg, games_played→gamesPlayed, wins→wins, losses→losses, off_poss→offPoss, def_poss→defPoss, rank_net_rtg→rankNet, rank_off_ppp→rankOff, rank_def_ppp→rankDef, off_pace→offPace, def_pace→defPace, team_id→teamId`

### `/api/teams/four-factors`
- Same filter params as summary + clutch
- Fast path: `SELECT * FROM team_four_factors_mv WHERE game_year = $1`
- Filtered path: `get_team_four_factors_dynamic(23 params)`
- **PR ranking (FF):** Compute server-side (all teams qualify, no baseline). Port the `pr_vec()` logic from `server_tab3.R:tr_ff_data` — 11 PR columns for heatmap coloring. Return as `prOffPpp, prOffTs, prOffOreb, prOffTov, prOffFtr, prDefPpp, prDefTs, prDefOreb, prDefTov, prDefFtr, prNet`.
- Pace columns: same `fetch_team_game_minutes()` logic
- Rename: `rename_team_ff(df)` maps all FF columns + PR columns + pace

---

## Step 3: Tab 3 TypeScript Types

**File:** `frontend-v2/src/types/index.ts`

```ts
export interface TeamRating {
  teamId: number;
  teamName: string;
  gamesPlayed: number;
  wins: number;
  losses: number;
  offPpp: number;
  defPpp: number;
  netRtg: number;
  rankNet: number;
  rankOff: number;
  rankDef: number;
  offPace: number;
  defPace: number;
  offPoss: number;
  defPoss: number;
}

export interface TeamFourFactors {
  teamId: number;
  teamName: string;
  offPpp: number; defPpp: number; netRtg: number;
  offTs: number; offOreb: number; offTov: number; offFtr: number;
  defTs: number; defOreb: number; defTov: number; defFtr: number;
  offPace: number; defPace: number;
  offPoss: number; defPoss: number;
  // PR columns (all 0-1, null not needed since all teams qualify)
  prOffPpp: number; prOffTs: number; prOffOreb: number; prOffTov: number; prOffFtr: number;
  prDefPpp: number; prDefTs: number; prDefOreb: number; prDefTov: number; prDefFtr: number;
  prNet: number;
}
```

---

## Step 4: Tab 3 React Page (`TeamRatingsPage.tsx`)

**File:** `frontend-v2/src/pages/TeamRatingsPage.tsx`

### Structure
- View toggle: Summary / Four Factors (local `mode` state)
- Clutch controls: local state (checkbox + margin/status/minutes/OT), same UI pattern as LineupsPage
- API params: `buildApiParams(filters)` + clutch params + starters (from shared state via `buildApiParams`)
- Two `useApi` calls: one for each view mode
- `useSorting` for sort state
- `DataTable` for rendering

### Summary View
- Column groups: `[empty | RECORD | RATINGS | PACE | USAGE]`
- Columns: Team | GP | W | L | Off PPP | Def PPP | Net Rtg | Net Rank | Off Rank | Def Rank | Off Pace | Def Pace | Off Poss | Def Poss
- **Rank columns:** Discrete color gradient (green→amber→red) based on rank value 1..N. Use inline style with `colorRampPalette` equivalent in JS (interpolate between green/amber/red based on `rank/maxRank`).
- Default sort: `netRtg` desc
- No TOTAL row (all teams shown)

### Four Factors View
- Column groups: `[empty | OFFENSE (7 cols) | DEFENSE (7 cols) | NET (1 col)]`
- Columns: Team | Off PPP | Off TS% | Off OREB% | Off TOV% | Off FTR | Off Pace | Off Poss | Def PPP | Def TS% | Def OREB% | Def TOV% | Def FTR | Def Pace | Def Poss | Net
- HeatCell with continuous PR background on all rate/PPP/net columns (same as Tab 2 FF)
- Pace + Poss columns: plain text, no heat coloring
- Default sort: `netRtg` desc

### Explainer + Legend
- Brief explainer (same pattern as Tab 1/2)
- No shot legend (no shot columns in Tab 3)

---

## Step 5: Tab 4 Plumber Endpoints

**File:** `frontend-v2/server/plumber.R` — add 2 endpoints + 2 rename functions

### `/api/gamelogs/summary`
- Params: `game_year, start_date, end_date, game_type, opp_ids, home_away, outcome, gn_min, gn_max, last_n, filter_team_id, num_starters_off_mode, num_starters_off, num_starters_def_mode, num_starters_def`
- **No SQL function** — mirrors `server_tab4.R` logic:
  1. Fetch `final_schedule_mv` for `game_year`
  2. Apply schedule-level filters (team, date, game type, opponents, home/away, outcome, GN, lastN) in R
  3. Fetch `mv_lineup_totals_by_day` for `game_year`
  4. Inner join on `(game_id, team_id)`, apply starters filter on `num_starters` per `type_lineup`
  5. Aggregate per `(game_id, team_id, type_lineup)` → pivot off/def → compute PPP + net
  6. Join schedule info (gn, game_date, team_name, opp_team_name, team_score, opp_score, has_won)
- **No opponent strength, no clutch** (matches Shiny Tab 4)
- Rename: `rename_gamelog_summary(df)` maps `gn→gn, game_date→gameDate, team_name→teamName, opp_team_name→opponent, result→result, score_display→score, off_ppp→offPpp, def_ppp→defPpp, net_rtg→netRtg, off_poss→offPoss, def_poss→defPoss, off_fg2m→offFg2Made, off_fg2a→offFg2Att, off_fg3m→offFg3Made, off_fg3a→offFg3Att, def_fg2m→defFg2Made, ...`

### `/api/gamelogs/four-factors`
- Same filter params
- Same pattern but fetches `lineup_four_factors_by_game` instead
- Aggregates FF raw counts per `(game_id, team_id, type_lineup)`, derives rates (TS%, OREB%, TOV%, FTR)
- Rename: `rename_gamelog_ff(df)` maps all columns

### Caching
- Cache schedule + lineup totals per game_year (same RESP_CACHE pattern)
- These are MV reads so cache TTL = 60s is fine

---

## Step 6: Tab 4 TypeScript Types

**File:** `frontend-v2/src/types/index.ts`

```ts
export interface GameLogSummary {
  gn: number;
  gameDate: string;
  teamName: string;
  opponent: string;
  result: string;  // "W" or "L"
  score: string;   // "114-60"
  offPpp: number;
  defPpp: number;
  netRtg: number;
  offPoss: number;
  defPoss: number;
  // Shot splits
  offFg2Made: number; offFg2Att: number; offFg3Made: number; offFg3Att: number;
  defFg2Made: number; defFg2Att: number; defFg3Made: number; defFg3Att: number;
  // Hidden key
  gameId: number;
  teamId: number;
}

export interface GameLogFourFactors {
  gn: number;
  gameDate: string;
  teamName: string;
  opponent: string;
  result: string;
  score: string;
  offPpp: number; offTsPct: number; offOrebPct: number; offTovPct: number; offFtrPct: number;
  defPpp: number; defTsPct: number; defOrebPct: number; defTovPct: number; defFtrPct: number;
  offPoss: number;
  defPoss: number;
  gameId: number;
  teamId: number;
}
```

---

## Step 7: Tab 4 React Page (`GameLogsPage.tsx`)

**File:** `frontend-v2/src/pages/GameLogsPage.tsx`

### Structure
- View toggle: Summary / Four Factors
- **Team dropdown** (local state, single-select, optional — "All teams"). Different from drawer Teams filter.
- Starters filter: from shared FilterDrawer
- **No clutch**, **no opponent strength**
- API params: `buildApiParams(filters)` + `filter_team_id` (local) + starters (shared)
- Two `useApi` calls: summary / ff
- `useSorting` for sort state
- `DataTable` for rendering

### Summary View
- Column groups: `[GAME INFO (6) | RATINGS (3) | SHOTS (2) | USAGE (2)]`
- Columns: GN | Date | Team | Opponent | W/L | Score | Off PPP | Def PPP | Net | Off Shot | Def Shot | Off Poss | Def Poss
- **W/L badge:** green for W, red for L (bold, colored text — same as Shiny's result_render)
- **ShotCell:** reuse existing component for Off Shot / Def Shot
- **Shot averages:** computed from dataset (same `computeShotAvgs()` pattern as Tabs 1-2)
- **Shot legend** above table (same as Tab 1/2 summary)
- Default sort: `gameDate` desc, then `gn` desc
- No PR heatmap (single-game data, no ranking)

### Four Factors View
- Column groups: `[GAME INFO (6) | OFFENSE (5) | DEFENSE (5) | USAGE (2)]`
- Columns: GN | Date | Team | Opponent | W/L | Score | Off PPP | Off TS% | Off OREB% | Off TOV% | Off FTR | Def PPP | Def TS% | Def OREB% | Def TOV% | Def FTR | Off Poss | Def Poss
- No PR heatmap (single-game data)
- Default sort: same as summary

---

## Step 8: Wire Up in AppShell

**File:** `frontend-v2/src/app/layout/AppShell.tsx`
- Import `TeamRatingsPage` and `GameLogsPage`
- Replace `PlaceholderTab` stubs:
  ```tsx
  {activeTab === 'teams' && <TeamRatingsPage />}
  {activeTab === 'gamelogs' && <GameLogsPage />}
  ```
- Remove `PlaceholderTab` component

---

## Implementation Order

1. **Step 1** — Starters in shared filter state (types, store, drawer, chips)
2. **Step 2** — Tab 3 Plumber endpoints (summary + ff + pace + PR)
3. **Step 3** — Tab 3 types
4. **Step 4** — Tab 3 React page
5. **Step 5** — Tab 4 Plumber endpoints (summary + ff)
6. **Step 6** — Tab 4 types
7. **Step 7** — Tab 4 React page
8. **Step 8** — Wire up in AppShell

Can do Steps 2-4 (Tab 3) and Steps 5-7 (Tab 4) in parallel since they're independent.

---

## Verification

1. `cd frontend-v2 && npm run dev` — no TS/build errors
2. `cd frontend-v2/server && Rscript run.R` — Plumber starts, no errors
3. Tab 3 Summary: all teams show with GP, W-L, PPP, ranks (colored), pace, poss
4. Tab 3 FF: all teams show with heatmap coloring on all rate columns
5. Tab 4 Summary: all games show with W/L badges, shot splits, PPP
6. Tab 4 FF: all games show with TS%, OREB%, TOV%, FTR columns
7. Starters filter in drawer: selecting values triggers filtered-path queries
8. Tab 3 clutch: enabling clutch changes data (fewer possessions)
9. Tab 4 team dropdown: selecting a team filters to that team's games
10. CSV export works on both tabs, both views
11. FilterChips show starters + clutch when active, clear properly on Reset
