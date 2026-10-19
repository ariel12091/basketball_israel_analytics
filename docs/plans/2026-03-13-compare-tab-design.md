# Compare Tab Design Spec
_2026-03-13_

## Problem

Coaches want to compare team/lineup/player performance under two different conditions (starters vs bench, clutch vs non-clutch, home vs away, etc.). Currently this requires manually switching filters and remembering numbers across page loads.

## Solution

New **"Compare"** tab (⇌) with three modes: Teams, Lineups, Players. Two independent filter panels (Side A / Side B) define conditions; results show all entities ranked by gap or a focused head-to-head.

## Layout

Sidebar (filter builder) + main content area. Consistent across all modes.

### Sidebar

- **Season** selector (shared, not per-side)
- **Presets** dropdown: fills both sides in one click
- **Side A** filter panel (blue badge `#7b8cde`)
- `— vs —` divider
- **Side B** filter panel (amber badge `#e8a435`)
- **Compare →** button

### Main content

1. **Mode tabs** at top: Teams | Lineups | Players
2. **Metric chips**: selects which metric to compare (changes per mode)
3. **Summary cards** (3): Side A avg, Side B avg, league avg gap
4. **Results table**: sorted by gap descending, with inline dumbbell visual

## Modes

### Teams mode

- **Rows**: all teams in the league
- **SQL**: `get_team_ratings_dynamic` / `get_team_four_factors_dynamic` — called twice (Side A params, Side B params), joined by `team_id`
- **Metric chips**: Net Rtg, Offense, Defense, TS%, TOV%, OREB%, FTR
- **Columns**: #, Team, Metric(A), Poss(A), Metric(B), Poss(B), Gap▼, Visual
- **Filters**: all 8 (starters count, clutch, home/away, win/loss, before/after, opponent, game type, opp rank)

### Lineups mode

- **Rows**: all lineups (across all teams or filtered to a team)
- **SQL**: `fetch_lineups_csv_v2` / `fetch_lineups_four_factors_csv` — called twice, joined by `sub_lineup_hash`
- **Metric chips**: Net Rtg, Offense, Defense, TS%, TOV%, OREB%, FTR
- **Columns**: #, Lineup (player names), Team, Metric(A), Poss(A), Metric(B), Poss(B), Gap▼, Visual
- **Filters**: all 8

### Players mode

- **Rows**: focused comparison (not a ranked list)
- **SQL**: `get_player_traditional_dynamic` — called once per side with the selected player's team as filter
- **Metric chips**: PPG, RPG, APG, SPG, FG%, 3P%, FT%, TS%, PER (whatever the function returns)
- **Rate mode toggle**: per game / per 75 poss / totals (same as Tab 5)
- **Filters**: 7 (no starters count — not in SQL function)
- **Player picker**: searchable dropdown per side

**Player comparison scenarios:**
1. Same player, different conditions → 1 row, stats under A vs B, gap column
2. Different players, same conditions → 2 rows, head-to-head
3. Different players, different conditions → 2 rows, different conditions each

## Filters (per side)

| Filter | UI control | Available in |
|--------|-----------|-------------|
| Starters count | dropdown: ≥N or ≤N (1-5) | Teams, Lineups |
| Clutch | margin ±, time remaining | All |
| Home / Away | radio: Home / Away / All | All |
| Win / Loss | radio: Win / Loss / All | All |
| Before / After | single cutoff — date OR game# | All |
| Opponent | multi-select team dropdown | All |
| Game type | radio: Regular / Playoffs / All | All |
| Opponent rank | side (off/def), top-N | All |

Season selector is shared (not per-side).

## Presets

Dropdown above the filter builder, populated per mode:

| Preset | Side A | Side B | Modes |
|--------|--------|--------|-------|
| Starters vs Bench | starters ≥ 3 | starters ≤ 2 | Teams, Lineups |
| Clutch vs Non-Clutch | margin ≤ 5, time ≤ 2:00 | defaults | All |
| Home vs Away | Home | Away | All |
| Win vs Loss | Win | Loss | All |

Selecting a preset fills both sides and triggers compare. User can tweak after.

## Home tab integration

New 5th card on the Home tab:

- **Question**: "How do starters compare to the bench?" (or similar phrasing)
- **Click**: navigates to Compare tab → Teams mode → "Starters vs Bench" preset pre-filled
- **Team filter**: if `home_team` is set on Home tab, carries through (same pending reactive pattern as other cards)

## Visual design

- Dark editorial theme (matches existing app)
- Blue (`#7b8cde`) for Side A, amber (`#e8a435`) for Side B
- Inline dumbbell in last table column: blue dot = A, amber dot = B, line = gap
- Positive values green (`#4caf7d`), negative red (`#e05c5c`)
- Gap column always amber

## Data flow

```
User sets filters → clicks "Compare →"
  → Side A params → SQL function call → result_a
  → Side B params → SQL function call → result_b
  → Join by entity key (team_id / sub_lineup_hash / player_id)
  → Compute gap = metric(A) - metric(B)
  → Sort by gap descending
  → Render table + summary cards
```

## Out of scope

- Cross-season comparisons (both sides use the same season)
- New SQL functions (reuse existing)
- React frontend (Shiny only for now)
- Adding starters filter to player traditional stats SQL

## Mockups

- Visualization options: `.superpowers/brainstorm/starters-vs-bench-options.html`
- Metric column options: `.superpowers/brainstorm/compare-metrics-options.html`
- Full tab layout: `.superpowers/brainstorm/compare-tab-layout.html`
