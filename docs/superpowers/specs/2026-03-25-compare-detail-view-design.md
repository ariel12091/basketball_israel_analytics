# Compare Tab — Detail View Design

**Date:** 2026-03-25
**Scope:** Presentation-only change to the Compare tab (Tab 7) in the Shiny app. No new SQL functions or data sources.

## Overview

Add a "Detail" view to the Compare tab's Teams and Lineups modes. Currently, these modes show a league-wide DT table sorted by gap for a single selected metric chip. The Detail view shows ALL metrics for a single team/lineup in a side-by-side card layout with diverging gap bars.

## Entry Points

1. **Row click** — clicking a row in the league-wide DT table drills into that team/lineup's Detail view.
2. **Toggle + dropdown** — a "League / Detail" toggle at the top. In Detail mode, a dropdown lets the user pick which team/lineup to inspect without going through the table first.
3. **Back button** — "← Back to league view" returns to the DT table.

## Detail View Layout

### Context Bar

A single horizontal bar above the comparison grid, outside the grid structure:

```
[A] 14 GP · 11-3 · 1,842 Poss  |  [B] 13 GP · 7-6 · 1,756 Poss
```

- A/B badges use existing side colors (blue A, amber B)
- GP, W, L, Poss are not compared — no gap bars for these

### Entity Header

- Team name (or lineup player names) centered above the context bar
- Filter summary below (e.g. "Home vs Away · 2024-25")

### Comparison Grid

Three-column CSS grid: Side A card | Gap column | Side B card. All three share grid rows so stat values align horizontally with their gap bars.

Column headers: "A · Home" (blue) | "Gap ↕" (sortable) | "B · Away" (amber)

Single centered section titles between groups (not duplicated per column):

**Ratings section:**
- Win% (higher = better)
- Net Rtg (higher = better)
- Off Rtg (higher = better) — PPP × 100
- Def Rtg (lower = better) — PPP × 100

**Offensive Four Factors section:**
- TS% (higher = better)
- TOV% (lower = better)
- OREB% (higher = better)
- FTR (higher = better)

All displayed as percentages with % suffix.

**Defensive Four Factors section:**
- Opp TS% (lower = better)
- Opp TOV% (higher = better — forcing turnovers)
- Opp OREB% (lower = better)
- Opp FTR (lower = better)

All displayed as percentages with % suffix.

### Gap Convention

The gap encodes two pieces of information:

1. **Sign** = is having more of this stat good (+) or bad (−)
   - "Higher is better" metrics: gap = max(A,B) − min(A,B) = always positive
   - "Lower is better" metrics: gap = min(A,B) − max(A,B) = always negative

2. **Color and bar direction** = who is better on this metric
   - Blue (bar toward A): A wins
   - Amber (bar toward B): B wins
   - Neutral metrics (shot shares, GP, minutes) have no better side, so there
     direction falls back to whichever side has the higher raw value

3. **Stat value colors** = polarity-aware winner/loser
   - White (winner): the side that is actually better for this metric
   - Gray (loser): the side that is actually worse

**Reading guide:**
| Pattern | Meaning |
|---------|---------|
| +blue→A | more is good, A has more → A wins |
| +amber→B | more is good, B has more → B wins |
| −blue→A | more is bad, A has less → A wins |
| −amber→B | more is bad, B has less → B wins |

> **Changed 2026-09-03.** Colour and bar direction previously followed *who has
> the higher raw value*, independently of polarity, and the winner was derived
> by combining the sign with the direction: `−amber→B` meant "B has more, more
> is bad, therefore A wins". That made the bar point at the loser on every
> lower-is-better metric, contradicting the winner/loser styling on the values
> beside it. Direction now follows the winner directly. The sign still says
> whether more of the stat is good, and the two raw values are printed either
> side of the bar, so "who has more" is still visible without being encoded
> twice. Polarity is read from `TEAM_RATING_METRICS$best_direction` in
> `app/R/helpers.R` — the same contract that decides which direction earns the
> better percentile rank — not from a list kept in the Compare tab.

**Polarity table:**

| Metric | Better when | Sign |
|--------|-------------|------|
| Win% | Higher | + |
| Net Rtg | Higher | + |
| Off Rtg | Higher | + |
| Def Rtg | Lower | − |
| TS% | Higher | + |
| TOV% | Lower | − |
| OREB% | Higher | + |
| FTR | Higher | + |
| Opp TS% | Lower | − |
| Opp TOV% | Higher | + |
| Opp OREB% | Lower | − |
| Opp FTR | Lower | − |

### Diverging Bar

- Centered zero line (thin white line at 50%)
- Bar extends LEFT (toward A) when A wins → blue
- Bar extends RIGHT (toward B) when B wins → amber
- Neutral metrics have no winner, so direction falls back to the higher raw value
- Bar width proportional to |gap| relative to the largest gap in the section

### Sorting

Click the "Gap ↕" header to cycle:
1. Default order (Ratings → Off FF → Def FF)
2. Largest |gap| first (within each section)
3. Smallest |gap| first (within each section)

Sorting reorders rows within each section independently — sections themselves don't reorder.

## Data Sources

No new SQL. The Detail view calls the same functions the league-wide view already uses, but fetches BOTH Summary (ratings) AND Four Factors data for the selected entity:

**Teams mode:**
- `run_team_ratings(params)` → Net Rtg, Off Rtg, Def Rtg, Win%, GP, W, L, Poss
- `run_team_ff(params)` → TS%, TOV%, OREB%, FTR (offensive and defensive)

**Lineups mode:**
- `run_lineups_summary(params)` → Net Rtg, Off Rtg, Def Rtg, Poss
- `run_lineups_ff(params)` → TS%, TOV%, OREB%, FTR (offensive and defensive)

The existing `cmp_joined` reactive only fetches one metric source at a time (ratings OR FF based on selected chip). The Detail view needs a **dedicated reactive** (`cmp_detail_data`) that calls BOTH ratings AND FF for each side, then joins them for the selected entity. This reactive only fires when `selected_detail_entity` is non-NULL, avoiding unnecessary queries in league view.

For lineups, Win% / GP / W / L may not be available — context bar should gracefully omit missing fields, and the Win% row in the Ratings section should be hidden entirely in Lineups mode.

## Formatting

Matches existing Shiny app conventions:
- Off Rtg / Def Rtg: PPP × 100 (e.g. 112.3)
- Net Rtg: Off Rtg − Def Rtg (e.g. +8.2)
- TS%, TOV%, OREB%, FTR: percentages with % suffix (e.g. 58.2%)
- Win%: percentage with % suffix
- GP, W, L: integers
- Poss: integer with comma separator

## Applies to Both Modes

The Detail view works identically in Teams and Lineups modes:
- **Teams:** entity = team_name, context bar shows GP/W/L/Poss
- **Lineups:** entity = player_names_str, context bar shows available fields (Poss at minimum)

## UI State

- `selected_detail_entity` reactiveVal — NULL (league view) or the entity key (team_id or sub_lineup_hash)
- Row click sets it, back button clears it
- Toggle switch also controls it
- Mode change (Teams ↔ Lineups ↔ Players) resets to NULL
- Filter change while in Detail mode: stay in Detail, re-fetch data. If the entity is no longer in results, show "No data for this entity with current filters" with the back button.
- Toggle to Detail without prior row click: show dropdown with placeholder prompt, no data until entity is selected.
- Row click sets both the toggle state AND the dropdown selection.

## Edge Cases

- **Equal values:** When A = B for a metric, both values display in white, gap shows ±0.0, no bar drawn.
- **Entity missing from one data source:** Show available sections, display "—" for metrics from the unavailable source.
- **Sorting does not recalculate bar widths** — max gap per section is computed once on render, not on sort.

## Mockup Reference

Final approved mockup: `.superpowers/brainstorm/compare-detail-mockup-v10.html`
