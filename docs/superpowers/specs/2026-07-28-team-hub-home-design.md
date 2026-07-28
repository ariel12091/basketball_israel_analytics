# Team Hub on Home — Design Spec

**Date:** 2026-07-28
**Status:** Approved design, pending implementation plan
**Goal:** Lower the adoption barrier. New visitors currently land on a static card grid and every tab greets them with a dense table — work before reward. Home becomes a team hub that leads with findings, zero clicks required.

**Audience:** hoops-literate fans, coaches/team staff, media. Advanced-stat jargon is fine; the fix is leading with interesting content, not simplifying metrics.

## Decisions made during brainstorming

- Barriers identified: wall of numbers, no narrative. NOT jargon, NOT filters.
- Surface: Home page morphs into a team hub (no new tab). Per-tab headline strips are Phase 2.
- Week-to-week league dashboards rejected as too volatile; hub content is season-level (last-10 form allowed as a storyline).
- A team is **always selected by default**: remembered team (localStorage) for returning visitors, current net-rating leader for first-timers.
- The Compare tab is a narrative engine: storylines are preset comparisons rendered as one-line findings that click into Compare preloaded.

## Section 1: Home page layout

Home keeps its title bar. The team selector gains prominence and always has a value. Below it, four hub blocks, then the existing nav cards (now pre-filtered to the team):

```
IBPL Analytics
[ Team: <selected team> ▼ ]

┌─ Identity ────────────────────────────────┐
│ 14–6 · 3rd | Off 112.4 (4th) Def 105.1    │
│ (6th) Net +7.3 (3rd) | FF mini-row vs lg  │
└───────────────────────────────────────────┘
┌─ Key players ──────────┐ ┌─ Lineups ──────────────┐
│ Top 5 by on/off        │ │ Best & worst 5-man     │
│ impact + top scorer    │ │ unit (min-poss qual.)  │
└────────────────────────┘ └────────────────────────┘
┌─ Storylines ──────────────────────────────┐
│ • Bench outscores starters by +4.1        │
│ • Clutch net rating: −6.2 (11th)     …    │
└───────────────────────────────────────────┘

[ nav cards, pre-filtered to the team ]
```

- Every number with a home elsewhere in the app is clickable and jumps to that tab pre-filtered.
- Changing the dropdown re-renders the hub and updates localStorage.
- The hub is a self-contained module: `app/R/mod_team_hub.R`, embedded by Tab 0. Home's server file stays thin.
- Mobile: blocks stack vertically in the order shown; nav cards collapse to a compact list.
- Styling follows the existing dark editorial theme (app/www/app.css; amber accent, DM Sans/JetBrains Mono).

## Section 2: Data sources & caching

No new MVs or SQL functions in v1. All pulls cached cross-session keyed on `game_year` + `shared_data_version(shared)`:

| Block | Source | Notes |
|-------|--------|-------|
| Identity card | `team_ppp_ratings_mv` + `team_four_factors_mv` | Season-wide pull via `cached_season_df()`; league ranks computed in R from the same pull |
| Key players | `onoff_default_mv` (all players; min-poss filter in R) + `player_traditional_stats_mv` | Up to 5 players by on/off rating diff, qualified at ≥100 ON possessions (the app's existing unranked threshold), with the Tab 1 `est. ±X pts` annotation; top scorer from traditional stats |
| Best/worst lineups | `fetch_lineups_csv_v2()` with default filters | Best and worst by net PPP among lineups with ≥100 possessions; one call per team per season, cached cross-session per team — league pays once per team per ETL cycle |
| Storylines | See Section 3 | Cheap paths only in v1 |

**v1 storyline set** (all use existing cheap paths):

1. **Starters vs bench** — uses the starters fast path (pre-aggregates, merged 2026-07-27).
2. **Clutch vs overall** — `get_team_ratings_dynamic()` with clutch params (Tab 3 clutch path).
3. **Last 10 games vs season** — the `schedule_ranked` last-N windowed-CTE pattern.

Storylines needing new slow filtered SQL are out of v1; additions require profiling first. The default team's hub (league leader) joins the existing prewarm so first paint is instant.

## Section 3: Storyline engine & deep-linking

**Storyline engine:** one spec list; each entry = `(id, fetcher, sentence template, Compare preset payload, min sample size in possessions)`. A single loop renders all entries — adding a storyline is one list entry, no new control flow. Below min sample size the line is skipped entirely (never shown gray — the hub must not lead with caveats). Numbers use the app's existing polarity coloring conventions.

**Deep-linking** (all via existing pending-state reactives + tab switch):

- Key players → Tab 1, team pre-selected
- Lineup rows → Tab 2 via `pending_ld_team`
- Identity numbers → Tab 3
- Storylines → Compare via `pending_compare_preset` carrying the A/B definitions (e.g., A = starters, B = bench, team fixed)
- Nav cards keep today's behavior but pass the hub's team along

**Remembered team:** a small JS handler stores `team_id` in localStorage on change and reports it at session start (same mechanism family as the idle-session state restore). On restore, validate against the current season's team list; if absent (season change, provider id drift), fall back to the net-rating leader. Fail-closed — the restored value is untrusted client input.

## Section 4: Edge cases & testing

- **Early season / thin data:** identity card always renders (it is just the MV row); players/lineups render only with min-poss survivors; storylines self-skip below their sample threshold. Empty blocks collapse — no placeholders.
- **Testing:** pure logic (rank computation, storyline qualification, localStorage validation fallback) lives in `R/helpers.R`-style pure functions, covered by testthat using the existing mock pattern (`helper-server-mocks.R`); no DB in tests.

## Phase 2 (separate spec → plan → implementation cycle)

Per-tab headline strips: 2–4 computed takeaway cards above each tab's table, driven by the same spec-list pattern and reusing the hub's storyline-card component, computed from the tab's current filtered data. Not part of this implementation.

## Out of scope

- New MVs, new SQL functions, or changes to existing ones
- League-wide "this week" dashboard content
- React frontend port (intentional drift; Shiny first)
- Casual-fan jargon simplification
