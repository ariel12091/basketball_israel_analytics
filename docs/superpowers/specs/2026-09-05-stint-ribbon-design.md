# Stint Ribbon — Design

Date: 2026-09-05
Status: design approved, not implemented

## 1. Purpose

Per-game player lanes over the game clock, with the score margin behind them.
Answers "who was on the floor while this game was won or lost", and turns the
Tab 1 on/off number into something you can watch happen in a single game.

Deferred from `docs/superpowers/specs/2026-09-02-app-design-review-design.md:177`,
which called it "the strongest single addition available".

## 2. Decisions

| # | Decision | Rationale |
|---|---|---|
| D1 | Lanes encode **occupancy only**; the verdict lives in the margin curve, drawn emphasised while the hovered player is on and dim while off | Simplest correct v1. No per-stint aggregation, so the offense/defense duplication trap never enters the render path |
| D2 | **Both teams**, clicked team above the curve, opponent mirrored below | The matchup read: which opposing unit was on during a run |
| D3 | **Hover** a lane to split the curve; no pinning | Chart reads as a rotation map at rest; 20 players inspectable in one pass with no clicking |
| D4 | **Server-built inline SVG** from `htmltools`, no plotting library | See §7 |
| D5 | Drill-down from the **game-log tabs** (Tab 4 Israeli, Tab 11 EuroLeague), reusing Tab 2's click→modal spine | Rows are already per `(game_id, team_id)`; no new game picker |
| D6 | **Both leagues in v1**, one canonical frame and two thin readers | A boundary validated by one implementation is the kind `CLAUDE.md` warns "demonstrably drifts" |
| D7 | x-axis extent from **nominal period structure**, never `max(elapsed)` | Verified: Israeli games end ragged (2344, 2351, …, and outliers at 961 and 1754) |

## 3. Data foundation (measured 2026-09-05, not assumed)

### 3.1 Sources

- Israeli: `basketball_test.df_pts_poss_lineups_longer_mv` (segment grain,
  canonical elapsed columns) + `basketball_test.lineups_lookup_on`
  (`lineup_hash` → five ON players; live, not cold storage).
- EuroLeague: `euroleague.matchup_segments_actions` (segment grain, native
  elapsed, both lineups inline) + `euroleague.actions` (running score).

### 3.2 Verified properties

| Property | Israeli | EuroLeague |
|---|---|---|
| Elapsed boundaries | `segment_start/end_elapsed_seconds` | `start/end_elapsed_seconds` |
| Both-team alignment | 10,219 of 10,241 segments (2026) carry both teams with identical boundaries; 22 single-team | 38,904 segments, **all** aligned |
| Perspective column | `type_lineup` ∈ {offense 94,376, defense 94,376, **NULL 74,447**} | **absent** |
| Roster integrity | 4,634 hashes = 5 players; **1 game with a 4-player hash, 14 six-player, 4 seven-player hashes, 10 hashes (664 rows, 4 games) with no roster row** | **5/5 on all 77,808 rows, no exceptions** |
| Zero-length segments | 37 rows in game 115 | **35,136 of 77,808 (45%)** |
| Segments per game (both teams share one timeline) | ~35 distinct, max 56 | ~36 distinct (72 rows at 2 per segment, max 116 rows) |
| Payload per game | ~1,191 MV rows, max 1,634 | ~72 segment rows after the zero-length filter, plus ~565 `actions` rows for the margin |
| Period lengths | quarters 600s, OT 300s | periods 1-4 `10:00`, period 5 `05:00`; max elapsed exactly 2400/2700/3000/3300 |

### 3.3 Why not `stints`

`stints` is the intuitive source and is the wrong one. Verified live: 0 rows,
16 kB — it is in `COLD_TABLES` (`etl/cold_storage.R:6`) and truncated every ETL
run. That part is cheap to undo (~6 MB hot against a 2,962 MB database; the
500 MB free-tier rationale is stale). The disqualifying part is shape:

- **No elapsed columns.** Only `final_start_seg` / `final_end_seg`, documented in
  `docs/database_context.md` as *"Legacy boundary field; canonical durations use
  elapsed columns downstream"* — source seconds-**remaining** space, per quarter.
  Using it means re-deriving the canonical clock inside the one feature whose
  entire geometry is time. That is the game-115 bug class, reopened.
- **`segment_id` means something else**: *"dense rank … repeats across games and
  can repeat across team perspectives"*, versus the shared, boundary-identical
  `segment_id` measured above.
- **No duration column.**
- Nothing reads it — zero MVs or views reference `stints`.

Its one advantage was `lineup_hash_offense` + `lineup_hash_defense` on one row.
Both leagues supply that pairing already (§3.2), so the advantage is void.

## 4. Canonical frame (the adapter contract)

`fetch_stint_ribbon(league, game_id, team_id)` returns:

```
$stints      side ∈ {own, opp}, player_label, player_key,
             start_elapsed, end_elapsed, is_starter
$margin      elapsed, margin        # signed to the clicked team, stepped
$meta        game_label, own_team, opp_team, nominal_seconds, period_seconds[]
$health      NULL, or a reason string when lane data is untrustworthy
```

The renderer consumes only this. Neither reader is visible above it, and no
league name appears in any shared function name.

## 5. Readers

### 5.1 Israeli

```
SELECT DISTINCT team_id, segment_id, lineup_hash,
       segment_start_elapsed_seconds, segment_end_elapsed_seconds,
       MAX(segment_seconds) …
GROUP BY team_id, segment_id, lineup_hash, <boundaries>   -- type_lineup ABSENT
HAVING MAX(segment_seconds) > 0
```

`type_lineup` **must not** appear in the filter or the grouping. Measured: game
115 team 7 has 32 offense and 34 defense segments but **45 under NULL** —
filtering to offense silently drops 13 of 45. This is the same defect as the
unattributed floor time fixed 2026-09-05 (0.586 min/team-game across 92% of
team-games); `player_traditional_stats_mv.segment_times` is the reference
implementation.

Segments are then exploded to players via `lineups_lookup_on`, and `$health` is
set when a hash resolves to ≠5 players or to no roster row at all.

Margin: `own_team_score - opp_team_score` at `event_elapsed_seconds`, from one
team's rows (verified mirrored: 83/71 vs 71/83 in game 115).

### 5.2 EuroLeague

```
SELECT team_id, segment_id, own_lineup, opp_lineup, own_starters, opp_starters,
       start_elapsed_seconds, end_elapsed_seconds
FROM euroleague.matchup_segments_actions
WHERE game_id = $1 AND segment_seconds > 0
```

One row yields both sides; `own_lineup` / `opp_lineup` unnest directly to player
names, so there is no roster join and `$health` is always NULL. Dropping
zero-length rows is load-bearing here, not cosmetic — it removes 45% of rows.

Margin: `actions.points_a` / `points_b` with elapsed derived as
`Σ(prior period lengths) + (period_length − marker_time)`, period length 600s
for periods 1-4 and 300s for period 5+. Verified exact against max elapsed
(2400 / 2700 / 3000 / 3300).

### 5.3 Security prerequisite

`app_readonly` has **no grant** on `euroleague.matchup_segments_actions` (only
`postgres` does). Add it to `sql/security/*.sql` and apply via
`scripts/apply_db_security.R` with `CONFIRM_DB_SECURITY_APPLY=1`. Per
`CLAUDE.md`, these grants are wiped by later `DROP`s — add the table to the
re-grant checklist.

## 6. Transforms (pure, in `helpers.R`)

- `merge_adjacent_stints()` — collapse consecutive segments in which a player
  remains on into one bar, **per player**, not per lineup hash. A player who
  survives a substitution around them keeps one continuous lane. Verified
  necessary: game 115 has runs of up to 6 segments sharing a hash, and
  per-player runs are longer still.
- `ribbon_geometry()` — elapsed → a 1000-unit viewBox space; lane index → y.
  Lane order: starters first, then total floor time descending, within each
  team block.

## 7. Rendering

`renderUI` returns an `htmltools` tag tree: explicit `xmlns`,
`viewBox = "0 0 1000 H"`, `width:100%; height:auto` so scaling happens in CSS.

- one `<rect>` per merged stint;
- one stepped `<path>` for the margin (scores change at discrete events), drawn
  twice — a dim base copy and an emphasised copy;
- one `<clipPath>` per player holding that player's on-intervals as rects;
- quarter/period gridlines from `$meta$period_seconds`, never from observed max.

**Hover** sets the emphasised curve's `clip-path` to the hovered player's
clipPath id — one attribute write, no client-side geometry, ~8 lines of JS in
`app.js`. Pre-rendering one clipped curve copy per player was rejected: the clip
paths are rects, but the curve is ~340 points, so 20 copies is 20× the DOM.

Why not a plotting library: `ggplot2` + `renderPlot` is a raster and cannot
hover at all, and would cost worker CPU on every view and resize on a deploy
whose cold start was fought from 22s down. `ggiraph` is the closest fit but its
hover model is one `data_id` per mark, while this interaction is many-to-one
(ten players are on at any instant), so custom JS is needed anyway — on top of
`ggiraph`'s own SVG. The mapping here is `x = elapsed × scale`,
`y = lane × height`: rectangles and one polyline, with no statistical layer to
outsource. `app/` currently loads no plotting package at all (`ggplot2` appears
only in `scripts/analysis/`, which writes PNGs into `docs/`).

## 8. Interaction and placement

The game-log DT renders its date/GN cell as a link carrying `data-game-id` /
`data-team-id`. A delegated handler in `app.js` — sibling to the existing
`ld_lineup_click`, routed through the same queue-and-replay helper so a click
before `shiny:connected` is not swallowed — fires `<prefix>_ribbon_click`. The
tab server answers with `modalDialog(size = "xl", easyClose = TRUE)`.

On touch, where hover does not exist, the same handler binds tap-to-toggle. The
chart still reads as a rotation map without it.

Each lane carries a `<title>` for native tooltip and screen-reader text. Hover
involves no motion, so `prefers-reduced-motion` needs no branch.

## 9. Testing

Transforms are pure functions over frames, covered directly in testthat:
adjacent-run merging, zero-length dropping, the ≠5-player and unmatched-roster
branches, nominal-vs-observed axis extent, and the EuroLeague elapsed
derivation including OT. One regression test asserts segment counts match the
`type_lineup`-absent grouping, so nobody reintroduces the offense filter.

Per `CLAUDE.md`, only impure pieces are stubbed in `helper-server-mocks.R`;
no helper implementation is copied into the mocks.

## 10. Scope

**In:** both readers, the canonical frame, the shared renderer, hover split,
Tab 4 and Tab 11 wiring, the EuroLeague grant.

**Out, deferred by decision:**

- **Per-stint outcome tinting** (lane tinted by net points/PPP over its
  interval). A `GROUP BY` on the data side, a rendering project on the UI side.
  The offense/defense duplication becomes a live trap the moment this lands.
- **Cross-game aggregated rotation view** (lanes over a normalised clock).
- Any use of `stints`, hot or cold.

## 11. Known traps

1. Never filter or group on `type_lineup` (Israeli) — §5.1.
2. Never take the axis extent from `max(elapsed)` — §2 D7.
3. Drop zero-length segments before laying out lanes, or they consume lane
   slots while rendering as invisible slivers.
4. Merge per player, not per lineup hash.
5. `IBPL_CACHE_UI=false` while editing `www/app.css` or `www/app.js`.
6. Launch with Run App / `runApp()`, never select-all + Ctrl+Enter.
7. Re-run `apply_db_security.R` after any `DROP` that touches the new grant.
