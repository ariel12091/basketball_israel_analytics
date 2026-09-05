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
| D8 | **One query per ribbon open**, returning one row with `lanes` and `margin` as `jsonb` | Budget is 500 ms and the round-trip floor is 238 ms; two queries fail on latency alone. §5.4 |

## 3. Data foundation (measured 2026-09-05, not assumed)

### 3.1 Sources

- Israeli: `basketball_test.df_pts_poss_lineups_longer_mv` (segment grain,
  canonical elapsed columns) + `basketball_test.lineups_lookup_on`
  (`lineup_hash` → five ON players; live, not cold storage).
- EuroLeague: `euroleague.matchup_segments_actions` (segment grain, native
  elapsed) + `euroleague.lineup_totals_by_game` (the existing lineup →
  `player_ids` link, already granted) + `euroleague.actions` (running score).

### 3.2 Verified properties

| Property | Israeli | EuroLeague |
|---|---|---|
| Elapsed boundaries | `segment_start/end_elapsed_seconds` | `start/end_elapsed_seconds` |
| Both-team alignment | 10,219 of 10,241 segments (2026) carry both teams with identical boundaries; 22 single-team | 38,904 segments, **all** aligned |
| Perspective column | `type_lineup` ∈ {offense 94,376, defense 94,376, **NULL 74,447**} | **absent** |
| Roster integrity | 4,634 hashes = 5 players; 1 game with a 4-player hash, 14 six-player, 4 seven-player, 10 hashes (4 games) with no roster row — all excluded by the app's existing `cardinality = 5` guard | **5/5 on all 77,808 rows, no exceptions** |
| Lineup → player_id link | `lineups_lookup_on`, expanded as in `fetch_lineups_all.sql:205-214` | `lineup_totals_by_game.player_ids` — **all 22,597 segment lineups resolve, zero fan-out** |
| Name-as-key safety | **unsafe**: a same-name/different-id pair inside one (game, team); team 4 has `NEW NEW` across 3 ids | **unsafe**: `own_lineup` and `player_ids` are sorted independently — **31,907 of 40,000 positional pairs mismatch** |
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
$stints      side ∈ {own, opp}, player_key, player_label,
             start_elapsed, end_elapsed, is_starter
$margin      elapsed, margin        # signed to the clicked team, stepped
$meta        n_periods              # game_label added by the calling observer
$health      NULL, or a reason string when segments were excluded
```

The renderer consumes only this. Neither reader is visible above it, and no
league name appears in any shared function name.

**`player_key` is always the `player_id`, never a name.** Both leagues carry
same-name/different-id players on one team — Israeli data has such a pair inside
a single (game, team), and team 4 carries the name `NEW NEW` across three
distinct ids. A name key does not lose a lane; it silently *merges two people's
floor time into one*, which is a wrong answer rather than a missing one. Names
are display labels, always fetched **by id**.

**`is_starter` is not read from either league's boxscore.** It is derived in the
pure layer by `ribbon_mark_starters()` (§6) from whoever is on the floor in a
side's earliest segment: measured exactly 5 per team-game across 1,178
EuroLeague and 878 Israeli team-games, while the EuroLeague boxscore carries 40
stray flags. One definition, both leagues, no extra column.

## 5. Readers

### 5.1 Israeli

```
SELECT DISTINCT team_id, segment_id, lineup_hash,
       segment_start_elapsed_seconds, segment_end_elapsed_seconds,
       MAX(segment_seconds) …
GROUP BY team_id, segment_id, lineup_hash, <boundaries>   -- type_lineup ABSENT
HAVING MAX(segment_seconds) > 0
```

`type_lineup` **must not** appear in the filter or the grouping.

The NULL rows are exactly two action types — `substitution` (146,155) and
`timeout` (6,540). They are not a hidden third perspective, and dropping them
costs little: of 9,495 NULL-only segments, 8,961 are zero-length and are
discarded anyway, leaving 534 segments worth 2,896 seconds — **0.14%** of floor
time, about 1.2 segments per team-game averaging 5.4 seconds, roughly two
viewBox units wide.

The filter that actually does damage is **offense-only**, which loses 30,860
seconds — **1.46%**, ten times worse. That is the defect behind the
unattributed floor time fixed 2026-09-05 (0.586 min/team-game across 92% of
team-games); `player_traditional_stats_mv.segment_times` is the reference
implementation.

So the rule is not "NULL rows are precious" — it is that no `type_lineup`
predicate is needed at all. Adding one buys nothing, and filtering to
`IS NOT NULL` reintroduces a seam risk when adjacent runs are merged (§6).

This collapse is not a new pattern: `server_tab3.R:1327-1345` already groups the
same MV by `(team_id, game_id, lineup_hash, segment_id)` without `type_lineup`
and takes `MAX(segment_seconds)`. The ribbon's **only** departure from every
existing reader is that it keeps the interval — `MIN(start_elapsed)` /
`MAX(end_elapsed)` — instead of collapsing a segment to a duration. Two extra
columns on an established query.

Segments are exploded to players with the canonical expansion copied from
`fetch_lineups_all.sql:205-214`, the version behind Tab 2:

```
SELECT l.team_id, l.lineup_hash,
       ARRAY_AGG(DISTINCT l.player_id ORDER BY l.player_id)::int4[] AS player_ids
FROM basketball_test.lineups_lookup_on l
WHERE l.game_year = <season>
GROUP BY l.team_id, l.lineup_hash
HAVING cardinality(ARRAY_AGG(DISTINCT l.player_id)) = 5
```

The `cardinality = 5` guard is the app's existing convention for odd-sized
lineups — exclude them — so the ribbon follows it rather than inventing its own
handling. `$health` is then a single count of segments that guard dropped,
existing only so the modal can say the lanes are incomplete instead of showing a
silent gap. It affects roughly 4-6 games in 221.

Labels come from `full_rosters` joined on `(game_id, team_id, player_id)`. The
`game_id` predicate is load-bearing: the provider reuses an id for a **different
person in specific games** (id 2060 is Josh Hagins season-wide but J'Von
McCormick in ~7), so a season-level name map would mislabel exactly those games.

Margin: `own_team_score - opp_team_score` at `event_elapsed_seconds`, from one
team's rows (verified mirrored: 83/71 vs 71/83 in game 115).

### 5.2 EuroLeague

```
SELECT team_id, segment_id, start_elapsed_seconds, end_elapsed_seconds, player_ids
FROM euroleague.ribbon_segments_v
WHERE game_id = $1
```

`matchup_segments_actions` stores lineups as `text[]` of **names** and carries no
ids. Resolving those names against the roster one by one would be a name-keyed
join — the shape that manufactured 258 false findings in the 2026-08-19
data-quality report. Instead the view reuses the lineup → `player_id` link the
on/off system is already built on: `lineup_totals_by_game` holds `own_lineup`
and `player_ids` for the same lineup, is already granted to `app_readonly`, and
covers **all 22,597 distinct segment lineups with zero fan-out**.

**The two arrays must never be paired positionally.** `own_lineup` and
`player_ids` are each sorted independently — names alphabetically, ids ascending
— so `names[i]` is unrelated to `ids[i]`: measured **31,907 mismatches in 40,000
pairs**. Only the id *set* is trustworthy; labels come from `full_rosters` by id
(12,674 player-games, 0 unresolved). Pairing positionally would mislabel four
lanes in five, and would look entirely plausible on screen.

`opp_lineup` is not used at all: both teams have a row for every segment, so the
opponent's lanes are simply the other team's own rows. `$health` is always NULL
here — EuroLeague has no odd-sized-lineup defect. Dropping zero-length rows is
load-bearing, not cosmetic — it removes 45% of rows.

Margin: `actions.points_a` / `points_b`, with elapsed derived as
`Σ(prior period lengths) + (period_length − marker_time)` — period length 600s
for periods 1-4, 300s for period 5+. Verified exact against observed max elapsed
(2400 / 2700 / 3000 / 3300). This derivation lives in `ribbon_margin_v`
(§5.3), not in R.

### 5.3 Security prerequisite — two narrow views, not two table grants

The EuroLeague tabs work today without any of this because they never read the
segment grain. `app_readonly` can `SELECT` 20 euro relations — every MV and
aggregate the tabs use (`lineup_totals_by_game`, `sub_lineups_stats_mv`,
`player_onoff_default_mv`, `final_schedule_mv`, …) — and is denied on 13, all
ETL intermediates, raw play-by-play, or QA tables. That is a deliberate
least-privilege boundary.

The ribbon is the first app feature needing segment-grain EuroLeague data, and
it needs **two** denied relations: `matchup_segments_actions` (lanes) and
`actions` (margin curve). Granting blanket `SELECT` on `actions` — 211 MB of raw
play-by-play, 40 columns including provider ids, parser traces and QA fields —
to draw a score line is the wrong trade.

Instead, add two narrow views in `euroleague` and grant `SELECT` on **those**:

- `ribbon_segments_v` — `game_id, team_id, segment_id, start_elapsed_seconds,
  end_elapsed_seconds, player_ids`, already filtered to `segment_seconds > 0`.
  It joins `lineup_totals_by_game` on `(game_id, team_id, own_lineup)` so the
  view emits **ids, never names**, and exposes no `opp_lineup` — the opponent's
  lanes are the other team's own rows.
- `ribbon_margin_v` — `game_id, elapsed_seconds, points_a, points_b`, with the
  period/`marker_time` → elapsed derivation (§5.2) done in the view so the
  clock rule lives in one place rather than in R.

This keeps the app's least-privilege posture intact, gives the reader a stable
contract, and puts the EuroLeague clock derivation next to the data it derives
from. Register both in `sql/security/*.sql` and apply via
`scripts/apply_db_security.R` with `CONFIRM_DB_SECURITY_APPLY=1`. Per
`CLAUDE.md`, `DROP`s wipe these grants — add both views to the re-grant
checklist, and note that changing either view means `DROP`+`CREATE`, hence
re-granting.

### 5.4 Performance budget — one round trip, non-negotiable

**Budget: under 500 ms per ribbon open.** Measured 2026-09-05; every number is a
median of n=7 unless stated.

The binding constraint is transport, not SQL. A bare `SELECT 1` on the pooler
costs **238 ms** (n=10, min 236, max 245). Server-side execution for the real
queries is 0.5-21.6 ms. So each round trip costs ~240 ms regardless of what it
asks for, and **two sequential queries spend the whole budget on latency alone**.

| Form | Israeli | EuroLeague |
|---|---|---|
| segments only | 240.6 ms (1.3 ms server) | — |
| margin only | 243-250 ms | — |
| segments + roster join | 256-260 ms (19.1 ms server) | — |
| **two queries, sequential** | **~490-510 ms — fails** | — |
| one query, `UNION ALL` | 256-269 ms (18.6 ms server) | 240-247 ms (0.5 ms server) |
| **one query, two `jsonb` columns** | **268-272 ms (21.6 ms server)** | — |

**Decision: one query per ribbon open, returning a single row with two `jsonb`
columns — `lanes` and `margin`.** It costs ~10 ms more than the `UNION ALL`
form and avoids stuffing margin values into generically-named lane columns, so
the reader gets a typed, self-describing contract. Payload is 29-39 KB per game.

Consequences for implementation:

- Never issue a second query to complete a ribbon. The roster explosion joins
  inside the query; the EuroLeague clock derivation lives in `ribbon_margin_v`
  (§5.3) so it costs no extra trip.
- Optimising the SQL is not the lever — 21.6 ms of a 270 ms open is 8%.
  Round-trip count is the only thing that moves this number.
- Cache the result per `game_id` + `shared_data_version(shared)`, following the
  `GL_DATA_CACHE` pattern Tab 4 already uses, so re-opening a ribbon costs
  nothing.

Headroom: ~230 ms of the budget is unused, which is the margin for tail latency.
One sample of 489 ms was observed under contention across ~90 timed calls. Note
these were measured with `C:` at 99% full; a past session recorded 2x timing
swings on a full disk, so treat the tails as pessimistic and the medians —
which were stable within a few ms — as sound.

## 6. Transforms (pure, in `helpers.R`)

- `merge_adjacent_stints()` — collapse consecutive segments in which a player
  remains on into one bar, **per player**, not per lineup hash. A player who
  survives a substitution around them keeps one continuous lane. Verified
  necessary: game 115 has runs of up to 6 segments sharing a hash, and
  per-player runs are longer still.
- `ribbon_mark_starters()` — flag whoever is on the floor in a side's earliest
  segment. One definition for both leagues, replacing two different boxscore
  columns; verified exactly 5 per team-game in 1,178 EuroLeague and 878 Israeli
  team-games.
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
Tab 4 and Tab 11 wiring, and the two EuroLeague views with their grants.

**Out, deferred by decision:**

- **Per-stint outcome tinting** (lane tinted by net points/PPP over its
  interval). A `GROUP BY` on the data side, a rendering project on the UI side.
  The offense/defense duplication becomes a live trap the moment this lands.
- **Cross-game aggregated rotation view** (lanes over a normalised clock).
- Any use of `stints`, hot or cold.

## 11. Known traps

1. Never filter to `type_lineup = 'offense'` (Israeli) — it loses 1.46% of floor
   time. No `type_lineup` predicate is needed at all; the NULL rows are subs and
   timeouts and are worth 0.14%, nearly all of it zero-length. §5.1.
2. Never take the axis extent from `max(elapsed)` — §2 D7.
3. Drop zero-length segments before laying out lanes, or they consume lane
   slots while rendering as invisible slivers.
4. Merge per player, not per lineup hash.
5. **Never key a lane on a player name**, and never pair a name array with an id
   array positionally. Both leagues carry same-name/different-id players, and the
   EuroLeague arrays are independently sorted (31,907/40,000 positional
   mismatches). Key on `player_id`; fetch labels by id.
6. Take the lineup → player_id link from what exists (`lineups_lookup_on` via
   the `fetch_lineups_all.sql` expansion; `lineup_totals_by_game` for
   EuroLeague) rather than resolving names.
7. `IBPL_CACHE_UI=false` while editing `www/app.css` or `www/app.js`.
8. Launch with Run App / `runApp()`, never select-all + Ctrl+Enter.
9. Re-run `apply_db_security.R` after any `DROP` touching `ribbon_segments_v` or
   `ribbon_margin_v` — editing either view means `DROP`+`CREATE`, which wipes
   the grant.
10. The EuroLeague tabs read only pre-aggregated relations today; `actions` and
    `matchup_segments_actions` are denied to `app_readonly` by design. Do not
    "fix" that with a table-level grant.
11. Never add a second query to a ribbon open. The round-trip floor is 238 ms, so
    a second trip costs more than the entire rest of the feature. §5.4.
