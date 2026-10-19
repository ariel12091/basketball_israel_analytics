# Shot Profile (rim + corner-3 shot-diet view) — Design

**Date:** 2026-07-16. **Status:** draft for review.
**Evidence base:** `docs/shot_type_interior_analysis.md` § "Shot generation vs offensive quality" (+ Appendix). Analysis scripts: `scripts/analysis/shotgen_*.R`.

## Goal

Add a descriptive **"Shot Profile"** display mode showing how the shot diet
(lay-up / dunk / 3PA with corner split / mid-range shares of FGA) differs:

- **Tab 1 (On/Off Impact):** team shot diet with player ON vs OFF (offense + defense sides), shares as ON / OFF / Δ.
- **Tab 3 (Team Ratings):** team-season shot-diet shares, offense + defense.
- **Tab 7 (Compare):** both of the above through the existing "reuses tab 1/3 paths per compare mode" mechanism.
- **Both paths from day one:** fast (MV) and filtered (SQL functions).

## Non-goals (analysis-mandated)

1. **No `est. ±pts` annotation and no impact framing.** ~90% of shot-generation's rating impact is mediated by eFG — a separate impact number would double-count the existing FF eFG annotation. Framing is "how the shot diet changes", period.
2. **No location-based RA / paint-non-RA columns.** Coordinates support only coarse cuts (user decision 2026-07-13); the live rim metric stays tag-based (lay-up/dunk/oop), with lay-up and dunk kept separate (opposite team-level meaning).
3. **Corner-3 is descriptive detail** (corner vs above-break split of 3PA). The analysis found no team-level signal; it is included for scouting texture at the user's request, not as a value claim.
4. **React/Plumber (frontend-v2) out of scope** — follow-up; the Plumber rename functions remain the contract point when it happens.
5. No xPPS-of-mix headline in v1 (possible later addition once shares ship).

## Architecture

### 1. `shot_zones` persistent table (new)

Corner-3 needs `parameters_coord_y`, which exists only in `actions_clean`
(cold storage, truncated after each ETL). Solution: a small **persistent**
table, never cold-purged:

```sql
CREATE TABLE basketball_test.shot_zones (
  game_id    int  NOT NULL,
  id         int  NOT NULL,   -- action id (same key as actions_clean / MV join)
  is_corner3 bool NOT NULL,
  PRIMARY KEY (game_id, id)
);
```

- Rows: one per 3PT shot only (~23k over two seasons; storing only 3s keeps it
  minimal — corner flag is meaningless for 2s). ~1–2 MB incl. index.
- **Corner rule = committed v2 cut, pure SQL:** `parameters_points = 3 AND
  parameters_coord_y <= 285` (raw provider units; ÷100 = court units, tangent
  line y ≤ 2.85 from `fiba_court_zones.R::corner3_height`). No geometry lib.
- **Incremental population:** new ETL step at the end of Phase 2 (while
  `actions_clean` is populated), upsert by PK for the games processed this run.
- **One-time backfill:** script reading `exports/cold/actions_clean.parquet`
  (+ recovered gap games, below) → INSERT ... ON CONFLICT DO NOTHING.
- Shots with NULL coords (or absent from the table): `is_corner3` treated as
  NULL → counted in 3PA share but excluded from the corner/AB split
  (fail-open to "unknown", never misclassified).

### 2. Cold-export gap fix (rider)

The cumulative parquet lags the MV by ~8 games (game_ids ~365–388) — those
games' coordinates currently exist nowhere queryable. In-scope tasks:

1. Diagnose why Phase 7's export missed them (export runs before TRUNCATE by
   design, so this is a bug or a mid-run failure ordering issue); fix so
   export provably covers every game processed in the run (add a row-count /
   game-id reconciliation check to `export_cold_table()`).
2. Recover the missing games: re-fetch their raw JSON via the existing
   per-game ETL path (they're in `etl_processed_games`) into `actions_clean`,
   re-export to parquet, populate `shot_zones`, then purge again.
3. Validation: parquet distinct game_ids == `etl_processed_games` game_ids.

### 3. MV changes (`sql/` branch; DROP+CREATE, not REFRESH)

**`onoff_default_mv`** — extend the existing `shot_agg` CTE flags
(structurally identical to `fg2/fg3`); new flags: `layup_att/made`,
`dunk_att/made` (dunk = dunk+allyhoop, tag-based from `parameters_type`),
`c3_att/made` (LEFT JOIN `shot_zones` ON (game_id, id)). Pivoted naming
follows the existing convention: `{off,def}_{on,off}_{layup,dunk,c3}_{att,made}`
→ **24 new columns**. Mid-range and above-break are derived in R
(`mid = fga − rim − fg3`; `ab3 = fg3 − c3`), not stored.

**`team_ppp_ratings_mv`** — add per (game_year, team_id) offense+defense
counts: `{off,def}_{fga,layup_att,dunk_att,fg3_att,c3_att}` → 10 new columns
(shares derived in R). Same `type_lineup` attribution as the existing base CTE.

Dependency check before merge: confirm no L3/L4 MV reads either of these
(expected: both are app-facing leaves; verify with pg_depend / grep of sql/).
Rebuild via `rebuild_all_mvs(from_level = 2)` after `shot_zones` exists.

### 4. SQL function changes (filtered-path parity)

- **`onoff_compute` (14 params):** add the same 24 outputs to its result set
  (it already computes from `df_pts_poss_lineups_longer_mv`, which has
  `parameters_type`; corner via LEFT JOIN `shot_zones`).
- **`get_team_ratings_dynamic` (14 params):** add the 10 team counts.
- `four_factors_compute` / `get_team_four_factors_dynamic`: **untouched**
  (Shot Profile is a sibling of Summary, not of Four Factors).
- `DROP FUNCTION` with exact signatures (pitfall list), redeploy, and update
  the `app_readonly` EXECUTE allowlist + `test-db-security-contracts.R` if
  signatures change.
- **Fast-path gate rule** honored: explicit full-season window must hit the
  same MV path as NULL dates.

### 5. Shiny app changes

- **Tab 1:** display-mode toggle becomes Summary / Four Factors / **Shot
  Profile**. Columns (offense side, mirrored for defense): Poss ON, lay-up
  share ON/OFF/Δ, dunk share ON/OFF/Δ, rim total Δ, 3PA share ON/OFF/Δ, a
  dedicated "Corner 3 Share" column (ON/OFF/Δ; renders "—" when the corner flag
  is NULL), mid share Δ. Rendering: existing
  continuous-gradient cell approach (ShotCell/HeatCell conventions), defense
  sign flip per the color-polarity rules, min-poss muting as in Summary.
  Formatting in JS `columnDefs` (never `formatRound`), `row` guard in renders.
- **Tab 3:** same toggle; columns are plain shares (no ON/OFF split) +
  existing W/L/poss context columns.
- **Tab 7:** register Shot Profile as a compare display for the tab-1 and
  tab-3 compare modes; two-sided A/B splits follow the existing compare
  contract (`server_tab7_compare.R`), Δ columns compare A vs B.
- Caches: fast-path pulls flow through the existing `cached_season_df()` /
  `GL_DATA_CACHE` keys (same dataset, more columns — no new cache keys).
- CSV export includes the share columns; chips/tooltips get a one-line
  explanation ("share of team FGA while player is on/off the floor").
- Wording: "Shot Profile — how the team's shot diet shifts" + tooltip noting
  descriptive nature. No est.± anywhere.

### 6. Tests

- `tests/testthat`: contract tests per touched tab (column presence, share
  arithmetic incl. mid/ab3 derivation and NULL-corner handling, defense sign
  conventions, filtered/fast parity on a fixture), mocks extended in
  `helper-server-mocks.R` (never copy helper implementations into mocks).
- SQL: EXPLAIN-validate new function bodies; MV row-count invariants
  (players×seasons unchanged; new columns sum-consistent with fg totals:
  `layup+dunk ≤ fg2_att`, `c3 ≤ fg3_att`).
- ETL: Phase-2 zones upsert covered by the per-game validation counts;
  export reconciliation check added in the gap fix.
- Analysis cross-check: shares for a few players/teams reconcile with
  `output/shotgen_*.csv` (same definitions, so numbers should match to
  rounding within the parquet-coverage caveat for corner-3).

## Delivery order (branches)

1. `etl/cold-export-gap` — export reconciliation fix + recover games ~365–388.
2. `sql/shot-zones` — table DDL + backfill script + ETL Phase-2 upsert step.
3. `sql/shot-profile-mv` — both MV changes + rebuild + invariant checks.
4. `sql/shot-profile-fns` — `onoff_compute` + `get_team_ratings_dynamic` +
   security allowlist.
5. `shiny/shot-profile` — Tabs 1, 3, 7 UI/server + tests.
6. Docs: CLAUDE.md tab table + MV/function param counts; analysis doc pointer.

Each step is independently mergeable; the app renders nothing new until step 5.

## Risks / open items

- `onoff_compute` is the hottest filtered-path function — adding a
  `shot_zones` join needs an EXPLAIN check (PK join on (game_id,id); expected
  cheap, verify no plan regression under `statement_timeout` 20s).
- Corner-3 NULL bucket (coordless shots + any pre-fix gap games) must render
  as "—", not 0 — fail-open display rule.
- Tab 7 compare column-count growth: check mobile collapse behavior.
- React Tab 1 will silently lack the new view until the follow-up — note in
  PROJECT.md so the drift is recorded.
