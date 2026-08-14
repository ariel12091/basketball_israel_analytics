# EuroLeague Lineups tab: UI parity with the Israeli tab

Date: 2026-08-14
Status: design approved, not implemented
Branch: `shiny/euro-tab1` (current)

## Goal

Tab 10 (`euro_ld`, EuroLeague Lineup Data) presents the same sidebar and the
same table as Tab 2 (`ld`, Israeli Lineup Data): same columns in the same
order, same headers, same heat colouring, same per-column filters, same
paging, same TOTAL row.

Scope is **table and sidebar only**. The explainer prose above the table stays
EuroLeague-specific — it carries caveats the Israeli tab has no reason to make
(independent possession engine, never compare the two leagues' numbers). Tab
2's collapsible example box, which embeds a screenshot of a real Rishon Lezion
lineup, is not reproduced.

The shot-splits legend IS reproduced, despite sitting outside a literal
reading of "table". It is the key to the `Off Shot` / `Def Shot` gradient
cells Tab 10 gains in step 2; without it those cells are unexplained.

## Why extraction, not a copy

`CLAUDE.md` states the rule as non-negotiable: a EuroLeague tab reuses the
Israeli implementation rather than growing a parallel `euro_` one. Tabs 2 and
10 are the pair with the *least* shared code (23% measured), and the divergence
this spec removes is exactly what that rule exists to prevent.

A copy would produce the same screen on the day it ships and drift on the next
change. One renderer, called twice, makes "exact" a property of the code rather
than a claim about a moment in time.

Merging the two tab files was considered and rejected — `euroleague/CLAUDE.md`
already decided against per-pair merges, and `server_tab2.R` is 1,280 lines.

## Current gap

### Sidebar

Same building blocks, different order, labels and widget types.

| | Tab 2 | Tab 10 |
|---|---|---|
| View label | `"View:"` | `"Select View:"` |
| Reset label | `"Reset Lineup Filters"` | `"Reset to defaults"` |
| Group size | inline `radioButtons`, `"2"`–`"5"` | `selectInput`, `"2 players"`–`"5 players"` |
| Min poss | slider + `helpText` | slider, no help text |
| Player filter | default layout, help + placeholders | `layout = "stacked"`, no help |
| Order | reset → slider → group size → player filter → starters → dates → clutch → accordion → game context | reset → group size → player filter → slider → dates → clutch → accordion → game context → starters |

### Summary table

| | Tab 2 | Tab 10 |
|---|---|---|
| Columns | Team, Players, Min, Total Poss, Off PPP, Def PPP, Net RTG, +/-, Off Shot, Def Shot, Off Poss, Off Pts, Def Poss, Def Pts, # Starters | Team, Unit, Off PPP, Def PPP, Net Rtg, Off eFG%, Def eFG%, Min, Off Poss, Def Poss |
| Header | flat, `filter = "top"` per-column search | 2-row grouped sketch (Ratings / Shooting / Usage), no filters |
| Shot cells | `Off Shot` / `Def Shot` 2PT/3PT frequency bars + accuracy gradient | none |
| Paging | 50, `lengthMenu` 25/50/100/200/1000 | 30, `dom = "tip"` |
| Extras | stat-filter chips, shot-splits legend | none |

### Four Factors table

Tab 2: PPP, eFG%, OREB%, TOV%, FTR, Poss per side, then Min / Poss / Net.
Tab 10: TS%, TOV%, OREB%, FTR per side, then Min / Off Poss / Def Poss.

**Decision:** Tab 10 switches to eFG%. `euroleague/CLAUDE.md` records TS% as a
deliberate choice (EuroLeague's own denominator, explicitly not `0.44 × FTA`);
that decision is reversed here in favour of parity, and `euroleague/CLAUDE.md`
must be updated to record the reversal. EuroLeague carries `off_fgm`,
`off_fga` and `off_fg3_made`, so eFG% is computable on both paths.

## Step 1 — Extract Tab 2's renderers into `helpers.R`

`server_tab2.R:441–835` becomes two functions in `helpers.R`, moved
byte-identically:

- `lineup_summary_datatable(df, stat_filters, spec)`
- `lineup_ff_datatable(df, stat_filters, spec)`

`spec` carries only what genuinely differs between leagues:

| Field | Tab 2 | Tab 10 |
|---|---|---|
| `link_class` | `ld-lineup-link` | `euro-ld-unit` |
| `click_js` | `window.handleLineupLinkClick(this)` | `Shiny.setInputValue('euro_ld_clicked_unit', …)` |

Everything else moves unchanged and is shared: column order, `pretty_labels`,
the shot-cell JS factory and its dynamically computed league averages,
`filter = "top"`, `orderFixed`, `lengthMenu`, `deferRender`, `scrollY`, the
TOTAL row arithmetic, and every heat `formatStyle` call.

Tab 2 then calls the two helpers with the Israeli spec. **No visible change.**

Verification: reverse the transform — inline the helper bodies back into
`server_tab2.R` — and diff against `HEAD`. No bespoke tests for moved code.
Run only the affected test file.

## Step 2 — EuroLeague adopts Tab 2's column contract

One rename/derive map in `server_tab10_euro_lineups.R`, applied after
`add_rates()`. Most columns already align: `off_ppp`, `def_ppp`, `net_rtg`,
`minutes`, `off_poss`, `def_poss`, `off_pts`, `def_pts`, `total_poss` and all
eight `off_fg2_made`-style shot counts already carry Tab 2's exact names.

What changes:

| Tab 2 expects | EuroLeague source |
|---|---|
| `Team` | `team_name` |
| `Players` | `player_names_str` |
| `sub_lineup_hash` | `unit_key` |
| `plus_minus` | `off_pts − def_pts` |
| `num_starters` | `unit_size` (see step 4) |
| `off_efg` | `(off_fgm + 0.5 × off_fg3_made) / off_fga × 100` |
| `off_oreb` | `off_oreb / off_oreb_opp × 100` |
| `off_tov` | `off_tov / off_poss × 100` |
| `off_ftr` | `off_fta / off_fga × 100` |
| `off_oreb_cnt` | `off_oreb` |
| `off_oreb_opps` | `off_oreb_opp` |
| `off_tov_cnt` | `off_tov` |
| `off_fga_cnt` | `off_fga` |
| `off_fgm_cnt` | `off_fgm` |
| `off_fg3m_cnt` | `off_fg3_made` |

Defensive columns mirror the offensive ones throughout.

**Ordering constraint.** The last six rows are not decoration. The FF TOTAL
row sums *raw counts* and derives its rates once, so the shared renderer reads
the count names, not the displayed rates. `off_tov` is a raw count in
EuroLeague and a rate in Tab 2 — the same name on both sides of the map. The
counts must therefore be renamed **before** the rates are derived. Deriving
first silently overwrites the count with the rate, and the TOTAL row's TOV%
becomes a rate divided by possessions.

Tab 10 also gains `make_stat_filter_state()` and
`setup_stat_filter_handlers("euro_ld", …)`. Its existing chip `bits` are
concatenated with `stat_filter_chips_ui("euro_ld", …)` in the
`extra_children` argument of `build_filter_chips()`, not replaced by them.

Both of Tab 10's data paths — `sub_lineups_stats_mv` (fast) and the three
`fetch_lineups_*` readers (filtered) — already return the same column names,
so the map applies once, downstream of the branch.

## Step 3 — Sidebar parity

Mechanical edit to `ui_tab10_euro_lineups.R`, reordering to Tab 2's sequence
and adopting its labels and widget types per the table above. `euro_ld_reset`
switches from `updateSelectInput` to `updateRadioButtons` for group size.

**Not renamed:** `euro_ld_date_range` stays as it is rather than becoming
`ld_dates`. The id is invisible to users, `build_filter_chips()` already takes
`date_input_id`, and renaming ripples through `setup_chip_clears()` and
bookmarking for no visual gain.

`shared_head_tags()` stays in Tab 10 and is not added to Tab 2.

After step 3 the two tabs are identical column-for-column and control-for-control.

## Step 4 — Real `# Starters` (OPTIONAL, deferred)

Tab 2's `# Starters` is not a starter count. `fetch_lineups_all` returns
`s.num_lineup::numeric AS num_starters` and the query filters
`WHERE s.num_lineup = p_num_lineup`, so the column is a constant equal to the
Group size radio. Step 2 reproduces that constant on the EuroLeague side
(`num_starters <- unit_size`), which keeps parity exact and makes this step a
pure source swap: both leagues change where the number comes from, the shared
renderer does not change at all.

Proposed definition: the possession-weighted mean of on-court own starters
over the unit's offensive possessions, `Σ(own_starters × off_poss) / Σ(off_poss)`,
one decimal.

Sources exist on both sides with matching 0–5 semantics:

- EuroLeague — `lineup_totals_by_game.own_starters`; add the weighted
  numerator to `sub_lineups_stats_mv` and to `fetch_lineups_pergame`,
  `fetch_lineups_dynamic`, `fetch_lineups_direct`.
- Israeli — `pws.num_starters_offense`, surfaced through
  `df_pts_poss_lineups_longer_mv`; add the weighted numerator to
  `sub_lineups_stats`, and have `fetch_lineups_all` return it in place of
  `s.num_lineup::numeric`.

Two risks that must be named in any plan that executes this step:

1. DROP+CREATE on `sub_lineups_stats` wipes its GRANTs, and DROP FUNCTION
   wipes EXECUTE. `scripts/apply_db_security.R` with
   `CONFIRM_DB_SECURITY_APPLY=1` runs afterwards, and the audit is read in
   full rather than tailed.
2. This changes a column's displayed values on the live Israeli tab. It is a
   visible product change, not a refactor, and it is the reason this step is
   optional and separately approvable.

## Verification

- Step 1 is output-identical by construction. Verified by reversing the
  transform and diffing against `HEAD`, never by writing tests for moved code.
- Steps 2 and 3 are replayed offline against real EuroLeague data before being
  claimed working — a blanket rename on this pair has already corrupted a join
  key once, and eyeballing did not catch it.
- Step 4, if executed, reconciles the new column against a known invariant
  before deploy.
- The full test suite runs once at the end, not per step.

## Out of scope

- Tab 2's example box and its screenshot assets.
- Tab 10's explainer prose, which stays EuroLeague-specific.
- The lineup game-log modal on either tab.
- `frontend-v2` / Plumber, which is archival.
- Any change that mixes the two leagues in one ranked table. Ranks stay
  league-scoped, and cache keys keep their league dimension.
