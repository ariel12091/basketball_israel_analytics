# Game Logs parity audit — Tab 4 (Israeli) vs Tab 11 (EuroLeague)

Date: 2026-09-05. Findings only; no code changes are proposed as part of this
document beyond the two already committed (see *Resolved*).

## Scope and method

Compared `app/R/ui_tab4_gamelogs.R` (156 lines) and `app/R/server_tab4.R`
(861) against `app/R/ui_tab11_euro_gamelogs.R` (77) and
`app/R/server_tab11_euro_gamelogs.R` (252), plus the live schema for the
data-grain claims. Every finding is classified:

| Class | Meaning |
|---|---|
| **Defect** | Wrong behaviour on one side. Fix. |
| **Gap** | A feature one tab has and the other does not. Parity work. |
| **Duplication** | A shared component exists but one tab hand-rolls it. |
| **League dimension** | A real difference between the competitions. Leave alone. |
| **Cosmetic** | Wording or markup drift with no functional effect. |

The direction matters: this is **not** a one-way "port Israeli features to
EuroLeague" list. Three findings run the other way.

## Resolved during this session

| # | Finding | Commit |
|---|---|---|
| R1 | Tab 11 had no `Min` column; Tab 4 has had one since the canonical-clock work | `21201b8` |
| R2 | Tab 11 set no DataTables `order`, so it sorted Rd **ascending** — oldest game first — silently discarding its own `arrange(desc(game_date), …)` | `21201b8` |
| R3 | Tab 4 had no CSV export at all, while Tab 11 has had one throughout | `093b039` |

## Findings

### P1 — EuroLeague CSVs leak hidden columns · **Defect** · Tab 11 (also Tab 9)

Tabs 3, 5 and 6 pass `exportOptions = list(columns = ":visible", stripHtml =
TRUE, …)`. Tabs 9 and 11 pass **no `exportOptions` at all**. DataTables exports
hidden columns unless told otherwise, so both EuroLeague CSVs currently ship
every `pr_*` percentile-rank column — internals the reader never asked for.
Tab 9 additionally exports whatever is in its `hidden` vector.

Both also omit `csv_export_stamp()`, so their filenames carry no date and
repeated downloads collide in the browser's download folder.

`csv_export_button()` (added to `helpers.R` in `093b039`) is the fix; tabs 9
and 11 should call it. Tabs 3, 5 and 6 should adopt it too, retiring three
inline copies.

### P2 — Tab 4 hand-rolls a shared filter component · **Duplication** · Tab 4

`ui_tab4_gamelogs.R:26-33` writes out the four starters inputs by hand.
`starter_context_filters_ui(prefix)` in `global.R:782-799` produces the same
four inputs — **same ids, same labels, same `tt()` tooltips, same choices**.
Tab 11 already calls it.

This is a byte-identical move: replace the two `fluidRow` blocks with
`starter_context_filters_ui("gl")` and verify by reversing the transform and
diffing against `HEAD`, not by writing new tests for moved code.

### P3 — Team filter is single-select on Tab 4, multi-select on Tab 11 · **Defect** · Tab 4

`selectizeInput("gl_team", "Team", multiple = FALSE)` versus
`selectizeInput("eurogl_teams", "Teams", multiple = TRUE)`. The EuroLeague
behaviour is the better one and the rest of the app is multi-select. Note this
is not a pure UI change: `gl_team` is singular throughout `server_tab4.R` and
feeds `shared$pending_gl_team` for cross-tab navigation, so the change has a
tail. Worth doing, worth scoping properly.

### P4 — Tab 11 Summary has no shot splits · **Gap** · Tab 11

Tab 4 Summary carries `Off Shot` / `Def Shot` — the 2PT/3PT frequency and
accuracy cells — plus the legend explaining them. Tab 11 has neither.

Per `CLAUDE.md` this is listed as an intentional EuroLeague feature flag
(no shot splits), so **confirm before building**. If it is wanted, the cells
are `make_shot_render_gl()` and the data question is whether the EuroLeague
per-game fact carries fg2/fg3 made/attempted at the starters-cross-tab grain.

### P5 — Four Factors column sets differ both ways · **Gap** · both

| | Tab 4 FF | Tab 11 FF |
|---|---|---|
| Off Poss / Def Poss | ✓ | ✗ |
| Net | ✗ | ✓ |
| Grouped two-tier header with section borders | ✓ (`container = sketch`) | ✗ flat `Off eFG%` labels |
| OREB% tooltips (`OFF_OREB_TOOLTIP`) | ✓ | ✗ |

Each tab has something the other lacks. The grouped header is the better
presentation and is the larger of the two jobs.

### P6 — Tab 11 has no stat filters · **Gap** · Tab 11

`server_tab4.R` wires `gl_stat_filter_state` and calls `apply_stat_filters()`
in both views (8 references). Tab 11 has none. This is the numeric
column-filter chip system from `docs/superpowers/plans/2026-04-06-tab5-stat-filter-chips.md`.

Relevant caveat: that popover is currently duplicated between
`helpers.R:434` and `server_tab5_traditional.R:1565` — finding F10 of the
2026-09-02 design review, still open. Extending it to a third tab should not
happen before that duplication is resolved, or it becomes a fourth copy.

### P7 — Tab 11 has no worked example · **Gap** · Tab 11

Tab 4 ships a "Show/Hide Example" collapse per view with a real annotated game
(GN 17, Bnei Herzliya 105-89) and a screenshot. Tab 11's explainers are three
bullets with no example and no image. Tab 4's explainers also run four bullets
to Tab 11's three.

### P10 — Tab 11 lost the game_id sort tiebreak · **Defect** · Tab 11 · FIXED

Recorded because this audit originally mis-classified it as cosmetic, on the
reasoning that the R-side arrange is "irrelevant once DT re-sorts". That is
wrong: **DataTables' sort is stable**, so rows tying on every sort key keep
their incoming order — and with the table sorted on Date then Rd, every row of
every game played on the same date in the same round ties on both.

Tab 4 arranges `desc(game_date), desc(gn), game_id, team_name`; Tab 11 omitted
`game_id`, leaving `team_name` as the only tiebreak, so a game's two rows sorted
apart alphabetically instead of sitting together.

Measured on EuroLeague 2025: **79 of 100 date+round buckets hold more than one
game**, up to 9 games / 18 tied rows in one bucket. On 2026-01-20 round 23,
game 224's two rows (EA7 Milan, Real Madrid) sat 11 rows apart. Fixed by
matching Tab 4's arrange.

### P8 — Different season-cache mechanisms · **Cosmetic**, verify · both

Tab 4 uses `bindCache(...) %>% bindEvent(...)` against `GL_DATA_CACHE`; Tab 11
uses `cached_season_df()`. Both are legitimate and both key on the ETL data
version. No defect found — but two mechanisms for one job is the kind of drift
that produces a third. Worth converging when either is next touched.

### P9 — `shared_head_tags()` on 7 of 12 tabs · **Cosmetic** · app-wide

Tabs 0, 1, 7, 8, 9, 10, 11 call it; 2, 3, 4, 5, 6 do not. Because it emits
`tags$head`, one occurrence suffices and the other six are redundant rather
than the five being broken. Not a Tab 4/11 defect; noted so it is not
"fixed" the wrong way round. It also loads Google Fonts and bootstrap-icons
from CDNs with no SRI, which is already item 2 of the security backlog.

### L1-L4 — League dimensions · **leave alone**

- `GN` / `Game Type` versus `Rd` / `Phase`. Already parameterised through
  `build_filter_chips(gn_label = )` and `game_type_input_id`.
- Reset label: "Reset Filters" vs "Reset to defaults". Cosmetic drift, but the
  chip-clear plumbing behind both is the shared `setup_chip_clears()`.
- Source grain: Tab 4 reads lineup grain (84.8 rows per game-team), Tab 11
  reads a starters cross-tab (22.1). Tab 4 can therefore filter by lineup and
  Tab 11 cannot. This is a data-model difference, not a bug.
- **`Min` is not numerically comparable across the two tabs.** Israel ends the
  last segment at the last recorded action (avg 39.42 min/team-game);
  EuroLeague ends at the nominal period boundary (min 40.00, avg 40.30). Same
  header, different definition. If the two are ever shown side by side, this
  needs a footnote.

## Suggested order

1. **P1** — a correctness defect, one call site each on tabs 9 and 11, and the
   helper already exists.
2. **P2** — byte-identical move, verified by diff.
3. **P5** grouped header, then **P3**.
4. **P4** and **P6** need a decision before any code: P4 contradicts a stated
   feature flag, P6 is blocked behind design-review finding F10.
5. **P7** is content work, not engineering.

P8, P9 and L1-L4 need no action.
