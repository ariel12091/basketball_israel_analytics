# EuroLeague data quality report — design

Date: 2026-08-19
Status: approved

## Goal

Give the `euroleague` schema the same standing, read-only data quality report
the Israeli schema has had since 2026-06, and run it to produce a first
overview of the loaded EuroLeague data.

## Existing thing being adapted

`etl/run_data_quality_report.R` (2,514 lines) is already two separable pieces:

- a generic engine — `truthy`, `script_path`, `repo_root`, `quote_table`,
  `sql_string`, `table_exists`, `escape_md`, `markdown_table`,
  `write_detail_csv`, `make_check_result`, `run_sql_check`, `run_r_check`, and
  `run_data_quality_report` — roughly 330 lines that know nothing about
  basketball;
- `build_checks(con, schema)`, which returns 35 declarative checks (`A` … `AJ`),
  every one of them Israeli-specific.

A check is a list with `id`, `title`, `severity` (`error` / `warning` / `todo`),
`purpose`, `required_tables`, and either `sql` or an R `runner`, plus an
optional `problem_count_col`. The engine skips a check whose `required_tables`
are absent, writes a detail CSV per check, and rolls the statuses up into
`PASS` / `WARNING` / `FAIL`.

## Decision: standalone script, verbatim engine copy

The report lives in one new file,
`euroleague/scripts/run_euro_data_quality_report.R`.

This was chosen over extracting a shared `etl/dq_engine.R`. It cuts against the
repository rule against parallel `euro_` implementations of logic that already
exists, and the two engines will drift as the Israeli catalogue grows. The
accepted mitigation is that the engine is copied **verbatim**, inside a marked
block, so a later extraction is a mechanical move rather than a reconciliation:

```
# --- BEGIN verbatim copy of etl/run_data_quality_report.R engine ---
# --- END verbatim copy ---
```

Only three edits are permitted inside that block, each marked with a
`# EUROLEAGUE:` comment:

1. schema is fixed to `euroleague` (no `APP_ENV` branch);
2. `env_file` resolves to `etl/.Renviron` from the repository root;
3. the default output directory is `euroleague/logs/data_quality`.

`etl/run_data_quality_report.R` is not modified.

## Check ID scheme

Ported checks keep the Israeli letter, so the two reports can be read side by
side. EuroLeague-native checks are `N1` … `N8`. Checks with no EuroLeague
analogue stay in the catalogue with status `not_automated` and a `purpose` that
names the reason, so the report states the gap rather than hiding it.

### No analogue — recorded as gaps (7)

| ID | Reason |
|---|---|
| `C`, `D` | EuroLeague has no correction layer (`player_id_aliases`, `player_id_game_overrides`) |
| `F` | No lineup derivative alias residue: no alias table, and lineup identity is derived per load |
| `M`, `N`, `O` | No identity dictionary (`player_identities`, `player_identity_map`, `resolved_player_identity_v`) |
| `AG` | No cold storage; the `euroleague` schema keeps every relation hot |

### Ported (26)

| ID | Israeli source | EuroLeague source |
|---|---|---|
| `A` | `full_rosters` names per team-season player | same, over `full_rosters` joined to `schedule.season` |
| `B` | roster name to many player IDs | same |
| `E` | aggregate names roster-valid | the four `*_mv` names resolve to a `players` row |
| `G` | `actions_clean` duplicate action IDs | `actions` duplicate `(game_id, source_event_order)` |
| `H` | base-loaded games missing `etl_processed_games` | loaded games missing a `game_qa` row |
| `I` | processed games missing base rows | `game_qa` clear with no `actions` or `full_rosters` |
| `J` | processed games missing downstream rows | `game_qa` clear with no `action_team_context_actions`, `player_four_factors_by_game`, `team_four_factors_by_game`, or `lineup_totals_by_game` |
| `K` | app aggregate duplicate player keys | duplicate keys in the four `*_mv` |
| `L` | raw PBP duplicate action IDs | duplicate `provider_play_number` per `actions_raw` game |
| `P0` | placeholder roster identities | `full_rosters` blank or placeholder `source_player_name` |
| `P` | non-participating placeholders in aggregates | same, against the `*_mv` |
| `P1` | reviewed non-actionable exceptions | `qa_incidents` with a resolved or accepted status |
| `Q` | event-team rows without a five-player lineup | `cardinality(own_lineup) <> 5` on `action_team_context_actions` |
| `R` | lineup states not exactly five distinct ON players | cardinality and distinctness on `action_team_context_actions` and `matchup_segments_actions` |
| `S` | missing starter context | `own_starters` / `opp_starters` outside 0-5; `full_rosters.is_starter` count per game and team |
| `T` | team minutes vs official duration | `SUM(matchup_segments_actions.segment_seconds)` per game and team vs `2400 + 300 * OT`, OT from `MAX(period) - 4` |
| `U` | lineup rows with invalid counts or minutes | `lineup_totals_by_game` negatives, made above attempted, non-positive seconds |
| `V` | team-game score reconciliation | three-way: `team_four_factors_by_game.off_pts`, `schedule.home_points` / `away_points`, `team_boxscores.points` |
| `W` | offense/defense possession reconciliation | team A `off_poss` equals opponent `def_poss` per game |
| `X` | player minute conservation | the five on-court players' `onoff_minutes` sum to five times team lineup minutes |
| `Y` | OT periods begin with a valid lineup | `matchup_segments_actions` coverage for `period > 4` |
| `Z` | OT event players absent from the attached lineup | `action_player_id` not in `own_lineup` for offense-perspective rows |
| `AA` | material clock or period order anomalies | `event_elapsed_seconds` monotonic by `source_event_order` within game and period |
| `AB` | clock-order jitter | same, below the material threshold |
| `AC` | missing regulation period coverage | periods 1-4 present per game |
| `AD` | clutch clock exposure | anomalies inside the clutch window, via `regulation_seconds_remaining` |
| `AE` | duplicate persisted action/stint keys | duplicate `(game_id, source_event_order, team_id)` in `action_team_context_actions` |
| `AF` | invalid persisted segment IDs | null or non-positive `segment_id` |
| `AJ` | free-throw progress domain | `ft_reverse_order` and `synthetic_ft_trip_id` domain |

### EuroLeague-native (8)

| ID | Purpose |
|---|---|
| `N1` | `game_qa` publication gates failing: `lineup_structure_valid`, `boxscore_metrics_exact`, `score_progression_exact`, `possession_structural_status` |
| `N2` | `reconciliation_metrics.matches = false`, grouped by metric |
| `N3` | Open `qa_incidents` by severity |
| `N4` | `load_runs` not `completed`, or with `failed_games > 0` |
| `N5` | `actions.grouping_status <> 'confirmed'` and low `grouping_confidence_pct` |
| `N6` | Schedule games finished at the provider but with no loaded actions |
| `N7` | Possession endpoint reasons outside the expected vocabulary |
| `N8` | Per-relation storage footprint against the recorded PROJECT.md baseline |

## Constraints

- Read-only. Every check is a `SELECT`; the report writes only to its own
  output directory.
- No schema changes, no migration, no change to `etl/run_data_quality_report.R`,
  no Shiny or app changes.
- The EuroLeague season convention is the provider season (2025 = 2025-26); the
  report does not apply the Israeli `+1` offset.
- `game_id` values collide numerically with Israeli ones, so no check joins
  across schemas.

## Verification

1. Diff the copied engine block against the Israeli original; the only
   differences are the three `# EUROLEAGUE:` edits.
2. Run the report against the live `euroleague` schema and read `latest.md`.
3. Cross-check a sample of `fail` results against the source relations before
   reporting them as defects, rather than trusting the check on first run.
