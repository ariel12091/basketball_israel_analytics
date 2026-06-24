# Tab 5 — Consistent identity display (name + grouping)

**Date:** 2026-06-24
**Area:** `app/R/server_tab5_traditional.R` (Shiny Tab 5 — Player Stats)
**Type:** Display-only. No data/stat semantics change.

## Background

The cross-team identity merge (commit `f6bbe8f`) links a player who appears under
different provider ids on different teams in one season into one `identity_id`
(e.g. DJ Burns: `1143` Rishon Lezion + `1982` Bnei Herzliya). The Tab 5 multi-team
TOTAL feature combines such a player's per-team rows into a summed TOTAL row,
grouping on `identity_id` via `resolved_player_identity_v`.

Three display rough edges remain:

1. The per-team rows show the **raw provider name per team** (from
   `player_traditional_stats_mv.player_name` / the meta players query), so the
   same person reads inconsistently — "DJ BURNS" on one team, "D.J. BURNS" on
   the other.
2. Rows are ordered by **PTS desc** with the TOTAL merely appended, so a player's
   two team rows and his TOTAL scatter across the table instead of sitting together.
3. The player filter **dropdown** (and the selected-player chips) show the same
   inconsistent per-team names.

`resolved_player_identity_v.display_name` already returns the **canonical** identity
name ("DJ BURNS") for every source id in the merged identity, and the Tab 5
identity lookup (`load_ts_identity_lookup(gy)`) already exposes
`(team_id, player_id, identity_id, display_name)`. So the canonical name is already
available everywhere it's needed; these changes are about using it.

## Requirements

- R1 — A multi-team identity's per-team rows in the table display the **canonical**
  name (identical across teams), matching the TOTAL row.
- R2 — In the default view, a multi-team identity's rows are contiguous: per-team
  rows first (PTS desc), TOTAL row last (footer), and the whole group is positioned
  at the group's combined PTS so the table still reads PTS-desc. Manual column sort
  reflows normally (standard DataTables behavior).
- R3 — The player dropdown and the selected-player chips show the canonical name.
- Non-goal: collapsing a multi-team player to a single dropdown entry. Keep one
  entry per `(team, player)`; only the displayed name is normalized.
- Non-goal: enforcing grouping across manual re-sorts (no RowGroup).

## Design

All changes are in `app/R/server_tab5_traditional.R`.

### R1 — canonical per-team name (in `add_ts_multi_team_totals`)

The function already builds `lkmap` `(team:player -> identity_id)` and, when the
lookup carries `display_name`, a `dispmap` `(identity_id -> display_name)`. After
identifying `multi_ids` (identities on >= `min_teams` teams), overwrite the
`Player` column of the resolved rows whose `.identity_id %in% multi_ids` with
`dispmap[.identity_id]` (only when a non-empty display name exists). The TOTAL
rows already use `dispmap` via `build_ts_total_row`. Single-team players are
untouched.

### R2 — group ordering (new helper `ts_group_display_order`)

New pure helper applied to the combined data frame just before the DT is built
(after `add_ts_multi_team_totals`, before `transmute`/render). For input with
`pts`, `.identity_id`, `is_multi_team_total`:

- group key `g` = `.identity_id` when present, else a unique per-row token
  (rows with no resolved identity never group). Single-team players carry their own
  unique `identity_id`, so they naturally form singleton groups; only multi-team
  identities have >1 row sharing a `g`.
- group sort value = `max(pts)` within `g`. Because the TOTAL's PTS is the sum of
  its parts, the max within a multi-team group equals the TOTAL's PTS; for single
  players it is their own PTS. This preserves the overall PTS-desc feel.
- order by: `group_sort` desc, then `g` (keeps a group's rows contiguous on ties),
  then `is_multi_team_total` asc (per-team rows before the TOTAL footer), then
  `pts` desc (orders the per-team rows).

The DataTable initial sort is set to **empty** (`order = list()`) so it renders in
this R-provided order. `ordering = TRUE` is kept, so clicking any header re-sorts
(reflow). Percent-rank/heat computations are row-wise and unaffected by row order.

### R3 — canonical dropdown + chip names

Add an optional `lookup` parameter to `ts_player_choices(players_df, teams_df,
team_ids, lookup = NULL)`. After `normalize_ts_players`, when `lookup` has
`(team_id, player_id, display_name)`, remap `players$player_name` to
`lookup$display_name` keyed by `team:player`, falling back to the original name
when unmatched. Labels and keys are otherwise unchanged
(`"<name> (<team>)"` -> `team:player`).

`refresh_ts_player_choices()` loads the lookup for the current season
(`load_ts_identity_lookup(as.integer(input$game_year))`) and passes it. The
selected-player chip label builder (~line 1172) passes the same lookup.

## Testing

Extend `app/tests/testthat/test-tab5-multi-team-totals.R`:

- R1: after `add_ts_multi_team_totals` on a fixture with a multi-team identity
  whose per-team rows have differing names, both per-team `Player` values equal
  the canonical lookup `display_name`, and equal the TOTAL row's `Player`.
- R2: `ts_group_display_order` places `[team, team, TOTAL]` contiguously and
  positions the group by the TOTAL's PTS relative to single-team players (e.g. a
  group totaling 507 sits above a single player with 400 but its 166-PTS team row
  is NOT separated down among the ~166 players).
- R3: `ts_player_choices(..., lookup = lk)` produces identical canonical names for
  a multi-team player's two `(team, player)` entries; keys remain `team:player`.

## Risks / notes

- With pagination a group can straddle a page boundary (team rows at the bottom of
  one page, TOTAL at the top of the next). Accepted; not addressed.
- Setting initial DT order to empty removes the "PTS column shows as sorted"
  affordance on load; the rows are still effectively PTS-ordered by group. Accepted
  per the chosen grouping behavior.
