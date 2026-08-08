# EuroLeague migration 009 — consumers read the fact

Date: 2026-08-08
Status: approved, not implemented
Scope: `euroleague` schema only. No Israeli object is read or changed.
Predecessor: `docs/superpowers/specs/2026-08-07-euroleague-action-team-context-design.md`

## Problem

Migration 008 built `euroleague.action_team_context`, a persisted event ×
team-perspective fact, and proved it reproduces the four-factor player grain
exactly. Nothing reads it. Until something does, the fact is pure cost — 69 MB
and ~0.5s of publication time per game — and the derivation it was built to
retire is still being paid.

That derivation is currently written **three times**, independently:

| where | what it derives |
|---|---|
| `refresh_player_four_factors_by_game_for_games()` (007) | own `event_base` off `actions_raw`, clock parse, monotonic guard, offense/defense expansion, six chained window CTEs for joint segments |
| `refresh_team_four_factors_by_game_for_games()` (006) | its own `event_base`, same shape |
| `refresh_action_team_context_for_games()` (008) | the same again, persisted |

Three copies of one rule is the condition this project has already been bitten
by. During 008, three separate places hardcoded a *consequence* of the player
grain, and all three broke when the grain was corrected. Copies of the
derivation itself are the same hazard one level down.

## What is already proven

- The fact reproduces `player_four_factors_by_game` exactly: bidirectional
  `EXCEPT ALL`, zero rows either way, across all 84 games and 115,034 stored
  rows.
- The refresh is deterministic: the rollback probe deletes both relations,
  republishes, and compares every measure and both lineup/stint sides against
  natural keys — byte-identical.
- Segment durations tile every team-game exactly (168/168), and each team's
  cumulative score reconciles to its official box-score points.

The player-side replacement query therefore already exists and is verified. It
is `PLAYER_GRAIN_FROM_FACT` in `euroleague/scripts/verify_action_team_context.py`.
009 promotes it into the function body rather than writing it anew.

## Decisions

| decision | choice | why |
|---|---|---|
| Retire `player_four_factors_by_game`? | **No** | It is not redundant with the fact. A player-level query over the fact must fan each event row out across the roster — 1,139,243 rows for 84 games — which the aggregate collapses ~10× to 115,034, once at load rather than per query. `onoff_compute()` is on the app's interactive path; moving that fan-out to query time would be a straight regression. |
| Which functions change | The two refresh functions only | The read layer above them — `player_game_context`, `onoff_compute()`, `four_factors_compute()`, `player_onoff_default_mv`, `player_advanced_stats_mv`, and the team MVs — is untouched. Their inputs keep the same grain, keys and values. |
| Team grain | Filtered `SUM` over the fact | Team four factors need no roster fan-out at all, so this side collapses to an aggregate grouped by `(game_id, team_id, type_lineup)`. |
| New metrics | None | 009 is an output-identical refactor. A metric change cannot ride inside one. |

## The two rewrites

### `refresh_player_four_factors_by_game_for_games(bigint[])`

Population and floor time come from `matchup_segments`; measures come from
`action_team_context`. The shape is the one 007 already has after its 2026-08-08
correction — `player_minutes` → `complete_grid` (× the two `type_lineup` values)
→ `LEFT JOIN counts` — with both sources repointed at the persisted relations
instead of a re-derivation from `actions_raw`.

`is_on_key` stays a plain membership test of the roster player against the
segment's own lineup. EuroLeague lineups are first-class; nothing is
reconstructed here.

Removed in the same change: the `PERFORM refresh_stint_timing_for_games()` at
the top of this function. Migration 008 moved that call into the fact's refresh
and deliberately left this one rather than modify a function it was not
verifying. With this function rewritten, the duplicate has no purpose.

### `refresh_team_four_factors_by_game_for_games(bigint[])`

Replace its `event_base` and expansion with a filtered aggregate over the fact,
grouped by `(game_id, team_id, type_lineup)` and pivoted into the existing
`off_*` / `def_*` column pairs. The output columns, keys and types do not
change.

## The check that must be replaced, not inherited

`PostgresTransactionBackend.validate_game()` compares the player four-factor row
count against an expectation derived from `matchup_segments`. Today that is a
real check: the actual comes from 007's independent re-derivation off
`actions_raw`, so the two sides are genuinely different sources and a
disagreement means something.

**The moment 007 reads `matchup_segments`, both sides read the same table and
the check proves nothing.** It must not be silently kept.

Replacement: promote the gate's bidirectional `EXCEPT ALL` into
`scripts/load_games.py --verify-only` as a standing check, so the property is
still enforced per load rather than only by a manual script. The per-game
`validate_game()` check becomes a coverage assertion — the refresh returned a
non-zero row count for the game — which is cheap and still catches a refresh
that silently produced nothing.

## Gate

Bidirectional `EXCEPT ALL` against **stored** rows, for **both** grains,
excluding `derived_at`, `load_run_id` and `derivation_version`. Zero rows either
way, or 009 does not land.

Compare against rows stored *before* the rewrite, not two fresh runs: `now()` is
the transaction timestamp, so a same-transaction comparison cannot reveal a
`derived_at` difference. This is the rule 008 followed and the reason its
correction was provable.

Additionally, and as the substantive acceptance test: every rate in
`player_onoff_by_season` and `player_four_factors_by_season` must be
byte-identical before and after, for every player. That is the check that
caught the difference between a legitimate grain correction and a defect during
008, and it is the one that matters to anything the app displays.

## Risks

- **`OF` is classified defensive.** It follows the contract's unqualified
  "fouls are defensive" and carries no measure today, so nothing moves. It
  becomes wrong the first time a consumer counts fouls by side. 009 adds no
  metrics, so it stays deferred — but it is recorded in the 008 DDL and must be
  settled before any foul measure lands.
- **Performance is not the driver and must not regress.** Publication is ~2s
  per game today. The player rewrite keeps the roster fan-out, so it should be
  comparable; the team rewrite should be faster. Measure both, and treat a
  regression as a defect rather than a cost of the refactor.
- **`event_base` divergence.** 007 built its segments from events surviving
  inner joins to `actions_clean` and the synthetic-parent root, while 008 builds
  from `action_lineups` directly. Those two event sets coincide today, and gate
  check 2 (`fact rows = 2 × actions_raw`) is what enforces the coincidence. That
  check is doing more work than its name suggests and must be kept.

## Non-goals

- Retiring `player_four_factors_by_game` — see Decisions; it earns its place.
- Any read-layer or app change.
- New metrics, clutch, lineup combos, corner-3 or shot zones.
- Season-scoped lineup identity and the player identity layer — still
  prerequisites for a second season, and still out of scope here.
- Dropping `pws` — that is migration 010.
