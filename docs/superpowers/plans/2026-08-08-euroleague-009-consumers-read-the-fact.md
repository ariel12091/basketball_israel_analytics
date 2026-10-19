# EuroLeague migration 009 — consumers read the fact

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Repoint the player and team four-factor refresh functions at
`euroleague.action_team_context` and `euroleague.matchup_segments`, so the
event × team-perspective expansion is written once instead of three times.

**Architecture:** Both functions keep their signature, their target table, their
column list and their output exactly. Only the source changes: each drops its
own `event_base`/clock/segment derivation and reads the persisted fact instead.
The player function keeps its roster fan-out because that is what produces
`is_on_key`; the team function needs no fan-out and collapses to a filtered
aggregate. Nothing above them changes.

**Tech Stack:** PostgreSQL 15 on Supabase (direct port 5432), Python 3.10 with
pinned `psycopg==3.2.9`, project venv at `euroleague/.venv`.

**Spec:** `docs/superpowers/specs/2026-08-08-euroleague-009-consumers-read-the-fact-design.md`

## Global Constraints

- Work on branch `shiny/euro-tab1`. Do **not** merge or push to `main`.
- `euroleague` schema only. No Israeli (`basketball`, `basketball_test`) object
  is read or written. `apply_shadow_schema()` rejects DDL containing
  `BASKETBALL.` or `BASKETBALL_TEST.` — including inside SQL comments, because
  it upper-cases the whole file before checking.
- Every migration file must contain the literal marker `EuroLeague shadow
  schema` or `apply_shadow_schema()` refuses it.
- No migration in this plan may contain the string `DROP ` (with a trailing
  space). Use `CREATE OR REPLACE FUNCTION`. Do not quote the guard's own
  trigger string inside a comment — two earlier tasks lost a cycle to that.
- Migration 003 is superseded and must never be applied. Applied order is
  `001 → 002 → 004 → 005 → 006 → 007 → 008 → 009`.
- All DB work uses direct port 5432 via
  `connect_from_env_file(Path("../etl/.Renviron"), direct_port=5432)`, run from
  the `euroleague/` directory. Never hardcode credentials.
- Parameterized SQL only. Never interpolate a value into SQL text.
- Run Python as `./.venv/Scripts/python.exe` from `euroleague/`.
- Session default `statement_timeout` is `2min`; anything longer must
  `SET LOCAL statement_timeout` inside its transaction.
- **This is an output-identical refactor.** Compare against rows stored
  *before* the change, never two fresh runs — `now()` is the transaction
  timestamp, so a same-transaction comparison cannot reveal a `derived_at`
  difference.
- Do **not** assert a literal stored row count anywhere. The gate is the
  bidirectional zero-diff. Row counts move whenever the grain is legitimately
  corrected, and hardcoding one turns an improvement into a spurious failure.
- Commit after each task. Commit messages end with:
  ```
  Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01VoYwMt63fpfNoeKjfgWmdL
  ```

## File Structure

| File | Responsibility |
|---|---|
| `euroleague/sql/009_consumers_read_the_fact.sql` | Create — `CREATE OR REPLACE` for both refresh functions, reading the fact. |
| `euroleague/scripts/snapshot_four_factor_grains.py` | Create — capture both grains to disk before the change, and diff after. The output-identical evidence. |
| `euroleague/scripts/load_games.py` | Modify — replace the coverage check with the promoted bidirectional diff. |
| `euroleague/src/euroleague_possessions/postgres_backend.py` | Modify — `validate_game()`'s expected-row-count check becomes a coverage assertion. |
| `euroleague/tests/test_postgres_backend.py` | Modify — cover the changed assertion. |
| `euroleague/RUNBOOK.md`, `euroleague/PROJECT.md` | Modify — migration order and status. |

---

## Task 1: Snapshot both grains before anything changes

Without a stored "before", the output-identical gate cannot be run at all. This
task must land before any function is touched.

**Files:**
- Create: `euroleague/scripts/snapshot_four_factor_grains.py`

**Interfaces:**
- Produces: `snapshot(conn, out_dir: Path) -> dict[str, int]` writing one `.tsv`
  per key of `GRAINS` — `player_grain`, `team_grain`, `season_onoff`,
  `season_four_factors` — and returning row counts per grain;
  `diff(conn, out_dir: Path) -> list[str]` returning human-readable differences,
  empty when identical. Tasks 2, 3 and 4 call both.

- [ ] **Step 1: Write the snapshot script**

Create `euroleague/scripts/snapshot_four_factor_grains.py`:

```python
#!/usr/bin/env python
"""Capture and diff the two four-factor grains around migration 009.

009 is an output-identical refactor, so the evidence is a comparison against
rows stored BEFORE the change. Two fresh runs inside one transaction cannot
show a derived_at difference, because now() is the transaction timestamp.

    .venv/Scripts/python.exe scripts/snapshot_four_factor_grains.py --save
    .venv/Scripts/python.exe scripts/snapshot_four_factor_grains.py --diff
"""
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))

from euroleague_possessions.postgres_backend import connect_from_env_file  # noqa: E402

# Lineage columns are expected to move on a re-derivation and are excluded.
PLAYER_GRAIN = """
SELECT player_id, team_id, game_id, game_year, is_on_key, type_lineup,
       num_starters, own_starters, opp_starters,
       total_points, total_poss, ts_poss_count, oreb_count,
       oreb_opportunities, tov_count, steal_count, total_ft_attempts,
       total_fga, total_fgm, total_fg3_made,
       player_ts_poss_count, player_tov_count, minutes,
       fg2_made, fg2_att, fg3_made, fg3_att,
       layup_made, layup_att, dunk_made, dunk_att,
       deflection_count, c3_made, c3_att, c3_known_att, onoff_minutes
  FROM euroleague.player_four_factors_by_game
 ORDER BY game_id, team_id, player_id, is_on_key, type_lineup,
          own_starters, opp_starters
"""

TEAM_GRAIN = """
SELECT game_id, team_id, game_year, own_starters, opp_starters,
       off_pts, off_poss, off_ts_poss, off_oreb, off_oreb_opp, off_tov,
       off_fta, off_fga, off_fgm, off_fg3m,
       def_pts, def_poss, def_ts_poss, def_oreb, def_oreb_opp, def_tov,
       def_fta, def_fga, def_fgm, def_fg3m, def_steals
  FROM euroleague.team_four_factors_by_game
 ORDER BY game_id, team_id, own_starters, opp_starters
"""

# Season-level rates: what anything the app displays actually reads. A grain
# change that leaves every rate untouched is a different thing from one that
# moves them, and this is the check that told those apart during 008.
# Column names verified against the live catalog on 2026-08-08 -- note it is
# net_on_off, NOT net_rating_on/net_rating_off.
SEASON_ONOFF = """
SELECT game_year, team_id, player_id,
       off_on_points, off_on_poss, off_off_points, off_off_poss,
       def_on_points, def_on_poss, def_off_points, def_off_poss,
       minutes_on, off_rating_on, off_rating_off,
       def_rating_on, def_rating_off, net_on_off
  FROM euroleague.player_onoff_by_season
 ORDER BY game_year, team_id, player_id
"""

SEASON_FOUR_FACTORS = """
SELECT game_year, team_id, player_id,
       off_on_ts, off_off_ts, def_on_ts, def_off_ts,
       off_on_efg, off_off_efg, def_on_efg, def_off_efg,
       off_on_oreb, off_off_oreb, def_on_oreb, def_off_oreb,
       off_on_tov, off_off_tov, def_on_tov, def_off_tov,
       off_on_ftr, off_off_ftr, def_on_ftr, def_off_ftr,
       off_on_poss, off_off_poss, def_on_poss, def_off_poss
  FROM euroleague.player_four_factors_by_season
 ORDER BY game_year, team_id, player_id
"""

GRAINS = {
    "player_grain": PLAYER_GRAIN,
    "team_grain": TEAM_GRAIN,
    "season_onoff": SEASON_ONOFF,
    "season_four_factors": SEASON_FOUR_FACTORS,
}


def _write(conn: Any, sql: str, path: Path) -> int:
    cur = conn.cursor()
    cur.execute("SET LOCAL statement_timeout = '15min'")
    cur.execute(sql)
    rows = cur.fetchall()
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, delimiter="\t")
        writer.writerow([d.name for d in cur.description])
        for row in rows:
            writer.writerow(["" if v is None else str(v) for v in row])
    cur.close()
    return len(rows)


def snapshot(conn: Any, out_dir: Path) -> dict[str, int]:
    out_dir.mkdir(parents=True, exist_ok=True)
    counts = {}
    for name, sql in GRAINS.items():
        counts[name] = _write(conn, sql, out_dir / f"{name}.tsv")
    return counts


def diff(conn: Any, out_dir: Path) -> list[str]:
    problems: list[str] = []
    for name, sql in GRAINS.items():
        before = out_dir / f"{name}.tsv"
        if not before.exists():
            problems.append(f"{name}: no snapshot at {before}")
            continue
        after = out_dir / f"{name}.after.tsv"
        _write(conn, sql, after)
        old = before.read_text(encoding="utf-8").splitlines()
        new = after.read_text(encoding="utf-8").splitlines()
        if old == new:
            continue
        if len(old) != len(new):
            problems.append(f"{name}: {len(old)} rows before, {len(new)} after")
        shown = 0
        for i, (a, b) in enumerate(zip(old, new)):
            if a != b and shown < 3:
                problems.append(f"{name}: line {i}\n  before: {a}\n  after:  {b}")
                shown += 1
    return problems


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--save", action="store_true")
    parser.add_argument("--diff", action="store_true")
    parser.add_argument("--dir", default=str(REPO / "exports" / "009_snapshot"))
    args = parser.parse_args()
    if args.save == args.diff:
        parser.error("pass exactly one of --save or --diff")

    conn = connect_from_env_file(Path("../etl/.Renviron"), direct_port=5432)
    out_dir = Path(args.dir)
    try:
        if args.save:
            for name, n in snapshot(conn, out_dir).items():
                print(f"  saved {name}: {n:,} rows")
            print(f"\nSnapshot written to {out_dir}")
        else:
            problems = diff(conn, out_dir)
            for line in problems:
                print(f"  {line}")
            if problems:
                print(f"\n{len(problems)} DIFFERENCE(S)")
                sys.exit(1)
            print("IDENTICAL — every comparison byte-for-byte")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Save the baseline**

```bash
cd euroleague
./.venv/Scripts/python.exe scripts/snapshot_four_factor_grains.py --save
```
Expected: four files written. Record the row counts in your report — do not
assert any particular value.

- [ ] **Step 3: Prove the diff detects a change**

The snapshot is worthless if `--diff` cannot fail. Verify it inside a
transaction you roll back:

```bash
cd euroleague
./.venv/Scripts/python.exe -c "
import sys; from pathlib import Path; sys.path.insert(0,'src')
from euroleague_possessions.postgres_backend import connect_from_env_file
conn = connect_from_env_file(Path('../etl/.Renviron'), direct_port=5432)
cur = conn.cursor(); cur.execute('BEGIN')
cur.execute(\"UPDATE euroleague.team_four_factors_by_game SET off_pts = off_pts + 1 WHERE game_id = 1\")
print('mutated rows:', cur.rowcount)
sys.path.insert(0,'scripts')
from snapshot_four_factor_grains import diff
print('differences found:', len(diff(conn, Path('exports/009_snapshot'))))
cur.execute('ROLLBACK'); conn.close()"
```
Expected: a non-zero mutation count and at least one difference reported. Then
re-run `--diff` normally and expect `IDENTICAL`.

- [ ] **Step 4: Commit**

```bash
git add euroleague/scripts/snapshot_four_factor_grains.py
git commit -m "Snapshot both four-factor grains before migration 009"
```

The `exports/` directory is gitignored; the snapshot files are working state,
not artifacts to commit.

---

## Task 2: Rewrite the team refresh to read the fact

The team grain goes first because it is the simpler of the two — no roster
fan-out — so a failure here is unambiguous.

**Files:**
- Create: `euroleague/sql/009_consumers_read_the_fact.sql`

**Interfaces:**
- Consumes: `snapshot_four_factor_grains.py` from Task 1.
- Produces: `euroleague.refresh_team_four_factors_by_game_for_games(bigint[])
  RETURNS bigint`, unchanged signature, reading the fact.

- [ ] **Step 1: Write the migration file**

Create `euroleague/sql/009_consumers_read_the_fact.sql`. It must open with the
guard marker:

```sql
-- EuroLeague shadow schema — migration 009: consumers read the event fact.
--
-- Both four-factor refresh functions stop re-deriving the event x team
-- perspective expansion from actions_raw and read
-- euroleague.action_team_context instead. Signatures, target tables, column
-- lists and outputs are unchanged; only the source differs. Migration 008
-- built and verified the fact; this is what makes it load-bearing.

BEGIN;

CREATE OR REPLACE FUNCTION euroleague.refresh_team_four_factors_by_game_for_games(
  game_ids bigint[]
)
RETURNS bigint
LANGUAGE plpgsql
AS $function$
DECLARE
  inserted_count bigint := 0;
BEGIN
  IF game_ids IS NULL OR array_length(game_ids, 1) IS NULL THEN
    DELETE FROM euroleague.team_four_factors_by_game;
  ELSE
    DELETE FROM euroleague.team_four_factors_by_game WHERE game_id = ANY(game_ids);
  END IF;

  INSERT INTO euroleague.team_four_factors_by_game (
    game_id, team_id, game_year, own_starters, opp_starters,
    off_pts, off_poss, off_ts_poss, off_oreb, off_oreb_opp, off_tov,
    off_fta, off_fga, off_fgm, off_fg3m,
    def_pts, def_poss, def_ts_poss, def_oreb, def_oreb_opp, def_tov,
    def_fta, def_fga, def_fgm, def_fg3m, def_steals,
    derivation_version
  )
  -- No roster fan-out: the team grain needs no player dimension, so this is a
  -- straight aggregate over the fact, pivoted on type_lineup into the existing
  -- off_*/def_* column pairs. Rows the contract leaves unsided (substitutions,
  -- timeouts, period markers) carry no measure and are excluded.
  SELECT
    atc.game_id,
    atc.team_id,
    s.season::smallint AS game_year,
    atc.own_starters,
    atc.opp_starters,
    sum(atc.points)             FILTER (WHERE atc.type_lineup = 'offense')::numeric,
    sum(atc.possession_flag)    FILTER (WHERE atc.type_lineup = 'offense')::bigint,
    sum(atc.ts_possessions)     FILTER (WHERE atc.type_lineup = 'offense')::bigint,
    sum(atc.orebounds)          FILTER (WHERE atc.type_lineup = 'offense')::bigint,
    sum(atc.oreb_opportunities) FILTER (WHERE atc.type_lineup = 'offense')::bigint,
    sum(atc.turnovers)          FILTER (WHERE atc.type_lineup = 'offense')::bigint,
    sum(atc.ft_attempts)        FILTER (WHERE atc.type_lineup = 'offense')::bigint,
    sum(atc.fga)                FILTER (WHERE atc.type_lineup = 'offense')::bigint,
    sum(atc.fgm)                FILTER (WHERE atc.type_lineup = 'offense')::bigint,
    sum(atc.fg3_made)           FILTER (WHERE atc.type_lineup = 'offense')::bigint,
    sum(atc.points)             FILTER (WHERE atc.type_lineup = 'defense')::numeric,
    sum(atc.possession_flag)    FILTER (WHERE atc.type_lineup = 'defense')::bigint,
    sum(atc.ts_possessions)     FILTER (WHERE atc.type_lineup = 'defense')::bigint,
    sum(atc.orebounds)          FILTER (WHERE atc.type_lineup = 'defense')::bigint,
    sum(atc.oreb_opportunities) FILTER (WHERE atc.type_lineup = 'defense')::bigint,
    sum(atc.turnovers)          FILTER (WHERE atc.type_lineup = 'defense')::bigint,
    sum(atc.ft_attempts)        FILTER (WHERE atc.type_lineup = 'defense')::bigint,
    sum(atc.fga)                FILTER (WHERE atc.type_lineup = 'defense')::bigint,
    sum(atc.fgm)                FILTER (WHERE atc.type_lineup = 'defense')::bigint,
    sum(atc.fg3_made)           FILTER (WHERE atc.type_lineup = 'defense')::bigint,
    sum(atc.steals)             FILTER (WHERE atc.type_lineup = 'defense')::bigint,
    'existing-israeli-contract-v1'
  FROM euroleague.action_team_context atc
  JOIN euroleague.schedule s ON s.game_id = atc.game_id
 WHERE (game_ids IS NULL OR atc.game_id = ANY(game_ids))
   AND atc.type_lineup IS NOT NULL
 GROUP BY atc.game_id, atc.team_id, s.season, atc.own_starters, atc.opp_starters;

  GET DIAGNOSTICS inserted_count = ROW_COUNT;
  RETURN inserted_count;
END;
$function$;

COMMIT;
```

**Verify the `def_steals` side before trusting this.** The steal flip is the one
place the side rule is counter-intuitive: `def_steals` is the row team's own
steals, recorded on its **defense** row. If the diff in Step 3 shows
`def_steals` differing, that is the cause — do not "fix" it by flipping the
filter until you have confirmed against migration 006's original expression.

- [ ] **Step 2: Apply and re-derive**

```bash
cd euroleague
./.venv/Scripts/python.exe -c "
import sys; from pathlib import Path; sys.path.insert(0,'src')
from euroleague_possessions.postgres_backend import connect_from_env_file, apply_shadow_schema
conn = connect_from_env_file(Path('../etl/.Renviron'), direct_port=5432)
apply_shadow_schema(conn, Path('sql/009_consumers_read_the_fact.sql'))
cur = conn.cursor(); cur.execute('BEGIN')
cur.execute(\"SET LOCAL statement_timeout = '30min'\")
cur.execute('SELECT euroleague.refresh_team_four_factors_by_game_for_games(NULL::bigint[])')
print('team rows:', cur.fetchone()[0])
cur.execute('COMMIT'); conn.close()"
```

- [ ] **Step 3: Diff against the baseline**

```bash
cd euroleague
./.venv/Scripts/python.exe scripts/snapshot_four_factor_grains.py --diff
```
Expected: `IDENTICAL`. Anything else is a defect in this task — report the
differing lines verbatim rather than adjusting the snapshot.

- [ ] **Step 4: Commit**

```bash
git add euroleague/sql/009_consumers_read_the_fact.sql
git commit -m "Read the event fact in the team four-factor refresh"
```

---

## Task 3: Rewrite the player refresh to read the fact

**Files:**
- Modify: `euroleague/sql/009_consumers_read_the_fact.sql` (append; do not create
  a second migration file)

**Interfaces:**
- Consumes: the fact and `euroleague.matchup_segments`.
- Produces: `euroleague.refresh_player_four_factors_by_game_for_games(bigint[])
  RETURNS bigint`, unchanged signature.

- [ ] **Step 1: Append the player function**

The replacement body is the query already proven by migration 008's gate —
`PLAYER_GRAIN_FROM_FACT` in `euroleague/scripts/verify_action_team_context.py`.
Read that file and port it, replacing its `%(game_ids)s::bigint[]` placeholders
with the function's `game_ids` parameter. Do not rewrite it from scratch: it is
verified against all 84 games and any deviation is a regression.

**Why this plan points at the file instead of inlining the SQL.** Every other
code step here gives you the exact text to write. This one deliberately does
not, because a copy in the plan would be a second version of a query whose only
guarantee is that it is byte-for-byte the verified one. Duplicating it is the
same hazard this entire migration exists to remove — three copies of one rule,
free to drift. The structure below is the checklist for confirming your port is
faithful, not a substitute for reading the source.

The structure to preserve exactly:

- `real_roster` — `full_rosters` joined to `players`, excluding
  `provider_player_id` and `display_name` in `('team','total')`.
- `player_minutes` — from `matchup_segments`, `is_on_key` as a plain
  `lineup_players` membership test against `ms.own_lineup_id`,
  `round(sum(ms.segment_seconds) / 60.0, 3) AS minutes`.
- `complete_grid` — `player_minutes CROSS JOIN (VALUES ('offense'),('defense'))`.
- `counts` — measures off `action_team_context` where `type_lineup IS NOT NULL`.
- Final `SELECT` — `complete_grid LEFT JOIN counts` on the full key, with
  `minutes` and `onoff_minutes` both wrapped in
  `CASE WHEN type_lineup = 'offense' THEN ... ELSE 0 END`.
- `deflection_count`, `c3_made`, `c3_att`, `c3_known_att` stay hardcoded `0`.
  EuroLeague has no shots endpoint and no deflection event.
- `derivation_version` stays `'existing-israeli-contract-v1'`.

**Remove** the `PERFORM euroleague.refresh_stint_timing_for_games(game_ids);`
that opens the current function (007 line 133). Migration 008 moved that call
into the fact's refresh and left this one deliberately, rather than modify a
function it was not verifying. With this rewrite it is redundant: the fact this
function now reads was built by a refresh that already ran it.

- [ ] **Step 2: Apply and re-derive**

```bash
cd euroleague
./.venv/Scripts/python.exe -c "
import sys; from pathlib import Path; sys.path.insert(0,'src')
from euroleague_possessions.postgres_backend import connect_from_env_file, apply_shadow_schema
conn = connect_from_env_file(Path('../etl/.Renviron'), direct_port=5432)
apply_shadow_schema(conn, Path('sql/009_consumers_read_the_fact.sql'))
cur = conn.cursor(); cur.execute('BEGIN')
cur.execute(\"SET LOCAL statement_timeout = '30min'\")
cur.execute('SELECT euroleague.refresh_player_four_factors_by_game_for_games(NULL::bigint[])')
print('player rows:', cur.fetchone()[0])
cur.execute('COMMIT'); conn.close()"
```

- [ ] **Step 3: Diff against the baseline**

```bash
cd euroleague
./.venv/Scripts/python.exe scripts/snapshot_four_factor_grains.py --diff
```
Expected: `IDENTICAL` across all four comparisons, including both season
views. Those two are what anything the app displays actually reads.

- [ ] **Step 4: Confirm the 008 gate still passes**

```bash
cd euroleague
./.venv/Scripts/python.exe scripts/verify_action_team_context.py
```
Expected: `GATE PASSED`, 8/8.

Note what this now means: the gate compares the fact against a table that is
itself derived from the fact. It is no longer an independent cross-check of the
grain, and it will pass trivially on checks 6-8. Checks 1-5 remain meaningful
(coverage, two rows per action, possession side, unmodelled columns, segment
tiling). Record this in your report — Task 4 acts on it.

- [ ] **Step 5: Measure the cost**

```bash
cd euroleague
./.venv/Scripts/python.exe scripts/probe_batched_publish.py --games 1-3
```
Expected: `ALL PROBES PASSED`. Record the `validate` timing per game and
compare it to the 1.86-2.32s recorded in `RUNBOOK.md`. A regression is a defect,
not a cost of the refactor — report it rather than accepting it.

- [ ] **Step 6: Commit**

```bash
git add euroleague/sql/009_consumers_read_the_fact.sql
git commit -m "Read the event fact in the player four-factor refresh"
```

---

## Task 4: Replace the checks that became tautological

Two checks lost their meaning in Task 3 and must not be silently kept.

**Files:**
- Modify: `euroleague/src/euroleague_possessions/postgres_backend.py`
- Modify: `euroleague/scripts/load_games.py`
- Modify: `euroleague/tests/test_postgres_backend.py`

**Interfaces:**
- Consumes: `snapshot_four_factor_grains.py` from Task 1.

- [ ] **Step 1: Write the failing test**

Append to `euroleague/tests/test_postgres_backend.py`, inside
`ActionTeamContextWiringTest`:

```python
    def test_validate_game_does_not_predict_the_grain_from_segments(self) -> None:
        """The row-count expectation must not be derived from matchup_segments.

        It was a real check only while the player refresh derived its own
        segments: the two sides then came from different sources. Once that
        refresh reads matchup_segments, an expectation derived from the same
        table compares a value against itself and can never fail.

        Asserted on source text on purpose, unlike
        test_schema_allowlist_accepts_the_derived_fact, which was deliberately
        moved off inspect.getsource because it had behaviour to drive. This one
        does not: the property is the ABSENCE of a query, and once the check is
        gone there is nothing left to observe at runtime. A mock-based version
        would assert over recorded SQL strings, which is the same text
        assertion one layer removed, with more machinery.
        """
        import inspect

        from euroleague_possessions import postgres_backend

        source = inspect.getsource(postgres_backend.PostgresTransactionBackend.validate_game)
        self.assertNotIn("matchup_segments", source)
```

- [ ] **Step 2: Run it to verify it fails**

```bash
cd /c/Users/ariel/documents/on_off_israel_pbp
./euroleague/.venv/Scripts/python.exe -m unittest discover -s euroleague/tests -k does_not_predict -v
```
Expected: FAIL — `matchup_segments` is currently in that function.

- [ ] **Step 3: Replace the expectation with a coverage assertion**

In `validate_game()`, delete the `WITH real_roster AS (... observed ...)` query
and its `expected_analytics_rows` comparison, and replace the check with:

```python
            # Coverage, not a predicted row count. Predicting the count meant
            # deriving the expectation from matchup_segments, which was a real
            # check only while this refresh derived its own segments. Migration
            # 009 pointed it at the same table, so the comparison would be a
            # value against itself. The bidirectional grain diff now runs in
            # load_games.py --verify-only, where it can still fail.
            if analytics_rows <= 0:
                mismatches.append("player four-factor refresh produced no rows")
```

- [ ] **Step 4: Run the suite**

```bash
cd /c/Users/ariel/documents/on_off_israel_pbp
./euroleague/.venv/Scripts/python.exe -m unittest discover -s euroleague/tests
```
Expected: OK. Record the test count in your report; do not assert a literal.

- [ ] **Step 5: Promote the grain diff into `--verify-only`**

In `load_games.py`'s `verify()`, after the existing
`"all games have the event fact"` check, add:

```python
    fact_only = q(
        "WITH from_fact AS ("
        "  SELECT atc.game_id, atc.team_id, atc.own_starters, atc.opp_starters, "
        "         sum(atc.points) FILTER (WHERE atc.type_lineup = 'offense') AS off_pts "
        "    FROM euroleague.action_team_context atc "
        "   WHERE atc.type_lineup IS NOT NULL "
        "   GROUP BY atc.game_id, atc.team_id, atc.own_starters, atc.opp_starters"
        ") SELECT count(*) FROM ("
        "  SELECT game_id, team_id, own_starters, opp_starters, off_pts FROM from_fact "
        "  EXCEPT ALL "
        "  SELECT game_id, team_id, own_starters, opp_starters, off_pts "
        "    FROM euroleague.team_four_factors_by_game) x"
    )[0][0]
    check(
        "team grain reproduces from the fact",
        fact_only == 0,
        f"{fact_only} rows differ",
    )
```

This is deliberately the team grain, not the player grain: the team side needs
no roster fan-out, so it is cheap enough to run on every load, and it exercises
the same fact columns.

- [ ] **Step 6: Run it**

```bash
cd euroleague
./.venv/Scripts/python.exe scripts/load_games.py --games 1-84 --verify-only
```
Expected: `ALL CHECKS PASSED`, with the new check present. Record the check
count; do not assert a literal.

- [ ] **Step 7: Commit**

```bash
git add euroleague/src/euroleague_possessions/postgres_backend.py euroleague/scripts/load_games.py euroleague/tests/test_postgres_backend.py
git commit -m "Replace the four-factor checks that migration 009 made tautological"
```

---

## Task 5: Update the docs

**Files:**
- Modify: `euroleague/RUNBOOK.md`
- Modify: `euroleague/PROJECT.md`

- [ ] **Step 1: Update RUNBOOK.md**

Change the applied-migration order to
`001 → 002 → 004 → 005 → 006 → 007 → 008 → 009`. Update the publication phase
table with the `validate` timing measured in Task 3 Step 5.

- [ ] **Step 2: Update PROJECT.md**

Add `| Consumers read the event fact | Migration 009 | Applied |` to the
repository/live state table, update the apply-order line, and rewrite the known
issue that says the fact "exists and is populated but nothing reads it" to say
both four-factor refreshes now read it and the expansion is written once.

Do **not** write a literal row count into either document. Three separate
places hardcoded a consequence of this grain during migration 008 and all three
broke when the grain was corrected.

- [ ] **Step 3: Commit**

```bash
git add euroleague/RUNBOOK.md euroleague/PROJECT.md
git commit -m "Record migration 009 in the runbook and project status"
```

---

## Done when

- `snapshot_four_factor_grains.py --diff` reports `IDENTICAL` across the player
  grain, the team grain and both season views.
- `verify_action_team_context.py` passes all eight checks.
- `probe_batched_publish.py --games 1-3` passes, with `validate` timing no worse
  than the range recorded in `RUNBOOK.md`.
- `load_games.py --games 1-84 --verify-only` passes, including the new team-grain
  reproduction check.
- The Python suite passes.
- Neither refresh function contains an `event_base` CTE or reads `actions_raw`.
- `main` is untouched; all work is on `shiny/euro-tab1`.

## Explicitly not in this plan

- Retiring `player_four_factors_by_game`. A player-level query over the fact
  must fan each event out across the roster — 1,139,243 rows for 84 games —
  which the aggregate collapses ~10× to 115,034, once at load rather than per
  query. `onoff_compute()` is on the app's interactive path.
- Any read-layer or app change: `player_game_context`, `onoff_compute()`,
  `four_factors_compute()` and the MVs keep the same inputs.
- New metrics, clutch, lineup combos, corner-3 or shot zones.
- Reclassifying `OF` (offensive foul), currently in the defence list. It carries
  no measure and is never a possession endpoint, so nothing moves until a
  consumer counts fouls by side.
- Season-scoped lineup identity and the player identity layer.
- Dropping `pws` — migration 010.
