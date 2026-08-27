#!/usr/bin/env python
"""Apply and verify EuroLeague migration 042.

The migration adds one private typed game/player fact and one scoped app
reader. Publication is allowed only after exact full-season official-field
parity with the existing materialized view, exact full-row bounded parity with
the legacy dynamic reader, security checks, and a <=500 ms warm median for
broad and last-10 app calls. ``--dry-run`` performs every gate and rolls the
transaction back.
"""

from __future__ import annotations

import argparse
import re
import statistics
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))

from euroleague_possessions.postgres_backend import (  # noqa: E402
    _split_sql_statements,
    connect_from_env_file,
    inspect_target,
)

DDL_PATH = REPO / "sql" / "042_player_traditional_pergame.sql"
ENV_PATH = REPO.parent / "etl" / ".Renviron"
FACT = "euroleague.player_traditional_by_game"
READER = "euroleague.get_player_traditional_pergame"
ORDER = 'team_id, player_id'
WARM_LIMIT_SECONDS = 0.500


def executable_text(statement: str) -> str:
    return "\n".join(
        line for line in statement.splitlines()
        if not line.strip().startswith("--")
    )


def validate_ddl(ddl: str) -> list[str]:
    if "EUROLEAGUE SHADOW SCHEMA" not in ddl.upper():
        raise ValueError("EuroLeague migration safety marker is missing")
    executable = executable_text(ddl).upper()
    if re.search(r"\bBASKETBALL(?:_TEST)?\s*\.", executable):
        raise ValueError("migration references a forbidden Israeli schema")
    if re.search(r"\b(?:DROP|TRUNCATE|CASCADE)\b", executable):
        raise ValueError("migration 042 must be additive")

    statements = _split_sql_statements(ddl)
    kept = []
    for statement in statements:
        normalized = " ".join(executable_text(statement).upper().split()).rstrip(";")
        if normalized in {"BEGIN", "COMMIT"}:
            continue
        kept.append(statement)
    return kept


def run(cursor, function: str, extra: str = "") -> tuple[list[tuple], float]:
    args = "p_competition => 'E', p_game_year => 2025"
    if extra:
        args += ", " + extra
    started = time.perf_counter()
    cursor.execute(
        f"SELECT * FROM {function}({args}) t ORDER BY {ORDER}"
    )
    rows = [tuple(row) for row in cursor.fetchall()]
    return rows, time.perf_counter() - started


def mv_rows(cursor) -> list[tuple]:
    cursor.execute(
        'SELECT team_id, player_id, team_name, "Player", gp, poss_on_floor, '
        'minutes, pts, reb, oreb, dreb, ast, stl, blk, dfl, tov, fgm, fga, '
        'fg_pct, "3pm", "3pa", tp_pct, ftm, fta, ft_pct, efg, ts, usg_pct '
        'FROM euroleague.player_traditional_stats_mv '
        "WHERE competition='E' AND game_year=2025 ORDER BY team_id, player_id"
    )
    return [tuple(row) for row in cursor.fetchall()]


def assert_equal(label: str, expected: list[tuple], actual: list[tuple]) -> None:
    if expected == actual:
        print(f"  OK  {label}: {len(actual)} rows identical")
        return
    print(f"  FAIL {label}: {len(expected)} expected vs {len(actual)} actual")
    for index, (left, right) in enumerate(zip(expected, actual)):
        if left != right:
            print(f"       first difference row {index}\n       old={left}\n       new={right}")
            break
    raise RuntimeError(f"{label} parity failed")


def compare_mv(cursor, actual: list[tuple]) -> None:
    """The legacy season MV is a useful independent count/exposure check.

    It falls back to official box-score minutes when a reconstructed exposure
    row is absent and carries team totals through every roster game before its
    season-level filter. The app reader uses reconstructed exposure and removes
    an ineligible player/game before aggregation. Therefore this independent
    check gates identities and official counting/percentage fields; GP,
    exposure, TS and USG are gated against the actual app reader below.
    """
    expected = mv_rows(cursor)
    assert_equal(
        "full season vs existing MV (official fields)",
        [row[:4] + row[7:26] for row in expected],
        [row[:4] + row[7:26] for row in actual],
    )
    labels = ((4, "GP"), (5, "poss"), (6, "minutes"), (26, "TS"), (27, "USG"))
    differences = {
        label: sum(left[index] != right[index]
                   for left, right in zip(expected, actual))
        for index, label in labels
    }
    print(f"       informational MV eligibility differences: {differences}")


def representative_team(cursor) -> int:
    cursor.execute(
        "SELECT team_id FROM euroleague.final_schedule_mv "
        "WHERE competition='E' AND game_year=2025 GROUP BY team_id "
        "ORDER BY count(*) DESC, team_id LIMIT 1"
    )
    row = cursor.fetchone()
    if row is None:
        raise RuntimeError("no EuroLeague 2025 team found")
    return int(row[0])


def benchmark(cursor, label: str, extra: str) -> None:
    _, cold = run(cursor, READER, extra)
    warm = [run(cursor, READER, extra)[1] for _ in range(3)]
    median = statistics.median(warm)
    print(
        f"  {label:<12} cold={cold * 1000:7.1f} ms  "
        f"warm={','.join(f'{x * 1000:.1f}' for x in warm)} ms  "
        f"median={median * 1000:.1f} ms"
    )
    if median > WARM_LIMIT_SECONDS:
        raise RuntimeError(
            f"{label} warm median {median * 1000:.1f} ms exceeds 500 ms"
        )


def security_gates(cursor) -> None:
    cursor.execute(
        "SELECT c.relrowsecurity, EXISTS (SELECT 1 FROM pg_policy p "
        "WHERE p.polrelid=c.oid AND p.polname='app_readonly_select_all') "
        "FROM pg_class c WHERE c.oid=%s::regclass",
        (FACT,),
    )
    rls, policy = cursor.fetchone()
    cursor.execute(
        "SELECT has_table_privilege('app_readonly', %s, 'SELECT')",
        (FACT,),
    )
    app_table_select = bool(cursor.fetchone()[0])
    reader_sig = (
        "euroleague.get_player_traditional_pergame(text,integer,date,date,"
        "text,text,text,text,text,text,integer,text,integer,integer,integer)"
    )
    refresh_sig = (
        "euroleague.refresh_player_traditional_by_game_for_games(bigint[])"
    )
    cursor.execute(
        "SELECT has_function_privilege('app_readonly', %s, 'EXECUTE'), "
        "has_function_privilege('app_readonly', %s, 'EXECUTE')",
        (reader_sig, refresh_sig),
    )
    can_read, can_refresh = map(bool, cursor.fetchone())
    if not rls or not policy or app_table_select or not can_read or can_refresh:
        raise RuntimeError(
            "security gate failed: "
            f"rls={rls} policy={policy} table_select={app_table_select} "
            f"reader_execute={can_read} refresh_execute={can_refresh}"
        )
    print("  OK  private fact, RLS policy, scoped reader EXECUTE")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    statements = validate_ddl(DDL_PATH.read_text(encoding="utf-8"))
    connection = connect_from_env_file(ENV_PATH, direct_port=5432)
    cursor = connection.cursor()
    try:
        target = inspect_target(connection)
        if target["euroleague_schema"] != "euroleague":
            raise RuntimeError("EuroLeague schema unavailable: " + str(target))
        print("target database={} user={} server={}:{}".format(
            target["database"], target["user"],
            target["server_address"], target["server_port"]))
        print("mode  : " + ("DRY RUN (rolled back)" if args.dry_run else "EXECUTE"))

        cursor.execute("SELECT to_regclass(%s), to_regprocedure(%s)", (
            FACT,
            "euroleague.get_player_traditional_pergame(text,integer,date,date,text,text,text,text,text,text,integer,text,integer,integer,integer)",
        ))
        if any(value is not None for value in cursor.fetchone()):
            raise RuntimeError("migration 042 objects already exist; refusing partial reapply")

        cursor.execute("BEGIN")
        cursor.execute("SET LOCAL lock_timeout='5s'")
        cursor.execute("SET LOCAL statement_timeout=0")
        print(f"applying {DDL_PATH.name}: {len(statements)} reviewed statements")
        for index, statement in enumerate(statements, 1):
            cursor.execute(statement)
            print(f"  [{index}/{len(statements)}] {statement.strip().splitlines()[-1][:72]}")

        cursor.execute(f"SELECT count(*), pg_total_relation_size('{FACT}') FROM {FACT}")
        rows, size = cursor.fetchone()
        print(f"\nfact: {rows} rows, {size / 1024 / 1024:.2f} MiB")

        print("\ncorrectness:")
        full_new, _ = run(cursor, READER)
        compare_mv(cursor, full_new)

        team_id = representative_team(cursor)
        legacy_presets = (
            ("team/full vs legacy", f"p_team_ids_csv => '{team_id}'"),
            ("team/last-5 vs legacy", f"p_team_ids_csv => '{team_id}', p_last_n_games => 5"),
            ("team/home vs legacy", f"p_team_ids_csv => '{team_id}', p_home_away => 'home'"),
        )
        for label, preset in legacy_presets:
            old_rows, old_s = run(
                cursor, "euroleague.get_player_traditional_dynamic", preset
            )
            new_rows, new_s = run(cursor, READER, preset)
            assert_equal(label, old_rows, new_rows)
            print(f"       legacy={old_s * 1000:.1f} ms new={new_s * 1000:.1f} ms")

        print("\nsecurity:")
        security_gates(cursor)

        print("\nperformance:")
        benchmark(cursor, "full season", "")
        benchmark(cursor, "last 10", "p_last_n_games => 10")

        if args.dry_run:
            cursor.execute("ROLLBACK")
            print("\nDRY RUN: all gates passed; transaction rolled back")
        else:
            cursor.execute("COMMIT")
            print("\nmigration 042 applied; all gates passed")
        return 0
    except Exception:
        try:
            cursor.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    raise SystemExit(main())
