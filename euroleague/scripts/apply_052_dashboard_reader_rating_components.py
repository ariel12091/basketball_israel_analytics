#!/usr/bin/env python
"""Apply the on/off rating components to both leagues' dashboard readers.

Both readers already compute off_on_ppp, off_off_ppp, def_on_ppp and
def_off_ppp in their `p` CTE, subtract them into "Net RTG Diff", "Off ON Diff"
and "Def ON Diff", and then discard the components. This exposes them so the
app can draw the on-court and off-court rating as a range rather than only
their difference. No new scan, join or index.

Both schemas are applied in ONE transaction, because a running Shiny process
holds the pre-edit closure and only the live path notices a mismatch -- a
half-applied pair is the failure mode this guards.

The gate is parity, not benchmarking:

1. Capture each preset's full result from the CURRENT function, per schema.
2. Apply both DDL files.
3. Re-run each preset and assert the first 47 columns are byte-identical to
   what they were. Additive means additive.
4. Assert the four new columns reproduce the three Diff columns by subtraction,
   on every row where the diff is not NULL. That is what makes them the
   components rather than four unrelated numbers.
5. Assert app_readonly still holds EXECUTE -- DROP FUNCTION wipes it and each
   DDL file re-GRANTs at its foot.
6. Roll back unless --apply is passed.
"""

from __future__ import annotations

import argparse
import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))
from euroleague_possessions.postgres_backend import (  # noqa: E402
    _split_sql_statements,
    connect_from_env_file,
)

ENV = ROOT.parent / "etl" / ".Renviron"

EURO_DDL = ROOT / "sql" / "052_dashboard_reader_rating_components.sql"
ISRAELI_DDL = ROOT.parent / "sql" / "functions" / "four_factors_dashboard_compute.sql"

# Columns as they stand before this migration. Everything after index 46 is new.
EXISTING_COLUMNS = 47

# (label, call arguments). Kept narrow and cheap: the point is to prove the
# existing columns did not move, which one broad preset already shows, plus a
# filtered one because that is the path with no MV behind it.
EURO_BASE = (
    "p_competition=>'E',p_game_year=>2025,"
    "p_start_date=>DATE '2025-09-01',p_end_date=>DATE '2026-07-01'"
)
ISRAELI_BASE = (
    "p_game_year=>2026,"
    "p_start_date=>DATE '2025-10-01',p_end_date=>DATE '2026-07-01'"
)

TARGETS = (
    (
        "euroleague",
        EURO_BASE,
        (("broad", ""), ("last 10", "p_last_n_games=>10")),
    ),
    (
        "basketball_test",
        ISRAELI_BASE,
        (("broad", ""), ("last 10", "p_last_n_games=>10")),
    ),
)


def options():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true",
                        help="commit; without it the transaction is rolled back")
    return parser.parse_args()


def call(cursor, schema: str, base: str, extra: str):
    """Rows plus the column names, so nothing here indexes by a guessed position."""
    args = base + ("," + extra if extra else "")
    cursor.execute(
        f"SELECT * FROM {schema}.four_factors_dashboard_compute({args}) "
        "ORDER BY player_id, team_id"
    )
    return cursor.fetchall(), [d[0] for d in cursor.description]


def statements_of(path: Path) -> list[str]:
    return [
        s for s in _split_sql_statements(path.read_text(encoding="utf-8"))
        if s.strip() and s.strip().upper().rstrip(";") not in {"BEGIN", "COMMIT"}
    ]


def close(a, b) -> bool:
    """Equal as the app would see them, tolerating numeric scale noise."""
    if a is None or b is None:
        return a is None and b is None
    return abs(Decimal(a) - Decimal(b)) <= Decimal("0.05")


def main() -> int:
    args = options()

    connection = connect_from_env_file(ENV, direct_port=5432)
    connection.autocommit = False
    cursor = connection.cursor()
    failures: list[str] = []
    try:
        cursor.execute("SET LOCAL statement_timeout='300s'")

        print("-- before --")
        before: dict[tuple[str, str], list[tuple]] = {}
        for schema, base, presets in TARGETS:
            for label, extra in presets:
                rows, names = call(cursor, schema, base, extra)
                before[(schema, label)] = rows
                if len(names) != EXISTING_COLUMNS:
                    raise RuntimeError(
                        f"{schema}: expected {EXISTING_COLUMNS} columns before the migration, "
                        f"found {len(names)} -- is it already applied?"
                    )
                print(f"  {schema:<16} {label:<8} rows={len(rows):<6} cols={len(names)}")

        print("\n-- applying --")
        for path in (EURO_DDL, ISRAELI_DDL):
            for statement in statements_of(path):
                cursor.execute(statement)
            print(f"  applied {path.name}")

        print("\n-- after: existing columns unchanged, new columns are the components --")
        for schema, base, presets in TARGETS:
            for label, extra in presets:
                rows, names = call(cursor, schema, base, extra)
                old = before[(schema, label)]
                tag = f"{schema}/{label}"
                at = {n: i for i, n in enumerate(names)}

                if len(rows) != len(old):
                    failures.append(f"{tag}: row count {len(old)} -> {len(rows)}")
                    continue
                if len(names) != EXISTING_COLUMNS + 4:
                    failures.append(f"{tag}: expected {EXISTING_COLUMNS + 4} columns, got {len(names)}")
                    continue
                missing = [c for c in ("Net RTG Diff", "Off ON Diff", "Def ON Diff",
                                       "off_on_ppp", "off_off_ppp", "def_on_ppp", "def_off_ppp")
                           if c not in at]
                if missing:
                    failures.append(f"{tag}: columns missing by name: {missing}")
                    continue

                moved = sum(1 for n, o in zip(rows, old) if n[:EXISTING_COLUMNS] != o)
                if moved:
                    failures.append(f"{tag}: {moved} rows changed in the existing {EXISTING_COLUMNS} columns")

                bad_off = bad_def = bad_net = 0
                for r in rows:
                    net_d = r[at["Net RTG Diff"]]
                    off_d = r[at["Off ON Diff"]]
                    def_d = r[at["Def ON Diff"]]
                    on_o, off_o = r[at["off_on_ppp"]], r[at["off_off_ppp"]]
                    on_d, off_dd = r[at["def_on_ppp"]], r[at["def_off_ppp"]]
                    if off_d is not None and not close(off_d, on_o - off_o):
                        bad_off += 1
                    if def_d is not None and not close(def_d, on_d - off_dd):
                        bad_def += 1
                    if net_d is not None and not close(net_d, (on_o - off_o) - (on_d - off_dd)):
                        bad_net += 1
                if bad_off or bad_def or bad_net:
                    failures.append(
                        f"{tag}: component identity broken (off={bad_off} def={bad_def} net={bad_net})"
                    )

                print(f"  {tag:<26} rows={len(rows):<6} existing-cols identical={not moved}  "
                      f"identity off/def/net ok={not (bad_off or bad_def or bad_net)}")

        print("\n-- grants --")
        for schema, _, _ in TARGETS:
            cursor.execute(
                "SELECT has_function_privilege('app_readonly', p.oid, 'EXECUTE') "
                "FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace "
                "WHERE n.nspname = %s AND p.proname = 'four_factors_dashboard_compute'",
                (schema,),
            )
            got = [r[0] for r in cursor.fetchall()]
            print(f"  {schema:<16} app_readonly EXECUTE={got}")
            if got != [True]:
                failures.append(f"{schema}: app_readonly lost EXECUTE ({got})")

        if failures:
            print("\nFAILED:")
            for f in failures:
                print("  -", f)
            connection.rollback()
            print("\nrolled back; nothing changed")
            return 1

        if args.apply:
            connection.commit()
            print("\nAPPLIED and committed.")
        else:
            connection.rollback()
            print("\nDry run OK; rolled back. Re-run with --apply to commit.")
        return 0
    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    raise SystemExit(main())
