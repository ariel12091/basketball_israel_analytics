#!/usr/bin/env python
"""Apply and verify function-only migration 045 without timeout overrides."""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from euroleague_possessions.postgres_backend import connect_from_env_file  # noqa: E402
from apply_045_tab8_query_shape import (  # noqa: E402
    ENV,
    FUNCTIONS,
    INDEX_NAME,
    MIGRATION,
    candidate_statements,
    function_state,
    index_state,
)


EXPECTED_PRE = {
    "onoff": "083d6ff31f82cbe62083b82f36d6b4c17ac994e613d064317e7fe0b2ddbd4f82",
    "ff": "3bac5d68cb82f0e0a0f7d8e3367eb26b57f728af2649673e192ea59e8bad6c3a",
}


def verify_aligned(cur) -> None:
    state = function_state(cur)
    for key, info in state.items():
        if info["reads_view"] or not info["reads_fact"]:
            raise RuntimeError(key + ": migration did not align the fact source")
        if info["security_definer"]:
            raise RuntimeError(key + ": SECURITY mode changed")
        if not info["app_readonly_execute"]:
            raise RuntimeError(key + ": app_readonly lost EXECUTE")
    if any(row["name"] == INDEX_NAME for row in index_state(cur)):
        raise RuntimeError("function-only migration unexpectedly created the candidate index")
    for key, signature in FUNCTIONS.items():
        cur.execute(
            "SELECT proconfig FROM pg_proc WHERE oid = %s::regprocedure",
            (signature,),
        )
        settings = cur.fetchone()[0] or []
        if any(setting.startswith("work_mem=") for setting in settings):
            raise RuntimeError(key + ": function-only migration changed work_mem")


def main() -> int:
    con = connect_from_env_file(ENV, direct_port=5432)
    cur = con.cursor()
    try:
        cur.execute("SHOW lock_timeout")
        lock_timeout = cur.fetchone()[0]
        cur.execute("SHOW statement_timeout")
        statement_timeout = cur.fetchone()[0]
        print("inherited timeouts: lock=%s statement=%s"
              % (lock_timeout, statement_timeout))

        before = function_state(cur)
        for key, expected in EXPECTED_PRE.items():
            if before[key]["sha256"] != expected:
                raise RuntimeError(
                    "%s pre-change hash mismatch: %s" %
                    (key, before[key]["sha256"]))
        if any(row["name"] == INDEX_NAME for row in index_state(cur)):
            raise RuntimeError("candidate index already exists; refusing ambiguous state")

        cur.execute("BEGIN")
        for statement in candidate_statements(MIGRATION):
            cur.execute(statement)
        cur.execute("SHOW lock_timeout")
        effective_lock_timeout = cur.fetchone()[0]
        cur.execute("SHOW statement_timeout")
        effective_statement_timeout = cur.fetchone()[0]
        if effective_lock_timeout != "0" or effective_statement_timeout != "0":
            raise RuntimeError("migration did not disable transaction timeouts")
        verify_aligned(cur)
        cur.execute("COMMIT")
        verify_aligned(cur)
        print("migration 045 committed: functions aligned; indexes/settings unchanged")
        return 0
    except Exception:
        try:
            cur.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        cur.close()
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
