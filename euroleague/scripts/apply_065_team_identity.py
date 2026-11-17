#!/usr/bin/env python
"""Apply EuroLeague migration 065 (club crest and colours on euroleague.teams).

Dry run by default: the migration runs, crests are filled from the provider's
clubs endpoint, every gate runs, and the transaction rolls back. Set
CONFIRM_EL_065_APPLY=1 to commit. Safe to re-run: it only fills crest_url
where it is NULL, so a hand-corrected URL is never overwritten.

The migration only adds columns (grants are kept); it refuses any DROP,
TRUNCATE, CASCADE or Israeli-schema reference.

Gates, all before COMMIT:
1. The three columns exist and the table's grants are unchanged.
2. Every club with a row in euroleague.teams for a season the provider lists
   gets a crest (a club the provider no longer lists is reported, not failed).
3. Every crest is an https URL.
"""

from __future__ import annotations

import json
import os
import re
import sys
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))

from euroleague_possessions.postgres_backend import (  # noqa: E402
    _split_sql_statements,
    connect_from_env_file,
    inspect_target,
)

DDL_PATH = REPO / "sql" / "065_team_identity.sql"
ENV_PATH = Path(os.environ.get("EL_ENV_PATH", REPO.parent / "etl" / ".Renviron"))
CLUBS_URL = "https://api-live.euroleague.net/v2/competitions/{c}/seasons/{c}{s}/clubs?limit=100"


def executable(sql: str) -> str:
    return "\n".join(line for line in sql.splitlines() if not line.strip().startswith("--"))


def validate_ddl(ddl: str) -> list[str]:
    if "EUROLEAGUE SHADOW SCHEMA" not in ddl.upper():
        raise ValueError("missing EuroLeague safety marker")
    body = executable(ddl).upper()
    if re.search(r"\bBASKETBALL(?:_TEST)?\s*\.", body):
        raise ValueError("forbidden Israeli schema reference")
    if re.search(r"\b(?:DROP|TRUNCATE|CASCADE)\b", body):
        raise ValueError("DROP / TRUNCATE / CASCADE not allowed")
    return [s for s in _split_sql_statements(ddl)
            if " ".join(executable(s).split()).rstrip(";").upper() not in {"BEGIN", "COMMIT", ""}]


def provider_crests(competition: str, season: int) -> dict[str, str]:
    """Club code -> crest URL for one competition-season."""
    req = urllib.request.Request(CLUBS_URL.format(c=competition, s=season),
                                 headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        data = json.load(r)["data"]
    return {c["code"]: c["images"]["crest"] for c in data
            if (c.get("images") or {}).get("crest")}


def table_acl(cursor) -> str:
    cursor.execute("SELECT relacl::text FROM pg_class WHERE oid = 'euroleague.teams'::regclass")
    return cursor.fetchone()[0]


def main() -> int:
    apply = os.environ.get("CONFIRM_EL_065_APPLY") == "1"
    statements = validate_ddl(DDL_PATH.read_text(encoding="utf-8"))
    connection = connect_from_env_file(ENV_PATH, direct_port=5432)
    cursor = connection.cursor()
    try:
        target = inspect_target(connection)
        if target["euroleague_schema"] != "euroleague":
            raise RuntimeError("EuroLeague schema unavailable: " + str(target))
        print("mode=" + ("EXECUTE" if apply else "DRY RUN (CONFIRM_EL_065_APPLY=1 to commit)"))
        cursor.execute("BEGIN")
        cursor.execute("SET LOCAL lock_timeout='5s'")
        acl_before = table_acl(cursor)
        for statement in statements:
            cursor.execute(statement)

        # Gate 1: columns and grants.
        cursor.execute("""
            SELECT count(*) FROM information_schema.columns
            WHERE table_schema = 'euroleague' AND table_name = 'teams'
              AND column_name IN ('crest_url', 'primary_color', 'secondary_color')""")
        if cursor.fetchone()[0] != 3:
            raise RuntimeError("columns missing after the migration")
        if table_acl(cursor) != acl_before:
            raise RuntimeError("grants on euroleague.teams changed")

        # Crests: each club's latest season the provider lists wins.
        cursor.execute("""
            SELECT competition, provider_team_code,
                   COALESCE(last_seen_season, first_seen_season) AS last_season
            FROM euroleague.teams""")
        clubs = cursor.fetchall()
        seasons = sorted({(c, s) for c, _, s in clubs if s is not None})
        crest: dict[tuple[str, str], str] = {}
        for competition, season in seasons:
            for code, url in provider_crests(competition, season).items():
                crest[(competition, code)] = url
        filled = 0
        for (competition, code), url in crest.items():
            cursor.execute(
                "UPDATE euroleague.teams SET crest_url = %s "
                "WHERE competition = %s AND provider_team_code = %s AND crest_url IS NULL",
                (url, competition, code))
            filled += cursor.rowcount

        # Gates 2-3.
        cursor.execute("""
            SELECT competition, count(*), count(crest_url),
                   count(*) FILTER (WHERE crest_url IS NOT NULL AND crest_url NOT LIKE 'https://%%')
            FROM euroleague.teams GROUP BY competition ORDER BY competition""")
        for competition, n, with_crest, bad in cursor.fetchall():
            print(f"{competition}: clubs={n} with crest={with_crest}")
            if bad:
                raise RuntimeError(f"{competition}: {bad} crest(s) not https")
        cursor.execute("SELECT competition, provider_team_code, display_name FROM euroleague.teams "
                       "WHERE crest_url IS NULL ORDER BY 1, 2")
        missing = cursor.fetchall()
        for row in missing:
            print("no crest from the provider:", *row)
        print(f"filled {filled} crest(s)")

        if apply:
            cursor.execute("COMMIT")
            print("committed")
        else:
            cursor.execute("ROLLBACK")
            print("rolled back (dry run)")
        return 0
    except Exception:
        cursor.execute("ROLLBACK")
        raise
    finally:
        connection.close()


if __name__ == "__main__":
    raise SystemExit(main())
