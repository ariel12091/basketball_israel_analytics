<#
.SYNOPSIS
  Reclaim ~427 MB of EuroLeague storage: drop two redundant indexes (056),
  remove the foreign keys into actions_raw (055), then back actions_raw up to
  Parquet and TRUNCATE it.

.DESCRIPTION
  Dry run unless -Apply is given. Steps run in this order, and the script stops
  at the first failure:

    1. 056 - DROP INDEX CONCURRENTLY on euroleague_player_stats_actions_team_idx
             and _filter_idx (never blocks app reads). -104 MB.
    2. 055 - drop actions -> actions_raw (it was ON DELETE CASCADE) and re-point
             qa_incidents at actions. MUST precede step 3.
    3. Archive - refuses unless no foreign key references actions_raw AND
             canonical actions reproduce every raw event exactly; writes
             data\exports\actions_raw_<date>.parquet, reads it back, compares
             row count and key set, then TRUNCATEs actions_raw. -323 MB.

  Run from anywhere:
    .\euroleague\scripts\reclaim_storage.ps1           # dry run
    .\euroleague\scripts\reclaim_storage.ps1 -Apply    # do it
#>
[CmdletBinding()]
param([switch] $Apply)

$ErrorActionPreference = 'Stop'
$Root   = Split-Path -Parent $PSScriptRoot
$Python = Join-Path $Root '.venv\Scripts\python.exe'
if (-not (Test-Path $Python)) { throw "python not found at $Python" }
Set-Location $Root

& $Python -c "import pyarrow" 2>$null
if ($LASTEXITCODE -ne 0) { & $Python -m pip install -q "pyarrow>=15"; if ($LASTEXITCODE -ne 0) { throw 'pyarrow install failed' } }

function Invoke-Step([string] $Title, [string[]] $PyArgs) {
  Write-Host ''
  Write-Host ('=' * 72) -ForegroundColor DarkGray
  Write-Host $Title -ForegroundColor Cyan
  Write-Host ('=' * 72) -ForegroundColor DarkGray
  & $Python @PyArgs
  if ($LASTEXITCODE -ne 0) { throw "$Title failed (exit $LASTEXITCODE); later steps not run" }
}

$applyArg = @(); if ($Apply) { $applyArg = @('--apply') }

Invoke-Step '1/3  Migration 056: drop redundant indexes' (@('scripts\apply_056_drop_redundant_action_fact_indexes.py') + $applyArg)
Invoke-Step '2/3  Migration 055: nothing references actions_raw' (@('scripts\apply_055_actions_raw_not_referenced.py') + $applyArg)

# Step 3 inline so it lives next to the guards that make it safe.
$archive = @'
import sys
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, "src")
from euroleague_possessions.postgres_backend import connect_from_env_file

apply = "--apply" in sys.argv
conn = connect_from_env_file(Path("../etl/.Renviron"))
cur = conn.cursor()

def one(sql):
    cur.execute(sql)
    return cur.fetchone()[0]

rows = one("SELECT count(*) FROM euroleague.actions_raw")
size = one("SELECT pg_total_relation_size('euroleague.actions_raw')") // 1048576
refs = one("SELECT count(*) FROM pg_constraint WHERE contype = 'f' "
           "AND confrelid = 'euroleague.actions_raw'::regclass")
print(f"actions_raw: {rows} rows, {size} MB; foreign keys into it: {refs}")
if refs and apply:
    sys.exit("REFUSED: foreign keys still reference actions_raw (apply 055 first; "
             "actions -> actions_raw was ON DELETE CASCADE)")
if refs:
    print("(dry run: 055 is not applied yet; with -Apply it runs first)")

unmatched = one("""SELECT count(*) FROM euroleague.actions_raw ar
  FULL JOIN euroleague.actions a
    ON a.game_id = ar.game_id AND a.source_event_order = ar.source_event_order
  WHERE ar.game_id IS NULL OR a.game_id IS NULL""")
unreproduced = one("""SELECT count(*) FROM euroleague.actions a
  JOIN euroleague.actions_raw ar
    ON ar.game_id = a.game_id AND ar.source_event_order = a.source_event_order
  WHERE jsonb_build_object(
    'Season',a.season, 'Gamecode',a.gamecode,
    'TYPE',a.provider_event_type, 'NUMBEROFPLAY',a.provider_play_number,
    'CODETEAM',a.provider_team_code, 'PLAYER_ID',a.provider_player_id,
    'PLAYTYPE',a.play_type, 'PLAYER',a.player_name, 'TEAM',a.team_name,
    'DORSAL',a.jersey_number, 'MINUTE',a.minute,
    'MARKERTIME',a.marker_time, 'POINTS_A',a.points_a,
    'POINTS_B',a.points_b, 'COMMENT',a.comment, 'PLAYINFO',a.play_info,
    'PERIOD',a.period, 'TRUE_NUMBEROFPLAY',a.source_event_order,
    'Lineup_A',a.lineup_a, 'Lineup_B',a.lineup_b,
    'IsHomeTeam',a.is_home_team,
    'validate_on_court_player',a.validate_on_court_player
  ) IS DISTINCT FROM ar.raw_event""")
print(f"unmatched events: {unmatched}; events actions does not reproduce: {unreproduced}")
if unmatched or unreproduced:
    sys.exit("REFUSED: canonical actions do not reproduce actions_raw exactly")
if not apply:
    print("DRY RUN: would write a Parquet backup and TRUNCATE actions_raw")
    sys.exit(0)

cur.execute("SELECT * FROM euroleague.actions_raw ORDER BY game_id, source_event_order")
cols = [d.name for d in cur.description]
data = cur.fetchall()
json_col = cols.index("raw_event")
import json
columns = {c: [r[i] for r in data] for i, c in enumerate(cols) if i != json_col}
columns["raw_event"] = [None if r[json_col] is None else json.dumps(r[json_col], ensure_ascii=False)
                        for r in data]
table = pa.table(columns)
out_dir = Path("data/exports"); out_dir.mkdir(parents=True, exist_ok=True)
out = out_dir / f"actions_raw_{date.today().isoformat()}.parquet"
pq.write_table(table, out, compression="zstd")

back = pq.read_table(out, columns=["game_id", "source_event_order"])
keys = set(zip(back.column("game_id").to_pylist(), back.column("source_event_order").to_pylist()))
expected = {(r[cols.index("game_id")], r[cols.index("source_event_order")]) for r in data}
print(f"wrote {out} ({out.stat().st_size // 1048576} MB, {back.num_rows} rows)")
if back.num_rows != rows or keys != expected:
    sys.exit("REFUSED: backup does not match the table; nothing truncated")

cur.execute("SET lock_timeout = '10s'")
cur.execute("TRUNCATE euroleague.actions_raw")
after = one("SELECT pg_total_relation_size('euroleague.actions_raw')") // 1048576
print(f"TRUNCATED actions_raw: {size} MB -> {after} MB; "
      f"actions still {one('SELECT count(*) FROM euroleague.actions')} rows")
'@
$tmp = Join-Path $env:TEMP 'euroleague_archive_actions_raw.py'
Set-Content -Path $tmp -Value $archive -Encoding utf8
try {
  Invoke-Step '3/3  Back up actions_raw to Parquet, then TRUNCATE' (@($tmp) + $applyArg)
} finally {
  Remove-Item $tmp -ErrorAction SilentlyContinue
}

Write-Host ''
if ($Apply) {
  Write-Host 'Done. Re-run the size check: .venv\Scripts\python.exe scripts\load_games.py --games 403-406 --season 2025 --competition E --verify-only' -ForegroundColor Green
} else {
  Write-Host 'Dry run complete. Re-run with -Apply to make the changes.' -ForegroundColor Green
}
