"""Print the reconciliation metrics that differ in staged checkpoints."""
import json
import sys
from pathlib import Path

for path in sorted(Path(sys.argv[1]).glob("*.stage.json")):
    rows = json.loads(path.read_text(encoding="utf-8"))["snapshot"]["rows"]
    for table, items in rows.items():
        if not isinstance(items, list):
            continue
        for r in items:
            if isinstance(r, dict) and "official_value" in r and r.get("pbp_value") != r.get("official_value"):
                print(path.stem, table, r.get("_team_code"), r.get("metric"), r.get("pbp_value"), r.get("official_value"))
