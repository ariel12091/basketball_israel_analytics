"""For staged games: each unresolved free throw with the feed rows around it.

usage: unresolved_ft.py <staging batch dir> <raw dir>
"""
import json
import sys
from pathlib import Path

from euroleague_possessions.pbp_collector import read_cached_pbp

batch, raw = Path(sys.argv[1]), Path(sys.argv[2])
for path in sorted(batch.glob("*.stage.json")):
    rows = json.loads(path.read_text(encoding="utf-8"))["snapshot"]["rows"]
    actions = next((v for k, v in rows.items() if k in ("actions", "actions_clean")), [])
    bad = [a for a in actions if a.get("grouping_status") == "unresolved"]
    if not bad:
        continue
    stem = path.name.replace(".stage.json", "")
    pbp = read_cached_pbp(raw / "pbp" / f"{stem}.pbp.json")
    print(f"=== {stem}: {len(bad)} unresolved")
    for a in bad:
        order = a.get("source_event_order")
        trace = a.get("decision_trace") or a.get("trace")
        print(f"  row {order} {a.get('play_type')} {a.get('team_code')} {a.get('player_name')}  trace={json.dumps(trace)[:300]}")
        lo, hi = max(order - 10, 0), order + 6
        for i, r in pbp.iloc[lo:hi].iterrows():
            mark = " <==" if i == order else ""
            print(f"     {i:4d} Q{r['PERIOD']} {str(r['MARKERTIME']).strip():5} {str(r['CODETEAM']).strip():4} {r['PLAYTYPE']:7} {str(r['PLAYER'])[:22]:22} {r['POINTS_A']} {r['POINTS_B']}{mark}")
