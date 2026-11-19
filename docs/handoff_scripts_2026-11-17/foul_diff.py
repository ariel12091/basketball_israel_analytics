"""Per player: committed fouls in the play-by-play vs the box score's FoulsCommited.

usage: foul_diff.py <raw dir> <competition> <season> <gamecode> [team]
"""
import json
import sys
from collections import Counter
from pathlib import Path

from euroleague_possessions.parser import is_committed_foul
from euroleague_possessions.pbp_collector import read_cached_pbp

root, comp, season, code = Path(sys.argv[1]), sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
team_filter = sys.argv[5] if len(sys.argv) > 5 else None
pbp = read_cached_pbp(root / "pbp" / f"{comp}{season}_{code}.pbp.json")
box = json.loads((root / "boxscores" / f"{comp}{season}_{code}.json").read_text(encoding="utf-8"))

pbp["team"] = pbp["CODETEAM"].fillna("").str.strip()
fouls = pbp.loc[pbp["PLAYTYPE"].map(lambda c: is_committed_foul(c) or c in ("B", "C", "CMD"))]
ours = Counter()
rows = {}
for _, r in fouls.iterrows():
    key = (r["team"], str(r["PLAYER"]).strip())
    ours[key] += 1
    rows.setdefault(key, []).append(f"Q{r['PERIOD']} {str(r['MARKERTIME']).strip()} {r['PLAYTYPE']}")

official = {}
for side in box["Stats"]:
    for p in side["PlayersStats"]:
        official[(str(p["Team"]).strip(), str(p["Player"]).strip())] = p.get("FoulsCommited")
    tm = side.get("tmr") or {}
    official[(str(tm.get("Team", "")).strip(), "(team/bench row)")] = tm.get("FoulsCommited")
    totr = side.get("totr") or {}
    official[(str(side["PlayersStats"][0]["Team"]).strip(), "(TOTAL)")] = totr.get("FoulsCommited")

for key in sorted(set(ours) | set(official)):
    team, player = key
    if team_filter and team != team_filter:
        continue
    o, b = ours.get(key, 0), official.get(key)
    mark = "  <<<" if b is not None and o != b else ""
    print(f"{team:4} {player:28} pbp {o:2} box {b}{mark}   {', '.join(rows.get(key, [])) if mark else ''}")
