"""Each player of one team: every feed row naming him, in order (subs in caps brackets).

usage: player_timeline.py <raw dir> <gamecode> <team code>
"""
import sys
from pathlib import Path

from euroleague_possessions.pbp_collector import read_cached_pbp

root, code, team = Path(sys.argv[1]), int(sys.argv[2]), sys.argv[3]
pbp = read_cached_pbp(root / "pbp" / f"U2026_{code}.pbp.json")
rows = pbp.loc[pbp["CODETEAM"].fillna("").str.strip().eq(team) & pbp["PLAYER"].notna()]
for player, g in rows.groupby("PLAYER", sort=True):
    items = []
    for _, r in g.iterrows():
        t = f"Q{r['PERIOD']} {str(r['MARKERTIME']).strip()}"
        if r["PLAYTYPE"] in ("IN", "OUT"):
            items.append(f"[{r['PLAYTYPE']} {t} #{r['TRUE_NUMBEROFPLAY']}]")
        else:
            items.append(f"{t} {r['PLAYTYPE']}")
    print(f"{player}:\n   " + ", ".join(items))
