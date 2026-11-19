"""One team's rows in a range: every feed row of that team, with the lineup we build for it.

usage: game_rows.py <raw dir> <gamecode> <team code> <first row> <last row>
"""
import logging
import sys
from pathlib import Path

from euroleague_possessions.package_lineups import apply_package_lineups, cached_boxscore_frame
from euroleague_possessions.pbp_collector import read_cached_pbp

logging.basicConfig(level=logging.WARNING)
root, code, team, first, last = Path(sys.argv[1]), int(sys.argv[2]), sys.argv[3], int(sys.argv[4]), int(sys.argv[5])
pbp = read_cached_pbp(root / "pbp" / f"U2026_{code}.pbp.json")
box = cached_boxscore_frame(root / "boxscores" / f"U2026_{code}.json", 2026, code, "U")
ev = apply_package_lineups(pbp, box, competition="U")
home = box.loc[box["Home"] == 1, "Team"].iloc[0]
side = "Lineup_A" if team == home else "Lineup_B"
print("teams:", sorted(set(box["Team"])), "home:", home)
starters = box.loc[(box["Team"] == team) & (box["IsStarter"] == 1), "Player"].tolist()
print("starters", team, starters)


def short(players):
    return "/".join(sorted(p.split(",")[0][:7] for p in players))


prev = None
for i, r in ev.iloc[first:last + 1].iterrows():
    mine = str(r["CODETEAM"]).strip() == team
    lineup = short(r[side])
    changed = lineup != prev
    prev = lineup
    if not mine and not changed and r["PLAYTYPE"] not in ("TOUT", "TOUT_TV", "BP", "EP", "EG"):
        continue
    flag = "" if r["validate_on_court_player"] else "  <-- off floor"
    dup = "  <-- DUP" if len(set(r[side])) != 5 else ""
    who = str(r["PLAYER"])[:20] if mine else f"({str(r['CODETEAM']).strip()} {r['PLAYTYPE']})"
    print(f"{i:4d} #{r['TRUE_NUMBEROFPLAY']:<4} Q{r['PERIOD']} {str(r['MARKERTIME']):5} {r['PLAYTYPE'] if mine else '':6} {who:22} [{lineup}]{flag}{dup}")
