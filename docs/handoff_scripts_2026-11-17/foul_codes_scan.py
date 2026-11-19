"""Across cached games: for players with a technical/unsportsmanlike-type foul,
does the box score count it?  For each such player, compares
  pbp ordinary fouls (CM, OF, CMD with a player), pbp special codes, box FoulsCommited.

usage: foul_codes_scan.py <raw dir>
"""
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

from euroleague_possessions.pbp_collector import read_cached_pbp

root = Path(sys.argv[1])
ORDINARY = {"CM", "OF", "CMD"}
summary = defaultdict(Counter)  # (comp, season, code) -> {counted, not_counted, unclear}
examples = defaultdict(list)

for pbp_path in sorted((root / "pbp").glob("*.pbp.json")):
    stem = pbp_path.name.replace(".pbp.json", "")
    comp, rest = stem[0], stem[1:]
    season, code = (int(x) for x in rest.split("_"))
    box_path = root / "boxscores" / f"{stem}.json"
    if not box_path.exists():
        continue
    try:
        pbp = read_cached_pbp(pbp_path)
        box = json.loads(box_path.read_text(encoding="utf-8"))
    except Exception:
        continue
    pbp["team"] = pbp["CODETEAM"].fillna("").str.strip()
    official = {}
    for side in box.get("Stats", []):
        for p in side.get("PlayersStats", []):
            official[(str(p["Team"]).strip(), str(p["Player"]).strip())] = p.get("FoulsCommited")
    fouls = pbp.loc[pbp["PLAYTYPE"].astype(str).str.startswith(("CM", "OF")) & pbp["PLAYER"].notna()]
    for (team, player), g in fouls.groupby(["team", "PLAYER"]):
        codes = Counter(g["PLAYTYPE"])
        special = {c: n for c, n in codes.items() if c not in ORDINARY}
        if not special:
            continue
        ordinary = sum(n for c, n in codes.items() if c in ORDINARY)
        box_n = official.get((team, str(player).strip()))
        if box_n is None:
            continue
        if len(special) == 1:
            (c, n), = special.items()
            if box_n == ordinary + n:
                verdict = "counted"
            elif box_n == ordinary:
                verdict = "not_counted"
            else:
                verdict = "unclear"
            summary[(comp, season, c)][verdict] += 1
            if verdict != "counted" and len(examples[(comp, season, c)]) < 6:
                examples[(comp, season, c)].append(f"{code}:{team}:{player} ord={ordinary} {c}={n} box={box_n}")
        else:
            summary[(comp, season, "+".join(sorted(special)))]["mixed"] += 1

for key in sorted(summary):
    print(*key, dict(summary[key]))
    for e in examples[key]:
        print("      ", e)
