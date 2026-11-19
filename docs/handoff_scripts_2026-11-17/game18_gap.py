"""EuroCup 2026-27 game 18: lineup minutes (current parser) against the box score."""
import logging
import sys
from pathlib import Path

import pandas as pd

from euroleague_possessions.package_lineups import (
    _game_audit, apply_package_lineups, cached_boxscore_frame,
)
from euroleague_possessions.pbp_collector import read_cached_pbp

logging.basicConfig(level=logging.INFO, format="%(message)s")
root = Path(sys.argv[1])
comp, season, code = "U", 2026, int(sys.argv[2]) if len(sys.argv) > 2 else 18
pbp = read_cached_pbp(root / "pbp" / f"{comp}{season}_{code}.pbp.json")
box = cached_boxscore_frame(root / "boxscores" / f"{comp}{season}_{code}.json", season, code, comp)
import os
from euroleague_possessions import package_lineups as pl
fix = os.environ.get("FIX", "")
if "trt" in fix:
    pl.KNOWN_SUBSTITUTION_CORRECTIONS[("U", 2026, 18, 68)] = ("IN", "PECCHIA, ANDREA", None)
    pl.KNOWN_SUBSTITUTION_CORRECTIONS[("U", 2026, 18, 70)] = ("OUT", "HUBB, PRENTISS", None)
if "lem" in fix:
    pl.KNOWN_SUBSTITUTION_CORRECTIONS[("U", 2026, 18, 56)] = ("IN", "PENDA, SWANN", None)
    pl.KNOWN_SUBSTITUTION_CORRECTIONS[("U", 2026, 18, 61)] = ("OUT", "AJINCA, MELVIN", None)
if "wood" in fix:  # simulate Dufeal-for-Wood at the Q1 1:25 stoppage (script only)
    pbp["TRUE_NUMBEROFPLAY"] = pbp["TRUE_NUMBEROFPLAY"].astype(float)
    for number, new in ((58, 140.5), (59, 140.6)):
        mask = pbp["TRUE_NUMBEROFPLAY"].eq(number)
        pbp.loc[mask, "MARKERTIME"] = "01:25"
        pbp.loc[mask, "TRUE_NUMBEROFPLAY"] = new
    pbp = pbp.sort_values("TRUE_NUMBEROFPLAY", kind="stable").reset_index(drop=True)
ev = apply_package_lineups(pbp, box, competition=comp)
print("AUDIT", _game_audit(ev, box))
print("COLUMNS", list(pbp.columns))


def remaining(text):
    if not isinstance(text, str) or ":" not in text:
        return None
    m, s = text.strip().split(":")
    return int(m) * 60 + int(s)


period_col = "PERIOD" if "PERIOD" in ev.columns else "Period"
elapsed, last = [], 0
for period, marker in zip(ev[period_col], ev["MARKERTIME"]):
    period = int(period)
    start = (period - 1) * 600 if period <= 4 else 2400 + (period - 5) * 300
    length = 600 if period <= 4 else 300
    rem = remaining(marker)
    value = last if rem is None else start + length - rem
    value = max(value, last)  # running maximum
    elapsed.append(value)
    last = value
ev["elapsed"] = elapsed
game_end = max(elapsed[-1], 2400)

home = box.loc[box["Home"] == 1, "Team"].iloc[0]
secs = {}
stints = {}
rows = ev.to_dict("records")
for i, row in enumerate(rows):
    nxt = rows[i + 1]["elapsed"] if i + 1 < len(rows) else game_end
    delta = nxt - row["elapsed"]
    for side in ("Lineup_A", "Lineup_B"):
        for player in row[side]:
            secs[player] = secs.get(player, 0) + delta
            spans = stints.setdefault(player, [])
            if spans and spans[-1][1] == row["elapsed"]:
                spans[-1][1] = nxt
            elif delta:
                spans.append([row["elapsed"], nxt])


def clean(name):
    return name.replace("  ", " ").replace(" , ", ", ").strip()


def clock(t):
    q = min(t // 600, 3) if t < 2400 else 4 + (t - 2400) // 300
    start = q * 600 if q < 4 else 2400 + (q - 4) * 300
    length = 600 if q < 4 else 300
    rem = length - (t - start)
    return f"Q{q + 1} {rem // 60}:{rem % 60:02d}"


print("\nPLAYER MINUTES: ours vs box score (seconds)")
out = []
for r in box.to_dict("records"):
    name = clean(r["Player"])
    rem = remaining(str(r.get("Minutes")))
    if rem is None and name not in secs:
        continue
    ours = secs.get(name, 0)
    out.append((r["Team"], name, ours, rem or 0, ours - (rem or 0)))
for team, name, ours, official, diff in sorted(out, key=lambda x: (x[0], -abs(x[4]))):
    flag = "  <<<" if abs(diff) >= 30 else ""
    print(f"{team:4} {name:28} ours {ours // 60:2d}:{ours % 60:02d}  box {official // 60:2d}:{official % 60:02d}  diff {diff:+5d}s{flag}")
for team in sorted({o[0] for o in out}):
    print(team, "sum |diff| =", sum(abs(o[4]) for o in out if o[0] == team), "s")

print("\nSTINTS of players off by 30s or more")
for team, name, ours, official, diff in out:
    if abs(diff) >= 30:
        print(f"{team} {name} ({diff:+d}s):", ", ".join(f"{clock(a)}-{clock(b)}" for a, b in stints.get(name, [])))

print("\nSUBSTITUTION ROWS")
for i, row in enumerate(rows):
    if row["PLAYTYPE"] in ("IN", "OUT"):
        print(f"{i:4d} #{row['TRUE_NUMBEROFPLAY']:4d} {clock(row['elapsed']):9} {str(row['MARKERTIME']):6} {row['CODETEAM'].strip():4} {row['PLAYTYPE']:3} {row['PLAYER']}")

print("\nACTORS NOT ON THE FLOOR")
bad = ev.loc[~ev["validate_on_court_player"].astype(bool)]
for i, row in bad.iterrows():
    print(f"{i:4d} {clock(row['elapsed']):9} {row['CODETEAM'].strip():4} {row['PLAYTYPE']:6} {row['PLAYER']}")
