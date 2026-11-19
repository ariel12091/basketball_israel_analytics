"""Replacement-shooter incidents: does the feed itself show why someone other
than the fouled player shot?  FIBA: when the fouled player is injured or
disqualified, his substitute shoots. Evidence looked for, in the incident's
saved source window:
  left    the fouled player goes OUT and the shooter comes IN (same team)
  dq      the fouled player commits his 5th foul ("Foul (5)") or a CMD
  none    neither

usage: replacement_evidence.py <contexts.json>
"""
import json
import re
import sys
from collections import Counter

FT = {"FTM", "FTA"}
contexts = json.load(open(sys.argv[1], encoding="utf-8"))
counts = Counter()
for inc in contexts:
    if inc["category"] != "replacement_condition_unverified":
        continue
    events = inc["events"]
    by_order = {e["source_event_order"]: e for e in events}
    fts = [by_order[o] for o in inc["ft_orders"] if o in by_order]
    if not fts:
        counts["no FT rows in window"] += len(inc["ft_orders"])
        continue
    shooter = fts[0]["player_name"]
    team = fts[0]["team_code"]
    first_ft = min(inc["ft_orders"])
    window = [e for e in events if e["source_event_order"] <= max(inc["ft_orders"])]
    # Fouled players of this team in the window, nearest first.
    drawn = [e for e in reversed(window) if e["play_type"] == "RV" and e["team_code"] == team
             and e["source_event_order"] < first_ft]
    fouled = drawn[0]["player_name"] if drawn else None
    left = fouled and any(e["play_type"] == "OUT" and e["player_name"] == fouled for e in window) \
        and any(e["play_type"] == "IN" and e["player_name"] == shooter for e in window)
    dq = fouled and any(
        e["player_name"] == fouled and (e["play_type"] == "CMD" or re.search(r"Foul \(5\)", e.get("play_info") or ""))
        for e in window)
    shooters = {by_order[o]["player_name"] for o in inc["ft_orders"] if o in by_order}
    if fouled is None:
        verdict = "no foul-drawn row for the shooting team"
    elif fouled in shooters and len(shooters) > 1:
        verdict = "fouled player shot some, another player the rest"
    elif left:
        verdict = "left: fouled player OUT, shooter IN"
    elif dq:
        verdict = "dq: fouled player disqualified"
    else:
        verdict = "none"
    counts[verdict] += inc["target_rows"]
    print(f"{inc['game']:10} Q{inc['period']} rows {inc['ft_orders']}  fouled={fouled}  shooter(s)={sorted(shooters)}  -> {verdict}")
print("\nFT rows by verdict:", dict(counts))
