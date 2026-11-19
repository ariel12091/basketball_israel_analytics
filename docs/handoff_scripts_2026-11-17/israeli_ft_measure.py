"""Free-throw trips in the Israeli archive, where the provider links each FT to
its parent and numbers it "n of m". Measures what a stoppage model for
EuroLeague would need: trip sizes per foul kind, logging order, interleaving,
shooter identity, completeness.

usage: israeli_ft_measure.py <actions_clean.parquet>
"""
import sys
from collections import Counter

import pandas as pd
import pyarrow.parquet as pq

df = pq.read_table(sys.argv[1]).to_pandas()
df = df.sort_values(["game_id", "id"]).reset_index(drop=True)
by_id = df.set_index(["game_id", "id"])
ft = df[df["type"] == "freeThrow"].copy()

SPECIAL = {"technical": "technical", "technical-behavioral": "technical",
           "technical-administrative": "technical", "unsportsmanlike": "unsportsmanlike",
           "unsportsmanlike-flagrant": "unsportsmanlike", "unsportsmanlike-disruptive": "unsportsmanlike",
           "disqualifying": "disqualifying", "OffensiveFoulUnsportsmanlike": "unsportsmanlike (offensive)"}


def parent_kind(row):
    key = (row.game_id, row.parent_action_id)
    if key not in by_id.index:
        return "parent missing"
    p = by_id.loc[key]
    if isinstance(p, pd.DataFrame):
        p = p.iloc[0]
    if p["type"] == "shot":
        return "and-one (parent = made shot)" if p["parameters_made"] == "made" else f"parent = {p['parameters_made']} shot"
    if p["type"] == "foul":
        if p["parameters_type"] == "personal":
            if p.get("parameters_is_coach_foul") or p.get("parameters_is_bench_foul"):
                return "personal (coach/bench)"
            return "personal"
        return SPECIAL.get(p["parameters_type"], f"foul:{p['parameters_type']}")
    return f"parent = {p['type']}"


ft["parent_kind"] = [parent_kind(r) for r in ft.itertuples(index=False)]
trips = ft.groupby(["game_id", "parent_action_id"])
summary = trips.agg(
    kind=("parent_kind", "first"),
    awarded=("parameters_free_throws_awarded", "max"),
    rows=("id", "size"),
    first_ft=("id", "min"),
    last_ft=("id", "max"),
    shooters=("player_id", "nunique"),
    secs=("end_game_seconds_remaining", "nunique"),
).reset_index()

print(f"games {df.game_id.nunique()}, free throws {len(ft)}, trips {len(summary)}\n")
print("1. TRIP SIZE (awarded) BY PARENT KIND  [rules of the game: should transfer]")
tab = summary.groupby(["kind", "awarded"]).size().unstack(fill_value=0)
print(tab.to_string(), "\n")

print("2. COMPLETENESS: FT rows in the trip vs awarded")
summary["complete"] = (summary["rows"] == summary["awarded"]).map({True: "rows == awarded", False: "rows != awarded"})
print(summary.groupby(["kind", "complete"]).size().unstack(fill_value=0).to_string(), "\n")

# Parent position relative to its FTs, in provider id order.
def parent_pos(r):
    key = (r.game_id, r.parent_action_id)
    if r.parent_action_id < r.first_ft:
        return "parent before FTs"
    if r.parent_action_id > r.last_ft:
        return "parent AFTER FTs"
    return "parent between FTs"

summary["parent_pos"] = [parent_pos(r) for r in summary.itertuples(index=False)]
print("3. LOGGING ORDER: parent row vs its free throws (provider id order)  [provider habit]")
print(summary.groupby(["kind", "parent_pos"]).size().unstack(fill_value=0).to_string(), "\n")

# And-one: where is the foul of an and-one logged relative to the made shot?
andone = summary[summary["kind"].str.startswith("and-one")]
fouls = df[df["type"] == "foul"]
pos = Counter()
for r in andone.itertuples(index=False):
    g = fouls[(fouls.game_id == r.game_id) & (fouls.parent_action_id == r.parent_action_id)]
    if g.empty:
        pos["no foul row whose parent is the shot"] += 1
    else:
        pos["foul logged before the shot" if g["id"].min() < r.parent_action_id else "foul logged after the shot"] += 1
print("4. AND-ONE: the foul row relative to the made shot  [provider habit]")
print(dict(pos), "\n")

print("5. SHOOTER: one player shoots the whole trip?")
print(summary.groupby(["kind", summary["shooters"] == 1]).size().unstack(fill_value=0).rename(columns={True: "one shooter", False: "several"}).to_string(), "\n")

# Shooter = the fouled player? foul-drawn rows hang off the foul.
drawn = df[df["type"] == "foul-drawn"][["game_id", "parent_action_id", "player_id"]].rename(columns={"player_id": "fouled"})
s1 = summary.merge(ft.groupby(["game_id", "parent_action_id"]).player_id.first().rename("shooter").reset_index(),
                   on=["game_id", "parent_action_id"]).merge(drawn, on=["game_id", "parent_action_id"], how="left")
s1["match"] = s1.apply(lambda r: "no foul-drawn row" if pd.isna(r.fouled) else ("shooter = fouled" if r.shooter == r.fouled else "shooter != fouled"), axis=1)
print("6. SHOOTER vs FOULED PLAYER (foul-drawn row of the same foul)")
print(s1.groupby(["kind", "match"]).size().unstack(fill_value=0).to_string(), "\n")

# Stoppages: FTs grouped by game and game clock. Several trips at one clock,
# and whether their FT rows interleave in id order.
ft["clock"] = ft["end_game_seconds_remaining"]
multi = Counter()
interleave = Counter()
combos = Counter()
for (g, clock), grp in ft.groupby(["game_id", "clock"]):
    n_trips = grp["parent_action_id"].nunique()
    if n_trips < 2:
        multi["1 trip"] += 1
        continue
    multi[f"{min(n_trips, 4)}+ trips" if n_trips >= 4 else f"{n_trips} trips"] += 1
    combos[" + ".join(sorted(grp.groupby("parent_action_id")["parent_kind"].first()))] += 1
    seq = grp.sort_values("id")["parent_action_id"].tolist()
    runs = sum(1 for a, b in zip(seq, seq[1:]) if a != b) + 1
    interleave["contiguous" if runs == n_trips else "INTERLEAVED"] += 1
print("7. STOPPAGES (same game clock): how many trips")
print(dict(multi))
print("   multi-trip stoppages, FT rows of different trips:", dict(interleave))
print("   most common multi-trip combinations:")
for c, n in combos.most_common(12):
    print(f"     {n:4d}  {c}")
