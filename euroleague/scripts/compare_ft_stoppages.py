"""Checkpointed offline comparison of experimental FT stoppages to the parser.

No provider, database or publication I/O. Resume invalidates checkpoints when
the input, parser, or experimental implementation changes.
"""
from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "euroleague/src"))
from euroleague_possessions.ft_stoppages import infer_ft_stoppages
from euroleague_possessions.parser import FT_TYPES, group_events
from euroleague_possessions.pbp_collector import read_cached_pbp


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def compare(path, baseline_rows=None):
    raw = read_cached_pbp(path)
    if baseline_rows is None:
        result = infer_ft_stoppages(group_events(raw, ft_model=False))
    else:
        # Canonical fields may be reused only with identical input and all
        # baseline implementation hashes. Shadow inference still reads every
        # raw event, including live boundaries and substitutions.
        canonical = ["synthetic_parent_order", "synthetic_ft_trip_id", "grouping_status",
                     "final_end_poss", "end_reason", "decision_trace"]
        prior = pd.DataFrame(baseline_rows)[["period", "source_event_order"] + canonical]
        result = infer_ft_stoppages(raw).merge(prior, on=["period", "source_event_order"],
                                             how="left", validate="one_to_one", sort=False)
    ft = result[result.play_type.isin(FT_TYPES)].copy()
    signatures = {}
    for _, trip in ft.groupby(["period", "synthetic_ft_trip_id"], dropna=False):
        orders = sorted(int(v) for v in trip.source_event_order)
        for i in trip.index:
            signatures[i] = orders
    shadow_signatures = {}
    for _, trip in ft[ft.shadow_status.eq("unique")].groupby("shadow_ft_trip_id"):
        orders = sorted(int(v) for v in trip.source_event_order)
        for i in trip.index:
            shadow_signatures[i] = orders
    records = []
    for i, row in ft.iterrows():
        record = {key: row[key] for key in ["period", "clock", "source_event_order", "play_type",
                  "team_code", "player_id", "player_name", "synthetic_parent_order", "synthetic_ft_trip_id",
                  "grouping_status", "final_end_poss", "end_reason", "decision_trace"]
                  + [c for c in ft.columns if c.startswith("shadow_")]}
        record["baseline_partition"] = signatures[i]
        record["shadow_partition"] = shadow_signatures.get(i)
        known = row.shadow_status == "unique"
        record["partition_changed"] = known and signatures[i] != shadow_signatures[i]
        record["parent_changed"] = known and int(row.synthetic_parent_order) != int(row.shadow_parent_order)
        record["ft_endpoint_changed"] = bool(row.final_end_poss != row.shadow_ft_end_poss) if pd.notna(row.shadow_ft_end_poss) else None
        record["game"] = path.name.removesuffix(".pbp.json")
        records.append(record)
    # JSON cleanly preserves nulls and nested trip-membership lists.
    return json.loads(pd.DataFrame(records).to_json(orient="records"))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-dir", type=Path, default=ROOT / "euroleague/data/raw/pbp")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "euroleague/data/diagnostics/ft_stoppage_shadow")
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()
    checkpoint = args.output_dir / "games"
    checkpoint.mkdir(parents=True, exist_ok=True)
    code_hashes = {name: digest(ROOT / f"euroleague/src/euroleague_possessions/{name}.py")
                   for name in ["parser", "models", "ft_stoppages", "pbp_collector"]}
    code_hashes["comparison"] = digest(Path(__file__))
    paths = sorted(args.raw_dir.glob("*.pbp.json"))
    if args.limit:
        paths = paths[:args.limit]
    records, errors, manifest = [], [], []
    for n, path in enumerate(paths, 1):
        game = path.name.removesuffix(".pbp.json")
        identity = dict(input_sha256=digest(path), code_hashes=code_hashes)
        saved = checkpoint / f"{game}.json"
        try:
            cached = json.loads(saved.read_text(encoding="utf-8")) if saved.exists() else None
            if cached and cached.get("identity") == identity:
                rows = cached["rows"]
            else:
                baseline_rows = None
                if cached and cached["identity"]["input_sha256"] == identity["input_sha256"]:
                    baseline_keys = ["parser", "models", "pbp_collector"]
                    if all(cached["identity"]["code_hashes"].get(k) == code_hashes[k] for k in baseline_keys):
                        baseline_rows = cached["rows"]
                rows = compare(path, baseline_rows)
                temporary = saved.with_suffix(".tmp")
                temporary.write_text(json.dumps(dict(identity=identity, rows=rows)), encoding="utf-8")
                temporary.replace(saved)
            records.extend(rows)
            manifest.append(dict(game=game, **identity))
        except Exception as exc:
            errors.append(dict(game=game, error=f"{type(exc).__name__}: {exc}"))
        if n % 50 == 0 or n == len(paths):
            print(f"{n}/{len(paths)} games; errors={len(errors)}", flush=True)
    result = pd.DataFrame(records)
    result.to_csv(args.output_dir / "ft_rows.csv", index=False)
    changed = result[result.partition_changed | result.parent_changed | result.ft_endpoint_changed.eq(True)]
    changed.to_csv(args.output_dir / "differences.csv", index=False)
    summary = dict(games=len(manifest), ft_rows=len(result), errors=errors,
                   code_hashes=code_hashes, statuses=result.shadow_status.value_counts().to_dict(),
                   by_competition={}, baseline_status_by_shadow=result.groupby(["grouping_status", "shadow_status"]).size().to_dict())
    summary["baseline_status_by_shadow"] = {"/".join(k): int(v) for k, v in summary["baseline_status_by_shadow"].items()}
    for competition, subset in result.groupby(result.game.str[0]):
        summary["by_competition"][competition] = dict(games=subset.game.nunique(), ft_rows=len(subset),
            statuses=subset.shadow_status.value_counts().to_dict(),
            partition_changed_rows=int(subset.partition_changed.sum()),
            partition_changed_games=int(subset.loc[subset.partition_changed, "game"].nunique()),
            parent_changed_rows=int(subset.parent_changed.sum()),
            endpoint_compared_rows=int(subset.shadow_ft_end_poss.notna().sum()),
            endpoint_changed_rows=int(subset.ft_endpoint_changed.eq(True).sum()))
    (args.output_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    (args.output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    if errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
