"""Keep a fixed review cohort and source-evidence register for FT coverage losses.

The seed contains review metadata, not downloaded game payloads. Context windows
and solver output stay in ignored diagnostics. Automatic categories are triage
labels, never provider-error verdicts or approval to publish.
"""
from __future__ import annotations

from collections import Counter
import hashlib
import json
from pathlib import Path
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "euroleague/src"))
from euroleague_possessions.ft_stoppages import infer_ft_stoppages, _incidents
from euroleague_possessions.parser import FT_TYPES, normalize_events, is_special_foul
from euroleague_possessions.pbp_collector import read_cached_pbp

DATA = ROOT / "euroleague/data/diagnostics/ft_stoppage_shadow"
SEED = ROOT / "docs/euroleague_ft_review_seed_2026-11-17.json"
REPORT = ROOT / "docs/euroleague_ft_review_register_2026-11-17.md"


def category(part, selected, initial_status):
    if initial_status == "replacement_requires_evidence":
        return "replacement_condition_unverified"
    fouls = part[part.play_type.eq("CM")]
    signature = ["team_code", "player_id", "play_info"]
    if len(fouls) > 1 and fouls.duplicated(signature, keep=False).any():
        return "repeated_foul_signature"
    if initial_status == "ambiguous":
        if len(fouls) > 1:
            return "competing_personal_fouls"
        return "multiple_penalties_attempt_membership"
    parents = {int(r["synthetic_parent_order"]) for r in selected}
    if not parents.issubset(set(part.source_event_order.astype(int))):
        return "parent_outside_candidate_window"
    ft = part[part.play_type.isin(FT_TYPES)]
    drawn = set(part.loc[part.play_type.eq("RV"), "player_id"].dropna())
    shooters = set(ft.player_id.dropna())
    if len(fouls) and drawn and shooters and not drawn.intersection(shooters) and not part.play_type.map(is_special_foul).any():
        return "fouled_shooter_mismatch"
    if len(ft) == 1 and len(fouls) and not part.play_type.isin({"2FGM", "3FGM"}).any():
        return "single_ft_personal_without_linked_basket"
    if part.play_type.isin({"2FGM", "3FGM"}).any():
        return "made_basket_award_or_boundary"
    return "shooter_award_or_scope"


def main():
    summary = json.loads((DATA / "summary.json").read_text())
    current_hash = hashlib.sha256((ROOT / "euroleague/src/euroleague_possessions/ft_stoppages.py").read_bytes()).hexdigest()
    if summary["code_hashes"]["ft_stoppages"] != current_hash:
        raise SystemExit("Run compare_ft_stoppages.py first: the comparison model hash is stale")
    if not SEED.exists():
        table = pd.read_csv(DATA / "ft_rows.csv")
        lost = table[table.grouping_status.eq("confirmed") & ~table.shadow_status.eq("unique")]
        if len(lost) != 163:
            raise SystemExit("Initial cohort must be frozen from the original 163-row comparison")
        columns = ["game", "period", "source_event_order", "synthetic_parent_order", "synthetic_ft_trip_id",
                   "shadow_incident_id", "shadow_status", "baseline_partition"]
        seed = dict(model_hash=current_hash, rows=json.loads(lost[columns].to_json(orient="records")))
        SEED.write_text(json.dumps(seed, indent=2), encoding="utf-8")
    seed = json.loads(SEED.read_text())
    records = pd.DataFrame(seed["rows"])
    register, contexts = [], []
    evidence_needed = {
        "replacement_condition_unverified": "Verify injury, foul-out or disqualification permitting the observed shooter substitution.",
        "competing_personal_fouls": "Review distinct foul counters and joint foul/RV assignments; retain competing FT allocations without additional evidence.",
        "multiple_penalties_attempt_membership": "Determine which individual FT attempts belong to each penalty; aggregate counts are insufficient.",
        "repeated_foul_signature": "Verify whether repeated actor/team/foul-counter rows are duplicates or separate infractions before suppressing anything.",
        "parent_outside_candidate_window": "Inspect the excluded baseline parent and intervening live events; establish the correct incident boundary.",
        "made_basket_award_or_boundary": "Verify the basket/foul relation and actual award before treating the FT as an and-one.",
        "shooter_award_or_scope": "Check shooter identity and completeness of the awarded attempts using PBP and available box-score evidence.",
        "fouled_shooter_mismatch": "The FT shooter differs from the observed foul-drawn player, with no verified replacement condition. Verify attribution externally.",
        "single_ft_personal_without_linked_basket": "A personal foul has one observed FT and no linked made basket. Establish whether the source omitted evidence, recorded a special award as CM, or an attempt is missing.",
        "foul_drawn_pairing_conflict": "Observed RV rows cannot provide distinct personal-foul pairings. Check missing or repeated foul/RV evidence before changing assignments.",
    }
    for game, group in records.groupby("game", sort=True):
        raw = normalize_events(read_cached_pbp(ROOT / f"euroleague/data/raw/pbp/{game}.pbp.json"))
        shadow = infer_ft_stoppages(raw)
        for (period, identity), targets in group.groupby(["period", "shadow_incident_id"], sort=True):
            orders = targets.source_event_order.astype(int).tolist()
            before = targets.iloc[0].shadow_status
            old_start = int(identity.split(":")[-1])
            part = raw[raw.period.eq(period)].sort_values("source_event_order").reset_index(drop=True)
            rows = list(part.itertuples(index=False))
            relevant = [inc for inc in _incidents(rows) if any(rows[i].source_event_order in orders for i in inc.indices)]
            indices = sorted({i for inc in relevant for i in inc.indices})
            window = part.iloc[indices]
            # Baseline parents and all baseline trip members are included in
            # the review context even if the new incident cuts them apart.
            memberships = [int(v) for r in targets.itertuples()
                           for v in json.loads(r.baseline_partition)]
            anchors = orders + memberships + targets.synthetic_parent_order.astype(int).tolist() + [old_start]
            positions = part.index[part.source_event_order.isin(anchors)]
            context = part.iloc[max(0, int(positions.min()) - 3):int(positions.max()) + 4]
            current = shadow[shadow.period.eq(period) & shadow.source_event_order.isin(orders)]
            if len(current) != len(orders):
                raise ValueError(f"Review source rows are missing for {game}: {orders}")
            counts = current.shadow_status.value_counts().to_dict()
            triage_status = "replacement_requires_evidence" if counts.get("replacement_requires_evidence") else before
            triage = category(window, targets.to_dict("records"), triage_status)
            if current.shadow_trace.str.contains("personal_foul_drawn_pairing_conflict", na=False).any():
                triage = "foul_drawn_pairing_conflict"
            row = dict(game=game, period=int(period), old_incident=identity, ft_orders=orders,
                original_status=before, category=triage, evidence_needed=evidence_needed[triage],
                target_rows=len(orders), unique_now=int(current.shadow_status.eq("unique").sum()),
                current_statuses=counts,
                disposition="candidate_recovered_requires_review" if current.shadow_status.eq("unique").all()
                    else "open_evidence_review")
            register.append(row)
            columns = ["source_event_order", "play_type", "team_code", "player_id", "player_name", "clock", "play_info"]
            contexts.append(dict(**row, current=json.loads(current[["source_event_order", "shadow_status", "shadow_trace",
                "shadow_parent_order", "shadow_ft_trip_id", "shadow_alternatives"]].to_json(orient="records")),
                candidate_window_orders=window.source_event_order.astype(int).tolist(),
                events=context[columns].to_dict("records")))
    out = DATA / "review"
    out.mkdir(exist_ok=True)
    (out / "register.json").write_text(json.dumps(register, indent=2), encoding="utf-8")
    (out / "contexts.json").write_text(json.dumps(contexts, indent=2, default=str), encoding="utf-8")
    by_category = Counter()
    recovered_by_category = Counter()
    for r in register:
        by_category[r["category"]] += r["target_rows"]
        recovered_by_category[r["category"]] += r["unique_now"]
    recovered = sum(r["unique_now"] for r in register)
    full = pd.read_csv(DATA / "ft_rows.csv")
    original_keys = set(zip(records.game, records.period, records.source_event_order))
    lost = full[full.grouping_status.eq("confirmed") & ~full.shadow_status.eq("unique")]
    loss_keys = set(zip(lost.game, lost.period, lost.source_event_order))
    new_losses = loss_keys - original_keys
    assert len(original_keys) == len(records) == sum(r["target_rows"] for r in register)
    assert len(original_keys - loss_keys) == recovered
    lines = ["# FT coverage-loss review register — 2026-11-17", "",
        f"Fixed original cohort: **{len(records)} FT rows / {len(register)} candidate incidents / {records.game.nunique()} games**. "
        f"Current model recovers unique provisional candidates for **{recovered}** original rows; "
        f"**{len(records)-recovered}** still lack unique assignments.", "",
        f"The all-game replay has **{len(new_losses)} new coverage losses outside this fixed cohort**.", "",
        "Automatic categories below are triage suggestions. A recovered candidate is not a verified feed correction. "
        "The source-order windows, baseline parents, and complete baseline trip membership are preserved in ignored "
        "`euroleague/data/diagnostics/ft_stoppage_shadow/review/contexts.json`.", "",
        f"Original model SHA256: `{seed['model_hash']}`. Current model SHA256: `{current_hash}`.", "",
        "## Category counts (original FT rows)", "", "| Category | Original FT rows | Unique candidates now | Still open |", "|---|---:|---:|---:|"]
    lines += [f"| {k} | {v} | {recovered_by_category[k]} | {v-recovered_by_category[k]} |" for k, v in sorted(by_category.items())]
    lines += ["", "## Evidence required", ""]
    lines += [f"- **{key}:** {value}" for key, value in evidence_needed.items() if key in by_category]
    lines += ["", "## Incident register", "",
        "| Game | Period | Original FT source orders | Original status | Triage category | Unique candidates now |",
        "|---|---:|---|---|---|---:|"]
    for r in register:
        lines.append(f"| {r['game']} | {r['period']} | {', '.join(map(str, r['ft_orders']))} | "
                     f"{r['original_status']} | {r['category']} | {r['unique_now']}/{r['target_rows']} |")
    lines += ["", "## Reproduce", "", "```powershell",
        "& euroleague/.venv/Scripts/python.exe euroleague/scripts/compare_ft_stoppages.py",
        "& euroleague/.venv/Scripts/python.exe euroleague/scripts/review_ft_stoppages.py", "```", "",
        "The checked-in seed fixes cohort identity across changes. The full comparison must also be checked for "
        "new coverage losses outside this cohort. Production and the canonical parser remain unchanged.", ""]
    REPORT.write_text("\n".join(lines), encoding="utf-8")
    print(json.dumps(dict(incidents=len(register), original_rows=len(records), recovered=recovered,
                          new_losses=len(new_losses), categories=dict(by_category)), indent=2))


if __name__ == "__main__":
    main()
