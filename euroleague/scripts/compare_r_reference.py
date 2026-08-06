"""Compare Python parser decisions with an exported R reference CSV."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd


SUBPROJECT = Path(__file__).resolve().parents[1]
SRC = SUBPROJECT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from euroleague_possessions import group_events  # noqa: E402


KEY = ["season", "gamecode", "period", "source_event_order"]
DECISIONS = [
    "synthetic_parent_order",
    "synthetic_ft_trip_id",
    "final_end_poss",
    "end_reason",
    "grouping_status",
    "grouping_confidence_pct",
]


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("raw_csv", type=Path)
    parser.add_argument("r_reference_csv", type=Path)
    return parser.parse_args()


def _normalize(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame[KEY + DECISIONS].copy()
    for column in KEY + ["synthetic_parent_order", "grouping_confidence_pct"]:
        result[column] = pd.to_numeric(result[column], errors="raise").astype(int)
    if result["final_end_poss"].dtype != bool:
        result["final_end_poss"] = (
            result["final_end_poss"].astype(str).str.upper().eq("TRUE")
        )
    for column in ("synthetic_ft_trip_id", "end_reason", "grouping_status"):
        result[column] = result[column].fillna("").astype(str)
    return result.sort_values(KEY, kind="stable").reset_index(drop=True)


def main() -> None:
    args = _arguments()
    python = _normalize(group_events(pd.read_csv(args.raw_csv)))
    reference = _normalize(pd.read_csv(args.r_reference_csv))

    if len(python) != len(reference):
        raise SystemExit(
            f"row_count_mismatch python={len(python)} r={len(reference)}"
        )
    if not python[KEY].equals(reference[KEY]):
        raise SystemExit("event_key_mismatch")

    mismatch_counts: dict[str, int] = {}
    for column in DECISIONS:
        mismatch_counts[column] = int((python[column] != reference[column]).sum())
    print(f"events={len(python)}")
    for column, count in mismatch_counts.items():
        print(f"{column}_mismatches={count}")

    total = sum(mismatch_counts.values())
    if total:
        mask = pd.Series(False, index=python.index)
        for column in DECISIONS:
            mask |= python[column] != reference[column]
        examples = python.loc[mask, KEY + DECISIONS].head(10).copy()
        for column in DECISIONS:
            examples[f"r_{column}"] = reference.loc[mask, column].head(10).to_numpy()
        print(examples.to_string(index=False))
        raise SystemExit(1)
    print("parity=exact")


if __name__ == "__main__":
    main()

