"""Convert McByte++ MOT output into analysis-ready CSV files and a quality report."""

from __future__ import annotations

import argparse
import csv
import json
import statistics
from collections import defaultdict
from pathlib import Path


MOT_COLUMNS = (
    "frame",
    "track_id",
    "bbox_x",
    "bbox_y",
    "bbox_width",
    "bbox_height",
    "confidence",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--fps", type=float, required=True)
    parser.add_argument("--width", type=int, required=True)
    parser.add_argument("--height", type=int, required=True)
    parser.add_argument("--source-start", type=float, default=0.0)
    return parser.parse_args()


def load_mot(path: Path) -> list[dict[str, float | int]]:
    observations = []
    with path.open(newline="", encoding="utf-8") as handle:
        for line_number, values in enumerate(csv.reader(handle), 1):
            if not values:
                continue
            if len(values) < 7:
                raise ValueError(f"Line {line_number} has fewer than seven MOT fields")
            observations.append(
                {
                    "frame": int(values[0]),
                    "track_id": int(values[1]),
                    "bbox_x": float(values[2]),
                    "bbox_y": float(values[3]),
                    "bbox_width": float(values[4]),
                    "bbox_height": float(values[5]),
                    "confidence": float(values[6]),
                }
            )
    observations.sort(key=lambda row: (row["frame"], row["track_id"]))
    return observations


def write_observations(
    path: Path,
    observations: list[dict[str, float | int]],
    fps: float,
    width: int,
    height: int,
    source_start: float,
) -> None:
    columns = list(MOT_COLUMNS) + [
        "clip_seconds",
        "estimated_source_seconds",
        "bbox_x2",
        "bbox_y2",
        "center_x",
        "center_y",
        "foot_x",
        "foot_y",
        "center_x_normalized",
        "center_y_normalized",
        "foot_x_normalized",
        "foot_y_normalized",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in observations:
            x = float(row["bbox_x"])
            y = float(row["bbox_y"])
            w = float(row["bbox_width"])
            h = float(row["bbox_height"])
            clip_seconds = (int(row["frame"]) - 1) / fps
            enriched = dict(row)
            enriched.update(
                {
                    "clip_seconds": round(clip_seconds, 6),
                    "estimated_source_seconds": round(source_start + clip_seconds, 6),
                    "bbox_x2": round(x + w, 3),
                    "bbox_y2": round(y + h, 3),
                    "center_x": round(x + w / 2, 3),
                    "center_y": round(y + h / 2, 3),
                    "foot_x": round(x + w / 2, 3),
                    "foot_y": round(y + h, 3),
                    "center_x_normalized": round((x + w / 2) / width, 8),
                    "center_y_normalized": round((y + h / 2) / height, 8),
                    "foot_x_normalized": round((x + w / 2) / width, 8),
                    "foot_y_normalized": round((y + h) / height, 8),
                }
            )
            writer.writerow(enriched)


def gap_statistics(frames: list[int]) -> tuple[int, int, int]:
    missing = 0
    gaps = []
    for previous, current in zip(frames, frames[1:]):
        gap = current - previous - 1
        if gap > 0:
            missing += gap
            gaps.append(gap)
    return missing, len(gaps), gaps or [0]


def write_track_summary(
    path: Path,
    by_track: dict[int, list[dict[str, float | int]]],
    fps: float,
    total_frames: int,
) -> list[dict[str, float | int]]:
    summaries = []
    for track_id, rows in sorted(by_track.items()):
        frames = sorted({int(row["frame"]) for row in rows})
        first_frame, last_frame = frames[0], frames[-1]
        span_frames = last_frame - first_frame + 1
        missing, gap_count, gaps = gap_statistics(frames)
        confidence = [float(row["confidence"]) for row in rows]
        summaries.append(
            {
                "track_id": track_id,
                "first_frame": first_frame,
                "last_frame": last_frame,
                "first_clip_seconds": round((first_frame - 1) / fps, 3),
                "last_clip_seconds": round((last_frame - 1) / fps, 3),
                "observation_frames": len(frames),
                "span_frames": span_frames,
                "span_seconds": round(span_frames / fps, 3),
                "whole_clip_coverage": round(len(frames) / total_frames, 4),
                "within_span_coverage": round(len(frames) / span_frames, 4),
                "missing_frames_within_span": missing,
                "gap_count": gap_count,
                "max_gap_frames": max(gaps),
                "mean_confidence": round(statistics.fmean(confidence), 4),
                "median_confidence": round(statistics.median(confidence), 4),
                "min_confidence": round(min(confidence), 4),
            }
        )
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(summaries[0]))
        writer.writeheader()
        writer.writerows(summaries)
    return summaries


def write_frame_summary(
    path: Path,
    by_frame: dict[int, list[dict[str, float | int]]],
    fps: float,
    total_frames: int,
) -> list[dict[str, float | int]]:
    summaries = []
    for frame in range(1, total_frames + 1):
        rows = by_frame.get(frame, [])
        scores = [float(row["confidence"]) for row in rows]
        summaries.append(
            {
                "frame": frame,
                "clip_seconds": round((frame - 1) / fps, 6),
                "tracked_objects": len(rows),
                "mean_confidence": round(statistics.fmean(scores), 4) if scores else "",
                "min_confidence": round(min(scores), 4) if scores else "",
            }
        )
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(summaries[0]))
        writer.writeheader()
        writer.writerows(summaries)
    return summaries


def main() -> None:
    args = parse_args()
    if args.fps <= 0 or args.width <= 0 or args.height <= 0:
        raise ValueError("FPS and dimensions must be positive")
    observations = load_mot(args.input)
    if not observations:
        raise ValueError("MOT file contains no observations")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    total_frames = max(int(row["frame"]) for row in observations)
    by_track: dict[int, list[dict[str, float | int]]] = defaultdict(list)
    by_frame: dict[int, list[dict[str, float | int]]] = defaultdict(list)
    for row in observations:
        by_track[int(row["track_id"])].append(row)
        by_frame[int(row["frame"])].append(row)

    write_observations(
        args.output_dir / "tracking_observations.csv",
        observations,
        args.fps,
        args.width,
        args.height,
        args.source_start,
    )
    tracks = write_track_summary(
        args.output_dir / "track_summary.csv", by_track, args.fps, total_frames
    )
    frames = write_frame_summary(
        args.output_dir / "frame_summary.csv", by_frame, args.fps, total_frames
    )
    counts = [int(row["tracked_objects"]) for row in frames]
    report = {
        "input": str(args.input),
        "coordinate_space": {"width": args.width, "height": args.height},
        "fps": args.fps,
        "source_start_seconds": args.source_start,
        "source_time_status": "estimated_from_requested_offset",
        "observation_rows": len(observations),
        "frames": total_frames,
        "unique_track_ids": len(by_track),
        "tracked_objects_per_frame": {
            "min": min(counts),
            "median": statistics.median(counts),
            "mean": round(statistics.fmean(counts), 3),
            "max": max(counts),
        },
        "tracks_present_at_least_half_clip": sum(
            float(row["whole_clip_coverage"]) >= 0.5 for row in tracks
        ),
        "limitations": [
            "Track IDs are anonymous and may fragment or switch identities.",
            "Objects include detected people; team/player/referee labels are unavailable.",
            "Pixel and normalized image positions are not calibrated court coordinates.",
            "Foot points are bottom-center bounding-box approximations.",
            "Source seconds are estimated from the requested download offset.",
        ],
    }
    (args.output_dir / "tracking_quality.json").write_text(
        json.dumps(report, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
