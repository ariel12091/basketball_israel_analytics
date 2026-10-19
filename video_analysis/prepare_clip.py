"""Inspect an extracted clip and produce reproducible tracking input (stdlib only)."""
import argparse
import csv
import hashlib
import json
import subprocess
from fractions import Fraction
from pathlib import Path


def probe(path, *args):
    result = subprocess.run(
        ["ffprobe", "-v", "error", *args, "-of", "json", str(path)],
        check=True, capture_output=True, text=True, encoding="utf-8",
    )
    return json.loads(result.stdout)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    start = manifest["range"]["start_seconds"]
    end = manifest["range"]["end_seconds"]
    if not 0 <= start < end or end - start > 90:
        raise ValueError("Expected a positive clip range of at most 90 seconds")
    media = probe(args.input, "-show_streams", "-show_format")
    stream = next(s for s in media["streams"] if s["codec_type"] == "video")
    fps = float(Fraction(stream["avg_frame_rate"]))
    duration = float(stream.get("duration", media["format"]["duration"]))
    if abs(duration - (end - start)) > max(0.1, 2 / fps):
        raise ValueError(f"Clip duration {duration} does not match requested range")
    args.output.mkdir(parents=True, exist_ok=True)
    frames = probe(args.input, "-select_streams", "v:0", "-show_frames",
                   "-show_entries", "frame=best_effort_timestamp,best_effort_timestamp_time")
    timestamps = [float(f["best_effort_timestamp_time"]) for f in frames["frames"]]
    if not timestamps or any(b <= a for a, b in zip(timestamps, timestamps[1:])):
        raise ValueError("Missing or nonmonotonic decoded timestamps")
    # Preserve decoded clip PTS. Source times below are requested-offset estimates,
    # not original stream PTS: the downloader re-encodes and resets the time base.
    with (args.output / "frame_times.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["frame_index_1based", "clip_pts", "clip_time_base",
                         "clip_seconds", "estimated_source_seconds"])
        for index, (frame, seconds) in enumerate(zip(frames["frames"], timestamps), 1):
            writer.writerow([index, frame["best_effort_timestamp"], stream["time_base"],
                             seconds, start + seconds])
    subprocess.run([
        "ffmpeg", "-hide_banner", "-loglevel", "error", "-nostdin", "-y",
        "-i", str(args.input), "-vf", "fps=1,scale=480:-2,tile=5x4",
        "-frames:v", "1", str(args.output / "contact_sheet.jpg"),
    ], check=True)
    with args.input.open("rb") as handle:
        checksum = hashlib.file_digest(handle, "sha256").hexdigest()
    report = {
        "clip_id": manifest["clip_id"], "sha256": checksum,
        "width": stream["width"], "height": stream["height"],
        "fps": fps, "duration_seconds": duration, "decoded_frames": len(timestamps),
        "first_clip_seconds": timestamps[0], "last_clip_seconds": timestamps[-1],
        "time_base": stream["time_base"],
        "source_alignment": "requested_offset_only_not_independently_verified",
        "tracking_status": "not_run",
    }
    (args.output / "media_report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
