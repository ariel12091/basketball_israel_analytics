# On-demand video analysis handoff

Date: 2026-09-12
Status: First McByte++ tracking run completed; court calibration not started.

## Objective and product direction

Build on-demand analysis for a selected Israeli basketball possession or short
clip. The existing PBP remains authoritative for events, lineups and player
identity. Video supplies tracks and, after calibration, spatial observations.
The initial product is asynchronous because the measured tracker is much slower
than real time.

The agreed sequence is:

1. Anonymous player detection/tracking and annotated video.
2. Court calibration and standardized 28 x 15 m coordinates.
3. Blue/white team classification and manual identity review using roster/lineup
   constraints.
4. Coverage-qualified spacing and nearest-opponent measurements.
5. PBP/video synchronization and application integration after the prototype is
   accurate enough.

Full design documents:

- [Product and technical specification](superpowers/specs/2026-09-11-on-demand-video-analysis-spec.md)
- [Implementation plan](superpowers/plans/2026-09-11-on-demand-video-analysis-plan.md)
- [Prototype README](../video_analysis/README.md)

## Source clip

- YouTube: `https://www.youtube.com/watch?v=urrtcQUzwmo`
- Requested half-open range: `[154, 174)` seconds (2:34-2:54).
- Title: Winner Cup Group C, Hapoel Eilat vs Bnei Herzliya.
- Upload date: 2026-09-10.
- Database game ID and exact game date have not been verified.
- Source metadata: 1920 x 1080, approximately 49.906 FPS.
- Extracted duration: 20.017633 seconds, 999 decoded frames.
- A close-up cut was detected at clip time 19.436541 seconds (estimated source
  time 173.436541). Tracking intentionally used only the continuous live-play
  shot before that cut.

The original extracted clip is ignored under
`output/video_analysis/urrtcQUzwmo_154_174/source.mp4`. The stable selection and
source facts are recorded in
`video_analysis/clips/urrtcQUzwmo_154_174.json`. Source timestamps are estimates
from the requested download offset because section extraction re-encoded the
video and reset its PTS.

## Environment and Colab workflow

The local machine has an NVIDIA GeForce MX450 with 2,048 MiB VRAM, which is not
a practical baseline for the full YOLOX/EdgeTAM/re-ID stack. FFmpeg/FFprobe are
available locally. An isolated `.venv-video` was created using the existing
Anaconda Python 3.12.3 and contains `yt-dlp==2026.8.19`; it is gitignored.

The Colab notebook is
`video_analysis/colab/mcbyte_first_clip.ipynb`. It creates an isolated Python
3.10 environment, installs the upstream documented dependencies, downloads the
checkpoint backup bundle, retrieves the exact clip, extracts 25 FPS frames,
runs McByte++ and saves a compact result ZIP to Google Drive.

Upstream tracker version used by the completed run:
`be1bbc03f18e33e93e0a359bbcbfdfc4dc4ab6b9`.

One Colab-specific failure was found and fixed. Colab exported
`MPLBACKEND=module://matplotlib_inline.backend_inline`, which is unavailable in
the isolated environment and caused McByte++ to fail while importing
Matplotlib. The correct headless setting is:

```python
import os
os.environ["MPLBACKEND"] = "Agg"
```

The current notebook passes `MPLBACKEND=Agg` to both preflight and tracking
subprocesses. The standalone diagnostic is
`video_analysis/colab/mcbyte_preflight.py`; run it in Colab after upload with:

```python
%run /content/mcbyte_preflight.py
```

The notebook structure and Python cell syntax were validated locally, but only
the user-operated Colab runtime executed the tracker.

## Completed tracking result

The first run completed successfully:

| Measurement | Result |
|---|---:|
| Processed FPS | 25 |
| Frames | 486 |
| Tracked duration | 19.44 seconds |
| Wall time | 649.674 seconds (10m 49.7s) |
| Effective throughput | 0.748 input frames/second |
| Sampled peak GPU memory | 14,357 MiB |
| MOT observation rows | 4,052 |
| Unique anonymous track IDs | 13 |
| Median tracked people/frame | 10 |
| Mean tracked people/frame | 8.337 |
| Minimum/maximum tracked people/frame | 3 / 10 |
| Tracks present for at least half the clip | 10 |

This is a single measured run on the allocated Colab GPU, not a performance
promise. The output suggests useful coverage, but 13 IDs for at most ten visible
players indicates short track fragments, entries/exits or other detected people.
Visual review is still required to identify actual switches and false detections.

Primary artifacts:

- `video_analysis/tracked_shot_001.mp4`: annotated visual QA output.
- `video_analysis/tracks_mot.txt`: canonical McByte++ rows in
  `frame,track_id,x,y,width,height,confidence,...` format.
- `video_analysis/benchmark.json`: runtime, GPU memory and upstream commit.
- `video_analysis/tracking_summary.json`: raw output counts.
- `video_analysis/data/tracked_contact_sheet.jpg`: one-frame-per-second visual
  sample of the annotated result.

## Extracted structured data

`video_analysis/extract_tracking_data.py` converts the MOT output to:

- `video_analysis/data/tracking_observations.csv`: every observation with clip
  time, estimated source time, bounding box, center, bottom-center foot proxy and
  normalized image positions.
- `video_analysis/data/track_summary.csv`: first/last frame, duration, clip
  coverage, within-span gaps and confidence by track ID.
- `video_analysis/data/frame_summary.csv`: observed-person count and confidence
  per frame.
- `video_analysis/data/tracking_quality.json`: aggregate and limitations.

Reproduction command from the repository root:

```powershell
& C:/Users/ariel/anaconda3/python.exe video_analysis/extract_tracking_data.py --input video_analysis/tracks_mot.txt --output-dir video_analysis/data --fps 25 --width 1920 --height 1080 --source-start 154
```

The video is for inspection; `tracks_mot.txt` and the generated CSV files are
the analytical output. Current positions are image pixels. They support visual
trajectory/continuity analysis but cannot yet support basketball distances,
spacing or speed because broadcast camera movement changes the pixel mapping.
Track IDs are anonymous and can fragment or switch. The detector supplies people,
not team/player/referee labels. The foot point is only a bounding-box proxy.

## Calibration decision

Calibrated court coordinates can be compared across games because all accepted
frames map onto the same FIBA court coordinate system. A homography itself is
camera/shot-specific. It changes with camera position, pan, tilt, zoom, cropping
and cuts. A same-arena preset can initialize another game but must be validated
and refined.

For this 19.44-second moving-camera shot, start with approximately 8-12 manually
marked keyframes (roughly every 1.5-2.5 seconds), with 6-10 well-distributed court
landmarks per keyframe and additional held-out landmarks for validation. Track or
interpolate calibration only inside the continuous shot. Reset at every cut.

Provisional spatial acceptance gate from the specification:

- held-out ground-landmark median error <= 0.5 m;
- held-out p95 error <= 1.0 m;
- frames without enough visible landmarks or acceptable error remain uncalibrated;
- dependent metrics are suppressed rather than filled from low-confidence mapping.

One clip is enough for the first calibration feasibility test. A reusable automatic
system requires representative data: begin with 30 clips across three games, then
roughly 10-20 camera shots per arena. A league-wide model may require hundreds of
varied shots; those figures are planning estimates and must be replaced by learning
curve evidence.

## Next session

Immediate next task: build a small calibration annotation workflow for this clip
and mark around ten keyframes. Use a canonical 28 x 15 m court definition with an
explicit origin, axis direction and landmark names. Keep fit landmarks separate
from validation landmarks. Then estimate per-keyframe homographies, calculate
held-out meter errors, inspect projected player foot points, and decide whether
inter-frame tracking is stable enough.

Recommended implementation order:

1. Define the canonical court and landmark schema under `video_analysis/`.
2. Extract the ten proposed calibration frames and retain exact frame/time mapping.
3. Create a local annotation tool that records image pixels against named court
   landmarks and distinguishes fit from validation points.
4. Fit and validate homographies; generate a visual overlay for every keyframe.
5. Propagate/interpolate only after keyframe results pass the error gate.
6. Project bottom-center track points with calibration validity attached.
7. Add team-color classification and review after spatial projection is trustworthy.

Do not start PBP synchronization, database DDL or Shiny integration until this
calibration test establishes that the source footage produces useful spatial data.

Useful resume instruction:

> Continue the on-demand video analysis work from
> `docs/video_analysis_handoff_2026-09-12.md`. Start the manual court-calibration
> prototype for the completed Hapoel Eilat vs Bnei Herzliya clip, preserve existing
> user changes, and validate held-out landmark error before projecting tracks.

## Repository state

No commit was created. Relevant work is currently uncommitted:

- modified: `.gitignore` (adds `.venv-video/`);
- untracked: `video_analysis/`;
- untracked: the specification, plan and this handoff document.

The wider worktree already contained many unrelated user changes and untracked
files. Preserve them. `CLAUDE.md` was not modified. No database changes, app
changes, cloud deployment or paid resource provisioning were performed.
