# Video prototype: first selected clip

Status 2026-09-11: footage acquired and inspected; tracking has not run.

Source: https://www.youtube.com/watch?v=urrtcQUzwmo
Selected interval: [154, 174) seconds, as requested.
Video title identifies Winner Cup Group C, Hapoel Eilat vs Bnei Herzliya.
Upload date is September 10, 2026; game date and database game ID remain unverified.

## Prepared artifacts

All paths below are relative to the repository root:

- `video_analysis/clips/urrtcQUzwmo_154_174.json`: selected clip manifest.
- `output/video_analysis/urrtcQUzwmo_154_174/source.mp4`: extracted clip.
- `output/video_analysis/urrtcQUzwmo_154_174/contact_sheet.jpg`: 20 frame samples.
- `output/video_analysis/urrtcQUzwmo_154_174/frame_times.csv`: decoded clip timestamps.
- `output/video_analysis/urrtcQUzwmo_154_174/media_report.json`: media validation/hash.

Decoded video: 1920x1080, 49.906 FPS, 999 frames, 20.017633 seconds. The slight
duration excess is frame quantization. The clip was re-encoded by FFmpeg during
section extraction. Original source PTS were not retained; source times in the
CSV are offset estimates, not independently verified source timestamps.

Frame samples show live transition/half-court play with blue and white uniforms,
camera panning, player occlusion, spectators and a visible scoreboard. A close-up
appears at the end. FFmpeg scene detection (320-pixel width, threshold 0.3) flags
clip time 19.436541 seconds, approximately source time 173.437 seconds. Treat it
as a candidate shot boundary: reset tracking there and exclude the close-up from
court metrics. This is a sampled inspection, not a frame-by-frame accuracy audit.

## Reproduce local preparation

The isolated `.venv-video` uses the existing Anaconda Python 3.12.3 installation.
The Windows `py` launcher did not find it; use the explicit environment executable.
FFmpeg/FFprobe are already available on PATH. No tracking packages were installed.

```powershell
& .venv-video/Scripts/python.exe video_analysis/prepare_clip.py --manifest video_analysis/clips/urrtcQUzwmo_154_174.json --input output/video_analysis/urrtcQUzwmo_154_174/source.mp4 --output output/video_analysis/urrtcQUzwmo_154_174
```

Preparation validates duration and monotonic timestamps, writes frame timestamps,
builds the contact sheet and hashes the video. It can regenerate those derived
artifacts. It does not overwrite the video, infer identities or run tracking.

Media and environment files are gitignored. The local downloader metadata contains
temporary source URLs; do not publish it as a benchmark artifact. A later provenance
manifest should retain only stable metadata, versions and hashes.

## Next execution requirement

Local GPU: NVIDIA GeForce MX450, 2048 MiB VRAM, driver 581.95. Ubuntu WSL2 is
installed. No full-pipeline GPU memory measurement has been made. This hardware is
not a practical assumed baseline for the combined YOLOX/EdgeTAM/re-ID stack.

Choose a Linux GPU runtime (for example a user-operated Colab runtime or an existing
GPU machine), then reproduce the pinned McByte++ environment and check allocated
GPU/VRAM before installing models. Colab dependency compatibility and available
GPU are not yet verified. No paid resources have been provisioned.

First tracking run: supplied SportsMOT detector + McByte++, anonymous identities,
shot reset at the reviewed cut, stage runtime and VRAM report. Retain all 999 frames
for the reference input; if testing lower FPS, emit a separate resampled artifact
with an explicit timestamp mapping and pass its actual FPS to the tracker.

The ready-to-run Colab notebook is
`video_analysis/colab/mcbyte_first_clip.ipynb`. Upload it at
https://colab.research.google.com/, choose a GPU runtime, and run cells from top to
bottom. It creates an isolated Python 3.10 environment, records the upstream commit,
downloads the documented checkpoint bundle, processes the live camera shot at 25
FPS, previews the result, and saves a compact ZIP to Google Drive. The notebook has
not yet been executed on Colab; free-tier GPU allocation and upstream dependency or
checkpoint availability can only be verified in a live runtime.

The McByte++ preflight cell captures and prints the demo process stderr before it
raises. If that cell fails, the final traceback above `RuntimeError` identifies the
dependency that needs repair. A bare `CalledProcessError` from an older notebook
revision hid this diagnostic; re-upload the current notebook or use its replacement
preflight cell.

Colab sets `MPLBACKEND=module://matplotlib_inline.backend_inline` for its notebook
kernel. That backend is not installed in the isolated McByte++ environment and made
the demo fail during import. The preflight and tracking subprocesses now explicitly
use the headless `Agg` backend; no package reinstall is required for this failure.

As a copy-safe alternative, upload `video_analysis/colab/mcbyte_preflight.py` to
the Colab session and run `%run /content/mcbyte_preflight.py` after the installation
cells. It uses the notebook's fixed `/content/ibpl_mcbyte` paths and prints the full
McByte++ import error without nested notebook indentation.

See the [implementation plan](../docs/superpowers/plans/2026-09-11-on-demand-video-analysis-plan.md)
and [specification](../docs/superpowers/specs/2026-09-11-on-demand-video-analysis-spec.md).

## First tracking result

The completed Colab run used McByte++ commit
`be1bbc03f18e33e93e0a359bbcbfdfc4dc4ab6b9`, processed 486 frames at 25 FPS,
and produced 4,052 observations with 13 anonymous track IDs. Wall time was
649.67 seconds (0.748 effective input FPS) and sampled peak GPU memory was
14,357 MiB. Ten tracks appeared in at least half of the clip. These are one-run
measurements on the allocated Colab GPU, not service performance targets.

Run `extract_tracking_data.py` against `tracks_mot.txt` to create:

- `data/tracking_observations.csv`: timestamped boxes, centers, approximate foot
  points, and image-normalized coordinates for every tracked frame/person.
- `data/track_summary.csv`: track spans, coverage, gaps, and confidence.
- `data/frame_summary.csv`: tracked-person count and confidence per frame.
- `data/tracking_quality.json`: machine-readable aggregate and limitations.
- `data/tracked_contact_sheet.jpg`: sampled visual tracking QA.

The data remains anonymous and image-relative. Track IDs may fragment or switch;
the detector can include non-player people. Court coordinates, distance, speed,
team and player identity are not available until calibration and identity review.
