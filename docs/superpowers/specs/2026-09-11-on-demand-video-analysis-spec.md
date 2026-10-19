# On-demand Israeli basketball video analysis

Date: 2026-09-11
Status: Proposed specification; implementation and footage benchmarks pending.
Companion: [Implementation plan](../plans/2026-09-11-on-demand-video-analysis-plan.md).

## 1. Product decision

Build an asynchronous tool for analyzing a selected possession or short live-play
clip. The initial useful result is an annotated video with anonymous player tracks,
an editable assignment of tracks to roster players, and downloadable observations.
Court diagrams and spatial metrics follow only after calibration passes validation.

The existing PBP remains the source of official events, lineups, and statistics.
Video adds observations with explicit coverage and uncertainty. An unknown identity
or off-screen player remains unknown. A roster constrains possible identities; it
does not uniquely identify five visually indistinguishable teammates.

Default scope: Israeli games, one operator, one GPU worker, clips of 10-60 seconds
with an absolute 90-second processing limit including context padding. Select three
games covering different arenas and broadcast conditions for evaluation. Begin with
local footage and manually selected timestamps. YouTube references are supported
as catalog entries, with analysis enabled when a processable source is available.

## 2. Releases and exclusions

| Release | User-visible result | Required evidence |
|---|---|---|
| R0: feasibility | Local clip, tracked video, observations, benchmark report | Reproducible run on representative footage |
| R1: operator pilot | Game/clip selection, queued job, result viewer, manual identity review | Timing, tracking, recovery and access tests pass |
| R2: spatial pilot | Court projection and qualified spatial measurements | Held-out calibration and identity gates pass |
| R3: scaled service | Assisted clock alignment, controlled multi-user queue, operational monitoring | Cost and reliability demonstrated on larger sample |

Excluded from R0/R1: automated ball tracking, jersey OCR, face recognition, passes,
screens, defensive assignments, automatic possession inference, season-wide
processing, and full-game persistent player identity. They require separate
evaluation and do not block the clip product. Live streaming is outside scope.

## 3. User journey

1. Open Video Analysis from an Israeli Game Logs game or game-flow modal.
2. Choose a registered source. If no source exists, show that clearly; an operator
   can register an approved local/uploaded asset and optionally a YouTube reference.
3. Select a PBP possession when a verified mapping exists, or enter video start/end
   timestamps. Preview and adjust boundaries before submitting.
4. Show duration, analysis profile, existing cached result, and current queue state.
5. Submit. Return a job reference immediately; the Shiny session remains responsive.
6. Show stages: Queued, Preparing clip, Tracking, Preparing result, Needs review,
   Ready, Failed, Cancelled. Progress is stage-based until measured estimates exist.
7. View the annotated clip beside official event context and a track roster.
   Anonymous labels remain available even when identification cannot be completed.
8. An operator can assign a player to a track for a specified interval, undo an
   assignment, split an incorrect identity interval, and mark unusable footage.
9. Export observations and a provenance/quality summary. R2 adds a synchronized
   court view, showing gaps whenever projection or identity is not trustworthy.

Initial controls: play/pause, seek, overlay toggle, track selection, player selector,
review status, retry and cancel. Public users may view explicitly published results;
GPU submission and editing initially require operator authorization. The current
public Shiny app must not expose an unrestricted GPU job button.

## 4. Architecture

```text
Shiny game/clip viewer -> authenticated Python API -> durable job metadata
                                                   -> GPU worker
Registered source -> exact clip decode -> detector -> McByte++ -> artifacts
PBP context + reviewed sync/identity ----------------------------^ 
Object storage holds video and observations; PostgreSQL holds small metadata.
```

Use an isolated Python service under `video_analysis/`, deployed separately from
Posit Connect. Shiny makes short API calls and polls active jobs with backoff; it
never runs inference or holds a long GPU request. Start with one worker and a
database-backed queue rather than introducing another queue service.

Keep the existing app database role read-only. The API/worker uses a dedicated
least-privilege role for a proposed `video_analysis` schema. PBP reads use existing
qualified surfaces. DDL, credentials and deployment are separate implementation
steps; this document does not install or provision anything.

Model baseline: supplied YOLOX SportsMOT detector plus McByte++. Compare RF-DETR
on the same clips only after the baseline is measured. RF-DETR is an interchangeable
detector, not an assumption that its generic weights outperform sports weights.
Keep detector and tracker environments separate if dependency pins conflict.

## 5. Source and time contract

A catalog record links a game to a source revision, provider reference, processable
asset, duration, dimensions, frame-rate/time-base metadata and checksum. Game matching
is reviewed using teams/date/competition, never inferred from a title alone. A game
may have multiple recordings or missing periods. Track source availability separately
from analysis availability.

Public YouTube availability does not itself establish permission for automated
download/reuse. Use a local file, broadcaster-provided asset or authorized ingestion
route for processing. An embedded/linkable recording can still support navigation.
Do not assume a YouTube timestamp URL supplies video bytes or that seeking downloads
only the requested interval. Measure retrieval separately from GPU time.

Store original presentation timestamps (PTS) and source time base. If frames are
resampled to a constant rate, retain the output-frame-to-source-PTS mapping. Extract
with exact decoded boundaries; keyframe-only seeking is insufficient for annotations.
All ranges are half-open: [start, end). Track IDs are local to a clip and camera shot.

Video time and basketball clock are different timelines. Timeouts, stopped clocks,
replays, missing footage and overtime prevent one global offset or linear mapping.
Store reviewed anchors with period, displayed clock, video PTS, optional action ID,
and live/replay status. Only interpolate inside a verified uninterrupted running-clock
interval. Repeated clock readings require event order/manual selection to disambiguate.
Never interpolate through stoppages, edits or missing coverage.

Preserve provider clock, canonical elapsed game time, video PTS, and the mapping
revision as separate values. Existing canonical timing is authoritative for existing
statistics, but must not be treated as exact visual shot-release timing. R1 allows
manual start/end selection when synchronization is unknown. R2 metrics at an event
require a reviewed visual frame or a validated event-alignment method.

## 6. Existing basketball context

Use `final_schedule_mv` for game metadata and `resolved_player_identity_v` plus
existing roster surfaces for display identity. Determine the current persistent
event/lineup contract before implementing a new reader. `actions_clean`, `possessions`,
`pws`, `stints` and `subs` are cold-archived and truncated after successful ETL.
Do not build a runtime feature that assumes they retain historical games.

Prefer the persistent `df_pts_poss_lineups_longer_mv` when sufficient; its mirrored
team perspectives must be deduplicated to source-event grain before possession
enumeration. Use the existing archive path for missing details. Validate actual
columns and timing/possession semantics against current SQL and value profiles.
One narrow context read per selected game/request; polling jobs must not reread PBP.

A clip can span multiple possessions, substitution boundaries or ambiguous lineup
states. Store event boundaries and a versioned context snapshot; do not reduce the
entire clip to one five-player lineup. Identity constraints apply only within verified
intervals. Invalid or missing lineups produce a review warning, not fabricated players.

## 7. Tracking and adapter contract

Canonical detection: frame index, source PTS, class, pixel-space xyxy box, floating
confidence, detector version. Record dimensions and explicitly reverse detector
resizing/letterboxing. Feed eligible people into the tracker; team/referee filtering
is a separate decision. Never assume every detected person is an active player.

The inspected McByte++ demo accepts external detections. Use a tested adapter for
its six-column `frame,x1,y1,x2,y2,confidence` route. The alternative ten-column reader
casts confidence to integer in the inspected source; fractional scores would be
lost. The demo also inspects the first detection line, so wholly empty input needs
explicit handling. Pin a commit and test these behaviors rather than relying on
the README alone. Output MOT records use xywh, requiring an explicit conversion.

Run tracking within a continuous camera shot. Reset state at hard cuts; persist
shot-local identities and link only through reviewed assignments in R1. A replay
must not become a second live observation. Manual live-play selection is the R0
baseline; automatic shot/replay classification is evaluated separately.

Keep observed detections distinct from tracker predictions. Predicted positions
through an occlusion are not measured positions. Retain missing/off-screen intervals.
Track IDs and model confidence are not player identity or calibrated probabilities.

## 8. Data model (logical proposal, not executable DDL)

| Entity | Grain and important fields |
|---|---|
| sources | source revision; game key, provider reference, asset key/hash, media metadata, availability |
| sync_revisions / anchors | source revision + mapping revision; period, clock, PTS, action, method, reviewer |
| clips | source revision + exact range; game/context revision, requested PBP event boundaries |
| jobs | job ID; cache key, profile, state, attempt, lease, heartbeat, stages, timing, failure code |
| artifacts | job + kind; immutable object key, hash, schema version, byte count, publication state |
| assignments | clip + shot + track + interval + revision; player/team, method, reviewer, superseded revision |
| calibrations | clip + shot + interval + revision; landmarks, transform, orientation, validation error |
| reviews | result revision; coverage, issues, reviewer, release decision |

Frame observations are Parquet in object storage, not database rows. Fields:
`clip_id, shot_id, frame_index, source_pts, source_time_base, clip_seconds,
track_id, bbox_x1, bbox_y1, bbox_x2, bbox_y2, detection_score, observation_status,
team_id?, player_id?, identity_revision?, court_x_m?, court_y_m?,
calibration_revision?, projection_valid, is_live_play, exclusion_reason?`.
Question marks denote nullable fields. Preserve raw outputs alongside derived views.
Use the project's actual game key types, including league/season disambiguation.

Cache keys include source content revision, exact time range, detector/tracker
commits and weight hashes, processing configuration and schema version. Derived
identity/spatial artifacts additionally include context, sync, assignment and
calibration revisions. A manual correction regenerates affected derived artifacts
without rerunning unchanged tracking. Publish artifacts atomically using a manifest.

## 9. API and job behavior

Proposed endpoints: `GET /games/{key}/sources`, `POST /clips`, `POST /jobs`,
`GET /jobs/{id}`, `POST /jobs/{id}/cancel`, `GET /results/{id}`,
`POST /results/{id}/assignments`, `POST /results/{id}/review`.
Catalog edits are operator-only. Job submission accepts registered source/clip IDs,
an allowlisted profile and an idempotency key, not arbitrary shell arguments/URLs.
Return 202 with a job ID or the reusable completed result. Use 409 for stale review
revisions and 422 for invalid ranges; expose stable failure codes without secrets.

Lifecycle: queued -> running -> ready | needs_review | failed | cancelled.
The current processing stage is separate from lifecycle. A worker atomically claims
a job with a lease; heartbeat expiry allows bounded recovery. Each attempt writes
to its own prefix. Retry transient retrieval/storage/worker failures at most twice;
invalid media, oversized clips and GPU OOM require changed input/configuration.
Cancellation is cooperative between stages/batches and must prevent publication
from a stale lease. Completed/cache hits are not charged new inference time.

Initial limits: one running job per worker, one active submission per operator,
queue cap 10, 90-second clip cap, configurable 15-minute attempt timeout and explicit
GPU-hour daily budget. These are operational defaults, not performance claims.
Resolve limits against benchmark evidence before enabling remote submissions.

## 10. Spatial measurements (R2)

Court mapping uses a versioned 28 x 15 m reference, with explicit origin, axes,
units and attack direction. Confirm competition geometry in implementation.
Fit a projective transform from well-distributed court landmarks, validate with
independent held-out landmarks, and refresh/invalidate as the camera moves.
Four non-collinear correspondences are a minimum for fitting, not validation.
Do not interpret camera-motion compensation as court calibration.

Use the projected ground contact point, initially a reviewed bottom-center proxy;
jumping players, occluded feet and inaccurate boxes can invalidate it. Record these
limits. A ball above the floor cannot be projected to true 3D position this way.

First metrics: visible-player trajectories; team width and depth when all five
required players are observed, identified and calibrated; nearest visible opponent
distance, explicitly labeled as such. A true nearest-defender measurement requires
coverage of all plausible defenders. Do not report coverage-biased distances as
complete team spacing or defensive pressure. No speed/distance totals until temporal
smoothing and projection error are validated against a separate reference.

## 11. Evaluation and release gates

Earlier conversational effort percentages and thresholds were estimates, not
measured evidence. The following are provisional engineering targets to validate.

Create 30 clips across three games (10 per game), covering half-court play,
transitions, occlusions, pans, cuts and poor footage. Reserve one entire game for
held-out evaluation. Annotate visible players at 2 FPS and dense temporal identity
around crossings/reappearances. Report per-game results and failure categories;
do not present sparse annotations as a full dense tracking benchmark.

| Gate | Measurement and provisional target |
|---|---|
| Decode integrity | All sampled frame overlays match source PTS within one decoded frame |
| Detection | Visible active-player recall >=95% and precision >=95% at IoU 0.5 on held-out labels |
| Identity continuity | >=90% evaluated uninterrupted clips need <=1 manual identity correction; report ID switches/minute separately |
| Unknown handling | No forced named identity; report verified named-player coverage separately from accuracy |
| Manual sync | 95% reviewed event anchors within 0.5 s of visual reference, excluding ambiguous events explicitly |
| Replay/cut handling | Every labeled hard cut resets tracking; known replay intervals excluded from live metrics |
| Court projection | Held-out ground landmarks median error <=0.5 m, p95 <=1 m on accepted intervals; report rejection coverage |
| Async UX | Submission/status return without waiting for inference; reload reconnects to the same job |
| Integrity/recovery | Duplicate submission, restart, stale lease and cancellation never publish competing results |

A failed spatial gate can still permit an anonymous tracked-video pilot. Missing
identity or calibration must suppress dependent metrics. Publish benchmark hardware,
resolution, clip duration, source FPS, processed FPS, model versions, peak VRAM,
retrieval/decode/detect/track/render/upload time, queue delay and correction effort.

No end-to-end latency promise yet. McByte++ reports 14.57 tracking FPS on H100,
excluding detection. For 900 frames this implies about 62 seconds of tracking
under that benchmark, not a service SLA. Measure cold and warm end-to-end p50/p95
on the selected hardware before displaying time estimates.

## 12. Operations, cost and release

Track GPU seconds, active/idle GPU hours, storage bytes, egress, failures and cache
hit rate. Cost per job = allocated GPU hours * actual hourly price + storage and
transfer; include worker idle time when pricing a warm service. A two-minute GPU
job uses 1/30 GPU hour before idle allocation. Provider and hardware remain open
until the baseline supplies VRAM/runtime evidence; H100 benchmark use is not an
H100 purchasing requirement.

Proposed retention: scratch frames 24 hours, unreviewed clips/results 7 days,
reviewed results 30 days, small provenance metadata 90 days. Registered originals
follow explicit source policy. Cleanup is reference-aware and scoped to job-owned
prefixes; a result with expired media shows Expired rather than Ready/playable.
Review these defaults before activating deletion.

Feature flag off by default. Deploy API/worker and run pilot before connecting
Shiny. Initially expose submission to an operator only. Rollback disables the
feature, stops new jobs and retains reviewed metadata; existing analytics continue.
Use short-lived artifact access, validate media size/duration, avoid arbitrary
network fetch targets and never expose worker credentials to the browser.

## 13. Evidence and unresolved choices

Inspected 2026-09-11; pin actual commits and weight checksums at implementation.

- [RF-DETR](https://github.com/roboflow/rf-detr): detector API, fine-tuning and component licensing. Prefer Apache-designated weights for initial comparison; Plus components have separate terms.
- [McByte++](https://github.com/tstanczyk95/McBytePlusPlus): sports tracking baseline and reported benchmark (tracking only).
- [Installation](https://github.com/tstanczyk95/McBytePlusPlus/blob/main/INSTALLATION.md): documented Linux/Python 3.10/CUDA 12.8 environment, PyTorch pins, EdgeTAM and re-ID dependencies. Reproduce before choosing a GPU host.
- [Demo source](https://github.com/tstanczyk95/McBytePlusPlus/blob/main/tools/demo_track__with_reid.py): external detection parsing and output formats. Adapter tests are required.
- [YouTube terms](https://www.youtube.com/static?template=terms): source availability and processing authorization are separate ingestion concerns.
- Local architecture: [PROJECT.md](../../../PROJECT.md), [database context](../../database_context.md), [canonical timing](../../canonical_clock_minutes.md), [app entry](../../../app/app.R), [Game Logs](../../../app/R/server_tab4.R).

Unresolved implementation inputs: representative footage, GPU availability/provider,
acceptable waiting time and budget, source ingestion arrangement, and whether named
tracks or court spacing is the user's first priority. Defaults above allow a local
anonymous-tracking experiment to proceed once a processable clip is selected.
