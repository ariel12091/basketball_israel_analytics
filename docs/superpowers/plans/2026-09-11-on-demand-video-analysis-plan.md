# On-demand video analysis implementation plan

Date: 2026-09-11
Status: Ready for staged implementation; no code or infrastructure deployed.
Specification: [Product and technical contract](../specs/2026-09-11-on-demand-video-analysis-spec.md).

## Working rules

Implement small vertical slices. Record model evidence before selecting a detector
or purchasing sustained GPU capacity. Preserve existing R/PBP behavior. Use a
separate Python package/environment. Do not change `CLAUDE.md`. Any SQL work must
first consult current SQL, database context and value profiles; live migrations
follow repository DDL procedures. No full-game ingestion is needed for the first demo.

Estimates below are engineering effort for one developer with GPU and footage
available. They overlap and are not delivery commitments; annotation, access and
model quality may extend elapsed time. The first checkpoint is a runnable local
clip and measured results, not a public application deployment.

## Phase 0: inputs and reproducibility (1-2 days)

1. Select three registered game sources; extract a 30-second continuous live-play
   clip for the first run and define the 30-clip evaluation set from the spec.
2. Inspect GPU/CUDA availability and source media metadata. Record what is actually
   available before choosing local Linux, WSL2 or a remote Linux GPU worker.
3. Pin McByte++, YOLOX, EdgeTAM and re-ID commits/weights; inventory component and
   checkpoint licensing. Reproduce the documented environment and smoke test.
4. Create `video_analysis/pyproject.toml`, an environment/container definition,
   `README.md`, `configs/baseline.yaml`, and a model manifest with hashes.
5. Define a JSON clip manifest containing game/source revision, video timestamps,
   media metadata, optional PBP references and requested profile.

Exit: environment recreates successfully and a source can be decoded; blockers
document exact missing dependencies/weights. No GPU sizing or latency claims yet.

## Phase 1: local tracked-clip vertical slice (2-5 days)

Proposed package: `video_analysis/src/ibpl_video/`.

1. Add `media.py`: probe, enforce limits, decode exact clip boundaries, preserve PTS,
   generate numerically sortable frame names and isolate scratch files per attempt.
2. Add `detectors/base.py` and `detectors/yolox.py`: normalize boxes/confidence and
   retain detector input/output dimensions. Use supplied SportsMOT weights first.
3. Add `tracking/mcbyte.py`: invoke pinned tracker in its own environment; adapt
   six-column detections; handle empty frames/clips; pass actual processing FPS.
4. Add `artifacts.py`: normalized Parquet observations, annotated MP4 and JSON
   provenance/coverage report. Publish manifest only when all artifacts are valid.
5. Add `cli.py`: proposed interface `ibpl-video analyze --manifest clip.json`.
   This is a target interface, not an existing executable.
6. Run a representative clip, inspect the complete annotated result, and record
   stage timings, VRAM, ID switches, bad detections and manual correction needs.

Tests: frame/PTS and xyxy/xywh round trips; noninteger confidences; empty detections;
no-detection clip; variable-frame-rate input; invalid range; missing weights; output
frame count and timestamp alignment. Visual QA is mandatory for the sample run.

Exit: R0 artifacts reproducibly generated for one clip, with honest limitations.

## Phase 2: tracking evaluation and detector decision (3-7 days plus labels)

1. Prepare annotations and reserve one game untouched by tuning.
2. Measure baseline across the defined conditions. Report per-game accuracy,
   correction effort and cold/warm end-to-end latency, not just model FPS.
3. Add `detectors/rfdetr.py` and its separate environment if necessary. Compare an
   Apache-designated pretrained model against YOLOX on identical frames/settings.
4. If detection remains the bottleneck, label additional basketball frames and
   fine-tune only using training games. Record a separate annotation/training budget.
5. Choose the detector using held-out tracking quality, latency and VRAM; retain
   the adapter boundary so changing a detector does not change downstream contracts.
6. Create `docs/video_analysis/baseline_report.md` with reproducible commands,
   environment/weight manifest, quality table, examples and measured cost formula.

Exit: a justified detector/configuration choice and R1 feasibility decision. If
identity continuity fails, narrow pilot clips or improve tracking before app work.
No claim of season-wide reliability from a three-game sample.

## Phase 3: PBP context, timestamp and identity review (4-8 days)

1. Verify persistent event grain and available possession boundaries against the
   existing ETL and canonical timing. Inspect archive retrieval for missing details.
2. Build `context.py` with a versioned game/event/lineup snapshot. Deduplicate mirrored
   team perspectives and test historical games whose raw hot tables are empty.
3. Add `sync.py`: reviewed timestamp anchors and interval mappings, explicit gaps,
   repeated-clock ambiguity, overtime and manual clip fallback.
4. Add shot boundaries and manual live/replay interval labels; reset tracking on cuts.
5. Add `identity.py`: interval-scoped assignments, lineup candidate validation,
   unknown state, revision history, undo and stale-edit conflict detection.
6. Implement local review commands or a minimal operator page before Shiny integration.

Tests: stopped clock, replay at repeated clock, missing footage, overtime, source
replacement, substitution within clip, inconsistent lineup, duplicate team rows,
roster alias correction, two tracks assigned the same player at the same time.

Exit: operator can align a possession and correct identities without rerunning
inference; invalid context cannot silently produce named-player metrics.

## Phase 4: durable service and storage (4-7 days)

1. Add migration proposal for source/clip/job/artifact/review entities from the spec;
   validate in an isolated database. Keep app read access and worker write grants narrow.
2. Add `api.py`, `jobs.py` and `worker.py`: validated endpoints, idempotent submission,
   lease claims, heartbeat, bounded retries, cancellation and queue limits.
3. Add `storage.py`: immutable artifacts, signed access, reference-aware retention,
   atomic manifest publication and expired-result behavior.
4. Add cache identity for raw tracking and separate derived review/spatial revisions.
5. Add authorization for operator submission/editing and a publication decision for
   results. Configure secrets outside source control and record deployment settings.
6. Simulate worker termination, GPU OOM, duplicate requests, cancellation, upload
   failure, expired lease and API restart. Verify recovery and absence of duplicate
   published outputs. Confirm cached jobs consume no new GPU inference time.

Exit: service can survive restart and return a reviewed result by stable ID; measured
resource budgets and deployment configuration documented before remote rollout.

## Phase 5: Shiny operator pilot (3-6 days)

1. Add `app/R/mod_video_analysis.R` with a feature flag and short API client calls.
2. Wire game context from `server_tab4.R` and `mod_ribbon_modal.R`; inspect current
   modal callbacks first and reuse game identity rather than deriving it from labels.
3. Provide source/clip selection, preview, submission, stage polling, cached result,
   result playback and identity review. Poll only while needed, with backoff; stop
   polling on modal close and reconnect via job ID when reopened.
4. Expose manual video timestamps when possession synchronization is absent. Disable
   analysis with an actionable reason when no processable source is registered.
5. Add inline error/retry/cancel states and preserve user selection after API failure.
6. Test desktop/mobile playback, seeking, overlay alignment, browser reload, expired
   artifact URLs and ordinary Game Logs behavior with the flag both off and on.

Use configured Rscript `C:/Program Files/R/R-4.4.2/bin/Rscript.exe` for R validation.
Run relevant existing tests plus module tests for request counts/status behavior.
Use browser verification when implemented, following the applicable browser skill.

Exit: an authorized operator can select a game, analyze a clip and review a result
without freezing Shiny. Existing analytics and database role privileges are intact.

## Phase 6: court calibration and qualified metrics (1-3 additional weeks)

1. Add `calibration.py`: court reference, reviewed landmarks, homography fitting,
   held-out error, interval validity and camera-motion invalidation.
2. Add frame-linked court view with explicit missing/off-screen states. Initially
   permit manual calibration for stable shots; automate only after measuring drift.
3. Validate player ground-point approximation independently of court landmark error.
4. Add `metrics.py` for only the spec's coverage-qualified distances/width/depth.
   Store numerator/denominator coverage and exclusion reasons with each measurement.
5. Evaluate held-out games and suppress outputs whenever required identity, coverage
   or calibration is absent. Review whole annotated clips and court trajectories.

Tests: unit/orientation errors, singular landmarks, pan/zoom, hard cut, foot occlusion,
jump, off-screen defender, incomplete team, attack-direction change, stale calibration.

Exit: R2 only if the spatial gates pass. Otherwise the tracked-video product remains
available and spatial results stay experimental.

## Phase 7: operational expansion (after pilot evidence)

Add assisted scoreboard OCR/sync, automatic shot/replay classification and source
ingestion as separate evaluated components. Establish an operator workflow for
ambiguous outputs before expanding submissions. Assess more arenas and source
formats before season processing. Ball/event models require their own spec and
ground truth. A season-wide named-player analytics service is not implied by R1.

## Delivery checklist and dependency decisions

- R0: clip manifest, tracked MP4, Parquet observations, reproducible model manifest.
- R1: benchmark report, context/sync review, durable API/worker, gated Shiny workflow,
  recovery evidence, actual cost/runtime observations and rollback instructions.
- R2: calibration validation, court viewer and coverage-qualified metric report.

Choose footage and GPU for Phase 0. Choose hosting and spending limits after Phase 2.
Choose public access/retention before remote rollout. These decisions do not prevent
writing or testing the local interfaces with fixtures. Cloud provisioning, live DDL,
deployment and new paid resources follow the authority available at execution time.

The immediate implementation task is Phase 0 plus one Phase 1 clip. The first review
should answer: does the video contain enough uninterrupted visible play, how often
do tracks need correction, and what is measured end-to-end latency on available GPU?
