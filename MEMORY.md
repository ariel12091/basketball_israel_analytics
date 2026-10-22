# Restart memory -- 2026-10-22

User priority change: **prove scalability before continuing Home**. Home has
not been started (only source reads). Audit verdict: greater end-to-end capacity
than Shiny is NOT proved. See `docs/react_scalability_evidence_2026-10-22.md`.
A bounded local production API probe passed 1,200 warm Gameflow requests;
at 150 concurrent HTTP requests, 300/300 succeeded, p95 506.6 ms, max 525.2 ms.
This is a short cached-only burst, NOT 150 browser users or a Shiny comparison.
Script: `web/scripts/benchmark-cache.mjs`; raw output is ignored under
`output/scalability/warm-cache.json`. No public stress test was performed.
The existing 150-user mixed cold/warm criterion remains unproved, and shared
SQL bottlenecks remain. Full shared-production-DB load testing must respect the
design's quiet, non-game-day scheduling constraint. Resume with this evidence
gate, not Home features. Gameflow checkpoint `a424af5` is committed and pushed.

Latest checkpoint: **Gameflow is implemented** for IL, EuroLeague and EuroCup,
from Game Logs in both views and at `/{league}/gameflow?game_id=…&team_id=…`.
Desktop uses a native dialog; phone/tablet widths use quarter/OT cards, with
an optional full timeline and pinned names. Player overlap, shared lineups,
lineup highlighting, stint detail, score inspection and health warnings are across.
Ten recorded Shiny input/output fixtures replay exactly, including per-period
details; ten live API comparisons match. All 237 unit tests pass, 92 unique
browser acceptance/regression checks pass, and six live SSR checks pass.
The first shared run passed 86/92: six old Game Logs checks assumed every
Gameflow cell read View. They now compare the recorded R cell (empty for a
missing scoring feed); the complete 32-check Game Logs rerun passes.
Build/typecheck pass; lint has two existing warnings. React Doctor's full scan
leaves only the existing OnOff cleanup error; new advisories are documented in
the handoff. Its remote score service was unavailable; the local scan finished.
Ready Preview: https://ibpl-game-logs-11drkc5az-ariel12091-8759.vercel.app/il/gameflow?game_id=390&team_id=2
Deployment `dpl_6ffRcJ8DZuqzcVAk6KJPU7TKbvZm`, Preview only. All 48 distinct
protected Preview checks pass (one transient old endpoint retried successfully).
Commit/push this checkpoint on `sql/web-foundation`, as the user requested.
Next: **Home, then Compare**. Home was surveyed but has no React implementation
yet. Default seasons remain 26–27. One production-mode server runs on 3100
through `node --env-file=../app/.Renviron … next start`, PG_PORT=5432;
check before starting another. No database/schema/ETL/role changes.
The Player Stats and older entries below are historical.

Player Stats continuation: implementation is at `/{league}/player-stats` for IL,
EuroLeague and EuroCup, with all four display modes, canonical selectors,
multi-team TOTAL rows, context/clutch filters, Min GP, stat chips, eligibility,
saved column order and CSV. 27 Shiny frames / 47 query fixtures are recorded and
replay identically offline (54 assertions). All 222 unit tests pass with two
workers; an earlier concurrent run hit one existing Lineups 5-second timeout.
All 114 browser checks pass (66 Player Stats + 48 shared regressions), with
phone/desktop screenshots inspected. Build/typecheck pass; lint has the two
existing warnings. Six live production-mode SSR checks pass. Ready Preview:
https://ibpl-game-logs-91aourr8p-ariel12091-8759.vercel.app/il/player-stats?season=2026
Deployment `dpl_8kpxTK9zf12i99LbqD66faBLUVbz`, target Preview only. All 37
protected Preview checks pass (migrated tabs, Player Stats context/clutch and
initial server-rendered HTML for every competition). The Player Stats checkpoint
belongs on `sql/web-foundation`, committed/pushed as requested by the user.
The original production server PID 21796 was stopped; one new production-mode
server now runs on 3100 with PG_PORT=5432. Check before starting another.
The rate cutoff must retain R's `1 - .85` and 1-based type-7 interpolation:
rounding it admits an extra boundary player. Next after this checkpoint:
Gameflow + Home, then Compare. The Team Ratings entry below is historical.

Latest continuation: **Team Ratings is implemented** at `/{league}/team-ratings`.
Summary/Four Factors cover IL, EuroLeague and EuroCup; Shot Profile/Traditional
are IL only. Measured-minutes pace, previous-matchday rank arrows, stat filters,
Traditional side/scaling, saved column order and CSV match Shiny.
Ready Preview: https://ibpl-game-logs-nh0jyt5it-ariel12091-8759.vercel.app/il/team-ratings?season=2026
Deployment `dpl_3g1BP3XZfPXGm1J5bMkBGsdkxxfK`, target Preview only.
Passed: 45 Shiny replays (90 assertions), 177 web unit tests, 98 Team Ratings
browser checks, six real server-render checks and 48 shared-table regressions,
at 390/1440 px. The unit suite requires all 45 oracle frames to be present.
All 31 protected Preview checks pass (views, filters, migrated tabs and SSR).
Build/typecheck pass; lint has the two existing warnings. See the newest handoff
entry for Preview checks, regressions, logs and React Doctor findings.
The user requested committing and pushing migration checkpoints; branch is
`sql/web-foundation` (Lineups checkpoint `609bf49` is already pushed).
One **production-mode** Next server runs on 3100 (PID 21796), with PG_PORT=5432
and existing read-only credentials. Check before starting another server.
**Next: Player Stats**, then Gameflow + Home, Compare. No DB/ETL/role changes.
The Lineups and earlier entries below are historical.

Current continuation: **Lineups is implemented** for IL, EuroLeague and EuroCup
at `/{league}/lineups`, with Summary/Four Factors, groups 2–5, roster selections,
all Shiny filters, TOTAL and season game-log modals. Included in the Lineups
migration checkpoint on `sql/web-foundation`; use `git log` for its commit.
Preview is Ready: https://ibpl-game-logs-db5d7rnxc-ariel12091-8759.vercel.app/il/lineups?season=2026
Deployment `dpl_7rmA7oZHFWP4sLGmU2UR3MWeFcth`, target Preview; no production promotion.
Passed: 36 Shiny replay presets (72 assertions), 125 unit tests, 92 Lineups
browser checks, plus the earlier 142 On/Off/Game Logs regression checks.
Phone/desktop layouts verified. Build/typecheck pass; lint has two existing warnings.
Final modal audit found null-type MV rows overwrote defense; explicit offense/
defense filtering now matches R, with a regression test. See the handoff for
final post-deploy checks and React Doctor findings. **Next: Team Ratings.**
All 24 protected Preview endpoint checks pass. Latest local live reruns hit
service-busy responses on cold loads and were stopped; see handoff/logs before
claiming a clean live rerun. The 92 acceptance checks and eight final modal
layout checks pass. One restarted dev server remains on 3100, PG_PORT=5432.

The older restart record below documents the preceding On/Off work; its old
Preview and "Lineups next" statements are historical.

The user is restarting to recover disk space and wants to continue the React
migration. Start with `docs/phase0_execution_status.md` (START HERE), then
`web/CLAUDE.md`. Those hold the detailed state and implementation rules.

- Active app: `web/` (Next.js/React), shared TabKit and **TanStack Table** in
  `web/src/components/shiny/DataTable.tsx`. Features first; preserve Shiny's
  controls/numbers/look; mobile first; one layout per width. No redesign.
- Code checkpoint: **`d361610`**, branch `sql/web-foundation`, not pushed/merged.
  The restart continuation's nuqs dependency fix and notes are uncommitted.
- Game Logs is across; Gameflow remains deferred. On/Off is implemented for IL,
  EuroLeague and EuroCup; Shot Profile is IL only. **On/Off browser acceptance
  now passes locally** (see verification below).
- Passed: 45 stored Shiny golden replays, 82 web unit tests, six live On/Off
  browser checks (390/1440 px), 16 protected Preview endpoint checks, builds,
  typecheck and lint (two existing warnings).
- Slider failure reproduced after restart (9/10 last-N checks passed).
  Diagnostics showed correct keyboard/setter values followed by stale URL state.
  Updated **nuqs 2.7.2 -> 2.10.2**, which includes upstream fix #1613 for
  Next.js 16.4+ URL/adapter lag. Temporary diagnostics removed; assertions unchanged.
- New verification: **102/102 On/Off**, **28/28 repeated last-N/interactions**,
  **40/40 Game Logs/selector regressions**, 82 unit tests, typecheck, build,
  lint (two existing warnings). Phone/desktop screenshots inspected.
  Logs: `tmp_onoff_resume_full.log`, `tmp_onoff_resume_repeat.log`,
  `tmp_onoff_resume_regression.log`. React Doctor changed-file scan covers zero
  React source files; do not treat its score as a full-branch review.
- Preview (Vercel login required):
  https://ibpl-game-logs-n69n42tt5-ariel12091-8759.vercel.app/il/onoff?season=2026
  EuroLeague/EuroCup use season 2025. This Preview still has the old nuqs version;
  the new fix has not been deployed. No production promotion.
- npm cache and disposable `_npx` cache were cleared for **disk pressure**,
  recovering roughly 550 MB. After restart free space was about **12 GiB**.
  One dev server is running on port 3100 with read-only DB access; check before
  starting another server. Cache cleanup did not fix the slider race.
- Keep existing roles/passwords and `app_readonly` server-side credentials.
  No DB/schema/ETL changes. Never merge this whole branch: older `87c3de2`
  publication/loader changes depend on absent production functions.
- Unrelated untracked paths must stay untouched: `bash.exe.stackdump`,
  `euroleague/src/euroleague_possessions.egg-info/`, `frontend/`, `video/`,
  `video_analysis/`.

Next: Lineups, then Team Ratings, Player Stats, Gameflow + Home, Compare.
The dependency fix is ready for review/checkpoint and a later Preview deployment.
Broad Phase 0/load/publication proofs remain parked under the user's amendment.

Logs and generated JSON are ignored local artifacts, expected to survive a
restart. The handoff lists their paths and regeneration commands. No need to
re-record the live R goldens just to export JSON; their RDS files are committed.
