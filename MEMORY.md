# Restart memory -- 2026-10-21

The user is restarting to recover disk space and wants to continue the React
migration. Start with `docs/phase0_execution_status.md` (START HERE), then
`web/CLAUDE.md`. Those hold the detailed state and implementation rules.

- Active app: `web/` (Next.js/React), shared TabKit and **TanStack Table** in
  `web/src/components/shiny/DataTable.tsx`. Features first; preserve Shiny's
  controls/numbers/look; mobile first; one layout per width. No redesign.
- Code checkpoint: **`d361610`**, branch `sql/web-foundation`, not pushed/merged.
  The later restart-doc commit contains only these notes.
- Game Logs is across; Gameflow remains deferred. On/Off is implemented for IL,
  EuroLeague and EuroCup; Shot Profile is IL only. **On/Off acceptance is OPEN.**
- Passed: 45 stored Shiny golden replays, 82 web unit tests, six live On/Off
  browser checks (390/1440 px), 16 protected Preview endpoint checks, builds,
  typecheck and lint (two existing warnings).
- Last full On/Off browser run: **97/102 passed**. Remaining failures involve
  last-N possession sliders under keyboard input and one stale-row interaction.
  Targeted reruns are inconsistent. Do not dismiss this as a test-only issue,
  weaken assertions, or mark the family complete. Investigate before Lineups.
- Preview (Vercel login required):
  https://ibpl-game-logs-n69n42tt5-ariel12091-8759.vercel.app/il/onoff?season=2026
  EuroLeague/EuroCup use season 2025. This is Preview, not a production promotion.
- npm cache and disposable `_npx` cache were cleared for **disk pressure**,
  recovering roughly 550 MB; last measured C: free space was **0.71 GB**.
  This is not evidence that slider failures are fixed. Recheck after restart.
- Keep existing roles/passwords and `app_readonly` server-side credentials.
  No DB/schema/ETL changes. Never merge this whole branch: older `87c3de2`
  publication/loader changes depend on absent production functions.
- Unrelated untracked paths must stay untouched: `bash.exe.stackdump`,
  `euroleague/src/euroleague_possessions.egg-info/`, `frontend/`, `video/`,
  `video_analysis/`.

Next: check space and port 3100; start one dev server if needed; reproduce and
fix On/Off keyboard behavior on phone and desktop; complete that family's browser
acceptance; then Lineups, Team Ratings, Player Stats, Gameflow + Home, Compare.
Broad Phase 0/load/publication proofs remain parked under the user's amendment.

Logs and generated JSON are ignored local artifacts, expected to survive a
restart. The handoff lists their paths and regeneration commands. No need to
re-record the live R goldens just to export JSON; their RDS files are committed.
