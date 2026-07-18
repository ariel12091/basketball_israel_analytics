# Shot Profile Plan B — Execution Progress & Handoff

Plan: `docs/superpowers/plans/2026-07-17-shot-profile-B-sql.md` (read it first).
Method: superpowers:subagent-driven-development. Ledger mirror: `.superpowers/sdd/progress.md` (gitignored — THIS file is the durable copy).
Model policy (user request — mind session budget): **haiku** implementers (briefs contain complete code), **sonnet** task reviewers, **opus** final review only. Controller runs Tasks 1/6/9 inline — they need gitignored `etl/.Renviron`/`app/.Renviron` creds, so do NOT subagent them and do NOT use a worktree.

## Status checklist

- [x] Task 1 — baseline + dependency check (inline). Branch `sql/shot-profile-mv` from main@`99b5eca`. Dep check: 0 rows (both objects are leaves). Baselines: onoff_compute ~1.7–2.3s warm, get_team_ratings_dynamic ~0.9–1.3s warm. Row counts: onoff 295 (2025) / 362 (2026); pff 359,925 / 376,121; team 14 / 14.
- [x] Task 2+3 — `onoff_mv.sql` + `refresh_onoff_default_for_games.sql`, 28 columns (commits `e092662`+`4760d39`). **Reviewed clean** (spec ✅, Approved; INSERT/SELECT alignment + file-mirror invariants verified column-by-column).
- [x] Task 4 — `player_four_factors_by_game.sql` + its refresh fn, 7 per-game columns (commit `af670ae`, report `.superpowers/sdd/task-4-report.md`). **Reviewed clean**: INSERT/final-SELECT order and CTAS/refresh derivations verified.
- [x] Task 5 — `team_ppp_ratings_mv.sql`, 12 team columns (commit `91da72f`). **Reviewed clean**: order, bigint types, join cardinality, and mirrored perspective semantics verified.
- [x] Task 6 — deployed L2–L4 and both refresh functions; invariants/smoke passed after two runtime fixes and one data-quality guard:
  - `8c10786`: removed a CTAS-comment semicolon that the rebuild helper split on; disabled statement timeout only on the dedicated DDL session.
  - `5d740d8`: constrained lay-up/dunk predicates to `parameters_points = 2` after eight mirrored malformed 3-point lay-up tags caused 72 per-game rim>fg2 cells. Reviewed clean; final rim/corner violations are all zero.
  - Final rows: onoff 295/362; PFF 359,925/376,370 (2026 is +249 vs Task-1 baseline because source rows landed after baseline); team 14/14.
  - League totals: c3 1,035/1,150; known 3PA offense 11,497/12,069, defense 11,496/12,069. The one-row 2025 delta is audited source orphan `(62452, 624520636)` (offense lineup present, defense lineup absent), not join fanout.
  - Incremental smoke game 388: PFF 1,612 rows, onoff 362 rows; new columns remain populated.
  - Branch merged and pushed to main as `0c39d62`; local `sql/shot-profile-mv` deleted.
- [x] Task 7+8 — branch `sql/shot-profile-fns` from main. `onoff_compute.sql` gained 28 ordered output columns (`fd328e9`); `get_team_ratings_dynamic.sql` gained 12 ordered output columns (`a99085b`). Independent review verified both RETURNS TABLE/final SELECT contracts and the team function's GROUP BY carry-through.
  - The original action-wide `shot_zones` join benchmarked at roughly 48% overhead, so it was not deployed. Commit `95be1cf` replaced it with PK-backed `EXISTS` checks evaluated only for 3PA.
  - Focused seven-run transactional benchmark (candidate rolled back): current bracketed warm median 1.00 s, optimized candidate 1.03 s (+0.03 s / 3%). Optimization and benchmark documentation were independently reviewed; stale join wording was corrected in `e70d4a0`.
- [x] Task 9 — both functions deployed on port 5432; security dry-run and apply succeeded; `test-db-security-contracts.R` passed 35/35. Team parity has 0 mismatches. On/off parity has 21 new-column mismatches, equal to the 21 pre-existing FG3 control mismatches, so this work introduced no additional drift. Live timing runs remained well below 20 s; the focused team benchmark above is within 1.5× baseline. `scripts/test_all.R` passed with no failures (four expected opt-in skips). `CLAUDE.md` was intentionally not modified because repository `AGENTS.md` says to keep it historical; `PROJECT.md` remains untouched user WIP. Branch merged to main as `a888315`.
- [x] Final whole-branch review — approved with no findings for `99b5eca..e319bd2`. It independently verified output/insert order, full-build/refresh parity, corner-known fail-open semantics, security posture, and the narrow PK-backed dynamic lookups.

## Reference

- Briefs already extracted: `.superpowers/sdd/task-{2,3,4,5,7,8}-brief.md`.
- Plan-B column vocabulary + invariants: see plan "Global Constraints"; corner share = `c3_att / c3_known_att`, NEVER `/ fg3_att`.
- After Plan B: Plan C (Shiny Tabs 1/3/7 Shot Profile UI) per spec `docs/superpowers/specs/2026-07-16-shot-profile-design.md`; also add the React-drift note to PROJECT.md in Plan C (deferred — PROJECT.md holds user WIP).

## Non-Plan-B open items (do not lose)

- User's uncommitted CLAUDE.md doc edits parked at `C:/Users/ariel/AppData/Local/Temp/claude_md_wip_backup` — restore into working tree or commit, per user's choice.
- `PROJECT.md`, `scripts/fit_ff_impact_weights.R`, `app/rsconnect/.../onoff-shiny.dcf` are user-WIP uncommitted — never sweep into commits.
- Live shinyapps.io app still behind main (deploy + worker settings pending since early July — see memory audit note).
