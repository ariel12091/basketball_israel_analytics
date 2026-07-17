# Shot Profile Plan B — Execution Progress & Handoff

Plan: `docs/superpowers/plans/2026-07-17-shot-profile-B-sql.md` (read it first).
Method: superpowers:subagent-driven-development. Ledger mirror: `.superpowers/sdd/progress.md` (gitignored — THIS file is the durable copy).
Model policy (user request — mind session budget): **haiku** implementers (briefs contain complete code), **sonnet** task reviewers, **opus** final review only. Controller runs Tasks 1/6/9 inline — they need gitignored `etl/.Renviron`/`app/.Renviron` creds, so do NOT subagent them and do NOT use a worktree.

## Status checklist

- [x] Task 1 — baseline + dependency check (inline). Branch `sql/shot-profile-mv` from main@`99b5eca`. Dep check: 0 rows (both objects are leaves). Baselines: onoff_compute ~1.7–2.3s warm, get_team_ratings_dynamic ~0.9–1.3s warm. Row counts: onoff 295 (2025) / 362 (2026); pff 359,925 / 376,121; team 14 / 14.
- [x] Task 2+3 — `onoff_mv.sql` + `refresh_onoff_default_for_games.sql`, 28 columns (commits `e092662`+`4760d39`). **Reviewed clean** (spec ✅, Approved; INSERT/SELECT alignment + file-mirror invariants verified column-by-column).
- [x] Task 4 — `player_four_factors_by_game.sql` + its refresh fn, 7 per-game columns (commit `af670ae`, report `.superpowers/sdd/task-4-report.md`). **NOT YET REVIEWED.**
- [ ] Task 4 review — `scripts/review-package 4760d39 af670ae` (skill dir: `~/.claude/plugins/cache/claude-plugins-official/superpowers/6.1.1/skills/subagent-driven-development`), sonnet reviewer with brief `.superpowers/sdd/task-4-brief.md` + report + package. CRITICAL invariants to have it check: refresh-fn INSERT list order == final SELECT order (7 new cols between `fg3_att` and `onoff_minutes`); CASE derivations identical between the two files.
- [ ] Task 5 — haiku implementer, brief `.superpowers/sdd/task-5-brief.md` (full-file rewrite of `sql/materialized_views/team_ppp_ratings_mv.sql`, 12 team columns) → sonnet review.
- [ ] Task 6 — INLINE deploy: `rebuild_all_mvs(from_level = 2)` + deploy both refresh fns (complete scripts in plan Task 6) + invariants. Expected: row counts match Task 1 baseline exactly; rim ≤ fg2 and c3 ≤ c3_known ≤ fg3 all zero violations; league totals off=def exactly and within ~1% of corners 1,035 (2025) / 1,150 (2026), known 3PA 11,497 / 12,091; incremental smoke on game 388. Then merge `sql/shot-profile-mv` → main, delete branch.
- [ ] Task 7+8 — branch `sql/shot-profile-fns` from main. ONE haiku implementer, briefs `.superpowers/sdd/task-7-brief.md` + `task-8-brief.md` (`onoff_compute.sql` 28 output cols; `get_team_ratings_dynamic.sql` 12 output cols) → sonnet review. CRITICAL: RETURNS TABLE order == final SELECT order in both functions; `final_calc` GROUP BY gains the 12 carried columns.
- [ ] Task 9 — INLINE: deploy both fns (port 5432, deploy_fn helper in plan) + re-run `scripts/apply_db_security.R` (dry-run, then its apply flag — DROP FUNCTION dropped app_readonly grants) + `test-db-security-contracts.R` + parity/timing script (plan Task 9; timings < 1.5× baseline above) + `scripts/test_all.R` + CLAUDE.md doc line (exact text in plan) → merge `sql/shot-profile-fns` → main.
- [ ] Final whole-branch review — opus, superpowers:requesting-code-review template, review-package covering BOTH branches (merge-base = `99b5eca` vs final main HEAD) → superpowers:finishing-a-development-branch.

## Reference

- Briefs already extracted: `.superpowers/sdd/task-{2,3,4,5,7,8}-brief.md`.
- Plan-B column vocabulary + invariants: see plan "Global Constraints"; corner share = `c3_att / c3_known_att`, NEVER `/ fg3_att`.
- After Plan B: Plan C (Shiny Tabs 1/3/7 Shot Profile UI) per spec `docs/superpowers/specs/2026-07-16-shot-profile-design.md`; also add the React-drift note to PROJECT.md in Plan C (deferred — PROJECT.md holds user WIP).

## Non-Plan-B open items (do not lose)

- User's uncommitted CLAUDE.md doc edits parked at `C:/Users/ariel/AppData/Local/Temp/claude_md_wip_backup` — restore into working tree or commit, per user's choice.
- `PROJECT.md`, `scripts/fit_ff_impact_weights.R`, `app/rsconnect/.../onoff-shiny.dcf` are user-WIP uncommitted — never sweep into commits.
- Live shinyapps.io app still behind main (deploy + worker settings pending since early July — see memory audit note).
