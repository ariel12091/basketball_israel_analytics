# Game 400 raw-data investigation + ETL Phase 6 quarantine design (2026-09-15)

**Status:** game 400 **RESOLVED** later the same day — see "Resolution" at the end. Quarantine design still sketched but **not implemented** (not needed for this specific case in the end, since the source recovered on its own; remains a good idea for the general case). The investigation below (including the basket.co.il id-collision finding) is left as originally written — it stayed accurate even though game 400 itself ended up fixed a different way.

## Game 400: why it shows 11.75 min / 23 poss per team

Game 400 (Rishon Lezion vs Hapoel Eilat, 2026-09-14, `game_year` 2027) committed to the DB with only a fraction of its play-by-play. The GitHub Actions scheduled run (34910732724, 2026-09-14 23:51 UTC) shows the actual sequence:

- Phase 2 (base load) **succeeded and committed**: `game 400 staged: schedule=1, actions_clean=196, subs=58, full_rosters=26` → `game 400 base tables committed`.
- Phase 6 (validation) then **failed**: `Lineup/stint coverage FAILED: game 400 has 8 zero-match and 0 multi-match rows out of 134 source rows`, `game 400 team 14/17: minutes 11.8 < 39.0`. `last_success` was correctly withheld.
- But Phase 6 failing does **not** roll back Phases 2-5 — each phase commits independently (`dbBegin`/`dbCommit` per phase in `etl_full.R`), and `mark_phase_failed()` is pure bookkeeping (sets a flag, appends a log string). So the partial game stayed live in `mv_lineup_totals_by_day` and everything downstream. Confirmed directly against the DB: `SELECT sum(total_poss), sum(minutes) FROM mv_lineup_totals_by_day WHERE game_id=400 AND type_lineup='offense' GROUP BY team_id` → 23 poss / 11.75 min for both teams 14 and 17.

**Root cause of the truncation:** the ETL's actual PBP source, `stats.segevstats.com/realtimestat_heb/get_team_action.php?game_id=400`, is stuck. `actions_clean`/`pws` stop at `end_game_seconds_remaining=1679` (Q2, 07:59 remaining) — Q1 complete, Q2 barely started. A fresh fetch of that same URL days later (2026-09-15) still returns only 263 actions frozen at the exact same clock reading, and the tail rows carry a `userTime` equal to the request's own wall-clock time rather than a real historical timestamp — the feed is not slowly catching up, it is corrupted/stuck and re-serves a stale, mis-stamped tail on every request.

## Is it in the nightly data-quality report? Yes — but only in 2 of the possible Critical checks

The **first** DQ summary checked (`etl/logs/data_quality/summary_20260915_003111.csv`) predates game 400's commit (it's from an earlier local run that itself rolled back). The **actual** production report — pulled from GH Actions run 34910732724's `data-quality` artifact (generated 2026-09-15 00:00 UTC, *after* game 400 committed) — does flag it:

- `AC_missing_regulation_period_coverage`: Critical, "A regulation quarter is missing" — 1 game (400, periods 3 & 4 absent for both teams).
- `T_invalid_team_minutes`: Critical, "Team minutes don't match the game's length" — 2 games (400 at -28.25 min, and 160 at -3.25 min, unrelated).

It is **not** flagged by 5 other Critical checks that ran (`V_team_game_score_reconciliation`, `W_team_game_possession_reconciliation`, `Q_persisted_rows_without_lineup_match`, `R_invalid_lineup_player_counts`, `X_player_minute_conservation`) — all five `JOIN etl_processed_games` in their SQL, and game 400 has zero rows there (Phase 2 committed but the pipeline never reached the step that marks a game processed). `H_base_loaded_games_missing_processed_marker` independently catches this exact gap (`game 400: 26, 196` — base-loaded but unmarked) but isn't currently one of the findings-page's labeled severity buckets.

## basket.co.il as a fallback source: schema-compatible, but has its own, worse defect for this game

`https://basket.co.il/livestats/data/game-400.json` is a genuinely different endpoint (not the same feed the ETL uses) with a near-identical schema: same 11 top-level action fields (`id, parameters, parentActionId, playerId, quarter, quarterTime, score, teamId, totalPlayerPoints, type, userTime`), same nested `parameters` keys (basket.co.il adds `event`/`initialCall`/`result`, nothing removed). For game 400 specifically it has the complete game (881 actions, all 4 quarters, `gameFinished: true`) where segevstats is stuck at 263.

**But it is not currently usable as a drop-in fallback for this game.** Feeding it through the real `clean_actions()` (offline test, no DB writes) reproduces the exact same `Duplicate PBP action IDs` error segevstats' own last attempt hit. Root-caused it: **226 action ids collide**, spanning ids `4000192`-`4000424` (essentially all of Q2 and most of Q3), cleanly bounded (id `4000425` onward is a single clean sequence to game end). Breaking down the 226:
- 59 are true resends (identical `team_id`/`player_id`/`type` on both copies) — trivially safe to collapse.
- **167 are two genuinely different real events sharing one id** — e.g. `id 4000200` is a team-17 substitution (player 2487) *and* a team-14 substitution (player 1198); `id 4000268` is a team-14 turnover *and* an unrelated team-14 shot.

This is a materially different defect than the 398/399 misclocked block (same events, one wrong timestamp field, fixable with a lookup-table correction — see `docs/game_398_399_misclocked_clock_fix_plan_2026-09-14.md`, **done and merged, `64c84ca`**). Here the primary key (`actions_clean (id, game_id)`) and every downstream FK hang off `id`, and `stints`/lineup reconstruction is ordered *by* `id` (`docs/canonical_clock_minutes.md`). A naive "keep one copy" collapse would silently **drop one of two real events** for 167 ids, corrupt `parent_action_id` linkages for children pointing at whichever copy lost, and scramble lineup/stint boundaries across most of Q2-Q3 — while *passing* Phase 6's minutes/coverage checks, since the result would look complete (4 quarters, ~40 min). That's strictly worse than the current, honestly-partial state, because nothing in today's DQ report would catch it.

**Why the rendered webpage (`basket.co.il/livestats/game.html?game_id=400`) looks complete and correct despite this:** its `boxscore` object and `gameInfo.*.Score` fields are empty/zero in this feed — the page has no independent score summary. The only score data is a `score: "N-N"` string embedded on ~97 individual scoring actions; the last one in array order (`id 4000657`, Q4 free throw, `"83-85"`) is presumably what gets displayed. The page is a linear, human-readable list — it iterates the actions array and prints each row's description/score/quarter grouping in order. It never looks anything up *by* `id`, so two colliding ids just render as two separate rows in their own place; nothing on screen depends on `id` being unique, ordered, or referentially intact. Our pipeline needs all three (primary key, canonical stint ordering, `parent_action_id` join target) — that's the entire gap between "the page looks fine" and "the data is fine."

**Conclusion: game 400 recovery via basket.co.il needs a real id-reconciliation** (split the two interleaved streams, assign the losing stream fresh synthetic ids in true chronological order, verify against an independent anchor) — comparable effort to the 398/399 investigation, but with a harder problem and, so far, no independent anchor found to verify a reconstruction against (398/399 had partial box-score corroboration; game 400's box in both sources is currently unpopulated).

## Game 398/399 audit: confirms the earlier fix, no new issues

Audited both `basket.co.il/livestats/data/game-{398,399}.json` against the live segevstats source, full row-by-row diff:

- **Game 398:** 930 actions in both sources, **zero content differences** anywhere (not just in the known block) — every `id`/`quarter`/`quarterTime`/`type`/`teamId`/`playerId` matches exactly, including the already-fixed dead-ball block (raw feed still shows the original wrong `00:28`→`00:00` stamps in both sources — the correction lives in `apply_known_clock_corrections()`, not the source).
- **Game 399:** 836 vs 835 actions; the one gap is segevstats carrying an extra `id 3990001` (`type: "game"`, `start-of-game` marker) that basket.co.il omits — cosmetic. All 835 shared ids match exactly, including the documented block (`3990420`-`3990435`, `00:23`→`00:00` in both sources).

**Conclusion:** for a *completed, settled* game, both providers carry identical data, including identical defects — the 398/399 misclocked block is a genuine scorekeeping-entry error present at the source, not a relay/corruption artifact on either distribution channel. Ids also agree 1:1 in both games, unlike game 400 (where segevstats' id 4000200 and basket.co.il's id 4000200 are different events entirely, because segevstats is uniquely stuck mid-feed for this one game). This means: basket.co.il is a legitimate fallback candidate for *mid-feed staleness* (game 400's failure mode) but would not have caught or avoided the 398/399 clock defect on its own — that fix is needed regardless of source.

## ETL Phase 6 quarantine design (sketched, not implemented)

**Goal:** when Phase 6 catches a bad game, don't just log it — clean up what Phases 2-5 already committed, in the same run, before the app can ever serve it.

### Phase placement

```
Phase 2: Base Table ETL        (unchanged — commits per game)
Phase 3: Sub-Lineup Generation (unchanged)
Phase 4: MV Refresh            (unchanged)
Phase 5: Sub-Lineup Stats      (unchanged)
Phase 6: Validation            (unchanged — still just detects + logs)
Phase 6b: Quarantine  <- NEW   (purges what Phase 6 flagged, re-refreshes)
Phase 7: Cold storage purge    (unchanged, already skips on any failure)
```

Kept as a bolt-on after Phase 6 rather than restructuring Phase 4/5 to validate before each per-game commit: the more informative Phase 6 checks (minute-sanity, stint coverage) need `df_pts_poss_lineups_longer_mv` (built in Phase 4) and `sub_lineups_stats` (built in Phase 5) to exist first, so a true pre-commit gate could only catch the crudest issues (zero-row checks). Running quarantine immediately after Phase 6, inside the same `etl_full()` invocation, shrinks the exposure window from "indefinite / until a human notices" to "never actually served," without reordering the phases that already work.

### `purge_game_from_db(pg, schema, game_id, reason)`

One reusable function (usable both by Phase 6b and by an operator running a manual recovery), built from the FK graph mapped while investigating game 400's recovery:

1. Delete children of `actions_clean` first (FK order): `lineups_lookup`, `possessions`, `pws`, `stints`, `subs`.
2. Delete `actions_clean`, `full_rosters`, `shot_zones`, any `player_id_game_overrides` rows for the game.
3. Delete the game's rows from the incrementally-refreshed tables: `df_pts_poss_lineups_longer_mv`, `player_four_factors_by_game`, `team_metrics_by_game_mv`, `player_traditional_by_game`, `default_clutch_player_totals_by_game`, `player_stats_actions_by_game`.
4. Re-run `refresh_sub_lineups_stats_for_games(game_id)` so `sub_lineups`/`sub_lineups_stats` (season-level, lineup-keyed, not game-keyed — can't be row-deleted by `game_id`) lose the game's contribution correctly.
5. `REFRESH MATERIALIZED VIEW` on the game_id-scoped matviews (`mv_lineup_totals_by_day`, `lineup_four_factors_by_game`, `team_metrics_rolling_mv`, `final_schedule_mv`) plus the season aggregates downstream of step 3 (`team_ppp_ratings_mv`, `team_four_factors_mv`, `player_traditional_stats_mv`, `onoff_default_mv`, `player_advanced_stats_mv`) — the same calls Phase 4 already makes.
6. Confirm no `etl_processed_games` row exists (it already wouldn't today — this makes it a guarantee).

`schedule` is deliberately left alone — its score/teams are independently correct, not derived from `actions_clean`, so the app can keep showing "final: 87-85, detailed stats pending" instead of the game vanishing outright.

### `game_ingest_quarantine` tracking table (new)

```
game_ingest_quarantine (game_id PK, first_flagged_at, last_attempt_at,
                         attempt_count, last_reason, status)
```

Phase 2 already retries any game missing from `etl_processed_games` every night — correct, since a stalled source like segevstats might recover. Phase 6b upserts this table on every quarantine event; once `attempt_count` crosses 3 (matching this repo's existing "3 failed fixes -> stop and reconsider" convention), flip `status` to `'stuck'` and have Phase 2 skip fetching it, so a permanently-broken source doesn't burn ETL time/log noise forever. Surface `status='stuck'` games as a new line in `findings_summary.md`, next to the existing Critical/High table.

### Known gap: this does not catch a "complete but internally scrambled" game

Phase 6's checks (minutes-vs-expected, missing-quarter coverage, stint match rate) catch *obviously incomplete* games — every failure mode observed so far (400's stall; 398/399 before their fix). They would **not** catch a badly-reconciled basket.co.il reload of game 400 done today: a naive id-collision fix would show 4 full quarters and ~40 minutes, passing T and AC cleanly while being internally wrong. If a basket.co.il fallback path is ever built, it needs its own check in this phase reusing `V`/`W`'s score/possession-reconciliation SQL **without** the `etl_processed_games` join that currently exempts freshly-loaded games — that join is precisely what would need to be dropped to catch this class of defect.

## Resolution (2026-09-15, later the same day)

segevstats' feed recovered on its own — a fresh fetch returned 859 actions across all 4 quarters, ending cleanly at `id 4000884` (Q4 00:00), zero duplicate ids (unlike basket.co.il's still-unresolved 226-id collision documented above, which was never used). All 196 previously-committed `actions_clean` ids were confirmed still present, unchanged, in the new fetch — zero orphans — so a plain upsert-based reload was safe with no manual purge needed first.

**First reload attempt exposed a real bug in the reload tooling.** `gh workflow run etl-full.yml -f game_ids=400` (run 34948628227) reported `conclusion=failure` with the exact same stale numbers as before (`minutes 11.8 < 39.0`). Cause: `scripts/run_etl_full.ps1`'s default path runs `etl_full(dry_run=TRUE)` first and only proceeds to the real run if that dry-run reports success — but dry-run mode validates whatever's *already* in the DB rather than fetching anything fresh. Since game 400's DB state was still the old partial commit, the dry-run's own Phase 6 check failed against the stale data and the wrapper exited before ever reaching the real fetch. 398/399 didn't hit this because their pre-existing (misclocked but complete) data still passed Phase 6's minute/coverage checks even before that fix landed. **Any game whose current DB state is bad enough to fail validation can never pass the default dry-run-then-real gate** — a real gap in the recovery workflow, independent of the quarantine design above, worth fixing if this pattern recurs.

Re-triggered with the workflow's existing `skip_dry_run` input (`-f skip_dry_run=true`, run [34949160061](https://github.com/ariel12091/basketball_israel_analytics/actions/runs/34949160061)) — succeeded cleanly: Phase 2 committed 631 `actions_clean` rows (up from 196, no duplicate-id error), Phase 6 showed `lineup/stint match coverage zero=0/430 (0.000%)` (down from `8/134`), Phase 7 purged cold storage 5/5, `etl_processed_games` got its row, `app_meta.etl_full_last_success` advanced. Verified in `mv_lineup_totals_by_day`: team 14 = 77 poss / 87 pts / 40 min, team 17 = 76 poss / 85 pts / 40 min — points match the schedule's final score (87-85) exactly.

Cleanup: removed the CLAUDE.md § Backlogs § Data entry and fully reverted the Tab 4 ⚠️ warning-badge feature added during the investigation (`INCOMPLETE_RAW_DATA_GAMES` in `global.R`, `gl_incomplete_flag_renderer()` in `server_tab4.R`) — restored both files byte-for-byte from `HEAD` rather than hand-editing back, since content was already identical and a hand edit risked the mixed-CRLF/LF corruption trap. Tab 4 tests re-run clean (73 assertions).

The quarantine design above (Phase 6b, `purge_game_from_db`, retry-budget table) was **not implemented** — this recovery didn't need it, since the source healed on its own and a plain reload sufficed. It remains a good idea for a game that *doesn't* self-heal, or as defense-in-depth generally, but isn't blocking anything now.

## Related

- [`docs/game_398_399_misclocked_clock_fix_plan_2026-09-14.md`](game_398_399_misclocked_clock_fix_plan_2026-09-14.md) — the 398/399 fix this session's audit re-verified.
- [`docs/canonical_clock_minutes.md`](canonical_clock_minutes.md) — why `id` order, not clock value, drives stint reconstruction.
