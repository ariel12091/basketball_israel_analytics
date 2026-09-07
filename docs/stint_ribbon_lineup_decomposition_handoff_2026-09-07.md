# Stint ribbon -- lineup decomposition: handoff

Date: 2026-09-07
Branch: `shiny/stint-ribbon` (unmerged, undeployed)
Plan: `docs/superpowers/plans/2026-09-07-stint-ribbon-lineup-decomposition.md`
Spec (binding authority): `docs/superpowers/specs/2026-09-07-stint-ribbon-lineup-decomposition-design.md`
Execution ledger: `.superpowers/sdd/2026-09-07-stint-ribbon-lineup-decomposition/progress.md`

> **Read the ledger, not this file, before writing code.** It carries per-task
> evidence, all eight Rulings and the review findings in full. This document is
> the orientation layer; the ledger is the record. The ledger is **git-ignored**,
> so it exists only on this machine -- if it is lost, the Rulings summarised
> below are all that survives.

## What the feature is

The stint ribbon draws one lane per player with a bar per stint. Today a bar
carries `with <player>` hover prose. This plan replaces that: **click a stint
and the bar splits in place** into the five-man lineups that composed it, with
a strip below listing them.

It rests on a measured fact from the spec: **86.3% of stint bars span more than
one five** (median 4). A bar's +/- is therefore a sum attributable to no single
unit, and the prose form could never say which five did what.

Lineup lanes were explored and **rejected** -- a lineup lane cannot show
occupancy. See spec sections 6 and 3.3 for the measurements, so nobody re-takes
them.

## Branch state

```
63169f4  plan committed
3175a1a  Task 1 part 1 -- scoreless-game gate helper + tests
693b272  Task 1 part 2 -- cached reference lookup wiring        <- Task 1 COMPLETE
07ee695  Task 2 part 1 -- extract the shared modal observer
9ad5e82  Task 2 part 2 -- fix a vacuous assertion               <- Task 2 COMPLETE
afe5613  Task 3        -- rate-limit the ribbon open            <- Task 3 COMPLETE
f70e46a  "manifesting" -- THE USER'S OWN COMMIT (app/manifest.json), not part of this plan
be8f28e  Tasks 4-7 draft (was labelled "unverified -- do not merge")
31a58a4  Tasks 4-7 verification + the three gaps it left        <- Tasks 4-7 COMPLETE
```

`be8f28e`'s label is now stale: it has been verified and closed out by
`31a58a4`. Its implementation was correct; every gap was in verification, which
is why `31a58a4` is test-only and changes no production code.

**The working tree also carries uncommitted, unrelated work** -- `app/R/global.R`,
`app/app.R`, `app/www/app.js`, `app/manifest.json`,
`app/tests/testthat/test-idle-restore-bookmarking.R`. That is the Posit Connect
Cloud cold-start change, not this plan. Do not sweep it into a ribbon commit.

## Status: Tasks 1-7 done, 8-11 not started

| # | Task | State |
|---|---|---|
| 1 | Gate the ribbon link on games that have score data | COMPLETE, reviewed clean |
| 2 | Extract the shared ribbon modal observer | COMPLETE, reviewed clean |
| 3 | Rate-limit the ribbon open | COMPLETE, reviewed, mutation-proved |
| 4 | Carry a lineup key on every lane row | COMPLETE, DB-verified |
| 5 | Build the per-game lineup dictionary | COMPLETE, mutation-proved |
| 6 | Decompose a merged bar into its lineup segments | COMPLETE, DB-verified |
| 7 | Each side's numbers in its own perspective | COMPLETE, mutation-proved |
| 8 | Emit segment data per bar; delete the overlap machinery | NOT STARTED |
| 9 | Click a stint to split the bar and list its fives | NOT STARTED |
| 10 | Hover a strip row to mark the five on the lane and the curve | NOT STARTED |
| 11 | Whole-feature verification | NOT STARTED |

### Verification evidence as of 31a58a4

- Offline full suite: **2276 pass / 1 fail / 13 skip**.
- The 1 failure is `test-companion-query-counts.R:293` (`object 'minutes' not
  found`), **pre-existing at the merge base** `c2ec291`. Any *second* failure is
  ours. Error text was matched, not assumed.
- Live database, `RUN_DB_TESTS=1` on `test-stint-ribbon-readers.R`: **0 skips,
  0 failures** across 3 Israeli and 3 EuroLeague games.

Reproduce:

```bash
RSCRIPT="/c/Program Files/R/R-4.4.2/bin/Rscript.exe"
cd app
"$RSCRIPT" -e 'library(testthat); test_dir("tests/testthat", reporter="summary")'
RUN_DB_TESTS=1 "$RSCRIPT" -e 'library(testthat); test_file("tests/testthat/test-stint-ribbon-readers.R", reporter="summary")'
```

`.Renviron` is read automatically because the run starts in `app/`. The suite
halts on the pre-existing failure before printing totals, so counts come from
the summary reporter's per-file dot/`S` markers.

## What Task 8 must honour

Task 8 is the builder rewrite and is where this plan can most easily go
silently wrong. Four constraints, all established rather than guessed:

1. **`build_stint_ribbon_svg()` destroys what Task 8 needs.** At
   `app/R/helpers.R:3343` it does `lanes <- merge_adjacent_stints(lanes)`,
   overwriting the frame. `ribbon_stint_segments()` consumes the **pre-merge**
   rows, so the builder must retain them explicitly.
2. **A merged bar carries only its FIRST segment's `lineup_key`**, because
   `merge_adjacent_stints()` does `row <- lanes[i[1], ]`. That value is stale
   for 86.3% of bars. Nothing may read `bar$lineup_key` as "the bar's lineup".
3. **Ruling 4 -- one dictionary object, one order.** Compute the dictionary
   ONCE into a single local, derive `dict_key` from that same object, and
   serialise `dict$members` from it. `match()` returns an index into whatever
   vector it was given; two separate `ribbon_lineup_dictionary()` calls with
   different row orders would make every bar point at the wrong five, and it
   would look entirely plausible on screen. This is a silent-wrong-answer
   class, not a crash.
4. **Ruling 5 -- Tasks 9 and 10 are prose, not code.** Unlike Tasks 1-8 the
   plan gives them structure, constraints and tests but no implementation.
   Dispatch them on a more capable model; they are design work, not
   transcription.

## Rulings that bind future work

Eight were made during execution. These are the ones a newcomer will otherwise
re-litigate:

- **Ruling 6 -- the scoreless-game set is a cached reference lookup.** The
  plan's per-row `EXISTS` measured **13.5x** buffer pages (156 -> 2100). The
  scoreless set is **8 rows for the entire database** (games 139/140/141/143 x
  2 teams) and changes only on ETL, so it goes through `cached_ref_query()`
  under a **single key with no season dimension** -- a deliberate, commented
  deviation from CLAUDE.md's per-season convention, because the underlying
  query is a sequential scan whose cost does not fall when filtered by season.
  The code says so at the call site. **Do not "fix" it into a per-season key.**
- **Ruling 7 -- EuroLeague uses TWO prefix literals.** `eurogl` for Shiny input
  ids, `eugl` for SVG clip ids. Israel uses `gl` for both. The plan's
  single-prefix parameterisation would have silently killed the EuroLeague
  ribbon click -- no error, no log, just a dead link. Hence
  `ribbon_modal_server(..., prefix, league, data_version_fn, svg_id_prefix = prefix)`.
  Israel passes no fourth argument.
  *Why the equivalence check missed it:* the normalising `sed` collapsed
  exactly the distinction under test. **A normalising diff cannot see a
  difference its own normalisation erases.**
- **Ruling 8 -- batching.** Tasks 3-7 were batched into one dispatch because
  their complete code was already in the plan. That was the right call for
  transcription work and is why the review surface was larger.

## Traps re-confirmed on this branch

- **`Edit` corrupts `app/R/helpers.R` line endings** (CR 2670 -> 3833 on first
  use). Use byte-level Python `str.replace()`. `sed -i` destroys them too.
  Every ribbon test file is fully CRLF (`lines == CR`).
- A CR count may legitimately **grow** by exactly the number of added lines in
  a CRLF region. Distinguish corruption from growth with `git diff --numstat`
  plus `git diff --ignore-cr-at-eol --stat` -- if the two diffstats are
  identical, no existing line's ending churned.
- **Run the whole suite, not the ribbon files.** Task 1 was caught by a missing
  `fetch_scoreless_games` stub in `helper-server-mocks.R` that no ribbon-only
  run could see.
- **Long `Rscript -e` segfaults.** Write a temp `.R` file. Hit again this
  session (exit 139).
- Three subagents died on infrastructure during the first execution session
  (expired login, a 600s stall, a rate limit). Re-dispatch fresh; **never
  credit a partial or absent verdict.**

## Lessons from the Tasks 3-7 review

Two are worth carrying beyond this feature.

**A unit test that calls a helper directly cannot prove the caller calls it.**
Deleting `lanes <- ribbon_side_perspective(lanes)` from
`build_stint_ribbon_svg()` broke **no offline test**. Both unit tests invoke
the helper themselves, and the live-DB reconciliation that would have caught it
has no CI runner in this repo. The fix tests the **wiring**: it reads the
rendered bars' `data-pm` / `data-pf` / `data-pa` attributes. The same deletion
now fails three assertions. Brief 7 had asserted this mutation outcome without
measuring it -- a plan defect, not an implementer one.

**A mutation harness must be self-tested in both directions before it is
believed.** The first harness this session grepped for failure lines with a
pattern anchored on digits; testthat's summary reporter prints its failure
headers with leading box-drawing characters, so the pattern matched nothing and
every mutation looked clean. One run reported a false "no failures". Prove the
harness returns 0 on a clean tree **and** 1 on a known-detectable mutation
before trusting any verdict from it.

Also: the plan's briefs named DB test helpers that **do not exist** --
`ribbon_test_con()`, `ribbon_sample_games(con, lg, n)`. The real idiom is
`ribbon_db_con()` plus `sprintf(RIBBON_LEAGUES[[league]]$games, n)`. Copy a
reference's actual call convention; never invent API into a plan.

## Non-vacuity guards that must not be "simplified" away

Each exists because its absence shipped a stub-passing test before.

- `test-stint-ribbon.R` "the ribbon modal observer is rate limited" needs
  **both** its assertions. If the guard were deleted outright, `regexpr()`
  returns `-1` and `expect_lt(-1, 15)` **passes** -- only the
  `expect_true(grepl(...))` covers deletion. Neither assertion is redundant.
- "segments reconcile with their bar on live data" carries
  `expect_gt(checked, 100L)` **and** `expect_gt(scored, 50L)`. A scoreless game
  yields `NA` on both sides, and `expect_equal(NA, NA)` passes without testing
  any arithmetic, so the first guard alone would not catch a sample drawn
  entirely from scoreless games.
- "a lineup key identifies exactly five players" carries
  `expect_gt(checked, 50L)` so an empty game list cannot pass having verified
  nothing.
- "both readers select a lineup key into the lanes CTE" looks fragile but is
  not. Verified this session: `expect_match(character(0), ...)` **fails**
  ("character(0) is empty"), and the two regexes are correctly scoped -- the
  Israeli block is 2411 chars, the EuroLeague 1393, neither bleeds into the
  other, and each contains only its own league's key expression. It
  discriminates between leagues rather than finding both strings anywhere in
  `global.R`. Leave it alone.

## Still owed -- do not silently mark these done

1. **The browser pass.** Nothing has yet rendered this chart and looked at it.
   Two plans' worth of visual steps are deferred. Launch with
   `IBPL_CACHE_UI=false` when touching `www/app.css` or `www/app.js`, and use
   **Run App / `runApp()`** -- never select-all + Ctrl+Enter, which caches a
   broken BS3-style navbar for the life of the process.
2. **An open decision for the user:** whether to merge Tasks 1-3 to `main` on
   their own, ahead of the redesign. Task 1 fixes a **wrong answer showing on
   live data** -- games 139/140/141/143 draw a fabricated flat "tied" curve --
   and has no dependency on Tasks 4-11. Asked once, never answered.
3. **The branch is unmerged and undeployed.** EuroLeague migration 054 **is**
   applied live.

## Deferred minors routed to final review

- `app.R:246-249`'s comment claims the prewarm means the scoreless scan "never
  lands on a user request". Overstated -- `REF_CACHE_TTL_SEC` is 300s, so after
  a 5-minute lapse the 2.56s scan **can** land on a request. Pre-existing
  behaviour shared with all four canonical lookups; the wording is what needs
  fixing.
- `fetch_scoreless_games()`'s `db_get_query` is not wrapped in `tryCatch`, so a
  transient DB error on a cold cache takes down the `gl_table` render. It adds
  an I/O failure point to `add_ribbon_link_column()`, which was pure before.
- `test-stint-ribbon.R`'s `expect_false(grepl("showModal(", src))` guard fires
  on **any** future `showModal(` added to either tab file, not only a ribbon
  re-clone. Consider narrowing to a ribbon-specific anchor.
- The EuroLeague `NULL` comment in Task 1 is true for the Israeli failure mode
  but does not cover a game with **zero rows** in the underlying table, which
  would pass a NULL-count test with no contribution. Hypothetical today.
