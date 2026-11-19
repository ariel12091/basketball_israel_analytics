# Handoff 2026-11-17: EuroLeague blocked games, defect register, free-throw redesign

## Continuation (session 3, 2026-11-17): replacement shooters have feed evidence

`docs/handoff_scripts_2026-11-17/replacement_evidence.py` over the review
contexts: **all 53 `replacement_condition_unverified` rows show, inside the
same stoppage, the fouled player's OUT row and the shooter's IN row** (same
team). 50 are a substitute shooting the whole trip; 3 (E2026_31 row 369,
U2025_79 row 55, U2026_109 row 134) are the fouled player shooting the first
attempt, then going OUT, and the incoming player shooting the rest. This is
FIBA's substitute-shoots case (injured or disqualified player). Proposed rule
for the state machine, pending the user's OK: accept a replacement shooter
when the fouled player goes OUT and the shooter comes IN within the stoppage,
before the shooter's first attempt; otherwise keep requiring evidence.
Still open after that: 27 competing personal fouls, 23 shooter/award/boundary.

Also this session: web PR #80 (`react/oct1-default`) -- Israeli pages default
to 1 October; the Lineups SQL full-season gate fix is already deployed.

## Continuation: coverage-loss incident register and corrections

The original 163 baseline-confirmed FT coverage losses are frozen in
`docs/euroleague_ft_review_seed_2026-11-17.json` and tracked in the
[113-incident review register](euroleague_ft_review_register_2026-11-17.md).
`euroleague/scripts/review_ft_stoppages.py` reproduces the register, keeps
source windows and competing FT allocations in ignored diagnostics, and
checks for new losses outside the fixed cohort after each full replay.

Final review replay: all 786 cached games / 32,731 FTs complete without errors.
**60 original rows now have unique provisional candidates, with no new coverage
losses elsewhere.** Remaining: 53 replacement-condition checks, 27 competing
personal-foul assignments, and 23 other shooter/award/boundary cases. Across
all FT rows: 32,357 unique, 256 ambiguous, 83 replacement candidates, 35 no-fit.
The register records exact identities and evidence requirements for the 103
original rows still withheld.

The experimental model now retains matching unpaired RV evidence across clock
differences, transfers a recent basket and its RV/administrative tail when an
older pending foul closes, and jointly enumerates one-to-one personal-foul/RV
pairings with FT allocations. Ambiguity includes two actual alternatives.
Shooter substitutions before the first attempt are flagged along with those
between attempts; none are automatically authorized as replacements.

Twenty focused tests pass, including new labelled E2025/104, 116, 171 and 194
excerpts. The three required R scripts pass. The broader 324-test Python run
still has the same two unrelated SQL-test issues. Latest counts and source
hashes are in the register and shadow replay report. Production, the canonical
parser and unrelated web work remain unchanged.

## Continuation: experimental state machine implemented

See [the implementation and replay report](euroleague_ft_stoppage_shadow_2026-11-17.md).
`infer_ft_stoppages()` in `euroleague/src/euroleague_possessions/ft_stoppages.py`
now constructs candidate incidents from event sequence and solves individual
FT assignments to explicit entitlements. It is **not connected to the loader**;
all unique results remain provisional. The canonical parser and production
data are unchanged.

The companion `euroleague/scripts/compare_ft_stoppages.py` replays both
competitions with hash-validated checkpoints. Fourteen focused tests pass,
including all existing labelled FT partitions and new labelled delayed-and-one
and interrupted-trip excerpts. The three required R regressions pass. The
broader Python suite has two independently reproduced, unrelated SQL-test
failures (details in the report).

The experiment gives candidate trips to 11 of the original 18 unresolved FTs.
Three require evidence for replacement shooters; four remain unexplained by
the conservative incident boundaries. Next work is review of baseline coverage
losses and complete possession-end integration, rather than production reload.

## Continuation: measurements completed 2026-11-17

Read [the measurement report](euroleague_ft_stoppage_measurements_2026-11-17.md)
before implementing the proposal below. Offline replay covered all 786 local
per-game caches (both competitions), 32,731 FTs. The combined CSVs add no games;
this is not the 819-game database population above.

- 17,254/17,757 FT-bearing clock buckets (97.2%) have a unique entitlement-count
  allocation under the experiment's assumptions. These contain 2,104/2,293
  provisional FTs and 12/18 unresolved FTs. **This does not confirm individual
  trips or possession endpoints.**
- Clock alone is insufficient: clocks shift within a trip and can span several
  live-ball plays. The labelled fixtures demonstrate both clock shifts and
  offsetting penalties. Build incidents from sequence and event evidence.
- All 15 unresolved-FT games were inspected. Three have a shooter substitution
  between attempts; investigate legitimate replacement-shooter exceptions
  before treating them as attribution defects.
- Three candidate trip partitions differ from baseline: E2025/188, E2025/355,
  U2025/2. All resume a personal trip after another player's technical FT.
- All PLAYINFO FT counters parse; 33/32,731 fail the incremental check. The
  Tonut/Mannion case passes, so counters cannot independently validate shooter
  attribution.
- Reproducible scripts: `euroleague_ft_measure.py`, `euroleague_ft_cases.py`,
  `euroleague_ft_report.py` in `docs/handoff_scripts_2026-11-17/`; five new
  experiment tests and 19 existing parser tests passed. Detailed ignored
  artifacts are in `euroleague/data/diagnostics/ft_stoppage_measure/`.

No canonical parser, production data, or unrelated web edits changed. Next:
resolve incident boundaries, cancellation, and replacement-shooter semantics;
then implement the parallel parser and compare complete trips and endpoints.

## Earlier session state

Session started from `docs/web_data_audit_2026-11-17.md` (also uncommitted).
Everything below about the database was done as the ETL role on port 5432; the
user approved each write. Scripts used are in `docs/handoff_scripts_2026-11-17/`
(run with `PYTHONPATH=euroleague/src euroleague/.venv/Scripts/python.exe ...`;
the DB ones take `etl/.Renviron`).

## Done and merged

| What | Where |
|---|---|
| EuroCup 2026-27 games 10, 18, 100 corrected (lineups) and loaded (run 93) | PR #78, main `232eaa3` |
| Loader schema guard knew nothing of `sub_lineups_stats_mv` (migration 063); every publish since 2026-11-14 would have failed | PR #78 |
| Register of accepted defects (`euroleague/data_quality/accepted_defects.csv`, N1/N1b/N1c in the EuroLeague DQ report, `euroleague/scripts/dq_accepted_defects.R`, test `app/tests/testthat/test-dq-accepted-defects.R`) | PR #79, main `194ab69` |
| 11 foul-mismatch games reloaded (runs 94-97): E 2025 235, 265; E 2026 97-99; U 2025 192; U 2026 104, 106, 108, 111, 112 | DB only |

Notes:
- `KNOWN_SUBSTITUTION_CORRECTIONS` / new `KNOWN_SUBSTITUTION_MOVES` in
  `euroleague/src/euroleague_possessions/package_lineups.py` hold the per-game
  lineup fixes; evidence in `docs/shared_definitions.md` D37/D38.
- E 2026 97-99: the provider corrected the box scores (+6 technical/flagrant
  fouls) after our first fetch ~3 h after tip-off. Old copies are gone with the
  scratchpad; the fresh ones are in `euroleague/data/raw/boxscores/`.
- The user's checkout is on `react/openapi` with **their own uncommitted web
  edits** -- do not touch them. Its `euroleague/` code predates #78: a load run
  from it fails the schema guard. Run loads from a `main` copy
  (`git archive origin/main euroleague/src euroleague/scripts`, set
  `PYTHONPATH` to its `src`, pass `--data-dir` to the real `euroleague/data`;
  `load_games.py` reads `<copy>/../etl/.Renviron`, so copy it in and delete it
  after).

## State now (latest game_qa per game, 819 games)

117 clear, 685 review, 17 blocked. Blocked:
- U 2026 100 -- running score, **accepted** in the register.
- U 2025 28 -- running score only. Not looked at.
- 15 with 1-2 unresolved free throws (`free_throw.no_compatible_parent`):
  E 2025 91, 98, 136, 302, 303, 383; E 2026 31, 73; U 2025 2, 79, 86;
  U 2026 5, 6, 56, 109. Patterns read so far (8 of them):
  - foul logged BEFORE the and-one basket, often a coach's challenge (CCH) in
    between: E 2025 302, 91 (both FTs)
  - technical's FT out of order (team TO between, or bench `B` logged after
    the FT): E 2025 136, 383
  - FT credited to the wrong player (Tonut fouled, 2nd FT under Mannion): E 2025 98
  - unsportsmanlike + double foul at one stoppage: E 2025 303
  - not yet read: E 2026 31, 73; U 2025 2, 79, 86; U 2026 5, 6, 56, 109
- Review population: 591 games carry 2,280 provisional FTs (score-based
  matches), 219 have actors off the floor, 152 possession difference > 1,
  123 same-team transitions.

The EuroLeague DQ report also fails on B (duplicate player ids, Browne /
Majauskas), N3 (foul mismatches -- should now be gone after runs 94-97),
N5 (15 load runs not completed). Not investigated.

## In progress: free-throw grouping redesign (user asked "why rules instead of a state machine")

Diagnosis: `PeriodParser` (`parser.py`) runs ~10 sequential passes; each FT is
matched to a foul by a score (row distance, +500 same player, +600 special,
+10 same clock). It assumes feed order = event order; every reordering became a
rule. Proposed model (not started, user has not approved code yet):
stoppage = rows between live-ball events at one clock; each foul creates FT
entitlements; FTs consume them by team/shooter regardless of logged order; an
exact fit is confirmed, no fit is a real feed error. Output the Israeli trip
fields (parent, attempt number, trip size) so possession ends reuse
`etl/possession_rules.R` rules (shared rule 7; D29-D31 rulings carry over).
Build beside the current parser, compare trip by trip over all cached games,
reload only changed games under a new parser version.

Facts gathered:
- The Israeli feed gives `parent_action_id` + `parameters_free_throw_number` of
  `parameters_free_throws_awarded`; the Israeli ETL never infers trips.
- EuroLeague has no equivalent. `PLAYINFO` "Free Throw In (1/2 - 3 pt)" is the
  player's running FT made/attempted and points; fouls show "Foul (3)" = his
  foul count. Usable as a shooter-consistency check (each FT must advance the
  shooter's own counter), not as trip numbering.
- `etl/euroleague/group_events.R` is an unused R twin of the parser (2026-09-14)
  with an evaluator against hand-labelled trips (`expected_ft_trip_id`,
  `etl/tests/test_euroleague_event_grouping_fixtures.R`) -- a ready test set;
  remove the twin once the redesign lands.

Israeli measurement (`israeli_ft_measure.py`, 474 games, 10,445 trips). The
user doubts transfer (different provider), so split:
- **Rules of the game (should transfer):** personal foul 2 FTs (7,377) or 3
  (313); and-one 1 (2,006 of 2,008); technical 1 (382 of 383); unsportsmanlike
  2 (300 of 301), offensive unsportsmanlike 2, disqualifying 2. One shooter per
  trip 99.95%; shooter = fouled player 99.8%. Trips complete (rows = awarded)
  99.9%. Multi-trip stoppages are rare: 141 of ~10,750 (1.3%), mostly
  personal + technical (80), and-one + technical (13), two personals (16).
- **Provider habit (Israeli only, do not assume for EuroLeague):** parent always
  logged before its FTs; the and-one foul logged after the made shot (2,005 of
  2,008); FT rows of different trips in one stoppage contiguous 136 of 141.
  (In the script output the label "personal (coach/bench)" is a bug -- it is
  all personal fouls.)

## Next steps

1. EuroLeague side of the measurement (was about to start): per stoppage
   (period + clock), fouls by type and FTs by team; how often entitlements
   (technical 1, unsportsmanlike/CMD 2, and-one 1, personal = rest, 2 or 3) fit
   exactly one way; join to the current parser's `grouping_status` from
   `parser.group_events()` to see how many provisional/unresolved FTs the model
   would confirm. Also test the PLAYINFO shooter counter.
2. Write the design for the user to check the basketball rules, then implement
   beside the parser and diff over all cached games.
3. Read the 7 unread unresolved-FT games; decide fixes or register rows.
4. Box-score refresh: re-fetch the last week's box scores on each scheduled
   load so late provider corrections (like E 2026 97-99) are picked up.
5. Unrelated, from the audit (`docs/web_data_audit_2026-11-17.md`): data
   version not moving on partial runs (finding 1), Israeli sub-lineup
   atomicity (3), wrong-size lineups (4), lost points (5). Not started. The user
   suggested looking at the review games "maybe with jev"; I asked what jev is
   and got no answer yet.

## Session 4 (2026-11-17): the two approved rules are in the shadow model

The user approved both rules. `ft_stoppages.py` (still uncommitted, shadow only):

- **Replacement shooter** (`_replacements`, `_shooters_ok`): a team-mate may shoot
  for a fouled player whose OUT and the team-mate's IN are in the same incident.
  A mixed trip (fouled player first, substitute after) needs the OUT/IN between
  the two shooters' attempts. It is a **fallback**: the solver first runs without
  replacements and uses them only on `no_fit`. Without that, E2025_259 Q2 (the
  substitute drew his own foul in the same stoppage) turned from unique to
  ambiguous.
- **Competing fouls** (`_resolve_competing_fouls`): on `ambiguous`, enumerate up to
  32 allocations; resolve only if every allocation splits the FTs into the same
  trips and each trip's candidate fouls share kind, fouling team and fouled
  player, with no IN/OUT between the candidate parents (same credited lineup).
  The trip goes to the latest candidate foul logged before its first attempt.
- Trace markers: `replacement_shooter_after_substitution`,
  `competing_fouls_resolved_to_latest_before_trip`. Neither nulls the endpoint.

Result (all 287 cached games replayed, register rebuilt): **132 of 163** cohort rows
recovered (was 60), **0 new losses** outside the cohort. All 53 replacement rows
recovered (the register now labels 50 of them `fouled_shooter_mismatch`, recovered);
competing fouls 31/39. Still open, 31 rows: competing fouls 8 (substitution between
the fouls or non-equivalent candidates), true shooter mismatch 11, shooter award or
scope 5, single FT with no linked basket 3, made-basket boundary 2, parent outside
the window 2. Tests: 25 pass (`euroleague/.venv`; the system Python lacks
`euroleague_api`).

Next: the 31 open rows by hand, then possession-end integration and a reload
under a new parser version.

## Session 5 (2026-11-18): the 31 open rows read by hand

The user had no clear video for the three examples but accepted all three readings.
Box scores equal the PBP on every player's FT and foul-drawn count in these games
(the box is built from the same feed), so they cannot arbitrate. Proposed, NOT yet
approved or implemented (all fallbacks run only on `no_fit`/`ambiguous`, flagged):

| Rule | Rows | Cases |
|---|---:|---|
| A. And-one across a clock gap: 1 FT by a player who scored just before, only fouls/RV/AS/subs between | 2 | E2025_121 (duplicate foul at 08:21), E2026_89 (basket 06:42, foul 06:40) |
| B. Only one compatible foul, illegal award count (1 FT, no basket) -> attach, flag `award_count_unexplained` | 3 | E2025_221, E2025_8, U2025_9 |
| C. Shooter is a team-mate of the fouled player, no sub -> attach to the one compatible foul, flag `drawn_player_mismatch` | 12 | E2025_227, 291 (scorer = shooter, RV wrong), 324, 374, 98; U2025_117, U2025_3; U2026_65 |
| D1. A foul cannot pair with a player not yet on the floor | 2 | E2026_67 (Grant IN before 2nd foul) |
| D2. Later foul's player entered between the fouls -> separate incident, FTs to later foul | 2 | U2026_56 Q2 (Maretto) |
| D3. Candidates differ only in which team-mate fouled, no sub between | 2 | E2025_23 |
| D4. Same fouled player, sub + coach's challenge between -> latest? USER TO DECIDE | 2 | U2026_19 |
| E. Foul by the scoring team logged before its own basket, FTs to opponent after -> foul after basket | 1 | U2026_6 Q3 |
| F. And-one with no RV row | 1 | U2026_4 |
| Per-game correction (rows out of order + wrong clock on Bilan's 2nd FT) | 3 | U2026_56 Q4 |
| Leave open (challenge, then one FT for EACH team) | 1 | U2025_86 |

Evidence windows: run the dump in this section's session or read `contexts.json`.

**Decision (user, 2026-11-19):** do not write rules for the other cases; at 31 of
32,731 FTs (0.1%) the impact is negligible. Rule A turned out to be a model bug:
an earlier no-FT foul on the same player absorbed a later basket as its "and-one",
and the hand-over to the new foul required equal clocks. Fixed in `_incidents()`:
a basket whose clock differs from the old foul's is a later play and moves to the
new foul. Test `test_basket_after_clock_ran_belongs_to_the_new_foul`; 26 tests pass.
Full replay: 133/163 recovered, 0 new losses, and exactly one FT row changed in all
32,731 (E2026_89 row 194 -> parent 189, foul 190). The remaining 30 stay unresolved.
Next: possession-end integration, then reload under a new parser version.

**Same bug elsewhere (2026-11-19):** a full scan found 805 baskets filed as the
and-one of an earlier no-FT foul on the scorer after the clock had run (802 with no
FTs in the group, 3 with FTs, all 3 already assigned correctly). Root fix in
`_incidents()`: a made basket joins a pending foul as its and-one only if the clock
has not run since a foul drawn by the scorer (`clock_ran`). Rescan: 0 left. Replay vs
before both fixes: 29 of 32,731 FT rows changed -- E2026_89 row 194 now assigned, and
28 rows in 14 trips where a stale basket made the model withhold the possession end
now get one. All 29 endpoints equal the production parser's `final_end_poss`
(15 False, 14 True). 26 tests pass; register 133/163, 0 new losses.

**Unsportsmanlike and-ones (user, 2026-11-19: "Unsportsmanlike does not finish possession").**
A made basket with an unsportsmanlike/flagrant foul on the scorer is one possession that
continues after the free throw. 22 in the cached games; the live parser ends the possession
at the basket in 21 (21 extra possessions). The Israeli `compute_possessions()` already
gives no end in a shot group holding a special foul (read from code, not measured), so this
is a twin drift. The shadow model suppresses the basket end in all 22. Four other cases
(E2025_211, E2026_99, U2026_4, U2026_53) are a U-foul on a different player by the team that
had the ball, with its turnover: those are real ends. Corrected simulation (suppression
applied even where the FT endpoint is withheld): same-team repeats 173 -> 144, ends -30.
Left: 2 phantom turnovers on the fouling team in U-foul and-ones (U2026_5 Q3 08:14,
U2026_68 Q1 07:15), the D31 shape; EuroLeague has no D31 rule yet. Handle both in the
possession-end integration.

**Box-score possession check (2026-11-19).** Estimate FGA - OREB + TO + 0.44 FTA from
`totr` vs counted ends, 1,566 team-games: counted is 0.6-0.9 below in every competition,
SD 1.1-1.3 (expected: 0.44 FTA and period-end possessions). 13 team-games are >= 3 from
their competition's mean gap; in all 13 turnovers match the box exactly and the two teams'
counts are level (gap 0-1, except E2025_14 at 3), so the formula, not the count, is off.
The sharper check is the gap between the two teams in a game: 0: 284, 1: 347, 2: 137,
3: 12, 4: 1, 5: 2. Worst: U2026_5 (5), U2026_65 (5), U2026_6 (4). U2026_5: the provider
charges Trento a turnover on 3 of its own technical/coach fouls while Tenerife had the ball
(Q3 00:36, Q4 09:13, Q4 01:13), plus the flagrant and-one phantom at Q3 08:14; the box score
counts them too. That shape (phantom TO on the fouling team's special foul) exists only in
that game in the cached data; with the 2 U-and-one phantoms it is 5 rows for a D31-style
rule. U2026_6 and U2026_65 not read yet.
