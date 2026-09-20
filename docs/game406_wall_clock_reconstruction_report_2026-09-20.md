# Game 406 period and wall-clock reconstruction report, 2026-09-20

Status: **accepted and reprocessed for game 406 on 2026-09-20.** The Gameflow
modal carries a Q4-specific timing warning locally and awaits the next app
deployment.

Game 406 is Bnei Herzliya (team 6) 99, Maccabi Rishon LeZion (team 14) 79,
played 2026-09-17. The official period split is 28-12, 26-24, 19-18, 26-25.

## Source defect

The provider labels a complete Q2 action stream as Q3. Its Q4 field then holds
the complete Q3 followed by Q4. Q3's clocked stream ends at action `4060590`
with 00:02 remaining and the cumulative score 73-54. The real Q4 begins with
the reset at `4060592`.

Every real-Q4 statistical action is stamped 00:00 except one 00:01 rebound.
The raw provider payload has no hidden clock to recover: its `clock` events stop
at action `4060589`, before the real Q4. The provider box minutes inherit the
same failure and credit the opening home five with the entire Q4, so they are
not an independent clock source.

## Implemented offline correction

`etl/etl_onoff.R` now contains a guarded game-specific correction:

1. Move Q2 opening reset OUT actions `4060230`-`4060234` to 10:00.
2. Relabel provider Q3 actions `4060239`-`4060417` as Q2.
3. Relabel provider Q4 actions `4060419`-`4060590` as Q3.
4. Drop false/duplicate quarter markers `4060238` and `4060239`.
5. Keep `4060592+` in Q4 and map wall entry time linearly from:
   - `4060592`, 16:16:09 -> 10:00;
   - `4060747`, 16:41:32 -> 00:00.
6. Hold the complete Q4 opening reset (`4060592`-`4060611`) at 10:00 and
   collapse every uninterrupted substitution block onto one synthetic clock.

The interpolation is intentionally approximate. It preserves order but cannot
distinguish live-clock time from dead-ball wall time. A provider repair is
guarded: if any target action later contains a clock above 00:01, the wall-time
correction refuses to overwrite it.

## Period reconciliation

| Quarter | Actions | ID range | Clock range | Team 6 | Team 14 | Official |
|---:|---:|---|---|---:|---:|---|
| Q1 | 141 | 4060001-4060227 | 10:00-00:00 | 28 | 12 | 28-12 |
| Q2 | 138 | 4060228-4060417 | 10:00-00:00 | 26 | 24 | 26-24 |
| Q3 | 124 | 4060419-4060590 | 10:00-00:02 | 19 | 18 | 19-18 |
| Q4 | 156 | 4060592-4060749 | 10:00-00:00 | 26 | 25 | 26-25 |

Q1, Q2 and reconstructed Q4 are monotone in action order. Q3 retains four
one-second local reversals already present in the provider's otherwise valid
clocked stream; the canonical-clock layer handles that existing class.

## Reconstructed Q4 scoring timeline

| Action | Approx. clock | Score |
|---:|---:|---:|
| 4060617 | 09:11 | 75-54 |
| 4060623 | 08:31 | 75-56 |
| 4060629 | 08:07 | 78-56 |
| 4060633 | 07:58 | 78-57 |
| 4060636 | 07:44 | 78-58 |
| 4060641 | 07:23 | 78-61 |
| 4060651 | 07:00 | 78-62 |
| 4060656 | 06:43 | 78-63 |
| 4060659 | 06:23 | 80-63 |
| 4060665 | 06:07 | 82-63 |
| 4060672 | 05:36 | 84-63 |
| 4060681 | 05:13 | 85-63 |
| 4060682 | 05:07 | 85-65 |
| 4060685 | 04:58 | 85-67 |
| 4060689 | 04:13 | 87-67 |
| 4060690 | 04:08 | 87-69 |
| 4060701 | 03:10 | 88-69 |
| 4060702 | 03:00 | 89-69 |
| 4060706 | 02:41 | 89-71 |
| 4060714 | 02:03 | 90-71 |
| 4060717 | 01:54 | 91-71 |
| 4060722 | 01:35 | 93-71 |
| 4060724 | 01:31 | 93-74 |
| 4060730 | 00:52 | 93-76 |
| 4060734 | 00:38 | 96-76 |
| 4060738 | 00:27 | 99-76 |
| 4060744 | 00:13 | 99-79 |

This is the primary sequence to compare with video or another clocked source.
The exact seconds can be replaced later without changing the period split.

## Wall-clock baseline from a correct game

Game 110 provides a particularly close control. Its Q4 has a complete,
monotone provider clock from 10:00 to 00:00, and its wall-time span is exactly
25:23 -- the same 1,523 seconds as reconstructed game 406. Treating game 110's
real game clock as hidden and applying the same whole-quarter linear wall-time
interpolation gives the following sample:

| Action | Recorded clock | Wall estimate | Score | Error (estimate - recorded) |
|---:|---:|---:|---:|---:|
| 1100604 | 09:47 | 09:00 | 54-61 | -47 s |
| 1100623 | 08:42 | 08:05 | 54-63 | -37 s |
| 1100639 | 07:08 | 07:24 | 54-65 | +16 s |
| 1100653 | 06:48 | 06:34 | 54-67 | -14 s |
| 1100658 | 06:16 | 06:21 | 54-69 | +5 s |
| 1100663 | 05:59 | 06:12 | 55-69 | +13 s |
| 1100668 | 05:54 | 05:57 | 56-69 | +3 s |
| 1100672 | 05:38 | 05:43 | 56-71 | +5 s |
| 1100678 | 05:16 | 04:55 | 59-71 | -21 s |
| 1100687 | 04:24 | 04:33 | 59-74 | +9 s |
| 1100692 | 04:07 | 04:26 | 59-76 | +19 s |
| 1100694 | 04:02 | 04:23 | 61-76 | +21 s |
| 1100705 | 03:41 | 04:02 | 61-78 | +21 s |
| 1100709 | 03:27 | 03:54 | 62-78 | +27 s |
| 1100717 | 03:04 | 03:32 | 62-80 | +28 s |
| 1100720 | 02:44 | 03:24 | 65-80 | +40 s |
| 1100730 | 02:02 | 02:53 | 67-80 | +51 s |
| 1100774 | 00:34 | 00:34 | 68-80 | 0 s |
| 1100779 | 00:34 | 00:25 | 69-80 | -9 s |
| 1100785 | 00:05 | 00:06 | 71-80 | +1 s |

Across all 20 made scoring actions in that quarter, the median absolute error
is 17.5 seconds and the mean is 19.4 seconds. Eleven of 20 are within 20
seconds, 16 of 20 are within 30 seconds, the 90th percentile is 40.7 seconds,
and the maximum is 51 seconds.

This supports treating a 10-20 second discrepancy in game 406 as normal for
this approximation, not as evidence that the period relabelling is wrong. It
also shows that the synthetic seconds are not precise: dead balls and timeouts
make the error drift within a quarter even when the total wall duration is an
exact match. For lineup exposure, clocks near a substitution should therefore
be read as approximate, with occasional errors around 30-50 seconds possible.

### 2026-27 control excerpt: game 404 Q4

Game 404 is a clean 2026-27 example with an intact Q4 clock. Using its quarter
markers as the wall-time anchors (12:55:28 -> 10:00 and 13:23:09 -> 00:00), the
opening scoring sequence compares as follows:

| Action | Recorded clock | Wall estimate | Score |
|---:|---:|---:|---:|
| 4040629 | 09:45 | 09:30 | 54-62 |
| 4040637 | 09:29 | 09:11 | 56-62 |
| 4040662 | 08:25 | 08:00 | 56-64 |
| 4040672 | 07:51 | 07:36 | 56-66 |
| 4040675 | 07:51 | 07:33 | 56-67 |
| 4040683 | 07:20 | 07:10 | 56-68 |

The absolute errors in this excerpt are 15, 18, 25, 15, 18 and 10 seconds.

## Player-minute sanity check

The reconstructed action states yield:

- team 6: **11,999 seconds** versus the 12,000 regulation target;
- team 14: **11,741 seconds**, 259 short of the target.

The team-14 deficit is not caused by Q4 interpolation. Its Q2 reset removes
five players at 10:00, while the five incoming declarations arrive from 09:10
through 09:05. Roughly 50 seconds times five players accounts for the deficit.

The wall-clock correction also gives minutes to three team-6 players whom the
broken provider box lists at 00:00 even though the action log substitutes them
into Q4:

| Player ID | Estimated minutes | Evidence |
|---:|---:|---|
| 2329 | 01:23 | enters at reconstructed 01:23 |
| 2491 | 01:46 | enters at reconstructed 01:46 and scores at 01:35/00:27 |
| 1380 | 00:48 | enters at reconstructed 00:48 |

That direction is consistent with the play-by-play and demonstrates why the
provider box minutes cannot validate the frozen Q4 clock.

## Reproduction and tests

Read-only report:

```powershell
& 'C:/Program Files/R/R-4.4.2/bin/Rscript.exe' scripts/report_game406_wall_clock_fix.R
```

Focused tests:

```powershell
& 'C:/Program Files/R/R-4.4.2/bin/Rscript.exe' -e "testthat::test_file('app/tests/testthat/test-game406-period-correction.R', reporter='summary')"
```

The focused suite passes. Only the repository's existing locale warnings are
emitted.

## Database application and verification

The scoped dry run and write both completed with exit code 0. The write staged
559 actions, 170 substitutions, 774 lineup rows and 108 stints, then refreshed
the derived app relations and republished the cumulative Parquet archive.

Because the cumulative archive merge is key-upsert based, it initially retained
the two deliberately deleted old marker keys `4060238` and `4060239`. They were
pruned from `actions_clean`, `possessions` and `pws` after validating the exact
IDs and action type. The six removed archive rows are recoverable from
`exports/cold/backup-2026-09-20-game406-stale-markers/`.

The ETL's game-level validation passed. It reported zero unmatched or multiply
matched lineup/stint gameplay rows across 380 checked rows. Direct reads of
the refreshed Game Flow query for both team perspectives confirmed four
periods, zero excluded gameplay segments, and populated lane and margin JSON.

One pre-existing 50-second Q2 reset straddle remains in the underlying health
signal. It is the team-14 opening reset gap documented above, not the Q4
reconstruction. For game 406, the Gameflow alert replaces that technical
message with the more relevant Q4 timing disclosure. The two 736-second false
straddles caused by the old period labels are gone.

The global data-quality report remains `FAIL` for unrelated league-wide
residue and flags game 406's reset-only invalid states, but reports zero
unmatched gameplay rows for game 406. Its team-14 minute-conservation residue
is 0.15 minutes (9 seconds).

Verified outcomes:

- final and quarter scores remain 99-79 and 28-12 / 26-24 / 19-18 / 26-25;
- Q1-Q4 each appear exactly once in actions and derived periods;
- team 6 minutes remain approximately 40:00 and the known team-14 Q2 gap is
  left unchanged rather than silently filled;
- the ribbon no longer reports the two 736-second false straddles produced by
  the mislabelled periods;
- app period cards and Q4 scoring order match the table above.
