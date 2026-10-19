# EuroLeague deterministic possession audit: 100 games

Date: 2026-08-05  
Warning classification revalidated: 2026-08-06  
Status: exploratory only; no database or application integration

## Outcome

The deterministic counter completed 100 distinct season-2025 EuroLeague games
without a structural failure. It produced the same 14,684 possession rows on
two independent runs.

The sample is a convenience sample, not a random one: sequential early-season
games were supplemented with distributed later gamecodes. The final download
contained 56,463 raw events and had no game-fetch failures.

| Diagnostic | Result |
|---|---:|
| Games | 100 |
| Raw events | 56,463 |
| Derived possessions | 14,684 |
| FT rows | 4,122 |
| FT rows assigned to a trip | 4,122 (100%) |
| Provisional FT rows | 235 (5.70%) |
| Unresolved FT rows | 0 |
| Provisional possession endpoints | 26 (0.18%) |
| Incidents with duplicate endpoints | 0 |
| Parent references missing their target | 0 |
| Games passing hard structural QA | 100 / 100 |
| Same-team endpoint transitions | 13 / 14,283 (0.091%) |
| Full counting runtime | approximately 7.7 seconds |

The game-level difference between the two teams' derived counts was zero in 32
games, one in 51, two in 16, and three in one. A difference above one is a QA
signal, not automatically an error: retained-ball penalties and period
boundaries legitimately interrupt strict alternation.

The conservative `game_qa` review rule marked 75 games for review and 25 clear.
That soft flag includes any provisional FT, same-team transition, or possession
difference above one. It is intentionally much broader than structural failure.

On 2026-08-06, the special-penalty warning rule was refined from an arbitrary
`+/-8` provider-row window to a live-play-bounded penalty cluster. Administrative
and annotation rows are transparent; shots, rebounds, turnovers, offensive
fouls, and period ends close the cluster. Compared with the prior warning set,
215 FT rows were retained, 52 were removed after a live boundary, and 20 were
added within the same uninterrupted penalty cluster. This reclassification did
not change any of the 14,684 possession endpoints or endpoint reasons.

## Effect of the broader audit

The first pass over these same 100 games had two unresolved FT rows, 26
same-team endpoint transitions, and 14,675 endpoints. General rules discovered
through case inspection changed the result to zero unresolved FTs, 13
same-team transitions, and 14,684 endpoints.

The relevant rule changes were:

- treat `CMU -> same-team TO -> opponent RV` as one unsportsmanlike incident;
- allow transparent administrative and rebound rows while matching a foul to
  its later FT trip;
- determine rebound control from rebound team versus shooting team instead of
  trusting a contradictory provider `O`/`D` label;
- combine a made basket with a compatible dead-ball FT trip shot by a different
  teammate, while keeping it distinct from an and-one;
- close an older special FT trip when a new committed foul appears, even if the
  same player shoots both trips;
- treat `TOUT_TV` as administrative rather than a live-ball boundary.
- scope compound-penalty warnings to an uninterrupted penalty cluster rather
  than a fixed number of nearby provider rows.

No rule contains a gamecode or fixture-specific exception.

## Remaining same-team transitions

Every remaining transition was inspected. They divide into three groups:

| Class | Count | Games / periods | Treatment |
|---|---:|---|---|
| Retained or compound possession | 5 | 5/P2, 60/P2, 38/P3, 70/P3, 75/P3 | Keep both endpoints; special cases remain provisional where entitlement is inferred. |
| Provider/control gap | 6 | 18/P1, 28/P1, 46/P1, 77/P2, 84/P3, 56/P4 | Preserve the deterministic result and flag QA; do not invent or merge possession changes without evidence. |
| Mixed special-penalty sequence | 2 | 73/P3, 73/P4 | Keep provisional and require manual/box-score reconciliation before publication. |

The provider/control-gap cases include consecutive duplicate-looking turnovers,
an opponent rebound followed by the original team taking the next shot without
a recorded change of control, and technical sequences whose next live-ball
owner conflicts with the inferred owner.

## Possession endpoint composition

| Endpoint rule | Possessions |
|---|---:|
| Made field goal | 5,592 |
| Miss followed by defensive rebound | 4,024 |
| Turnover | 2,539 |
| Ordinary made final FT | 1,385 |
| Missed final FT followed by defensive rebound | 358 |
| Blocked shot followed by defensive rebound | 308 |
| And-one final FT | 279 |
| Miss at period end | 177 |
| Offensive rebound at period end | 10 |
| Blocked miss at period end | 6 |
| Compound-penalty resolution | 3 |
| Made basket plus different-shooter dead-ball FTs | 3 |

## Regression evidence

The permanent labelled suite now contains 20 fixtures, 210 raw events, 73
incidents, 34 FT rows, and 42 possession endpoints. The implementation exactly
matched every labelled parent, FT-trip partition, endpoint, and endpoint reason.
All three R test scripts pass.

This 100% figure is a regression result on known manually labelled cases. The
100-game data has no provider-supplied parent or possession ground truth, so its
structural pass rate must not be presented as 100% semantic accuracy.

## Assessment

This is effective enough for a shadow EuroLeague schema and continued
reconciliation. It is not yet sufficient to publish canonical possessions to
the app. The remaining release gate is box-score and score-progression
reconciliation, followed by review of provisional and source-gap sequences.

Reproduce the broad diagnostic with:

```powershell
& 'C:\Program Files\R\R-4.4.2\bin\Rscript.exe' `
  etl/euroleague/evaluate_grouping_sample.R `
  C:\tmp\euroleague_pbp_2025_100games.csv
```

The downloaded sample is intentionally stored in `C:\tmp`, not committed to
the repository.
