# OT Null-Lineup Recovery Design

Date: 2026-06-19

## Objective

When an overtime period begins with null lineup coverage, seed the period from
the final valid lineup of the preceding period. Accept the recovery only when
the reconstructed lineup and subsequent substitutions explain player
participation throughout the overtime period.

This document covers design and implementation planning only.

## Current Failure Mode

Overtime periods are partitioned separately during stint construction:

- Regulation periods use a shared `q_bucket`.
- Each overtime period uses its own quarter as the `q_bucket`.

Lineup states are currently generated from substitution rows. If an overtime
period begins without a provider lineup reset or substitution, there is no
starting lineup state for that period. Stint construction consequently cannot
match the early OT events to valid offense and defense lineups.

## Proposed Recovery Flow

Process each overtime period independently:

1. Build the normal `lineups_lookup`, stints, and provisional PWS.
2. Detect whether the start of the OT period has no valid five-player lineup.
3. Limit recovery to a leading OT lineup gap. Do not repair isolated null
   lineup rows later in the period.
4. Find each team's final valid five-player lineup from the preceding period:
   - Q5 uses the final Q4 lineup.
   - Q6 uses the final Q5 lineup, including an accepted Q5 recovery.
5. Apply substitutions recorded at `05:00`:
   - Normal OUT/IN pairs update the carried lineup.
   - Five-player declarations are treated as atomic lineup resets.
   - Transitional four-player or six-player states inside a complete reset are
     ignored.
6. Simulate the remaining overtime substitutions and validate player
   participation.
7. If both teams pass validation, create a synthetic OT-start lineup snapshot
   and rebuild stints and PWS.
8. If either team fails validation, reject the recovery.

## Scope

The fallback applies only when:

- `quarter >= 5`;
- the beginning of that OT period lacks a valid lineup;
- a final valid lineup exists in the immediately preceding period; and
- the candidate recovery passes all validation gates.

It must not:

- alter regulation lineup reconstruction;
- overwrite an already valid OT starting lineup;
- fill arbitrary null lineup rows in the middle of OT;
- infer an unknown player without substitution or event evidence; or
- silently accept a partial or ambiguous reconstruction.

## Acceptance Gates

Accept an OT lineup recovery only when all of the following pass:

1. The previous-period lineup contains exactly five unique players.
2. Every candidate player belongs to the team's roster for that game.
3. Each normal substitution is valid:
   - the outgoing player is currently on the court;
   - the incoming player is currently off the court;
   - both players belong to the correct team.
4. Every applicable player event is attributable to a player in the simulated
   lineup at that point in the timeline.
5. If a valid provider lineup appears later in the OT period, the simulated
   lineup at that point matches it.
6. Rebuilt PWS produces exactly one valid lineup/stint match per source action.
7. Both offense and defense hashes resolve to five-player lineups.

Recovery is atomic at the game/OT-period level. Both teams must pass before the
period is accepted.

## Period-Start Reset Handling

Provider period-start rows can represent either normal substitutions or a
complete lineup declaration.

### Normal substitutions

Apply an OUT/IN pair to the carried lineup when it produces a valid five-player
state.

### Complete lineup reset

Treat the rows as an atomic reset when the provider clearly declares the five
players starting the period. Supported patterns include:

- five `player_in` rows without corresponding OUT rows;
- five OUT rows followed by five IN rows; and
- reset sequences containing temporary transitional players, provided the
  final declared state contains exactly five valid roster players.

Do not persist intermediate four-player or six-player states generated while
processing a complete reset.

## Participant Validation

Validate player references from:

- shots;
- free throws;
- rebounds;
- assists;
- steals;
- blocks;
- turnovers;
- player fouls; and
- `parameters_fouled_on`, when populated.

Exclude:

- coach fouls;
- bench fouls;
- timeouts;
- clock events;
- period markers; and
- team events without an individual player.

### Event classification

| Condition | Classification | Recovery result |
| --- | --- | --- |
| Player is in the simulated lineup | Explained | Pass |
| Player is explained by an explicit same-clock substitution or the same free-throw sequence | Ordering warning | Pass with warning |
| Player is explained only by a later substitution at a different clock | Unexplained | Reject |
| No substitution or lineup evidence explains the player | Unexplained | Reject |

For example, game 62587 would remain rejected under this rule because Yiftach
Ziv's recorded return occurs 13 seconds after the foul attributed to him.
Same-clock ordering cases such as those found in games 62439 and 62541 would be
warnings rather than unexplained participation.

## Failure Behaviour

If recovery is required but rejected:

1. Do not publish reconstructed OT lineups.
2. Roll back that game's ETL transaction, preserving any previously published
   version.
3. Log:
   - game ID;
   - team ID;
   - OT quarter;
   - candidate carried lineup;
   - period-start reset rows;
   - unexplained event IDs and player IDs; and
   - the rejection reason.
4. Mark the game as failed rather than silently retaining null lineup data.

## Implementation Location

The main integration point is Phase 2 of `etl/etl_full.R`, after normal
`lineups_lookup` generation and before final stint persistence.

Recommended structure:

1. Keep `compute_lineups_lookup()` responsible for normal provider-derived
   lineup states.
2. Add a separate OT recovery helper that:
   - detects leading OT gaps;
   - derives candidate seeds;
   - simulates substitutions;
   - validates participation; and
   - returns accepted synthetic lineup states plus audit metadata.
3. Combine accepted synthetic states with the normal lineup states.
4. Build stints and PWS from the combined result.
5. Run final coverage and participant checks before committing the game.

This separation keeps the fallback explicit and prevents OT-specific rules from
changing normal lineup reconstruction.

## Audit Metadata

The recovery helper should return one audit record per game, team, and OT
period with fields similar to:

- `game_id`
- `team_id`
- `quarter`
- `recovery_required`
- `recovery_status`
- `source_previous_quarter`
- `seed_lineup_hash`
- `resolved_lineup_hash`
- `period_start_reset_type`
- `recovered_action_rows`
- `ordering_warning_count`
- `unexplained_event_count`
- `unexplained_event_ids`
- `reason`

Possible statuses:

- `not_required`
- `accepted_carry_forward`
- `accepted_period_reset`
- `rejected_missing_previous_lineup`
- `rejected_invalid_player_count`
- `rejected_invalid_substitution`
- `rejected_provider_disagreement`
- `rejected_unexplained_participant`
- `rejected_final_coverage`

## ETL Logging and Report Changes

Add an `OT lineup recovery` section to the ETL report showing:

- OT periods inspected;
- periods with leading null coverage;
- recoveries accepted;
- recoveries rejected;
- rows recovered;
- same-clock ordering warnings;
- unexplained participant events; and
- affected game/team/quarter with rejection reasons.

Add a corresponding data-quality check that reports:

- OT periods still starting without valid lineups;
- accepted recovery rows with invalid player counts;
- participant events whose player is absent from the attached lineup; and
- accepted simulated lineups that disagree with a later provider lineup.

The existing lineup/stint coverage, five-player state, starter-count, team
minute, score, possession, and player-minute checks must continue to run after
the recovery.

## Test Plan

Required fixtures:

1. No OT reset and the same five players continue: accept.
2. One or more substitutions at `05:00`: accept the updated lineup.
3. Five-IN complete lineup declaration: apply an atomic reset.
4. Five-OUT/five-IN reset: apply an atomic reset.
5. A player participates without substitution evidence: reject.
6. Same-clock action/substitution ordering: accept with a warning.
7. A substitution is recorded later at a different clock: reject.
8. Double overtime: Q6 uses Q5's finishing lineup.
9. The previous period ends with four or six players: reject.
10. A later provider lineup disagrees with reconstruction: reject.
11. A candidate contains a player outside the game roster: reject.
12. Final PWS contains zero-match or multi-match rows: reject.
13. Regulation and healthy OT games remain unchanged.

## Historical Verification

Initial recovery candidates:

- game 137;
- game 188; and
- game 211.

The other 17 audited overtime games should remain unchanged. They should be
used as regression cases to verify that the fallback does not alter OT periods
that already produce coherent lineups.

## Implementation Sequence

1. Add pure R helpers for lineup simulation and participant validation.
2. Add unit fixtures covering all acceptance and rejection paths.
3. Integrate recovery after normal lineup generation in Phase 2.
4. Rebuild stints and PWS from the accepted combined lineup states.
5. Add transactional rejection and structured ETL logs.
6. Add data-quality checks and HTML report metrics.
7. Backfill games 137, 188, and 211 in the test schema.
8. Compare all downstream rows and metrics for the 17 healthy OT regression
   games.
9. Run the complete ETL and data-quality suite before production use.

## Backlog: Overlapping Regulation and OT Action IDs

Game 211 cannot currently be handled safely by the standard stint boundary
logic because its action IDs overlap across periods:

- Q4 maximum action ID: `2110902`
- Q5 minimum action ID: `2110802`

`compute_stints()` orders lineup states across regulation and OT and uses
`lead(id)` as the exclusive action boundary. In game 211, the lower Q5 IDs can
prematurely terminate a Q4 stint, leaving the Q4 `00:00` assist at action
`2110902` unmatched.

Do not use a broad `last_id + 1` adjustment. That fallback affects the final
lineup state of every game and is not reached for the game 211 failure.

Required future design:

1. Derive stint boundaries within the same clock-period bucket used by PWS:
   regulation as one bucket and each OT period as a separate bucket.
2. Use a bucket-specific exclusive final action boundary.
3. Verify that no action can be assigned across regulation/OT boundaries when
   provider IDs overlap.
4. Regression-test normal sequential IDs, overlapping IDs, double overtime,
   and final actions at `00:00`.

Until this is implemented and verified, game 211 is excluded from automatic OT
lineup recovery and remains a known data-quality exception.
