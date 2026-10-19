# Lineup Anomalies and ETL Guard Memory

Date: **2026-07-22**  
Environment checked: **`basketball_test`**  
Primary live report: `etl/logs/data_quality/latest.md`

## Purpose

This note preserves the investigation into invalid lineup sizes, missing
event-to-lineup matches, and game 62461. It distinguishes genuine source-data
failures from harmless intermediate states created by the provider's bulk
substitution/reset encoding.

## Game 62461: resolved

The source game roster omitted player **2543, Cody Demps**, although 26 PBP
actions for team 8 identified him. That omission caused four-player lineups,
18 invalid lineup states, 430 unmatched event-team rows, and a player-minute
conservation failure.

The scoped ETL reprocess recovered Cody from another roster for the same
season/team and published the game successfully:

- downstream lineup/stint validation: **0/457 unmatched** and **0/457 multiply
  matched** action contexts;
- the terminal assist, action `624610872`, now has both offense and defense
  lineup rows;
- a full cold-history audit now finds **zero** nonzero PBP
  `(game_id, team_id, player_id)` contexts absent from `full_rosters`.

## Permanent ETL protection

The normal ETL now performs two checks before writing a game:

1. `complete_roster_from_action_players()` attempts to recover a PBP player
   missing from the game roster using same-season, same-team roster history.
2. `assert_action_players_in_roster()` fails that individual game before its
   transaction begins if any nonzero action-player context is still absent.

This is deliberately fail-closed: the ETL must not guess a five-player lineup
when it cannot identify the missing player.

The PWS stint join was also corrected for a separate terminal-boundary issue.
Persisted `final_end_id` remains a real action ID for FK integrity, while only
the final interval receives a temporary `end + 1` upper bound during the
half-open action-to-stint join. This keeps the maximum action attributable.

Relevant implementation and tests:

- `etl/etl_onoff.R`
- `etl/etl_full.R`
- `app/tests/testthat/test-roster-action-contracts.R`
- `app/tests/testthat/test-stint-boundary-contracts.R`

## Remaining lineup findings

The current 52 invalid lineup states are not one uniform source failure.

### Confirmed material source failures

| Game | Evidence | Impact |
|---|---|---|
| 178 | Teams 4 and 11 remain at 6-7 players over sustained Q3/Q4 intervals. The source also contains a documented late Q2 reset block. | 18 invalid states, 362 unmatched event-team rows, and player-minute overcounts of 13.03 and 17.50 minutes. |
| 62452 | Team 11 begins Q4 at zero and only recovers to 2-3 players; the source substitution declarations never establish five. | 5 invalid states, 116 unmatched event-team rows, invalid starter context, and a 23.42-minute player-minute deficit. |

Game 178 has additional evidence in
`docs/game_178_invalid_q2_reset_2026-05-30.md`.

### Likely small source substitution failure

| Game | Evidence | Impact |
|---|---|---|
| 62479 | Team 7 has six players from Q2 00:26 until the next correcting substitution at 00:02, consistent with an unpaired or misordered IN/OUT declaration. | 1 invalid state, 2 unmatched mirrored rows, and a 0.40-minute player-minute excess. |

Do not automatically choose which player to remove without corroborating box,
video, or external play-by-play evidence.

### Reset/declaration artifacts, not sustained gameplay failures

The other **28 invalid states across 10 games** are transient states inside
bulk five-out/five-in or period-reset declarations:

`157`, `168`, `190`, `205`, `209`, `211`, `357`, `381`, `62447`, and `62534`.

Most occur at `00:00`; games 62447 and 62534 contain the same pattern at a
non-boundary clock. The provider emits the five OUT and five IN declarations as
separate action rows, so sequential lineup reconstruction briefly observes
zero or four players before the declaration completes.

These are not sustained missing-gameplay intervals. Game 209's six unmatched
rows are reset/substitution rows from this pattern, rather than missing
statistical events. The other reset games do not appear in the unmatched-row
check.

## Current DQ snapshot after repairing 62461

- `Q_persisted_rows_without_lineup_match`: **486 rows in four games**
  - 178: 362
  - 62452: 116
  - 209: 6 reset rows
  - 62479: 2
- `R_invalid_lineup_player_counts`: **52 states across 24 game/team groups**
- `X_player_minute_conservation`: **4 team-games**
  - 178/team 4
  - 178/team 11
  - 62452/team 11
  - 62479/team 7

The overall DQ report remains `FAIL` because the genuine historical source
defects are intentionally still visible.

## Recommended follow-up

1. Change the quality classification, or reconstruct bulk substitutions
   atomically, so a transient reset is not reported like a sustained gameplay
   failure. Only classify it as benign when five players are restored before
   the next statistical action.
2. Keep games 178 and 62452 as explicit source failures until an evidence-based
   historical correction is available.
3. Investigate game 62479's two substitutions with an external source before
   applying any manual correction.
4. Never silence all `n_on = 0` states globally: game 62452 demonstrates that a
   zero-player state can begin a genuine sustained failure.

## Authoritative references

- Database/query model and current anomalies: `docs/database_context.md`
- Live column values and anomaly counts: `docs/database_value_profiles.md`
- Current generated DQ report: `etl/logs/data_quality/latest.md`
- Known excluded source actions: `etl/known_invalid_pbp_actions.csv`

