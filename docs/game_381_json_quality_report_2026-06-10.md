# Game 381 JSON Quality Report

Date: 2026-06-10

Source reviewed:

```text
https://stats.segevstats.com/realtimestat_heb/get_team_action.php?game_id=381
```

## Grade

Data-quality score: **70 / 100**

Faultiness score, inverted: **30 / 100**

`100` means the game JSON is clean, internally consistent, and safe to load as-is. This game is mostly accurate for scoring and player participation, but it has one severe transition/identity defect that makes the current DB load fail and could corrupt lineup/minute derivations if handled naively.

Per request, the duplicate action ID problem is counted as **one mistake**, not as 11 separate duplicate IDs.

## Executive Summary

The game is not globally bad. The core game story is represented well:

- Final score reconstructs cleanly as **Hapoel Tel Aviv 88, Hapoel Jerusalem 84**.
- All scoring actions with score strings match the reconstructed running score.
- No made scoring action is missing a score.
- All nonzero `parentActionId` values point to existing action IDs.
- All players used in actions exist in the game roster.
- No used player is flagged with `inRoster = 0`.
- Quarter clocks do not increase within a quarter.
- There are no true JSON `null` `playerId` values, but there are 15 rebound rows with the provider placeholder `playerId = 0`.

The serious issue is localized around the end of Q2 / start of Q3:

- Action IDs `3810375` through `3810385` are duplicated.
- The duplicated rows are not harmless copies; some duplicated IDs represent different events.
- ID `3810385` appears both as **Q2 end-of-quarter** and **Q3 start-of-quarter**.
- Q2 has **three** end-of-quarter markers.
- The duplicate transition creates 10 impossible substitution events where players are recorded as subbing out after they are already off the floor.

## Checks Run

```text
actions_total: 798
gameInfo home: team 4 HAPOEL JERUSALEM, Score = 0
gameInfo away: team 3 HAPOEL TEL AVIV, Score = 0

action type counts:
clock: 211
substitution: 156
shot: 128
rebound: 74
freeThrow: 49
foul: 45
foul-drawn: 45
assist: 30
turnover: 16
deflection: 12
quarter: 10
timeout: 10
steal: 8
block: 2
game: 2
```

## Findings

### 1. Duplicate ID / Q2-Q3 Transition Defect

Counted as one mistake.

The duplicated action IDs are:

```text
3810375, 3810376, 3810377, 3810378, 3810379,
3810380, 3810381, 3810382, 3810383, 3810384,
3810385
```

There are 11 duplicate ID groups and 11 extra rows.

This is the exact reason the ETL failed:

```text
(id, game_id) = (3810379, 381) already exists
```

The more important quality issue is that the duplicated block conflicts semantically. It repeats the halftime transition and changes event meaning under the same IDs. Most notably:

```text
3810385 Q2 00:00 quarter end-of-quarter
3810385 Q3 10:00 quarter start-of-quarter
```

Q2 quarter/game markers in the raw JSON include:

```text
3810385 Q2 00:00 end-of-quarter
3810372 Q2 00:00 end-of-quarter
3810384 Q2 00:00 end-of-quarter
3810385 Q3 10:00 start-of-quarter
```

DB impact:

- Current behavior: the game is rejected during `actions_clean` staging, so it does not silently pollute downstream tables.
- If deduped by simply taking the first or last row per `(game_id, id)`, the ETL could preserve the wrong transition rows.
- That could misstate halftime lineups, `lineups_lookup`, `stints`, and minutes around the Q2/Q3 boundary.

### 2. Substitution Integrity Breaks At The Duplicate Block

Tracking lineup state from the raw JSON produced 10 substitution faults. These are all in the duplicated Q2 00:00 block.

Each fault is an `OUT` event for a player who is already off the court:

```text
team 3: players 1026, 1033, 1036, 2170, 1037
team 4: players 1002, 1044, 2076, 2230, 1054
```

This did not create bad lineup sizes for live non-substitution play rows in the raw-order check, because the bad rows occur at the quarter boundary and Q3 reinitializes lineups. Still, this is exactly the kind of defect that can damage stint/minute segmentation if loaded without a targeted fix.

### 3. Stale `gameInfo` Team Scores

`gameInfo.homeTeam.Score` and `gameInfo.awayTeam.Score` are both `0`, even though the action stream reconstructs the final score as `88-84`.

This is a metadata fault, but lower risk for the app because the ETL schedule path normally uses the schedule feed for game-level scores and the action stream for possession/player scoring.

DB impact:

- Low if `gameInfo.Score` is ignored.
- High only if a future loader treats `gameInfo.Score` as authoritative.

### 4. Action ID Ordering Is Not Strictly Raw-Order Monotonic

There are 22 places where raw array order goes backward by action ID. Most are same-clock clusters around substitutions, free throws, assists, or rebounds.

Examples:

```text
raw positions 112 -> 113: 3810114 -> 3810112
raw positions 175 -> 176: 3810178 -> 3810174
raw positions 379 -> 380: 3810385 -> 3810372
```

This is not automatically a fatal data error, because same-clock events often have ambiguous provider ordering. It matters because the ETL uses action `id` ordering in possession and lineup windows. The Q2/Q3 backward jump is part of the severe duplicate/transition defect above; the other jumps are mostly lower-confidence ordering warnings.

### 5. Score Feed Is Internally Strong

The score data is the best part of the file.

```text
scoring actions with score: 94
score mismatches: 0
scoring actions missing score: 0
reconstructed team 3 score: 88
reconstructed team 4 score: 84
last score string: 88-84
```

This means the JSON represents the scoreboard and scoring events well.

### 6. Parent References And Player/Roster Links Are Good

```text
nonzero parent refs: 216
missing parent IDs: 0
roster players total: 41
players used in actions: 19
used players missing from roster: 0
used players flagged inRoster_0: 0
```

This reduces the likelihood of broad player-stat corruption.

### 7. `playerId = 0` Placeholders

There are **no true JSON `null` `playerId` values** and no action is missing the `playerId` property.

There are also expected parameter-level nulls on non-action rows:

```text
parameters.team = null: 223
parameters.player = null: 404
parameters.team and parameters.player both null: 223
parameters.player null while parameters.team is present: 181
parameters.player present while parameters.team is null: 0
```

All rows where both `parameters.team` and `parameters.player` are null are control events:

```text
clock: 211
quarter: 10
game: 2
```

Example:

```text
id=3810025 type=clock
parameters.team=null
parameters.player=null
playerId=0
teamId=0
currentQuarter=1
currentQuarterTime=08:44
```

That is expected and should not be counted as a data-quality fault. The row is a clock tick, not a team/player event. The ETL already treats clock rows as non-player actions.

Rows where `parameters.player` is null but `parameters.team` is present are different:

```text
substitution: 156
rebound: 15
timeout: 10
```

The substitution rows are expected because they use `parameters.playerIn` / `parameters.playerOut` and the top-level `playerId`. The timeout rows are expected team events. The rebound rows are the real attribution issue, because they are basketball events with team attribution but no player attribution.

The provider does use `playerId = 0` as a placeholder:

```text
playerId = 0 rows: 248
clock: 211
timeout: 10
quarter: 10
game: 2
rebound: 15
```

The clock, timeout, quarter, and game rows are expected non-player control events. The meaningful concern is the **15 rebound rows** with `playerId = 0`:

```text
offensive team/unattributed rebounds: 8
defensive team/unattributed rebounds: 7
```

All 15 rebound rows have a valid `teamId`, rebound type, and parent shot reference, so they should still work for possession flow and team-level rebounding logic. They are incomplete for player-level attribution: no individual player can receive those rebounds unless a separate box-score reconciliation layer fills them in.

DB impact:

- Low to medium for possession/team metrics, because `teamId`, rebound type, and parent action are present.
- Medium for player traditional stats or player four-factor derivations if those expect every rebound to belong to a player.

## Overall Assessment

Game 381 is **not safe to load as-is** because the duplicate/transition defect violates the `actions_clean` primary key and creates impossible substitution state. That said, it is not a bad representation of the entire game. The scoring stream, player references, parent-action links, and clock progression are mostly coherent.

The data-quality grade is therefore **70 / 100**:

- Strong score/event representation: + high confidence.
- Localized but severe Q2/Q3 transition defect: major penalty.
- 15 team/unattributed rebound rows with `playerId = 0`: moderate player-stat attribution penalty.
- Current DB load fails loudly instead of silently misrepresenting the game: reduces downstream damage.
- Naive dedupe would be risky: keeps the grade below the high 70s.

Recommended handling:

1. Treat the duplicated Q2/Q3 transition block as one known provider defect.
2. Do not fix by generic `distinct(id, game_id, .keep_all = TRUE)` without choosing the valid transition rows.
3. Add a targeted cleanup rule for game `381` after manually selecting the canonical Q2 end and Q3 start rows.
4. Re-run the local transform through `actions_clean`, `lineups_lookup`, `stints`, and `pws` before allowing the game into the DB.
