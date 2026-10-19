# Game 178 Invalid Q2 Reset Actions

Date: 2026-05-30

## Summary

Game 178 had a late Q2 10:00 reset block in the provider play-by-play after live Q2 00:00 actions. These rows are not part of the playable chronological sequence and must not enter `actions_clean`.

The canonical cleanup now removes these rows in `clean_actions()`:

```text
1780466-1780481
```

## Removed Rows

```text
1780466 Q2 10:00 substitution team 4  OUT #1
1780467 Q2 10:00 substitution team 4  OUT #5
1780468 Q2 10:00 substitution team 4  OUT #6
1780469 Q2 10:00 substitution team 4  OUT #15
1780470 Q2 10:00 substitution team 4  OUT #18
1780471 Q2 10:00 substitution team 11 OUT #4
1780472 Q2 10:00 substitution team 11 OUT #5
1780473 Q2 10:00 substitution team 11 OUT #6
1780474 Q2 10:00 substitution team 11 OUT #14
1780475 Q2 10:00 substitution team 11 OUT #30
1780476 Q2 10:00 substitution team 4  IN  #1
1780477 Q2 10:00 substitution team 4  IN  #3
1780478 Q2 10:00 substitution team 4  IN  #5
1780479 Q2 10:00 substitution team 4  IN  #15
1780480 Q2 10:00 substitution team 4  IN  #20
1780481 Q2 10:00 quarter end-of-quarter
```

`1780481` is invalid because Q2 end-of-quarter cannot occur at Q2 10:00. The Q3 start marker and Q3 substitutions remain untouched.

## Failure Mode

The reset block arrived after Q2 00:00 actions by action id but carried a Q2 10:00 clock. The lineup derivation ordered player state by action id, so these rows overwrote valid Q2 state after the quarter had effectively ended.

That created impossible lineup segments. In the original game-178 transform, `stints` produced duplicate primary-key candidates such as:

```text
game_id=178, team_id=11, final_start_id=1780255, final_end_id=1780265
game_id=178, team_id=4,  final_start_id=1780352, final_end_id=1780362
game_id=178, team_id=11, final_start_id=1780352, final_end_id=1780362
```

The duplicate stints came from pairing valid Q2 segments with impossible segments that crossed chronological boundaries by action id.

## Court-State Evidence

At Q2 09:42, Beer Sheva's actual five:

```text
#4  Jake O'Neil
#6  T.J Bickerstaff
#24 Yoav Vitlem
#30 L.J Figueroa
#44 Marquis Barnett
```

At Q2 09:42, Hapoel's actual five:

```text
#1  Jared Harper
#5  Austin Wiley
#11 Cassius Winston
#15 Nimrod Levi
#18 Tsuf Ben Moshe
```

The late reset block would incorrectly replace Hapoel with `#1/#3/#5/#15/#20` and would wipe Beer Sheva to an invalid state.

## Verification

A rollback-only transform with rows `1780466-1780481` excluded compiled cleanly through `pws`:

```text
removed actions = 16
actions = 662
subs = 167
possessions = 662
lineups = 1202
stints = 124
pws = 662
duplicate stints = 0
multi-match possessions = 0
zero-or-missing stint matches = 0
max matches per possession row = 1
```

