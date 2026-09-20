# Game Flow lineup-boundary handoff (2026-09-19)

## Status

Fix plan: `docs/plans/2026-09-19-period-opening-lineup-anchors-design.md`, which
corrects the Mechanism section below — the missing period-opening state belongs
to the team that did NOT substitute at the boundary.

The raw provider PBP and box-score feeds contain the same scoring information.
The Israeli Game Flow/ribbon path can still start too late at a period boundary
because it reads only lineup-attributed rows from
`df_pts_poss_lineups_longer_mv`.

No runtime code was changed in this investigation.

## Confirmed example: game 404, Q4

The raw action feed contains the following Q4 boundary substitutions:

- Action `4040623`, `10:00`: Kenneth Lofton Jr. (`player_id=2278`) enters.
- Action `4040624`, `10:00`: player `1135` exits.
- Further substitutions occur at `09:36` and `08:49`.

However, the first lineup-attributed action in the derived Israeli fact is
`4040670` at approximately `08:03`. Therefore the Game Flow ribbon cannot show
Lofton’s entrance at 10:00 and loses all boundary substitutions before 08:03.

## Mechanism

`compute_stints()` builds lineup segments from substitution-derived states for
each team, then intersects the two teams’ segments. The shared stint begins at
the later team boundary:

```r
final_start_id = pmax(start_id_offense, start_id_defense)
```

Actions before that overlap have no complete two-team stint. The Israeli
longer fact then drops rows with no lineup assignment:

```sql
WHERE lineup_hash IS NOT NULL
```

The ribbon reader consumes those derived segments, so it cannot reconstruct a
substitution that occurred inside the discarded opening interval.

## Other verified affected intervals

| Game | Period | Scoring actions before first attributed action | First attributed action |
|---|---:|---|---:|
| 401 | Q4 | `4010683`, `4010686`, `4010688` | `4010698` |
| 404 | Q2 | `4040230` | `4040232` |
| 404 | Q4 | `4040629`, `4040637`, `4040662` | `4040670` |
| 406 | Q3 | `4060250` | `4060258` |

These are the same structural failure: valid period-opening actions precede
the first complete overlapping lineup interval. The resulting app point deficit
is downstream of lineup attribution, not a missing basket in raw PBP.

## Required fix direction

At each period boundary, seed each team’s lineup independently from the prior
period’s closing state, then replay all substitutions stamped at the new
period’s opening time (normally 10:00). Create the opening lineup segment
before intersecting it with the opponent’s segment. This preserves exact
substitution timing, including Kenneth Lofton Jr.’s Q4 entrance, without
backfilling the later 08:03 lineup across earlier substitutions.

The ribbon should then consume those seeded segments. Do not solve this by
simply extending the first later segment backward: game 404 has additional
substitutions at 09:36 and 08:49, so that would assign the wrong players to the
opening minutes.

## Validation checklist

After implementing the fix, verify:

1. Game 404 Q4 shows Lofton Jr. entering at 10:00 and player 1135 exiting.
2. The four boundary substitutions at 09:36/08:49 remain separate events.
3. The scoring actions listed above receive lineup hashes.
4. PBP-derived team points equal the official box score for games 401, 404,
   and 406.
5. Game Flow lanes begin at the period boundary, with no artificial 08:03
   start for game 404 Q4.
