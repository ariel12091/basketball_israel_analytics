# EuroLeague custom five-player Lineups diagnosis handoff

**Date:** 2026-08-29  
**Status:** read-only diagnosis complete; no DDL or data writes performed  
**Target:** `euroleague.fetch_lineups_direct`, custom five-player route

## Scope and live request

The diagnosis used the existing broad custom-clutch benchmark request:

```sql
SELECT *
FROM euroleague.fetch_lineups_direct(
  p_competition => 'E',
  p_game_year => 2025,
  p_max_margin => 3,
  p_margin_status => 'all',
  p_max_time_remaining => 240,
  p_ot_margin_filter => false,
  p_unit_size => 5
);
```

It ran through direct port 5432. The live function body MD5 was
`cc54fe28b824090cf79cac6f1c2f11b7`. The body was read from `pg_proc.prosrc`,
expanded with literal request parameters, and explained outside the
`SECURITY DEFINER` boundary so PostgreSQL exposed its inner nodes.

Every experiment used only `SELECT`, `EXPLAIN (ANALYZE, BUFFERS)`, and a
session-local `SET enable_nestloop`. No schema object, function, index, row,
role setting, or persistent database setting was changed.

## Row reduction

| Stage | Rows | Observation |
|---|---:|---|
| Filtered game/team pairs | 796 | Schedule work is small, about 36 ms |
| Eligible action rows | 14,034 | Custom time/margin/status window |
| Five-player lineup identities | 1,016 | No 2-4 player expansion |
| Event-count groups | 1,522 | Game/team/lineup/side |
| Segment-duration groups | 2,168 | Game/team/lineup/segment |
| Final five-player lineups | 751 | Complete public output |

Five-player identity already bypasses `sub_lineups`; combination expansion is
not the cause. Output size and player-name rendering were not leading costs.

## Relevant indexes

| Index | Size | Observed role |
|---|---:|---|
| `euroleague_player_stats_actions_filter_idx` | 32 MB | Chosen by normal plan |
| `euroleague_player_stats_actions_lineups_idx` | 179 MB | Existing covering index; not chosen |
| `euroleague_player_stats_actions_minutes_idx` | 43 MB | Not chosen |
| `euroleague_player_stats_actions_team_idx` | 72 MB | Not chosen; retained for Team readers |
| `player_stats_actions_by_game_pkey` | 26 MB | Not material to broad custom plan |
| `lineup_totals_by_game_pkey` | 19 MB | Normal plan probes it 1,016 times |
| `euroleague_lineup_totals_by_game_season_idx` | 5.7 MB | Used by ceiling plan |

Do not add another wide action index before explaining why the existing 179 MB
covering index is rejected.

## Normal-plan diagnosis

Three warm extracted-plan samples were variable on the shared instance, but
their shape and buffer counts were stable:

- execution: 6.994, 6.436, and 9.567 seconds;
- 289,883 shared-buffer hits and zero shared reads each time;
- 646 temporary blocks written;
- 12 nested-loop nodes;
- estimates collapse to one row at several materialized CTE boundaries.

The dominant probe patterns were:

1. **Action retrieval:** 796 probes through
   `euroleague_player_stats_actions_filter_idx`, returning about 18 eligible
   rows per probe. This accounted for 193,876 buffer hits and roughly
   4.5-5.2 seconds in the warm plans.
2. **Lineup identity:** 1,016 probes through
   `lineup_totals_by_game_pkey`, returning about four rows per probe before
   `DISTINCT`. This accounted for 83,627 buffer hits and approximately
   0.8-3.1 seconds depending on contention.

The external merge sort wrote about 260 of the 646 temporary blocks and
usually cost tens to hundreds of milliseconds. Work memory is not the first
lever: the repeated cached probes dominate.

Complete public-function calls in the initial warm pass were 5.213, 4.064, and
3.993 seconds for the same 751 rows.

## Read-only nested-loop ceiling

The exact live inner SQL and public function returned equal 751-row value
multisets. Representation hashes differed only because the declared function
return types normalize numeric scale; value-aware comparison passed. Planner
comparisons alternated normal and session-local nested-loop-off calls.

| Metric | Normal | Nested-loop-off ceiling | Change |
|---|---:|---:|---:|
| Public function median, 3 samples | 5.520 s | 3.453 s | 37% faster |
| Extracted inner SQL median | 2.710 s | 1.881 s | 31% faster |
| Shared buffers, median | 289,883 | 90,818 | 69% lower |
| Nested-loop nodes | 12 | 5 | 7 fewer |
| Temporary blocks, median | 646 | 665 | 19 more |
| Result | 751 rows | 751 rows | equal value multiset |

The ceiling samples still showed about 8.8k shared reads while normal samples
were fully cached, yet total buffer traffic and median latency were materially
lower. The alternative plan used the existing 5.7 MB season index on
`lineup_totals_by_game` and avoided the two large probe families.

One normal extracted-plan sample suffered a 10.393-second shared-instance
stall. Raw samples must remain visible; blocking comparisons should use the
repository's established central-60% estimator once there are at least 15.

## Conclusions

1. Broad custom cost is a cardinality/query-shape problem, not physical I/O,
   rendering, five-player expansion, or primarily a sort spill.
2. One-row estimates make repeated probes appear cheap.
3. Avoiding those probes has a real broad ceiling: roughly 31-37% latency and
   69% buffer improvement in this small paired experiment.
4. Existing indexes support the alternative plan. A new index is not justified
   yet.
5. `enable_nestloop = off` is diagnostic evidence only. It must not be
   persisted from this result because narrow requests may need nested loops.

## Next steps

### 1. Read-only custom-preset matrix

Run the public function under normal and session-local nested-loop-off settings
for:

- broad custom, with and without explicit app dates;
- last 10 and one team;
- phase, opponent, home/away, outcome, and opponent rank;
- own- and opponent-starter bounds;
- players-on and players-off membership;
- minimum possessions and an empty-result request.

Compare complete value multisets and declared columns. Alternate normal/off
order, retain raw samples, and report complete-fetch latency, shared
hits/reads, temp blocks, and used indexes. This remains read-only.

### 2. Decide whether a query-only rewrite is needed

If nested-loop-off improves broad requests but regresses narrow ones, do not
ship the setting. Build rollback-only query candidates that preserve narrow
probes while making the broad shape robust:

- filter the custom action window once, then hash/semi-join selected games;
- materialize the season's existing five-player identities once and join them
  to the distinct eligible lineup set;
- keep event counts and max-minus-min segment duration separate unless an
  exact single-pass derivation is proven.

Test action access and identity changes independently.

### 3. Consider physical tuning only after query shape

Only if identity remains material, test a rollback-only index shaped around
`(competition, game_year, game_id, team_id, own_lineup)` with `lineup_key` and
`player_ids` available for index-only identity lookup. Record size and
publication cost. Do not retain it if the existing season index is sufficient.

Investigate the unused 179 MB covering index separately. Do not remove or
replace it without a full consumer and rollback gate.

### 4. Acceptance gates

- Exact current 35 public columns, row values, NULLs, and broad 751-row
  multiset.
- Exact parity across the complete custom-preset matrix.
- Broad median improvement greater than `max(10%, 100 ms)`.
- Material reduction from roughly 290k broad buffer accesses.
- No preset regression beyond `max(10%, 100 ms)`.
- No new spill class or unsafe memory increase at three concurrent calls.
- Standard-clutch and per-game Lineups remain separate routes.
- No persistent DDL without a reviewed rollback candidate and explicit
  approval.

