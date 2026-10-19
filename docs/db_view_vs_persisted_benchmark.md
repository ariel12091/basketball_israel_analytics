# View vs Persisted Benchmark (2026-03-15)

## Scope
Compared query execution on live DB for 3 high-impact objects:
- persisted object (`table`/`materialized view`)
- equivalent "view-style" query (inlined definition from SQL build scripts)

Method: `EXPLAIN (ANALYZE, BUFFERS)`; 2 runs each.

## Results

| Object | Direct run2 (ms) | Virtual run2 (ms) | Slowdown (virtual/direct) |
|---|---:|---:|---:|
| `player_four_factors_by_game` | 62.34 | 2227.52 | 35.73x |
| `player_onoff_by_game` | 69.04 | 806.84 | 11.69x |
| `df_pts_poss_lineups_longer_mv` | 209.87 | 1985.72 | 9.46x |

Full run logs:
- `tmp_view_benchmark_runs.csv`
- `tmp_view_benchmark_summary.csv`

## Interpretation
- For these hot analytical paths, replacing persisted objects with plain views is materially slower.
- Slowdown was roughly one order of magnitude (and much higher for player four-factors in this query shape).
- This supports keeping the heaviest serving layers persisted if app latency is a priority.

## Caveats
- Query shape tested is representative, not exhaustive.
- Only 2 runs per case (cold/warm effects visible).
- Different filters may produce different multipliers.
