# Shot-Type / Interior Shot Analysis (2025–2026)

**Dates:** 2026-07-08 → 2026-07-15. **Sources:** `basketball_test.df_pts_poss_lineups_longer_mv` (live action-level MV) + `exports/cold/actions_clean.parquet` (shot coordinates — see the Shot Location section).

**Scripts (rerunnable, repo):** `scripts/analysis/` — `layup_team_fixed.R`, `layup_net_rim.R`, `layup_onoff.R`, `layup_onoff_decomp.R`, `fiba_court_zones.R` (court geometry + `classify_shot_zone()`), `shot_zone_proxy_validation.R`, `shot_zone_overview.R`, `shot_tag_coord_disagreement_chart.R`, `shot_zone_classification_chart.R` (shot-chart PNGs in `docs/`), `shotgen_player_onoff.R`, `shotgen_team_corr.R`, `shotgen_regressions.R`, `shotgen_collinearity.R`, `shotgen_zone_regression.R`, `shotgen_zone_sensitivity.R` (shot-generation vs offense — see § Shot generation).
**Full results (CSV):** `output/` — `team_interior_profiles.csv` (28 team-seasons), `layup_onoff_2026.csv` (137 players), `layup_onoff_decomp_2026.csv` (shot-mix decomposition), `shot_zone_overview.csv`, `three_pt_corner_split.csv`, `shot_type_zone_crosstab.csv`, `shotgen_player_onoff_2026.csv`, `shotgen_team_2025_2026.csv`, `shotgen_team_zone_2025_2026.csv`, `shotgen_player_zone_2026.csv`.

## Terminology

"Interior" here means **shot-type proxy**: `parameters_type IN ('lay-up','dunk','allyhoop')`. The tag describes execution, not location — a running floater can be tagged `lay-up`. Real locations DO exist: `actions_clean.parameters_coord_x/y` (raw JSON PBP, 100% shot coverage), but they are **dropped at the MV layer** and survive only in cold storage / `exports/cold/*.parquet` — so live analyses are coordinate-blind and the tag is the workhorse. The Shot Location section below validates the proxy against the real coordinates.

## Method rules (hard-won)

1. **Dedup:** every action appears twice in the MV (offense/defense `type_lineup`). League-wide counts need `SELECT DISTINCT (game_id, id)`. For team attribution skip dedup and filter instead: `type_lineup = 'offense'` yields exactly one row per shot with `team_id` = shooting team (verified 0 mismatches vs rosters); `'defense'` = defending team.
2. **Season-scoped ids:** `team_id` and `player_id` are only meaningful per `(id, game_year)` — the provider re-enumerates both every season and **recycles ids to different clubs/people** (see § Team-id namespaces). Any name map must `GROUP BY id, game_year` (this is the committed convention in all `sql/` team MVs).
3. **Blocked = miss** throughout.
4. Roster names: `full_rosters.firstname`/`lastname` (there is no `player_name` column); team names: `full_rosters.team_name`.

## League shot-type table (both seasons, deduped)

| type | freq of FGA | FG% | PPS (FG only) | PPS incl. and-1 FT | and-1 rate |
|---|---|---|---|---|---|
| jump-shot 3pt | 40.3% | 34.7 | 1.041 | 1.043 | 0.3% |
| lay-up | 39.8% | 56.0 | 1.121 | 1.164 | 6.3% |
| jump-shot 2pt | 16.0% | 38.3 | 0.767 | 0.786 | 2.6% |
| dunk | 3.7% | 90.7 | 1.813 | 1.829 | 2.8% |
| allyhoop | 0.1% | 93.4 | 1.869 | 1.902 | 3.3% |

Hierarchy: interior > 3pt > mid-range; lay-up beats the 2pt jumper by ~+48% PPS incl. and-1s. Stable across seasons.

**FT attribution limit:** and-1 FTs parent to the made shot row directly; all other FTs parent to the **foul** row, and *no shot row is logged for a fouled miss*. So per-type PPS-incl-FT is a lower bound, biased against interior attempts (true lay-up PPS likely ~1.25+).

## Assisted vs unassisted (made shots only — assists are unknowable for misses)

| type | assisted % of makes |
|---|---|
| allyhoop | 100 (definitional) |
| dunk | 77.0 |
| jump-shot 3pt | 76.3 |
| **lay-up** | **47.7** |
| jump-shot 2pt | 34.7 |

Lay-ups are the league's self-created shot (drives); stable per season (47.6 → 47.8). Assist linkage: `assist.parent_action_id` → made-shot row. Assists also parent to fouls (~1.8k) = pass leading to FTs.

## Team interior profiles (corrected names, season-scoped)

Full table (share = % of FGA; fg/pps with blocked = miss; ast = assisted % of interior makes; net = off PPS − def PPS per attempt; att_diff = interior attempts taken − conceded):

| yr | team | share off | fg off | pps off | ast off | share def | fg def | pps def | ast def | net pps | att diff |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 2025 | Hapoel Tel Aviv | 44.6 | 64.3 | 1.286 | 54.9 | 44.3 | 55.1 | 1.101 | 46.7 | **+0.185** | −17 |
| 2025 | Maccabi Tel Aviv | 38.9 | 64.1 | 1.283 | 59.0 | 41.9 | 57.0 | 1.140 | 48.6 | +0.143 | −86 |
| 2025 | Hapoel Holon | 42.5 | 62.7 | 1.255 | 55.4 | 43.3 | 55.7 | 1.115 | 48.3 | +0.140 | −2 |
| 2025 | Hapoel Galil Elion | 39.4 | 57.5 | 1.150 | 56.2 | 43.3 | 56.2 | 1.125 | 52.4 | +0.025 | −102 |
| 2025 | Hapoel Afula | 48.1 | 60.4 | 1.209 | 52.1 | 38.3 | 59.3 | 1.188 | 49.3 | +0.021 | +175 |
| 2025 | Gilboa/Galil | 45.7 | 58.9 | 1.177 | 54.3 | 39.9 | 58.6 | 1.172 | 55.6 | +0.005 | +124 |
| 2025 | Maccabi Ramat Gan | 46.0 | 60.4 | 1.209 | 48.2 | 40.7 | 60.4 | 1.210 | 51.1 | −0.001 | +91 |
| 2025 | Hapoel Jerusalem | 43.8 | 56.5 | 1.129 | 46.0 | 44.6 | 56.7 | 1.134 | 47.8 | −0.005 | +38 |
| 2025 | Ness Ziona | 40.8 | 57.2 | 1.144 | 47.7 | 44.7 | 58.8 | 1.176 | 56.1 | −0.033 | −108 |
| 2025 | Bnei Herzliya | 43.0 | 57.8 | 1.157 | 48.6 | 48.0 | 60.0 | 1.199 | 53.5 | −0.042 | −147 |
| 2025 | Kiryat Ata | 47.6 | 57.4 | 1.148 | 50.2 | 41.3 | 60.4 | 1.209 | 55.7 | −0.060 | +135 |
| 2025 | Elitzur Netanya | 40.6 | 53.7 | 1.076 | 51.1 | 44.5 | 59.7 | 1.194 | 49.5 | −0.118 | −36 |
| 2025 | Beer Sheva | 41.4 | 55.0 | 1.099 | 45.6 | 44.4 | 61.6 | 1.232 | 53.5 | −0.133 | +7 |
| 2025 | Hapoel Haifa | 41.9 | 57.4 | 1.148 | 47.8 | 45.2 | 64.3 | 1.286 | 50.5 | **−0.138** | −72 |
| 2026 | Bnei Herzliya | 45.4 | 64.4 | 1.288 | 57.3 | 44.1 | 55.2 | 1.106 | 49.5 | **+0.182** | −16 |
| 2026 | Hapoel Haemek | 38.2 | 61.9 | 1.238 | 54.6 | 40.3 | 56.1 | 1.121 | 50.2 | +0.117 | −70 |
| 2026 | Maccabi Tel Aviv | 42.6 | 62.1 | 1.241 | 56.0 | 40.4 | 59.1 | 1.181 | 51.6 | +0.060 | +105 |
| 2026 | Hapoel Tel Aviv | 42.9 | 63.0 | 1.261 | 49.5 | 43.8 | 60.0 | 1.200 | 48.4 | +0.060 | +18 |
| 2026 | Kiryat Ata | 39.5 | 60.6 | 1.211 | 49.4 | 48.5 | 58.8 | 1.176 | 50.8 | +0.035 | −171 |
| 2026 | Ness Ziona | 43.9 | 61.2 | 1.223 | 61.0 | 43.9 | 59.6 | 1.192 | 50.9 | +0.031 | −1 |
| 2026 | Hapoel Jerusalem | 43.1 | 57.6 | 1.152 | 50.4 | 45.1 | 56.1 | 1.122 | 50.0 | +0.030 | −76 |
| 2026 | Hapoel Holon | 42.9 | 60.6 | 1.212 | 54.7 | 43.5 | 60.8 | 1.215 | 53.4 | −0.003 | −20 |
| 2026 | Rishon Lezion | 45.4 | 60.7 | 1.214 | 47.9 | 42.3 | 61.1 | 1.223 | 51.3 | −0.008 | +114 |
| 2026 | Maccabi Ramat Gan | 46.3 | 56.7 | 1.133 | 48.8 | 43.1 | 59.2 | 1.185 | 50.9 | −0.052 | +30 |
| 2026 | Elizur Netanya | 49.5 | 57.7 | 1.154 | 45.8 | 47.9 | 60.7 | 1.213 | 55.8 | −0.059 | +15 |
| 2026 | Galil Elion | 47.6 | 52.9 | 1.058 | 51.0 | 41.0 | 58.9 | 1.178 | 57.1 | −0.120 | +93 |
| 2026 | Maccabi Raanana | 47.9 | 54.8 | 1.097 | 49.1 | 45.3 | 61.5 | 1.230 | 56.2 | −0.134 | +74 |
| 2026 | Beer Sheva | 42.7 | 56.4 | 1.128 | 54.0 | 49.6 | 63.2 | 1.263 | 53.5 | **−0.135** | −95 |

Per-shot-type FG% (lay-up vs dunk) per team: `output/team_interior_profiles.csv` + `scripts/analysis/layup_team_fixed.R`.

Reading notes:

- **Bnei Herzliya** owns the interior on both ends: best finishing (1.288 PPS/att, 61.7 lay-up FG%) **and** best defense (1.106 allowed) → net +0.182. Was −0.042 in 2025 — one-season, roster-driven swing.
- **Hapoel Haemek** = shot-selection profile: lowest interior share (38.2% of FGA) but converts (1.238), plus the league's best lay-up block rate on D (12.3%). Net +0.117.
- **Galil Elion / Maccabi Raanana**: force interior volume (47–48% share) they can't convert (~1.06–1.10 PPS).
- **Beer Sheva**: worst defense on both axes (opponents 49.6% interior share at 1.263 PPS) and stably bad across seasons (net −0.133 → −0.135).
- Defensive fingerprint via assisted share allowed: Hapoel TA's D forces self-created interior makes (lowest assisted share allowed both seasons); Galil Elion concedes ball-movement interior makes (57.1% assisted). Ness Ziona has the most assisted interior offense (61%).
- 2025 net leaders: Hapoel TA +0.185, MTA +0.143; worst Haifa −0.138.

Caveat: net PPS is per-attempt quality; check `rim_att_diff` for the volume side (e.g. Kiryat Ata 2026 wins per-attempt but concedes 171 more attempts than it takes).

## Player on/off interior splits (2026)

Method mirrors committed `onoff_mv.sql`: `lineups_lookup` (player × `lineup_hash` × `is_on_verdict`) joined to the action MV by `lineup_hash`, shot flags per (player, team, is_on, type_lineup). Threshold: ≥500 ON / ≥300 OFF poss → 137 players.

All 137 players: `output/layup_onoff_2026.csv`. Extremes (Δ = ON − OFF, percentage points):

**Shot diet — Δ team interior share of FGA:**

| player | team | on poss | share on | share off | Δ share | Δ int FG |
|---|---|---|---|---|---|---|
| Joirdon Nicholis | M. Ramat Gan | 811 | 51.2 | 37.4 | **+13.9** | +5.9 |
| Adam Ariel | M. Ramat Gan | 1613 | 48.1 | 39.9 | +8.2 | +2.0 |
| Tomer Ginat | Hapoel TA | 977 | 47.8 | 40.1 | +7.7 | −4.3 |
| Bar Timor | Hapoel TA | 1008 | 47.6 | 40.2 | +7.4 | +7.5 |
| Cyril Langevine | Galil Elion | 1713 | 49.7 | 42.8 | +6.9 | −1.0 |
| Jimmy Clark III | Maccabi TA | 1328 | 46.2 | 39.6 | +6.6 | +6.0 |
| DJ Burns | Rishon Lezion | 1448 | 47.5 | 41.4 | +6.1 | +9.4 |
| Chinanu Onuaku | Bnei Herzliya | 1067 | 46.4 | 40.3 | +6.1 | +6.6 |
| … | | | | | | |
| Darin Green Jr. | Hapoel Holon | 1113 | 39.1 | 46.3 | −7.3 | −7.2 |
| Golan Gutt | Rishon Lezion | 873 | 40.6 | 48.5 | −7.8 | −0.7 |
| Amit Alon | M. Ramat Gan | 665 | 40.8 | 48.7 | **−7.9** | +6.0 |

**Finishing quality — Δ team interior FG% (top/bottom):** L.J Figueroa (Beer Sheva) **+13.7** (58.1 on / 44.4 off), DJ Burns +9.4, C.J. Elleby +8.0, Bar Timor +7.5 / … / Joe Ragland −7.4, TJ Leaf −9.4, T.J Bickerstaff (Beer Sheva) **−15.7** (46.5 on / 62.2 off). Beer Sheva's interior problem is heavily lineup-dependent; Figueroa is also −7.6 on opponent interior FG — the strongest two-way interior signal in the league.

**Defense — Δ opponent interior FG% (best suppression):** Darius Hannah (Kiryat Ata) **−9.6**, Joaquin Szuchman −9.2, Evan Bruinsma −8.8, Yair Kravitz −8.0, Khalil Ahmad −7.9, Aaron Wheeler −7.9, Jemaya Neal −7.8, Bodie Hume −7.8. Kiryat Ata has 3 of the top 8 (Hannah, Neal, Hume) — roster-wide rim defense.

Standard on/off caveats: raw splits, no teammate/opponent adjustment.

## Shot location data: coordinates, zones & calibration (2026-07-12/13)

**Where coordinates live:** `parameters_coord_x/y` come from the raw JSON PBP into `actions_clean` (÷100 → court units), flow through `possessions`/`pws`, and are dropped at the MV layer. All three tables are cold storage, so the working source is `exports/cold/actions_clean.parquet`. Coverage is **100%** of shots. Joins to the MV on `(game_id, id)`: 57,314 / 57,398 matched (99.85%). ⚠ The cumulative parquet **lags the MV by ~8 recent games** (57,398 vs 58,399 shots, missing game_ids ~365–388) — cold-export timing gap.

**Coordinate quality (calibration):**

- The data is a **~0.15-unit click grid**: 57,398 shots land on only 3,674 distinct spots (104 x-values × 51 y-values). Two "magnet" cells — (7.5, 2.8) and (7.5, 2.24) — hold 1,828 shots (3.2%), suspected operator default clicks (`is_magnet_spot()` flags them).
- The court is **not true FIBA scale** (fitted 3pt arc ≈ 9.10 units vs a real 6.75 m) — absolute distances are meaningless; only zone membership is usable.
- **Dunks calibrate the error** (a dunk's true location is always at the rim): median click distance from the rim 0.32 units, 90th pct 1.40, 99th pct 2.56. 93.1% fall within r ≤ 1.45; only 1.3% beyond the paint half-width are true blunders. So: ±1 unit for ~90% of entries, ~1–2% nonsense tail.

**Zone classification, v1 → v2.** The first pass (v1) assigned zones by point-in-polygon against the painted court markings. Calibration exposed two v1 artifacts, fixed in `classify_shot_zone()` (v2):

| Rule | v1 (polygons) | v2 (classifier) | Why |
|---|---|---|---|
| Rim zone | painted RA shape (exists only above y = 1.2) | radial disc r ≤ 1.45 around the rim | RA's lower edge rejected under-rim clicks: **dunks in rim zone 69.8% → 93.1%**. The "30% of dunks mislocated" was geometry, not data. |
| 2 vs 3 | by coordinate vs the arc | by `parameters_points` (scoreboard truth) | eliminated the "(inside arc)" bucket — 4.6% of 3s were coordinate jitter, not tag errors |
| Corner vs arc | full arc-radius geometry | y-threshold + outer-strip x only | no dependence on the imprecise fitted arc radius |

**League zone overview (both seasons), v2 with v1 comparison:**

| Zone | Freq v1→v2 | FG% v1→v2 | PPS v1→v2 | Assisted (v2) |
|---|---|---|---|---|
| Rim (r≤1.45) | 12.3 → 13.5 | 73.7 → 75.2 | 1.475 → **1.503** | 52.5 |
| Corner 3 (L+R) | 16.4 → 17.3 | 36.6 → 36.4 | ~1.10 → 1.093 | **90.5** |
| Paint (non-rim) | 33.0 → 32.1 | 52.7 → 51.4 | 1.054 → 1.028 | 50.4 |
| Above Break 3 | 22.5 → 23.0 | 33.4 → 33.4 | 1.003 → 1.002 | 64.9 |
| Mid-Range | 15.9 → 14.1 | 37.1 → 37.6 | 0.780 → **0.752** | 34.5 |

Mid-range lost its smuggled-in mislocated 3s and got *worse* — the worst-shot verdict strengthened. The rim zone got purer and richer. Everything is stable per season (nothing moves > ~1pp between 2025 and 2026).

**How zones refine the original shot-type table** (comparison to the earlier data points):

- **Lay-up (56.0 FG% overall) hides a steep location gradient:** 69.5 in the rim disc / 52.4 in the rest of the paint / 33.3 outside. Unchanged v1→v2 (69.2/52.7/33.3) — a real property, not a boundary artifact. Three-quarters of lay-ups are recorded *outside* the immediate rim area.
- **2pt jump-shot (38.3 overall) is FLAT by location:** 38.4 in the paint vs 38.0 mid-range. A paint floater is the same bad trade as an 18-footer; location adds nothing to this shot type.
- **3pt (34.7 overall) splits into corner 36.4 vs above-break 33.4** (+3.0pp, ≈ +0.09 PPS, stable both seasons). Corners are 42.9% of all 3s and a 90.5%-assisted catch-and-shoot shot vs 64.9% above the break. Left vs right corner (35.7 vs 37.2) is noise.
- **Interior proxy validation:** lay-up/dunk/oop tags are **97.2% paint-wide accurate** but only 30.6% strictly rim-disc; conversely the rim disc is 98.9% proxy-tagged. The tag is a paint detector, not a rim detector.

**Tag–coordinate cross-validation (2026-07-13/14, shot charts):** the two independent operator inputs (type tag, click) agree ~97% per type, and the ~3% disagreements are boundary-adjacent, not noise — see `docs/shot_tag_vs_coord_disagreement.png` + `docs/shot_zone_classification_map.png` (scripts: `shot_tag_coord_disagreement_chart.R`, `shot_zone_classification_chart.R`).

- 3pt-tagged shots clicking inside the arc: **638 / 23,114 = 2.8%** (strict geometric test; supersedes the earlier v1-polygon "4.6%"), median depth inside the boundary 0.06 units (90th pct 0.22, max 0.38 ≈ 1–2 grid cells), FG 31.3 vs 34.8 agreeing — long wing 3s with a click one cell off the line. Only 5 two-pointers click outside the arc.
- Lay-ups clicking outside paint-wide: 701 / 22,870 = 3.1%, a shell tracing the key's edges (697 mid-range, 4 in 3pt zones), FG 33.5 vs 56.8 inside — on the smooth radial decay curve, i.e. real long floaters/runners. They do NOT cluster at the magnet spots (both are deep in the paint), ruling out operator default-click laziness.
- Disagreement cells shoot like their *type* predicts, not their host zone → tags are trustworthy; location's incremental value concentrates in lay-up finishing depth.

**Corner-3 redefinition (2026-07-14, user decision):** corner 3 = 3pt shot below the full-width horizontal line tangent to the top of the RA arc (`y ≤ corner3_height = 2.85`), replacing the corner-break-height + outer-strip rule in `classify_shot_zone()`. The old "corner" was really corner+wing. New numbers (both seasons): corner 3 = **2,139 att (9.3% of 3s), 39.2 FG% (~1.18 PPS)** vs above-break 34.2 — a **+5.0pp** premium (was +3.0pp under the wide cut), moving true corners ahead of paint-non-rim in the PPS ladder, second only to the rim. Accepted caveat: full-width line means baseline-jitter 3s at any x (even behind the backboard) count as corner. `output/three_pt_corner_split.csv` and the zone-overview corner rows above still reflect the OLD wide cut; assisted-share for the new cut not yet recomputed.

**Verdict — where location data helps vs distracts:**

- *Helps:* lay-up finishing gradients (rim vs paint), corner-vs-arc 3pt splits and roles, league expected-PPS-by-zone baselines (season-stable), catching tag/coordinate inconsistencies.
- *Distracts:* locating 2pt jumpers (tag suffices), continuous/absolute distances (grid + non-FIBA scale), L/R corner asymmetries, live-app features (coordinates are cold-storage-only and lag the ETL).

## Team-id namespaces & the "two Netanyas" incident

The apparent duplicate ELIZUR/ELITZUR NETANYA in 2026 was **not** real: roster/PBP `team_id` 8 = Elitzur Netanya in 2025 but **Hapoel Haemek in 2026**; a season-unaware name map (`MIN(team_name) GROUP BY team_id`) mislabeled Haemek and silently merged other recycled ids (7: Holon→Ramat Gan, 9: Herzliya→Ness Ziona, 11: Gilboa/Galil→Beer Sheva, 13: Ness Ziona→Raanana). Netanya is id 8→15; the ELITZUR→ELIZUR spelling flip is roster-feed noise.

Additionally, `schedule.team1/team2` uses a **separate id namespace** (1092–1124, 2109), also re-minted per season; the committed bridge is `schedule_team_dict` keyed `(game_year, team_id_schedule) → team_id_rosters` (used by `sched_long`). Map ad-hoc via same-game co-occurrence if needed.

The live app is structurally safe (global season selector; `fetch_teams_*(gy)` are season-scoped). The trap exists only for ad-hoc cross-season SQL.

## Key reusable SQL fragments

```sql
-- one row per shot, shooting team attribution
FROM basketball_test.df_pts_poss_lineups_longer_mv d
JOIN basketball_test.schedule s ON s.game_id = d.game_id
WHERE d.type = 'shot' AND d.type_lineup = 'offense'   -- 'defense' = defending team

-- season-scoped team names (the ONLY correct name map)
SELECT team_id, game_year, MIN(team_name) AS team_name
FROM basketball_test.full_rosters GROUP BY team_id, game_year

-- assisted flag for a made shot m
EXISTS (SELECT 1 FROM acts a WHERE a.type = 'assist'
        AND a.game_id = m.game_id AND a.parent_action_id = m.id)
```

## Shot generation vs offensive quality (regressions, 2026-07-15)

Question: is "generates lay-ups/dunks/threes" (the on/off shift in the team's shot diet) a useful metric for offensive rating / eFG / TS, and should it become an app section (Tabs 1/7)?

**Scripts:** `shotgen_player_onoff.R` (tag-based player ON/OFF panel: zone shares + PPP/eFG/TS deltas; eFG/TS joined from `player_advanced_stats_mv`), `shotgen_team_corr.R` (team-season mix + ratings + corner-3), `shotgen_regressions.R` (offense-tier models), `shotgen_collinearity.R` (VIFs, corner-3-in-regression), `shotgen_zone_regression.R` (location-based rerun via `classify_shot_zone()` v2).

### Player level (2026, n=137 @ ≥500 ON / ≥300 OFF poss; all offense-side)

- **Standalone: weak.** Δ(rim share) / Δ(3PA share) ON−OFF explain ~1–2% of the variance in Δ off rating, Δ eFG, or Δ TS (generation-only OLS p≈.3–.4). What moves on/off outcomes is conversion: Δ team 3pt FG% correlates 0.76–0.80 with all three outcomes, Δ rim FG% 0.45–0.68.
- **Controlled for finishing: real but second-order.** +1pp rim share ≈ +0.32 pts/100 (p=.009), +1pp 3PA share ≈ +0.23 (p=.044) — consistent with shot-value arithmetic. Generation and finishing deltas are nearly orthogonal (|r| ≤ .09), so they're genuinely separate axes.
- **Tier interaction (offensive-rating tiers top4/mid4/bottom6):** generation predicts Δ rating only on bottom-6 offenses — r = .52 (.44 eFG / .46 TS); slope +1.77 pts/100 per pp vs top4 (p<.001). Flat-to-negative on top/mid tiers. A "shot-diet fixer" only registers where the baseline diet is broken. (Single season; 2025 replication pending.)
- **Mediation via eFG:** (generation → eFG coef) × FF eFG weight 1.45 reproduces the direct generation → rating coef (0.28 vs 0.32 rim; 0.19 vs 0.23 3PA) → **~85–90% of generation's rating impact flows through eFG**.

### Team level (n=28 team-seasons; compositional shares, baseline = mid-range)

- **Tag-based:** dunk+oop share is the robust positive (+2.2 pts/100 per pp, p=.025, VIF 1.4); 3PA share positive; **lay-up share ~0 in regression and r = −0.42 raw**; mid share negative. Corner-3 share: no signal in any spec (p=.49–.72; only 9.3% of 3s under the v2 cut — too thin a slice). Single-number `xpps_mix` (shares × league per-type PPS) is significant vs all three outcomes (~+2.5 pts/100 per 0.01 xPPS) and sidesteps the collinearity.
- **Collinearity:** shares are compositional; VIF ≤ 3.2; the big trade-off is lay-up ↔ above-break-3 (r = −0.78), so lay-up's null is "can't tell" while dunk's positive is well-identified.
- **Zone-based rerun (resolves the lay-up puzzle).** League PPS: **RA 1.50** (75% FG) ≫ corner-3 1.17 > paint-non-RA 1.03 ≈ above-break-3 1.03 ≫ mid 0.75. RA share predicts eFG (+0.61/pp, p=.026) and TS (+0.61, p=.028), rating +1.09 (p=.12); **paint-non-RA share is worthless (+0.09, p=.91)**. The lay-up tag is mostly paint-non-RA volume → hence its null; dunks are ~all RA → hence their signal. Good offenses = RA + 3s (Maccabi TA 2026: RA 16.2 / paint 27.1 / AB3 41.3); bad = floater-zone volume (Galil Elion: RA 10.0 / paint 37.7). Zone VIFs: paint 5.25 ↔ AB3 4.32 (r = −0.82, individually "can't tell"); RA 1.33. Player-level zone deltas ≈ tag deltas (r = .94/.99) and standalone stays ~0 → conclusions robust to tag-vs-location definition. ⚠ zone shares use the parquet (lags MV ~8 games); outcomes cover all games.
- **Coordinate-noise sensitivity (`shotgen_zone_sensitivity.R`, 2026-07-16):** the RA-share → eFG coefficient survives every noise correction — magnet spots excluded 0.67 (p=.010, *stronger* than baseline 0.61), dunks force-assigned to RA 0.57 (p=.039), rim radius 1.25 → 0.68 (p=.016), radius 1.69 → 0.49 (p=.050), all-corrections 0.63 (p=.017). The monotone radius gradient (tighter rim ⇒ bigger per-pp coefficient) matches the smooth FG-by-radius decay — "true rim" is what carries the value, and the finding is not a boundary artifact. Magnet cells are 93% lay-up-tagged and their rate varies by team-season (3.8–5.0% at the top = operator habit), so **exclude `is_magnet_spot()` clicks in zone stats by default** — it removes a team-correlated bias and only sharpens results. No better classification scheme found or needed: points-first + y-cut corners + radial rim disc (minus magnets) is the accuracy ceiling this click grid supports; per the 2026-07-13 decision, no finer radial metrics.

### FF-weights cross-checks (878 team-games)

Diagnostics now built into `scripts/fit_ff_impact_weights.R`: refit reproduces `FF_IMPACT_WEIGHTS` exactly; factor VIFs 1.02–1.05 (FTR's small +0.13 is precisely estimated, unlike lay-up share); per-tier weights stable (eFG 1.42–1.44) → the est.± annotations are tier-safe. Reminder: the fit's R² = .948 is an accounting identity — conversion weights, not causal importance.

### Verdict for the app

Frame as a **descriptive "Shot Profile ON/OFF" section**, not an impact metric: shares + deltas, lay-up vs dunk split (or RA vs non-RA paint if coordinates ever reach the MVs), no corner-3 column. Optional headline: Δ expected-PPS of the generated mix. **No own est.± annotation** — generation's impact is already ~90% inside the eFG factor's annotation; showing both would double-count. Better: present the section as a decomposition of *why* eFG changed (mix vs finishing).

## Open threads

- Player-level interior finishing leaderboard (per-player FG%/PPS, not just on/off) — now upgradeable with the rim/paint gradient.
- Feed interior creation/suppression + zone expected-PPS into the Trends player-grading metric.
- Possible app feature: shot-type split columns in `onoff_default_mv` (flags are structurally identical to the existing fg2/fg3 ones) — design constraints in § Shot generation → Verdict.
- 2025 replication of the bottom-tier generation interaction; split-half stability test (does generation stabilize faster than FG%-based on/off?).
- If location features should ever go live: carry `parameters_coord_x/y` (or a precomputed zone column) into an MV — currently dropped at the MV layer.
- Fix the cold-export timing gap (parquet ~8 games behind the MV).
- Shot-clock derivation (`tmp_shotclock.R`) to separate "bad shot" from "late-clock bailout" — especially for the 35%-assisted 2pt jumper.

## Appendix: regression methods notes (2026-07-15/16 session)

Conversational material behind § Shot generation — model specs, interpretation rules, and discarded variants. Kept for reproducibility of the *reasoning*, not just the numbers.

### Player-level model specs (n = 137, one row per player×team, 2026, ≥500 ON / ≥300 OFF poss)

All variables are ON−OFF deltas. Outcome `d_ppp` = 100×(on_pts/on_poss − off_pts/off_poss), exact `onoff_mv.sql` recipe (`SUM(team_score)`/`SUM(final_end_poss)`). Generation predictors in pp of team FGA (`d_rim_share`, `d_fg3_share`); finishing predictors in pp of FG% (`d_rim_fg`, `d_fg3_fg`). eFG/TS outcomes joined from `player_advanced_stats_mv` (`off_on_efg`−`off_off_efg` etc.), ×100 → pp.

1. **M1 generation-only:** `d_ppp ~ d_rim_share + d_fg3_share`. The "is it a standalone metric?" test. R² = .014, ns.
2. **M2 + finishing:** adds `d_rim_fg + d_fg3_fg`. R² = .68. Each coefficient = effect holding the other three fixed. Generation "wakes up" here (+0.32/+0.23, both sig) because mix and conversion deltas are nearly uncorrelated (|r| ≤ .09) but conversion deltas are much larger — in M1 they sit in the error term and drown the mix signal. Sanity arithmetic: a made 3 = 3 pts at ~40% of attempts ⇒ +1pp 3pt FG% ≈ 3×0.40 ≈ 1.2 pts/100 vs fitted 1.06 — coherent. Swapping a mid attempt for rim/3 at league PPS ≈ +0.3–0.4 pts/100 vs fitted 0.32/0.23 — coherent.
3. **M3 tier fixed effects:** `+ tier` (top4/mid4/bottom6). Dummies shift intercepts only — tests confounding, not slope differences. Result: dummies ns, slopes unchanged ⇒ pooled result not confounded by team strength.
4. **M4 tier interaction:** `d_ppp ~ d_good_share × tier`. Lets the *slope* differ. Bottom-6 slope +1.77 pts/100 per pp above top4 (p<.001, offensive tiers).

**Superseded variant:** the first tier run used **net-rating** tiers (top4/mid4/bottom6 by `net_rtg`; Kiryat Ata & Elizur Netanya tied at rank 11) → interaction +1.20 (p=.017), per-tier r = −.06/−.10/+.38. Re-ranking by **off_ppp** (correct, since the metric is offense-only — Bnei Herzliya→top4, Haemek/Beer Sheva→mid4, Rishon→bottom6) strengthened it: +1.77 (p<.001), r = −.11/−.20/+.52. Offensive tiers are the version of record.

### Interpretation rules that came up

- **Correlated predictors violate no OLS assumption.** Only *perfect* collinearity is forbidden (hence mid_share omitted — all five shares sum to 100). Coefficients stay unbiased; the cost is **precision, not validity**, and the reported SEs/p-values already include the inflation. VIF = variance inflation factor; SE inflation = √VIF (lay-up's VIF 3.2 ⇒ SE ×1.8).
- Reading rule: **significant despite collinearity ⇒ trustworthy** (dunk share, VIF 1.4); **non-significant with high VIF ⇒ "can't tell", not "no effect"** (lay-up share vs above-break-3, r = −.78). Individual attribution between correlated shares is unstable at n=28; their *sum* (and the `xpps_mix` rollup) is the stable statement. Correlated predictors can also be jointly significant while individually weak — model R² and xpps carry that.
- **Compositional coefficients** (baseline = mid): each reads "swap 1pp of mid-range attempts for this type".
- **High R² by near-identity is not evidence.** `Δshares+ΔFG% → ΔeFG` hit R² .93 because eFG decomposes ≈ mechanically into mix × conversion; same for the FF fit's .948. Good for conversion weights, never for causal/strategic importance claims.
- **Same-possession caveat:** predictors and outcomes are computed from the same possessions ⇒ descriptive covariation, not out-of-sample prediction (rim makes sit in both `d_rim_fg` and `d_ppp`). The stricter test is split-half: predict H2 `d_ppp` from H1 generation — also the classic "mix stats stabilize faster than FG%" argument. Deferred (open thread).
- **FF-weights anova gotcha:** the joint F-test comparing factors-only vs factors×tier lumped tier *intercepts* in with slopes — its significance came from intercepts. The per-tier refits + individually-ns interactions are the correct slope evidence (eFG weight 1.42–1.44, stable).
- Session ops note: R analysis scripts must run from the repo root (relative `source()`/`output/` paths); Git Bash showed spurious "Segmentation fault" noise when the cwd was wrong — PowerShell + absolute script path + `Set-Location` repo root is the reliable launch pattern.
