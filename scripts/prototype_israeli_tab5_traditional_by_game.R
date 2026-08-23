# Reversible performance/parity prototype for the Israeli Tab 5 per-game fact.
#
# Creates, populates, and queries only a PostgreSQL TEMP table inside one
# rollback-only transaction. No persistent relation is created.

suppressPackageStartupMessages({
  library(DBI)
  library(RPostgres)
})

file_arg <- grep("^--file=", commandArgs(trailingOnly = FALSE), value = TRUE)
script_path <- if (length(file_arg)) sub("^--file=", "", file_arg[[1]]) else "scripts/x"
repo_root <- normalizePath(file.path(dirname(script_path), ".."),
                           winslash = "/", mustWork = TRUE)
readRenviron(file.path(repo_root, "app", ".Renviron"))

elapsed <- function(label, expr) {
  started <- proc.time()[["elapsed"]]
  value <- force(expr)
  seconds <- proc.time()[["elapsed"]] - started
  cat(sprintf("%-34s %.2fs\n", label, seconds))
  flush.console()
  list(value = value, seconds = seconds)
}

con <- dbConnect(
  Postgres(), host = Sys.getenv("PG_HOST"), port = 6543L,
  dbname = Sys.getenv("PG_DB"), user = Sys.getenv("PG_USER"),
  password = Sys.getenv("PG_PASS"), sslmode = Sys.getenv("PG_SSLMODE", "require"),
  bigint = "numeric", connect_timeout = 15L
)
on.exit(if (dbIsValid(con)) dbDisconnect(con), add = TRUE)

target <- dbGetQuery(con, "SELECT current_database() database, current_user username")
cat(sprintf("target database=%s user=%s client_path=pooler:6543\n",
            target$database[[1]], target$username[[1]]))

dbBegin(con)
on.exit(try(dbRollback(con), silent = TRUE), add = TRUE)
dbExecute(con, "SET LOCAL statement_timeout = '180s'")
dbExecute(con, "
CREATE TEMP TABLE tmp_player_traditional_by_game (
  game_year int4 NOT NULL,
  game_id int4 NOT NULL,
  team_id int4 NOT NULL,
  player_id int4 NOT NULL,
  has_actor_stats boolean NOT NULL,
  gp int4 NOT NULL,
  poss_on_floor numeric NOT NULL,
  minutes numeric NOT NULL,
  pts numeric NOT NULL,
  reb numeric NOT NULL,
  oreb numeric NOT NULL,
  dreb numeric NOT NULL,
  ast numeric NOT NULL,
  stl numeric NOT NULL,
  blk numeric NOT NULL,
  dfl numeric NOT NULL,
  tov numeric NOT NULL,
  fgm numeric NOT NULL,
  fga numeric NOT NULL,
  three_pm numeric NOT NULL,
  three_pa numeric NOT NULL,
  ftm numeric NOT NULL,
  fta numeric NOT NULL,
  player_ts_poss_count numeric NOT NULL,
  player_ft_parent_ids text[] NOT NULL,
  team_ts_poss_count numeric NOT NULL,
  team_ft_parent_ids text[] NOT NULL,
  team_tov numeric NOT NULL,
  team_poss numeric NOT NULL,
  exposure_player_ts_poss_count numeric NOT NULL,
  exposure_player_tov_count numeric NOT NULL
)")

build_sql <- "
INSERT INTO tmp_player_traditional_by_game
WITH sched_year AS (
  SELECT DISTINCT game_id, game_year
  FROM basketball_test.final_schedule_mv
),
lineup_map AS (
  SELECT DISTINCT game_id, team_id, lineup_hash, player_id
  FROM basketball_test.lineups_lookup
  WHERE game_year IS NOT NULL
    AND COALESCE(is_on_verdict, 0)::int = 1
),
complex_flags AS (
  SELECT DISTINCT ON (d.game_id, d.id)
    d.game_id,
    d.id AS main_id,
    t2.type AS parent_type,
    t2.parameters_type AS parent_param
  FROM basketball_test.df_pts_poss_lineups_longer_mv d
  JOIN basketball_test.df_pts_poss_lineups_longer_mv t2
    ON t2.game_id = d.game_id
   AND t2.id = d.parent_action_id
   AND t2.type = 'foul'
  WHERE d.parent_action_id IS NOT NULL
  ORDER BY d.game_id, d.id
),
actor_game AS (
  SELECT
    sy.game_year,
    d.game_id,
    d.team_id,
    d.player_id,
    SUM(CASE WHEN d.type = 'shot' AND d.parameters_made = 'made' AND d.type_lineup = 'offense'
             THEN COALESCE(d.parameters_points, 0) ELSE 0 END)
      + SUM(CASE WHEN d.type = 'freeThrow' AND d.parameters_made = 'made' AND d.type_lineup = 'offense'
                 THEN 1 ELSE 0 END) AS pts,
    SUM(CASE WHEN d.type = 'rebound' AND d.type_lineup = 'offense' AND d.parameters_type = 'offensive'
             THEN 1 ELSE 0 END) AS oreb,
    SUM(CASE WHEN d.type = 'rebound' AND d.type_lineup = 'defense' AND d.parameters_type = 'defensive'
             THEN 1 ELSE 0 END) AS dreb,
    SUM(CASE WHEN d.type = 'assist' AND d.type_lineup = 'offense' THEN 1 ELSE 0 END) AS ast,
    SUM(CASE WHEN d.type = 'steal' AND d.type_lineup = 'defense' THEN 1 ELSE 0 END) AS stl,
    SUM(CASE WHEN d.type = 'block' AND d.type_lineup = 'defense' THEN 1 ELSE 0 END) AS blk,
    SUM(CASE WHEN d.type = 'deflection' AND d.type_lineup = 'defense' THEN 1 ELSE 0 END) AS dfl,
    SUM(CASE WHEN d.type = 'turnover' AND d.type_lineup = 'offense' THEN 1 ELSE 0 END) AS tov,
    SUM(CASE WHEN d.type = 'shot' AND d.parameters_made = 'made' AND d.type_lineup = 'offense'
             THEN 1 ELSE 0 END) AS fgm,
    SUM(CASE WHEN d.type = 'shot' AND d.type_lineup = 'offense' THEN 1 ELSE 0 END) AS fga,
    SUM(CASE WHEN d.type = 'shot' AND d.parameters_made = 'made' AND d.parameters_points = 3
                  AND d.type_lineup = 'offense' THEN 1 ELSE 0 END) AS three_pm,
    SUM(CASE WHEN d.type = 'shot' AND d.parameters_points = 3 AND d.type_lineup = 'offense'
             THEN 1 ELSE 0 END) AS three_pa,
    SUM(CASE WHEN d.type = 'freeThrow' AND d.parameters_made = 'made' AND d.type_lineup = 'offense'
             THEN 1 ELSE 0 END) AS ftm,
    SUM(CASE WHEN d.type = 'freeThrow' AND d.type_lineup = 'offense' THEN 1 ELSE 0 END) AS fta,
    COUNT(CASE WHEN d.type = 'shot' AND d.type_lineup = 'offense' THEN 1 END)
      + COUNT(DISTINCT CASE
          WHEN d.type = 'freeThrow' AND d.type_lineup = 'offense'
           AND cf.parent_type = 'foul' AND cf.parent_param = 'personal'
          THEN d.parent_action_id END) AS player_ts_poss_count,
    COALESCE(array_agg(DISTINCT d.parent_action_id::text) FILTER (
      WHERE d.type = 'freeThrow' AND d.type_lineup = 'offense'
        AND cf.parent_type = 'foul' AND cf.parent_param = 'personal'
    ), ARRAY[]::text[]) AS player_ft_parent_ids
  FROM basketball_test.df_pts_poss_lineups_longer_mv d
  JOIN sched_year sy ON sy.game_id = d.game_id
  LEFT JOIN complex_flags cf ON cf.game_id = d.game_id AND cf.main_id = d.id
  WHERE d.player_id IS NOT NULL AND d.player_id > 0
  GROUP BY sy.game_year, d.game_id, d.team_id, d.player_id
),
poss_end AS (
  SELECT DISTINCT
    sy.game_year,
    d.game_id,
    d.team_id,
    d.lineup_hash,
    d.id AS poss_end_id
  FROM basketball_test.df_pts_poss_lineups_longer_mv d
  JOIN sched_year sy ON sy.game_id = d.game_id
  WHERE d.type_lineup = 'offense'
    AND d.final_end_poss
    AND d.id IS NOT NULL
    AND d.lineup_hash IS NOT NULL
),
usage_totals AS (
  SELECT
    pe.game_year,
    pe.game_id,
    lm.team_id,
    lm.player_id,
    1::int AS gp,
    COUNT(DISTINCT (pe.game_id, pe.team_id, pe.poss_end_id)) AS poss_on_floor
  FROM poss_end pe
  JOIN lineup_map lm
    ON lm.game_id = pe.game_id
   AND lm.team_id = pe.team_id
   AND lm.lineup_hash = pe.lineup_hash
  GROUP BY pe.game_year, pe.game_id, lm.team_id, lm.player_id
),
team_possession_totals AS (
  SELECT
    game_year,
    game_id,
    team_id,
    COUNT(DISTINCT (game_id, team_id, poss_end_id)) AS team_poss
  FROM poss_end
  GROUP BY game_year, game_id, team_id
),
segment_times AS (
  SELECT
    sy.game_year,
    d.game_id,
    d.team_id,
    d.lineup_hash,
    d.segment_id,
    MAX(d.segment_seconds) AS seg_seconds
  FROM basketball_test.df_pts_poss_lineups_longer_mv d
  JOIN sched_year sy ON sy.game_id = d.game_id
  WHERE d.lineup_hash IS NOT NULL
    AND d.segment_id IS NOT NULL
    AND d.segment_seconds IS NOT NULL
  GROUP BY sy.game_year, d.game_id, d.team_id, d.lineup_hash, d.segment_id
),
minutes_totals AS (
  SELECT
    st.game_year,
    st.game_id,
    lm.team_id,
    lm.player_id,
    SUM(st.seg_seconds) / 60.0 AS minutes
  FROM segment_times st
  JOIN lineup_map lm
    ON lm.game_id = st.game_id
   AND lm.team_id = st.team_id
   AND lm.lineup_hash = st.lineup_hash
  GROUP BY st.game_year, st.game_id, lm.team_id, lm.player_id
),
player_exposure AS (
  SELECT
    COALESCE(u.game_year, m.game_year) AS game_year,
    COALESCE(u.game_id, m.game_id) AS game_id,
    COALESCE(u.team_id, m.team_id) AS team_id,
    COALESCE(u.player_id, m.player_id) AS player_id,
    COALESCE(u.gp, 0) AS gp,
    COALESCE(u.poss_on_floor, 0) AS poss_on_floor,
    COALESCE(m.minutes, 0) AS minutes
  FROM usage_totals u
  FULL JOIN minutes_totals m
    ON m.game_year = u.game_year AND m.game_id = u.game_id
   AND m.team_id = u.team_id AND m.player_id = u.player_id
),
team_usage AS (
  SELECT
    sy.game_year,
    d.game_id,
    d.team_id,
    COUNT(CASE WHEN d.type = 'shot' AND d.type_lineup = 'offense' THEN 1 END)
      + COUNT(DISTINCT CASE
          WHEN d.type = 'freeThrow' AND d.type_lineup = 'offense'
           AND cf.parent_type = 'foul' AND cf.parent_param = 'personal'
          THEN d.parent_action_id END) AS team_ts_poss_count,
    COALESCE(array_agg(DISTINCT d.parent_action_id::text) FILTER (
      WHERE d.type = 'freeThrow' AND d.type_lineup = 'offense'
        AND cf.parent_type = 'foul' AND cf.parent_param = 'personal'
    ), ARRAY[]::text[]) AS team_ft_parent_ids,
    SUM(CASE WHEN d.type = 'turnover' AND d.type_lineup = 'offense' THEN 1 ELSE 0 END) AS team_tov,
    MAX(COALESCE(tp.team_poss, 0)) AS team_poss
  FROM basketball_test.df_pts_poss_lineups_longer_mv d
  JOIN sched_year sy ON sy.game_id = d.game_id
  LEFT JOIN complex_flags cf ON cf.game_id = d.game_id AND cf.main_id = d.id
  LEFT JOIN team_possession_totals tp
    ON tp.game_year = sy.game_year AND tp.game_id = d.game_id AND tp.team_id = d.team_id
  GROUP BY sy.game_year, d.game_id, d.team_id
)
SELECT
  COALESCE(a.game_year, e.game_year)::int4,
  COALESCE(a.game_id, e.game_id)::int4,
  COALESCE(a.team_id, e.team_id)::int4,
  COALESCE(a.player_id, e.player_id)::int4,
  (a.player_id IS NOT NULL) AS has_actor_stats,
  COALESCE(e.gp, 0)::int4 AS gp,
  COALESCE(e.poss_on_floor, 0)::numeric AS poss_on_floor,
  COALESCE(e.minutes, 0)::numeric AS minutes,
  COALESCE(a.pts, 0)::numeric,
  COALESCE(a.oreb + a.dreb, 0)::numeric AS reb,
  COALESCE(a.oreb, 0)::numeric,
  COALESCE(a.dreb, 0)::numeric,
  COALESCE(a.ast, 0)::numeric,
  COALESCE(a.stl, 0)::numeric,
  COALESCE(a.blk, 0)::numeric,
  COALESCE(a.dfl, 0)::numeric,
  COALESCE(a.tov, 0)::numeric,
  COALESCE(a.fgm, 0)::numeric,
  COALESCE(a.fga, 0)::numeric,
  COALESCE(a.three_pm, 0)::numeric,
  COALESCE(a.three_pa, 0)::numeric,
  COALESCE(a.ftm, 0)::numeric,
  COALESCE(a.fta, 0)::numeric,
  COALESCE(a.player_ts_poss_count, 0)::numeric,
  COALESCE(a.player_ft_parent_ids, ARRAY[]::text[]),
  COALESCE(t.team_ts_poss_count, 0)::numeric AS team_ts_poss_count,
  COALESCE(t.team_ft_parent_ids, ARRAY[]::text[]) AS team_ft_parent_ids,
  COALESCE(t.team_tov, 0)::numeric AS team_tov,
  COALESCE(t.team_poss, 0)::numeric AS team_poss,
  COALESCE(a.player_ts_poss_count, 0)::numeric AS exposure_player_ts_poss_count,
  COALESCE(a.tov, 0)::numeric AS exposure_player_tov_count
FROM actor_game a
FULL JOIN player_exposure e
  ON e.game_year = a.game_year AND e.game_id = a.game_id
 AND e.team_id = a.team_id AND e.player_id = a.player_id
LEFT JOIN team_usage t
  ON t.game_year = COALESCE(a.game_year, e.game_year)
 AND t.game_id = COALESCE(a.game_id, e.game_id)
 AND t.team_id = COALESCE(a.team_id, e.team_id)
WHERE COALESCE(e.gp, 0) > 0
   OR COALESCE(e.poss_on_floor, 0) > 0
   OR COALESCE(e.minutes, 0) > 0
"

build <- elapsed("temporary table build", dbExecute(con, build_sql))

profile <- dbGetQuery(con, "
SELECT count(*) AS rows,
       count(DISTINCT game_id) AS games,
       pg_total_relation_size('pg_temp.tmp_player_traditional_by_game'::regclass) AS total_bytes
FROM tmp_player_traditional_by_game")
cat(sprintf("rows=%s games=%s total_bytes=%s (%.2f MiB)\n",
            profile$rows[[1]], profile$games[[1]], profile$total_bytes[[1]],
            as.numeric(profile$total_bytes[[1]]) / 1024^2))

bounds <- dbGetQuery(con, "
SELECT min(game_date) AS mn, max(game_date) AS mx
FROM basketball_test.final_schedule_mv WHERE game_year = 2026")
season_start <- as.Date(bounds$mn[[1]])
season_end <- as.Date(bounds$mx[[1]])
season_mid <- season_start + as.integer((season_end - season_start) * 0.5)

filtered <- elapsed("second-half aggregate", dbGetQuery(con, "
WITH eligible_games AS (
  SELECT DISTINCT game_id
  FROM basketball_test.final_schedule_mv
  WHERE game_year = $1 AND game_date BETWEEN $2 AND $3
)
SELECT
  p.team_id,
  p.player_id,
  SUM(p.gp)::int AS gp,
  SUM(p.poss_on_floor) AS poss_on_floor,
  ROUND(SUM(p.minutes), 1) AS minutes,
  SUM(p.pts)::int AS pts,
  SUM(p.reb)::int AS reb,
  SUM(p.ast)::int AS ast,
  SUM(p.stl)::int AS stl,
  SUM(p.blk)::int AS blk,
  SUM(p.dfl)::int AS dfl,
  SUM(p.tov)::int AS tov,
  SUM(p.fgm)::int AS fgm,
  SUM(p.fga)::int AS fga,
  SUM(p.three_pm)::int AS three_pm,
  SUM(p.three_pa)::int AS three_pa,
  SUM(p.ftm)::int AS ftm,
  SUM(p.fta)::int AS fta,
  CASE WHEN SUM(p.fga) > 0 THEN ROUND(100 * SUM(p.fgm) / SUM(p.fga), 1) END AS fg_pct,
  CASE WHEN SUM(p.three_pa) > 0 THEN ROUND(100 * SUM(p.three_pm) / SUM(p.three_pa), 1) END AS tp_pct,
  CASE WHEN SUM(p.fta) > 0 THEN ROUND(100 * SUM(p.ftm) / SUM(p.fta), 1) END AS ft_pct,
  CASE WHEN SUM(p.fga) > 0 THEN ROUND(100 * (SUM(p.fgm) + 0.5 * SUM(p.three_pm)) / SUM(p.fga), 1) END AS efg,
  CASE WHEN SUM(p.fga) + 0.44 * SUM(p.fta) > 0
       THEN ROUND(100 * SUM(p.pts) / (2 * (SUM(p.fga) + 0.44 * SUM(p.fta))), 1) END AS ts,
  CASE WHEN SUM(p.player_ts_poss_count + p.tov) > 0
         AND SUM(p.team_ts_poss_count + p.team_tov) > 0
         AND SUM(p.poss_on_floor) > 0 AND SUM(p.team_poss) > 0
       THEN ROUND(100 * SUM(p.player_ts_poss_count + p.tov) * SUM(p.team_poss)
                  / (SUM(p.team_ts_poss_count + p.team_tov) * SUM(p.poss_on_floor)), 1) END AS usg_pct
FROM tmp_player_traditional_by_game p
JOIN eligible_games g USING (game_id)
WHERE p.game_year = $1
GROUP BY p.team_id, p.player_id
HAVING bool_or(p.has_actor_stats)
", params = list(2026L, season_mid, season_end)))
cat(sprintf("second_half_start=%s rows=%d\n", season_mid, nrow(filtered$value)))

parity <- elapsed("full-season parity", dbGetQuery(con, "
WITH team_game AS (
  SELECT game_year, game_id, team_id,
         MAX(team_ts_poss_count) AS team_ts_poss_count,
         MAX(team_tov) AS team_tov,
         MAX(team_poss) AS team_poss
  FROM tmp_player_traditional_by_game
  GROUP BY game_year, game_id, team_id
),
team_totals AS (
  SELECT game_year, team_id,
         SUM(team_ts_poss_count) AS team_ts_poss_count,
         SUM(team_tov) AS team_tov,
         SUM(team_poss) AS team_poss
  FROM team_game
  GROUP BY game_year, team_id
),
candidate AS (
  SELECT
    p.game_year, p.team_id, p.player_id,
    SUM(gp)::int AS gp,
    SUM(poss_on_floor)::int AS poss_on_floor,
    ROUND(SUM(minutes), 1)::numeric(10,1) AS minutes,
    SUM(pts)::int AS pts,
    SUM(reb)::int AS reb,
    SUM(oreb)::int AS oreb,
    SUM(dreb)::int AS dreb,
    SUM(ast)::int AS ast,
    SUM(stl)::int AS stl,
    SUM(blk)::int AS blk,
    SUM(dfl)::int AS dfl,
    SUM(tov)::int AS tov,
    SUM(fgm)::int AS fgm,
    SUM(fga)::int AS fga,
    SUM(three_pm)::int AS three_pm,
    SUM(three_pa)::int AS three_pa,
    SUM(ftm)::int AS ftm,
    SUM(fta)::int AS fta,
    CASE WHEN SUM(fga) > 0 THEN ROUND(100 * SUM(fgm) / SUM(fga), 1) END AS fg_pct,
    CASE WHEN SUM(three_pa) > 0 THEN ROUND(100 * SUM(three_pm) / SUM(three_pa), 1) END AS tp_pct,
    CASE WHEN SUM(fta) > 0 THEN ROUND(100 * SUM(ftm) / SUM(fta), 1) END AS ft_pct,
    CASE WHEN SUM(fga) > 0 THEN ROUND(100 * (SUM(fgm) + 0.5 * SUM(three_pm)) / SUM(fga), 1) END AS efg,
    CASE WHEN SUM(fga) + 0.44 * SUM(fta) > 0
         THEN ROUND(100 * SUM(pts) / (2 * (SUM(fga) + 0.44 * SUM(fta))), 1) END AS ts,
    CASE WHEN SUM(p.player_ts_poss_count + p.tov) > 0
           AND (t.team_ts_poss_count + t.team_tov) > 0
           AND SUM(p.poss_on_floor) > 0 AND t.team_poss > 0
         THEN ROUND(100 * SUM(p.player_ts_poss_count + p.tov) * t.team_poss
                    / ((t.team_ts_poss_count + t.team_tov) * SUM(p.poss_on_floor)), 1) END AS usg_pct
  FROM tmp_player_traditional_by_game p
  JOIN team_totals t ON t.game_year = p.game_year AND t.team_id = p.team_id
  GROUP BY p.game_year, p.team_id, p.player_id,
           t.team_ts_poss_count, t.team_tov, t.team_poss
  HAVING bool_or(p.has_actor_stats)
),
comparison AS (
  SELECT
    COALESCE(c.game_year, m.game_year) AS game_year,
    COALESCE(c.team_id, m.team_id) AS team_id,
    COALESCE(c.player_id, m.player_id) AS player_id,
    (c.player_id IS NULL) AS missing_candidate,
    (m.player_id IS NULL) AS extra_candidate,
    concat_ws(',',
      CASE WHEN c.gp IS DISTINCT FROM m.gp THEN 'gp' END,
      CASE WHEN c.poss_on_floor IS DISTINCT FROM m.poss_on_floor THEN 'poss' END,
      CASE WHEN c.minutes IS DISTINCT FROM m.minutes THEN 'minutes' END,
      CASE WHEN c.pts IS DISTINCT FROM m.pts THEN 'pts' END,
      CASE WHEN c.reb IS DISTINCT FROM m.reb THEN 'reb' END,
      CASE WHEN c.oreb IS DISTINCT FROM m.oreb THEN 'oreb' END,
      CASE WHEN c.dreb IS DISTINCT FROM m.dreb THEN 'dreb' END,
      CASE WHEN c.ast IS DISTINCT FROM m.ast THEN 'ast' END,
      CASE WHEN c.stl IS DISTINCT FROM m.stl THEN 'stl' END,
      CASE WHEN c.blk IS DISTINCT FROM m.blk THEN 'blk' END,
      CASE WHEN c.dfl IS DISTINCT FROM m.dfl THEN 'dfl' END,
      CASE WHEN c.tov IS DISTINCT FROM m.tov THEN 'tov' END,
      CASE WHEN c.fgm IS DISTINCT FROM m.fgm THEN 'fgm' END,
      CASE WHEN c.fga IS DISTINCT FROM m.fga THEN 'fga' END,
      CASE WHEN c.three_pm IS DISTINCT FROM m.\"3pm\" THEN '3pm' END,
      CASE WHEN c.three_pa IS DISTINCT FROM m.\"3pa\" THEN '3pa' END,
      CASE WHEN c.ftm IS DISTINCT FROM m.ftm THEN 'ftm' END,
      CASE WHEN c.fta IS DISTINCT FROM m.fta THEN 'fta' END,
      CASE WHEN c.fg_pct IS DISTINCT FROM m.fg_pct THEN 'fg_pct' END,
      CASE WHEN c.tp_pct IS DISTINCT FROM m.tp_pct THEN 'tp_pct' END,
      CASE WHEN c.ft_pct IS DISTINCT FROM m.ft_pct THEN 'ft_pct' END,
      CASE WHEN c.efg IS DISTINCT FROM m.efg THEN 'efg' END,
      CASE WHEN c.ts IS DISTINCT FROM m.ts THEN 'ts' END,
      CASE WHEN c.usg_pct IS DISTINCT FROM m.usg_pct THEN 'usg_pct' END
    ) AS differences
  FROM candidate c
  FULL JOIN basketball_test.player_traditional_stats_mv m
    ON m.game_year = c.game_year AND m.team_id = c.team_id AND m.player_id = c.player_id
)
SELECT
  count(*) AS compared_rows,
  count(*) FILTER (WHERE missing_candidate) AS missing_candidate,
  count(*) FILTER (WHERE extra_candidate) AS extra_candidate,
  count(*) FILTER (WHERE differences <> '') AS differing_rows,
  count(*) FILTER (WHERE differences LIKE '%minutes%') AS minutes_differences,
  count(*) FILTER (WHERE differences LIKE '%usg_pct%') AS usg_differences,
  string_agg(DISTINCT differences, ' | ' ORDER BY differences)
    FILTER (WHERE differences <> '') AS difference_sets
FROM comparison
"))
print(parity$value, row.names = FALSE)

denominator_check <- dbGetQuery(con, "
SELECT
  count(*) AS rows,
  count(*) FILTER (WHERE player_ts_poss_count <> exposure_player_ts_poss_count) AS ts_numerator_mismatches,
  count(*) FILTER (WHERE tov <> exposure_player_tov_count) AS tov_numerator_mismatches
FROM tmp_player_traditional_by_game")
print(denominator_check, row.names = FALSE)

id_scope <- dbGetQuery(con, "
WITH player_ft AS (
  SELECT p.game_year, p.game_id, p.team_id, p.player_id, trip.parent_id
  FROM tmp_player_traditional_by_game p
  CROSS JOIN LATERAL unnest(p.player_ft_parent_ids) AS trip(parent_id)
),
player_scope AS (
  SELECT game_year, team_id, player_id,
         count(DISTINCT parent_id) AS legacy_count,
         count(DISTINCT (game_id, parent_id)) AS game_scoped_count
  FROM player_ft
  GROUP BY game_year, team_id, player_id
),
team_game AS (
  SELECT DISTINCT game_year, game_id, team_id, team_ft_parent_ids
  FROM tmp_player_traditional_by_game
),
team_ft AS (
  SELECT t.game_year, t.game_id, t.team_id, trip.parent_id
  FROM team_game t
  CROSS JOIN LATERAL unnest(t.team_ft_parent_ids) AS trip(parent_id)
),
team_scope AS (
  SELECT game_year, team_id,
         count(DISTINCT parent_id) AS legacy_count,
         count(DISTINCT (game_id, parent_id)) AS game_scoped_count
  FROM team_ft
  GROUP BY game_year, team_id
)
SELECT
  (SELECT count(*) FROM player_scope WHERE legacy_count <> game_scoped_count)
    AS affected_player_seasons,
  (SELECT COALESCE(sum(game_scoped_count - legacy_count), 0)
     FROM player_scope WHERE legacy_count <> game_scoped_count)
    AS player_trip_undercount,
  (SELECT count(*) FROM team_scope WHERE legacy_count <> game_scoped_count)
    AS affected_team_seasons,
  (SELECT COALESCE(sum(game_scoped_count - legacy_count), 0)
     FROM team_scope WHERE legacy_count <> game_scoped_count)
    AS team_trip_undercount
")
cat("free_throw_parent_id_scope:\n")
print(id_scope, row.names = FALSE)

mismatch_samples <- dbGetQuery(con, "
WITH team_game AS (
  SELECT game_year, game_id, team_id,
         MAX(team_ts_poss_count) AS team_ts_poss_count,
         MAX(team_tov) AS team_tov,
         MAX(team_poss) AS team_poss
  FROM tmp_player_traditional_by_game
  GROUP BY game_year, game_id, team_id
),
team_totals AS (
  SELECT game_year, team_id,
         SUM(team_ts_poss_count) AS team_ts_poss_count,
         SUM(team_tov) AS team_tov,
         SUM(team_poss) AS team_poss
  FROM team_game
  GROUP BY game_year, team_id
),
candidate AS (
  SELECT
    p.game_year, p.team_id, p.player_id,
    ROUND(SUM(minutes), 1)::numeric(10,1) AS minutes,
    SUM(reb)::int AS reb,
    SUM(dreb)::int AS dreb,
    CASE WHEN SUM(p.player_ts_poss_count + p.tov) > 0
           AND (t.team_ts_poss_count + t.team_tov) > 0
           AND SUM(p.poss_on_floor) > 0 AND t.team_poss > 0
         THEN ROUND(100 * SUM(p.player_ts_poss_count + p.tov) * t.team_poss
                    / ((t.team_ts_poss_count + t.team_tov) * SUM(p.poss_on_floor)), 1) END AS usg_pct
  FROM tmp_player_traditional_by_game p
  JOIN team_totals t ON t.game_year = p.game_year AND t.team_id = p.team_id
  GROUP BY p.game_year, p.team_id, p.player_id,
           t.team_ts_poss_count, t.team_tov, t.team_poss
  HAVING bool_or(p.has_actor_stats)
)
SELECT
  c.game_year, c.team_id, c.player_id, m.player_name,
  c.minutes AS candidate_minutes, m.minutes AS mv_minutes,
  c.reb AS candidate_reb, m.reb AS mv_reb,
  c.dreb AS candidate_dreb, m.dreb AS mv_dreb,
  c.usg_pct AS candidate_usg, m.usg_pct AS mv_usg
FROM candidate c
JOIN basketball_test.player_traditional_stats_mv m
  ON m.game_year = c.game_year AND m.team_id = c.team_id AND m.player_id = c.player_id
WHERE c.minutes IS DISTINCT FROM m.minutes
   OR c.reb IS DISTINCT FROM m.reb
   OR c.dreb IS DISTINCT FROM m.dreb
   OR c.usg_pct IS DISTINCT FROM m.usg_pct
ORDER BY
  (c.reb IS DISTINCT FROM m.reb OR c.dreb IS DISTINCT FROM m.dreb) DESC,
  abs(COALESCE(c.usg_pct, 0) - COALESCE(m.usg_pct, 0)) DESC,
  c.game_year, c.team_id, c.player_id
LIMIT 20
")
cat("mismatch_samples:\n")
print(mismatch_samples, row.names = FALSE)

dbRollback(con)
cat("rollback=complete temporary_table=persisted:false\n")
