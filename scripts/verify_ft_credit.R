# Baseline snapshot and gates for the free-throw credit migration
# (docs/superpowers/plans/2026-10-12-free-throw-foul-lineup-attribution.md).
#
#   Rscript scripts/verify_ft_credit.R --snapshot [--force]   copy every derived object and
#                                                             reader-function result aside
#   Rscript scripts/verify_ft_credit.R --check-base           G1: credit columns well formed
#   Rscript scripts/verify_ft_credit.R --check <object>       G2 totals, G3 trips, G4 time
#   Rscript scripts/verify_ft_credit.R --check-fn <function>  G6: reader function vs snapshot
#   Rscript scripts/verify_ft_credit.R --drop                 remove the baseline schema
#
# Read-only apart from the ft_credit_migration schema. Connects as the ETL
# role on the session-mode port. Exits non-zero when a gate fails.

args <- commandArgs(trailingOnly = TRUE)
readRenviron("etl/.Renviron")
suppressPackageStartupMessages({ library(DBI); library(RPostgres) })

con <- dbConnect(
  Postgres(),
  host = Sys.getenv("PG_HOST"), port = 5432L,
  dbname = Sys.getenv("PG_DB"), user = Sys.getenv("PG_USER"),
  password = Sys.getenv("PG_PASS"), sslmode = Sys.getenv("PG_SSLMODE", "require"),
  connect_timeout = 15L, bigint = "numeric"
)
invisible(dbExecute(con, "SET statement_timeout = '15min'", immediate = TRUE))
invisible(dbExecute(con, "SET client_min_messages = warning", immediate = TRUE))

SCHEMA <- "basketball_test"
BASE <- "basketball_test.df_pts_poss_lineups_longer_mv"
SNAP <- "ft_credit_migration"
q <- function(sql) dbGetQuery(con, sql, immediate = TRUE)
x <- function(sql) invisible(dbExecute(con, sql, immediate = TRUE))
ident <- function(cols) paste(sprintf('"%s"', cols), collapse = ", ")

team_box <- c("pts", "reb", "ast", "stl", "blk", "dfl", "tov", "fgm", "fga", "3pm", "3pa", "ftm", "fta")
team_raw <- c("off_points_raw", "def_points_raw", "off_poss_raw", "def_poss_raw",
              "off_oreb_count_raw", "def_oreb_count_raw", "off_oreb_opp_raw", "def_oreb_opp_raw",
              "off_tov_raw", "def_tov_raw", "off_fta_raw", "def_fta_raw", "off_fga_raw",
              "def_fga_raw", "off_fgm_raw", "def_fgm_raw", "off_fg3m_raw", "def_fg3m_raw")
player_box <- c("pts", "reb", "oreb", "dreb", "ast", "stl", "blk", "dfl", "tov", "fgm", "fga",
                "3pm", "3pa", "ftm", "fta")
ratings_counts <- c("off_poss", "def_poss", "off_fga", "off_layup_att", "off_dunk_att", "off_fg3_att",
                    "off_c3_att", "off_c3_known_att", "def_fga", "def_layup_att", "def_dunk_att",
                    "def_fg3_att", "def_c3_att", "def_c3_known_att")

# key      : grain at which re-crediting must leave every `additive` sum unchanged
# additive : count columns (or SQL expressions) summed per key
# trips    : c(offense = <col>, defense = <col>) checked against distinct trips (game grain only)
# time     : minutes columns; time_key is the grain at which they must not move
object_map <- list(
  lineup_four_factors_by_game = list(
    key = c("game_id", "team_id", "type_lineup"),
    additive = c("total_points", "total_poss", "oreb_count", "oreb_opportunities", "tov_count",
                 "total_ft_attempts", "total_fga", "total_fgm", "total_fg3_made"),
    trips_long = "ts_poss_count",
    time = "minutes",
    time_key = c("game_id", "team_id", "lineup_hash", "type_lineup", "opp_starters")),
  player_four_factors_by_game = list(
    key = c("game_id", "team_id", "player_id", "type_lineup"),
    additive = c("total_points", "total_poss", "oreb_count", "oreb_opportunities", "tov_count",
                 "steal_count", "deflection_count", "total_ft_attempts", "total_fga", "total_fgm",
                 "total_fg3_made", "player_ts_poss_count", "player_tov_count", "fg2_made", "fg2_att",
                 "fg3_made", "fg3_att", "layup_made", "layup_att", "dunk_made", "dunk_att",
                 "c3_made", "c3_att", "c3_known_att"),
    trips_long = "ts_poss_count",
    time = c("minutes", "onoff_minutes"),
    time_key = c("game_id", "team_id", "player_id", "is_on_key", "type_lineup",
                 "num_starters", "own_starters", "opp_starters")),
  team_metrics_by_game_mv = list(
    key = c("game_id", "team_id"), additive = c(team_raw, team_box),
    trips = c(offense = "off_ts_poss_raw", defense = "def_ts_poss_raw"),
    time = c("off_minutes", "def_minutes"), time_key = c("game_id", "team_id")),
  team_metrics_rolling_mv = list(
    key = c("game_id", "team_id"), additive = c(team_raw, team_box),
    trips = c(offense = "off_ts_poss_raw", defense = "def_ts_poss_raw"),
    time = c("off_minutes", "def_minutes"), time_key = c("game_id", "team_id")),
  team_four_factors_mv = list(
    key = c("game_year", "team_id"),
    additive = c("off_poss", "off_pts", "off_oreb_cnt", "off_oreb_opps", "off_tov_cnt", "off_fta",
                 "off_fga_cnt", "off_fgm_cnt", "off_fg3m_cnt", "def_poss", "def_pts", "def_oreb_cnt",
                 "def_oreb_opps", "def_tov_cnt", "def_fta", "def_fga_cnt", "def_fgm_cnt", "def_fg3m_cnt")),
  onoff_default_mv = list(
    key = c("Year", "Team", "player_id"),
    additive = c('"ON Poss" + "OFF Poss"', "off_on_fg2_att + off_off_fg2_att",
                 "off_on_fg3_att + off_off_fg3_att", "def_on_fg2_att + def_off_fg2_att",
                 "def_on_fg3_att + def_off_fg3_att", "off_on_layup_att + off_off_layup_att"),
    time = "minutes", time_key = c("Year", "Team", "player_id")),
  player_advanced_stats_mv = list(
    key = c("game_year", "team_id", "player_id"),
    additive = c("off_on_poss + off_off_poss", "def_on_poss + def_off_poss")),
  mv_lineup_totals_by_day = list(
    key = c("game_id", "team_id", "type_lineup"),
    additive = c("total_poss", "total_pts", "fg2_made", "fg2_att", "fg3_made", "fg3_att"),
    time = "minutes",
    time_key = c("game_id", "team_id", "lineup_hash", "type_lineup", "opp_starters", "num_starters")),
  sub_lineups_stats = list(
    key = c("game_year", "team_id", "num_lineup"),
    additive = c("off_poss", "off_pts", "def_poss", "def_pts", "off_fg2_made", "off_fg2_att",
                 "off_fg3_made", "off_fg3_att", "def_fg2_made", "def_fg2_att", "def_fg3_made", "def_fg3_att"),
    time = "minutes", time_key = c("game_year", "team_id", "sub_lineup_hash")),
  team_ppp_ratings_mv = list(key = c("game_year", "team_id"), additive = ratings_counts),
  team_ratings_preset_cache = list(key = c("preset_variant", "game_year", "team_id"), additive = ratings_counts),
  player_traditional_by_game = list(
    key = c("game_id", "team_id"), additive = c(player_box, "player_ts_poss_count", "poss_on_floor"),
    time = "seconds_on_floor", time_key = c("game_id", "team_id", "player_id")),
  default_clutch_player_totals_by_game = list(
    key = c("game_id", "team_id"), additive = c(player_box, "player_ts_poss_count", "poss_on_floor"),
    time = "seconds_on_floor", time_key = c("game_id", "team_id", "player_id")),
  player_stats_actions_by_game = list(
    key = c("game_id", "team_id", "type_lineup"),
    additive = c("points", "possession_flag", "oreb", "dreb", "assists", "steals", "blocks",
                 "deflections", "turnovers", "fgm", "fga", "fg3_made", "fg3_att", "ftm", "ft_attempts",
                 "shot_ts_possession")),
  player_traditional_stats_mv = list(
    key = c("game_year", "team_id"), additive = c(player_box, "poss_on_floor"),
    time = "minutes", time_key = c("game_year", "team_id", "player_id"))
)

# Reader functions snapshotted for G6. Each case is the argument list after the season.
fn_cases <- c(
  none = "",
  home = ", p_home_away := 'home'",
  last5 = ", p_last_n_games := 5",
  clutch = ", p_max_margin := 5, p_max_time_remaining := 300",
  starters5 = ", p_num_starters_off := 5"
)
fn_names <- c("onoff_compute", "four_factors_compute", "four_factors_dashboard_compute",
              "fetch_lineups_csv_v2", "fetch_lineups_four_factors_csv",
              "get_team_ratings_dynamic", "get_team_four_factors_dynamic",
              "get_team_four_factors_dashboard_dynamic", "get_team_traditional_dynamic",
              "get_player_traditional_dynamic", "get_player_traditional_from_games",
              "get_player_traditional_custom_clutch")
# Leading arguments up to and including the season; most readers take only the season.
lineup_lead <- "5, NULL::text, NULL::text, NULL::text, FALSE, NULL::date, NULL::date, 0, 2026"
fn_lead <- c(
  onoff_compute = "'2025-09-01'::date, '2026-07-01'::date, NULL::text, 0, 0, -1e9, '2026'",
  fetch_lineups_csv_v2 = lineup_lead,
  fetch_lineups_four_factors_csv = lineup_lead
)
fn_call <- function(fn, case) {
  lead <- if (fn %in% names(fn_lead)) fn_lead[[fn]] else "2026"
  sprintf("SELECT * FROM %s.%s(%s%s)", SCHEMA, fn, lead, fn_cases[[case]])
}
fn_table <- function(fn, case) sprintf('%s."fn_%s__%s"', SNAP, fn, case)

expr_alias <- function(i) sprintf("a%d", i)
sum_list <- function(exprs) {
  paste(sprintf("SUM((%s)::numeric) AS %s",
                ifelse(grepl("[ +]", exprs), exprs, sprintf('"%s"', exprs)),
                expr_alias(seq_along(exprs))), collapse = ", ")
}

# Sums of `exprs` per `key` in two relations; returns how many keys differ.
# Live rows count positive and baseline rows negative, then GROUP BY nets them
# out. GROUP BY treats NULL keys (e.g. opp_starters) as equal, which a join
# would not.
compare_sums <- function(live, base, key, exprs, tol = 1e-6) {
  al <- expr_alias(seq_along(exprs))
  signed <- function(sign) paste(sprintf("%s SUM((%s)::numeric) AS %s", sign,
    ifelse(grepl("[ +]", exprs), exprs, sprintf('"%s"', exprs)), al), collapse = ", ")
  sql <- sprintf("
    WITH u AS (
      SELECT %1$s, %2$s FROM %3$s GROUP BY %1$s
      UNION ALL
      SELECT %1$s, %4$s FROM %5$s GROUP BY %1$s
    ),
    net AS (SELECT %1$s, %6$s FROM u GROUP BY %1$s)
    SELECT COUNT(*) AS keys_total, COUNT(*) FILTER (WHERE %7$s) AS keys_differ, %8$s FROM net",
    ident(key), signed(""), live, signed("-"), base,
    paste(sprintf("SUM(COALESCE(%1$s, 0)) AS %1$s", al), collapse = ", "),
    paste(sprintf("ABS(%s) > %g", al, tol), collapse = " OR "),
    paste(sprintf("COUNT(*) FILTER (WHERE ABS(%1$s) > %2$g) AS d_%1$s", al, tol), collapse = ", "))
  res <- q(sql)
  per_col <- as.numeric(unlist(res[, paste0("d_", al), drop = FALSE]))
  list(total = as.numeric(res$keys_total), differ = as.numeric(res$keys_differ),
       cols = stats::setNames(per_col, exprs)[per_col > 0])
}

expected_trips_sql <- function(side) sprintf("
  SELECT a.game_id, a.team_id,
         (COUNT(*) FILTER (WHERE a.type = 'shot')
          + COUNT(DISTINCT a.parent_action_id) FILTER (
              WHERE a.type = 'freeThrow' AND p.id IS NOT NULL))::numeric AS expected
  FROM %1$s a
  LEFT JOIN %1$s p
    ON p.game_id = a.game_id AND p.id = a.parent_action_id
   AND p.type = 'foul' AND p.parameters_type = 'personal'
   AND p.team_id = a.team_id AND p.type_lineup = a.type_lineup
  WHERE a.type_lineup = '%2$s'
  GROUP BY a.game_id, a.team_id", BASE, side)

failures <- character(0)
report <- function(gate, ok, detail) {
  cat(sprintf("  %-4s %s  %s\n", gate, if (ok) "ok  " else "FAIL", detail))
  if (!ok) failures <<- c(failures, gate)
}

check_object <- function(name) {
  spec <- object_map[[name]]
  if (is.null(spec)) stop("unknown object: ", name)
  live <- sprintf('%s."%s"', SCHEMA, name)
  base <- sprintf('%s."baseline_%s"', SNAP, name)
  cat("== ", name, "\n", sep = "")

  g2 <- compare_sums(live, base, spec$key, spec$additive)
  report("G2", g2$differ == 0, sprintf("%d of %d keys differ in an additive total%s", g2$differ, g2$total,
         if (length(g2$cols)) paste0(" [", paste(names(g2$cols), g2$cols, sep = "=", collapse = ", "), "]") else ""))

  trip_specs <- list()
  if (!is.null(spec$trips)) for (side in names(spec$trips)) {
    trip_specs[[side]] <- sprintf('SELECT game_id, team_id, SUM("%s")::numeric AS got FROM %s GROUP BY 1, 2',
                                  spec$trips[[side]], live)
  }
  if (!is.null(spec$trips_long)) for (side in c("offense", "defense")) {
    grain <- if ("player_id" %in% spec$key) "game_id, team_id, player_id" else "game_id, team_id"
    trip_specs[[side]] <- sprintf(
      "SELECT game_id, team_id, MIN(got) AS got, MAX(got) AS got_max FROM (
         SELECT %s, SUM(\"%s\")::numeric AS got FROM %s WHERE type_lineup = '%s' GROUP BY %s) z GROUP BY 1, 2",
      grain, spec$trips_long, live, side, grain)
  }
  for (side in names(trip_specs)) {
    res <- q(sprintf("
      WITH got AS (%s), exp AS (%s)
      SELECT COUNT(*) AS n, COUNT(*) FILTER (WHERE g.got IS DISTINCT FROM e.expected) AS bad
      FROM got g JOIN exp e USING (game_id, team_id)", trip_specs[[side]], expected_trips_sql(side)))
    report("G3", as.numeric(res$bad) == 0,
           sprintf("%s: %s of %s team-games do not count each trip once", side, res$bad, res$n))
  }

  if (!is.null(spec$time)) {
    g4 <- compare_sums(live, base, spec$time_key, spec$time, tol = 1e-9)
    report("G4", g4$differ == 0, sprintf("%d of %d keys differ in floor time", g4$differ, g4$total))
  }
}

check_base <- function() {
  cat("== base table credit columns\n")
  n <- function(sql) as.numeric(q(sql)[[1]])
  report("G1a", n(sprintf("
    SELECT COUNT(*) FROM %s
    WHERE credit_lineup_hash IS NULL OR credit_segment_id IS NULL
       OR (num_starters IS NOT NULL AND credit_num_starters IS NULL)
       OR (own_starters IS NOT NULL AND credit_own_starters IS NULL)
       OR (opp_starters IS NOT NULL AND credit_opp_starters IS NULL)", BASE)) == 0,
    "no NULL credit key where the physical key is set")
  report("G1b", n(sprintf("
    SELECT COUNT(*) FROM %s WHERE type <> 'freeThrow'
      AND (credit_lineup_hash, credit_segment_id) IS DISTINCT FROM (lineup_hash, segment_id)", BASE)) == 0,
    "non-free-throw rows are credited to their own lineup")
  report("G1c", n(sprintf("
    SELECT COUNT(*) FROM %1$s a JOIN %1$s p
      ON p.game_id = a.game_id AND p.id = a.parent_action_id
     AND p.team_id = a.team_id AND p.type_lineup IS NOT DISTINCT FROM a.type_lineup
    WHERE a.type = 'freeThrow'
      AND (a.credit_lineup_hash, a.credit_segment_id, a.credit_num_starters,
           a.credit_own_starters, a.credit_opp_starters)
          IS DISTINCT FROM
          (p.lineup_hash, p.segment_id, p.num_starters, p.own_starters, p.opp_starters)", BASE)) == 0,
    "free throws are credited to their parent row's lineup")
  report("G1d", n(sprintf("
    SELECT COUNT(*) FROM (SELECT 1 FROM %s WHERE type = 'freeThrow'
      GROUP BY game_id, team_id, type_lineup, parent_action_id
      HAVING COUNT(DISTINCT (credit_lineup_hash, credit_segment_id)) > 1) z", BASE)) == 0,
    "all free throws of one trip share one credit segment")
  moved <- q(sprintf("
    SELECT COUNT(*) AS rows_moved, COALESCE(SUM(COALESCE(team_score, 0)), 0) AS points
    FROM %s WHERE (credit_lineup_hash, credit_segment_id) IS DISTINCT FROM (lineup_hash, segment_id)", BASE))
  cat(sprintf("  info re-credited rows: %s, points on them: %s\n", moved$rows_moved, moved$points))
}

snapshot <- function(force) {
  x(sprintf("CREATE SCHEMA IF NOT EXISTS %s", SNAP))
  exists <- function(tbl) nrow(q(sprintf(
    "SELECT 1 FROM pg_tables WHERE schemaname = '%s' AND tablename = '%s'", SNAP, tbl))) > 0
  for (name in names(object_map)) {
    tbl <- paste0("baseline_", name)
    if (exists(tbl)) {
      if (!force) { cat(tbl, ": exists, kept (use --force to replace)\n"); next }
      x(sprintf('DROP TABLE %s."%s"', SNAP, tbl))
    }
    x(sprintf('CREATE TABLE %s."%s" AS SELECT * FROM %s."%s"', SNAP, tbl, SCHEMA, name))
    cat(sprintf("%s: %s rows\n", tbl, q(sprintf('SELECT COUNT(*) FROM %s."%s"', SNAP, tbl))[[1]]))
  }
  for (fn in fn_names) for (case in names(fn_cases)) {
    tbl <- sprintf("fn_%s__%s", fn, case)
    if (exists(tbl)) { if (!force) next; x(sprintf('DROP TABLE %s."%s"', SNAP, tbl)) }
    res <- tryCatch({
      x(sprintf("CREATE TABLE %s AS %s", fn_table(fn, case), fn_call(fn, case)))
      sprintf("%s rows", q(sprintf("SELECT COUNT(*) FROM %s", fn_table(fn, case)))[[1]])
    }, error = function(e) paste("not snapshotted:", substr(gsub("\\s+", " ", conditionMessage(e)), 1, 90)))
    cat(sprintf("%s: %s\n", tbl, res))
  }
}

# G6: every column of the function's result, case by case, against the snapshot.
check_fn <- function(fn, ignore = character(0)) {
  cat("== ", fn, "\n", sep = "")
  for (case in names(fn_cases)) {
    tbl <- sprintf("fn_%s__%s", fn, case)
    if (!nrow(q(sprintf("SELECT 1 FROM pg_tables WHERE schemaname = '%s' AND tablename = '%s'", SNAP, tbl)))) next
    old <- q(sprintf("SELECT * FROM %s", fn_table(fn, case)))
    new <- q(fn_call(fn, case))
    key <- intersect(c("game_year", "team_id", "player_id", "sub_lineup_hash", "lineup_hash",
                       "player_ids_str", "player_names_str"), names(old))
    if (!length(key) || anyDuplicated(old[key])) key <- names(old)[vapply(old, function(v) !is.numeric(v), logical(1))]
    ord <- function(d) d[do.call(order, lapply(key, function(k) as.character(d[[k]]))), , drop = FALSE]
    old <- ord(old); new <- ord(new)
    if (nrow(old) != nrow(new)) {
      report("G6", FALSE, sprintf("%s: %d rows before, %d after", case, nrow(old), nrow(new))); next
    }
    cols <- setdiff(intersect(names(old), names(new)), ignore)
    changed <- Filter(function(cc) !isTRUE(all.equal(old[[cc]], new[[cc]], check.attributes = FALSE)), cols)
    cat(sprintf("  G6   %-9s %d rows; %d of %d columns changed%s\n", case, nrow(new), length(changed),
                length(cols), if (length(changed)) paste0(": ", paste(utils::head(changed, 14), collapse = ", ")) else ""))
  }
}

mode <- if (length(args)) args[[1]] else ""
if (identical(mode, "--snapshot")) {
  snapshot("--force" %in% args)
} else if (identical(mode, "--check-base")) {
  check_base()
} else if (identical(mode, "--check")) {
  for (name in args[-1]) check_object(name)
} else if (identical(mode, "--check-fn")) {
  for (fn in args[-1]) check_fn(fn)
} else if (identical(mode, "--drop")) {
  x(sprintf("DROP SCHEMA IF EXISTS %s CASCADE", SNAP)); cat("dropped schema", SNAP, "\n")
} else {
  stop("usage: --snapshot [--force] | --check-base | --check <object>... | --check-fn <function>... | --drop")
}
dbDisconnect(con)
if (length(failures)) quit(status = 1L)
