# Resumable online apply for the free-throw credit migration
# (docs/superpowers/plans/2026-10-12-free-throw-foul-lineup-attribution.md).
#
#   Rscript scripts/apply_ft_credit_migration.R [--apply]
#       credit columns on the base table + backfill
#   Rscript scripts/apply_ft_credit_migration.R --object <name> [--apply]
#       move one derived object onto the credit columns
#   Rscript scripts/apply_ft_credit_migration.R --bump-cache --apply
#       advance the app's cache marker
#   Rscript scripts/apply_ft_credit_migration.R --cleanup --apply
#       drop the retired __ftc_old objects
#
# Without --apply nothing is changed: the script prints what it would do.
# No migration-wide transaction: columns are metadata-only, backfills commit
# per game batch, and a materialized view is staged under <name>__ftc_next
# and swapped in by a short, retried rename. Never rebuilds the base table.

args <- commandArgs(trailingOnly = TRUE)
apply <- "--apply" %in% args
arg_value <- function(flag) {
  i <- match(flag, args)
  if (is.na(i) || i == length(args)) NULL else args[[i + 1L]]
}

readRenviron("etl/.Renviron")
suppressPackageStartupMessages({ library(DBI); library(RPostgres) })

SCHEMA <- "basketball_test"
BASE <- "basketball_test.df_pts_poss_lineups_longer_mv"
BATCH <- 25L

con <- dbConnect(
  Postgres(),
  host = Sys.getenv("PG_HOST"), port = 5432L,
  dbname = Sys.getenv("PG_DB"), user = Sys.getenv("PG_USER"),
  password = Sys.getenv("PG_PASS"), sslmode = Sys.getenv("PG_SSLMODE", "require"),
  connect_timeout = 15L, bigint = "numeric"
)
q <- function(sql) dbGetQuery(con, sql, immediate = TRUE)
x <- function(sql) invisible(dbExecute(con, sql, immediate = TRUE))
read_sql <- function(path) paste(readLines(path, warn = FALSE), collapse = "\n")
say <- function(...) message(sprintf(...))

x("SET client_min_messages = warning")
x("SET statement_timeout = '30min'")
x("SET lock_timeout = '10s'")
# Some definition files name their source tables without the schema.
x(sprintf("SET search_path TO %s, public", SCHEMA))
if (apply && !isTRUE(q("SELECT pg_try_advisory_lock(hashtext('basketball_test.ft_credit.2026_10_12')) AS ok")$ok)) {
  stop("another apply_ft_credit_migration run holds the lock")
}
say("Mode: %s", if (apply) "APPLY" else "DRY RUN (nothing is changed)")

# --game-min / --game-max restrict a batched refresh to a range of game ids, so
# an interrupted run can be resumed, or a long one split into shorter calls.
all_game_ids <- function() {
  ids <- sort(as.integer(q(sprintf("SELECT DISTINCT game_id FROM %s", BASE))$game_id))
  lo <- arg_value("--game-min"); hi <- arg_value("--game-max")
  if (!is.null(lo)) ids <- ids[ids >= as.integer(lo)]
  if (!is.null(hi)) ids <- ids[ids <= as.integer(hi)]
  ids
}

# Call fn(int4[]) for every game in batches, one committed statement each.
call_game_batches <- function(fn, label) {
  ids <- all_game_ids()
  batches <- split(ids, ceiling(seq_along(ids) / BATCH))
  say("  %s: %d games in %d batches", label, length(ids), length(batches))
  if (!apply) return(invisible(0))
  total <- 0
  started <- Sys.time()
  for (i in seq_along(batches)) {
    n <- q(sprintf("SELECT %s.%s(ARRAY[%s]::int4[]) AS n", SCHEMA, fn, paste(batches[[i]], collapse = ",")))$n
    total <- total + as.numeric(n)
    if (i %% 5L == 0L || i == length(batches)) {
      say("    batch %d/%d, %s rows so far, %.0fs", i, length(batches), format(total, big.mark = ","),
          as.numeric(difftime(Sys.time(), started, units = "secs")))
    }
  }
  invisible(total)
}

deploy_file <- function(path) {
  say("  deploy %s", path)
  if (apply) x(read_sql(path))
}

relkind <- function(name) {
  r <- q(sprintf("SELECT c.relkind FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                  WHERE n.nspname = '%s' AND c.relname = '%s'", SCHEMA, name))$relkind
  if (length(r)) r[[1]] else NA_character_
}

# ---- derived objects -------------------------------------------------------
# table  : redeploy `functions`, then call `refresh` for every game in batches
# matview: stage `file` under <name>__ftc_next, then swap by rename
# refresh_only: the definition does not change; REFRESH MATERIALIZED VIEW
objects <- list(
  lineup_four_factors_by_game = list(kind = "matview",
    file = "sql/materialized_views/lineup_four_factors_by_game.sql"),
  team_metrics_by_game_mv = list(kind = "table",
    functions = "sql/functions/refresh_team_metrics_by_game_for_games.sql",
    refresh = "refresh_team_metrics_by_game_for_games"),
  # Reads lineup_four_factors_by_game by OID, so it must be re-created after
  # that view is swapped; its definition itself does not change.
  team_four_factors_mv = list(kind = "matview",
    file = "sql/materialized_views/team_four_factors_mv.sql"),
  team_metrics_rolling_mv = list(kind = "refresh_only"),
  player_four_factors_by_game = list(kind = "table",
    functions = "sql/functions/refresh_player_four_factors_by_game_for_games.sql",
    refresh = "refresh_player_four_factors_by_game_for_games"),
  onoff_default_mv = list(kind = "season_table",
    functions = "sql/functions/refresh_onoff_default_for_games.sql",
    refresh = "refresh_onoff_default_for_games"),
  player_advanced_stats_mv = list(kind = "season_table",
    functions = "sql/functions/refresh_player_advanced_stats_for_games.sql",
    refresh = "refresh_player_advanced_stats_for_games"),
  mv_lineup_totals_by_day = list(kind = "matview",
    file = "sql/materialized_views/sub_lineups_by_day.sql"),
  sub_lineups_stats = list(kind = "table",
    functions = c("sql/functions/refresh_sub_lineups.sql",
                  "sql/functions/refresh_sub_lineups_incremental.sql"),
    refresh = "refresh_sub_lineups_stats_for_games"),
  team_ppp_ratings_mv = list(kind = "matview",
    file = "sql/materialized_views/team_ppp_ratings_mv.sql"),
  player_traditional_by_game = list(kind = "table",
    functions = c("sql/functions/compute_player_traditional_by_game.sql",
                  "sql/functions/refresh_player_traditional_by_game_for_games.sql"),
    refresh = "refresh_player_traditional_by_game_for_games"),
  default_clutch_player_totals_by_game = list(kind = "table",
    functions = "sql/functions/refresh_default_clutch_player_totals_for_games.sql",
    refresh = "refresh_default_clutch_player_totals_for_games"),
  player_stats_actions_by_game = list(kind = "table",
    functions = c("sql/functions/compute_player_stats_actions_by_game.sql",
                  "sql/functions/refresh_player_stats_actions_for_games.sql"),
    refresh = "refresh_player_stats_actions_for_games"),
  player_traditional_stats_mv = list(kind = "matview",
    file = "sql/materialized_views/player_traditional_stats_mv.sql"),
  team_ratings_preset_cache = list(kind = "season_table", functions = character(0),
    refresh = "refresh_team_ratings_preset_cache_for_games")
)

# Rewrite a matview definition file so it builds <name>__ftc_next with its own
# index names. Exact substitutions only; stops if the file is not what it expects.
staged_matview_sql <- function(name, path) {
  sql <- read_sql(path)
  qualified <- sprintf("%s.%s", SCHEMA, name)
  create_stmt <- sprintf("CREATE MATERIALIZED VIEW %s", qualified)
  n_create <- lengths(regmatches(sql, gregexpr(create_stmt, sql, fixed = TRUE)))
  if (n_create != 1L) stop(path, ": expected exactly one '", create_stmt, "'")
  # Drop statements in the file target the live object: remove them.
  lines <- strsplit(sql, "\n", fixed = TRUE)[[1]]
  lines <- lines[!grepl("^\\s*DROP (MATERIALIZED VIEW|TABLE|INDEX)", lines)]
  sql <- paste(lines, collapse = "\n")
  idx <- q(sprintf("SELECT indexname FROM pg_indexes WHERE schemaname = '%s' AND tablename = '%s'", SCHEMA, name))$indexname
  for (i in idx) {
    if (!grepl(i, sql, fixed = TRUE)) stop(path, ": live index ", i, " is not created by the file")
    sql <- gsub(paste0(" ", i, "\n"), paste0(" ", i, "__ftc_next\n"), sql, fixed = TRUE)
    sql <- gsub(paste0(" ", i, " "), paste0(" ", i, "__ftc_next "), sql, fixed = TRUE)
  }
  sql <- gsub(qualified, paste0(qualified, "__ftc_next"), sql, fixed = TRUE)
  list(sql = sql, indexes = idx)
}

swap_matview <- function(name, indexes) {
  stmts <- c(
    sprintf("ALTER MATERIALIZED VIEW %s.%s RENAME TO %s__ftc_old", SCHEMA, name, name),
    sprintf("ALTER MATERIALIZED VIEW %s.%s__ftc_next RENAME TO %s", SCHEMA, name, name),
    unlist(lapply(indexes, function(i) c(
      sprintf("ALTER INDEX %s.%s RENAME TO %s__ftc_old", SCHEMA, i, i),
      sprintf("ALTER INDEX %s.%s__ftc_next RENAME TO %s", SCHEMA, i, i)))))
  for (attempt in 1:12) {
    ok <- tryCatch({
      dbBegin(con); x("SET LOCAL lock_timeout = '5s'")
      for (s in stmts) x(s)
      dbCommit(con); TRUE
    }, error = function(e) {
      try(dbRollback(con), silent = TRUE)
      say("    swap attempt %d/12 deferred: %s", attempt, substr(conditionMessage(e), 1, 100))
      FALSE
    })
    if (ok) return(invisible(TRUE))
    Sys.sleep(5)
  }
  stop("could not swap ", name, " after 12 attempts")
}

apply_object <- function(name) {
  spec <- objects[[name]]
  if (is.null(spec)) stop("unknown object: ", name, " (known: ", paste(names(objects), collapse = ", "), ")")
  say("Object %s [%s]", name, spec$kind)
  if (identical(spec$kind, "table")) {
    for (f in spec$functions) deploy_file(f)
    call_game_batches(spec$refresh, sprintf("refresh %s", name))
  } else if (identical(spec$kind, "season_table")) {
    # The refresh function recomputes the whole season of every game it is
    # given, so one game per season refreshes everything.
    for (f in spec$functions) deploy_file(f)
    reps <- q(sprintf("SELECT s.game_year, MIN(d.game_id) AS game_id
                       FROM (SELECT DISTINCT game_id FROM %s) d
                       JOIN %s.schedule s ON s.game_id = d.game_id
                       GROUP BY s.game_year ORDER BY s.game_year", BASE, SCHEMA))
    for (i in seq_len(nrow(reps))) {
      say("  refresh %s for season %s (via game %s)", name, reps$game_year[i], reps$game_id[i])
      if (apply) {
        n <- q(sprintf("SELECT %s.%s(ARRAY[%d]::int4[]) AS n", SCHEMA, spec$refresh, as.integer(reps$game_id[i])))$n
        say("    %s rows", n)
      }
    }
  } else if (identical(spec$kind, "refresh_only")) {
    say("  REFRESH MATERIALIZED VIEW %s", name)
    if (apply) x(sprintf("REFRESH MATERIALIZED VIEW %s.%s", SCHEMA, name))
  } else {
    if (!identical(relkind(name), "m")) stop(name, " is not a materialized view in the database")
    dependents <- q(sprintf("
      SELECT DISTINCT dc.relname FROM pg_depend d
      JOIN pg_rewrite r ON r.oid = d.objid
      JOIN pg_class dc ON dc.oid = r.ev_class
      JOIN pg_class sc ON sc.oid = d.refobjid
      JOIN pg_namespace n ON n.oid = sc.relnamespace
      WHERE n.nspname = '%s' AND sc.relname = '%s' AND dc.relname <> '%s'", SCHEMA, name, name))$relname
    if (length(dependents)) say("  objects that read %s by OID and keep reading the retired copy until rebuilt: %s",
                                name, paste(dependents, collapse = ", "))
    staged <- staged_matview_sql(name, spec$file)
    say("  stage %s__ftc_next from %s (%d indexes)", name, spec$file, length(staged$indexes))
    if (!apply) return(invisible())
    if (!is.na(relkind(paste0(name, "__ftc_old")))) stop(name, "__ftc_old already exists: run --cleanup or resolve by hand")
    # --reuse-staged: a long build can outlive the pooler's connection. The
    # build still finishes on the server (one implicit transaction, so the
    # staged view is either complete with its indexes or absent); this resumes
    # from it instead of building again.
    reuse <- "--reuse-staged" %in% args && identical(relkind(paste0(name, "__ftc_next")), "m")
    if (reuse) {
      have <- q(sprintf("SELECT indexname FROM pg_indexes WHERE schemaname = '%s' AND tablename = '%s__ftc_next'",
                        SCHEMA, name))$indexname
      if (!setequal(have, paste0(staged$indexes, "__ftc_next"))) stop("staged view's indexes are incomplete: rebuild it")
      say("  reusing the staged view already on the server")
    } else {
      x(sprintf("DROP MATERIALIZED VIEW IF EXISTS %s.%s__ftc_next", SCHEMA, name))
      x(staged$sql)
    }
    counts <- q(sprintf("SELECT (SELECT COUNT(*) FROM %1$s.%2$s) AS old_rows,
                                (SELECT COUNT(*) FROM %1$s.%2$s__ftc_next) AS new_rows", SCHEMA, name))
    say("  rows: %s live, %s staged", counts$old_rows, counts$new_rows)
    # A new view has no grants. Give the app its read grant before the swap so
    # there is no window in which the live name is unreadable; the full
    # hardening script still has to run afterwards.
    x(sprintf("GRANT SELECT ON %s.%s__ftc_next TO app_readonly", SCHEMA, name))
    swap_matview(name, staged$indexes)
    say("  swapped; previous definition kept as %s__ftc_old", name)
  }
  invisible()
}

# ---- stages ----------------------------------------------------------------
if (!is.null(arg_value("--object"))) {
  apply_object(arg_value("--object"))
} else if ("--bump-cache" %in% args) {
  cur <- q(sprintf("SELECT * FROM %s.app_meta WHERE key = 'etl_full_last_success'", SCHEMA))
  say("app_meta etl_full_last_success is: %s", paste(utils::capture.output(print(cur)), collapse = " | "))
  if (apply) {
    stamp <- q("SELECT to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS') AS s")$s
    stopifnot(nrow(cur) == 1L, "value" %in% names(cur),
              grepl("^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}", as.character(cur$value)))
    x(sprintf("UPDATE %s.app_meta SET value = '%s' WHERE key = 'etl_full_last_success'", SCHEMA, stamp))
    say("  advanced to %s", stamp)
  }
} else if ("--cleanup" %in% args) {
  old <- q(sprintf("SELECT c.relname, c.relkind FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = '%s' AND c.relname LIKE '%%\\_\\_ftc\\_old' AND c.relkind IN ('m', 'r')", SCHEMA))
  for (i in seq_len(nrow(old))) {
    say("  drop %s", old$relname[i])
    if (apply) x(sprintf("DROP %s %s.%s", if (old$relkind[i] == "m") "MATERIALIZED VIEW" else "TABLE", SCHEMA, old$relname[i]))
  }
} else {
  say("Stage 1: credit columns")
  say("  sql/migrations/2026-10-12_ft_credit_columns.sql")
  if (apply) x(read_sql("sql/migrations/2026-10-12_ft_credit_columns.sql"))
  say("Stage 2: functions")
  deploy_file("sql/functions/refresh_ft_credit_fields_for_games.sql")
  deploy_file("sql/functions/refresh_df_pts_poss_lineups_longer_for_games.sql")
  say("Stage 3: backfill")
  total <- call_game_batches("refresh_ft_credit_fields_for_games", "credit backfill")
  if (apply) say("  rows updated: %s", format(total, big.mark = ","))
}
dbDisconnect(con)
say("Done.")
