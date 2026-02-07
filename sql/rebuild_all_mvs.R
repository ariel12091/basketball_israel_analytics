# rebuild_all_mvs.R
# Drops and recreates ALL materialized views in dependency order.
# Use after schema changes that require DROP CASCADE on a base MV.
#
# Usage:
#   source("sql/rebuild_all_mvs.R")
#   rebuild_all_mvs()                        # rebuild everything
#   rebuild_all_mvs(from_level = 2)          # skip L1, rebuild L2-L4 only
#   rebuild_all_mvs(skip = "final_schedule_mv")  # skip specific MVs

library(DBI)
library(RPostgres)

# MV dependency order — each entry: list(name, sql_file, level)
MV_REGISTRY <- list(
  list(name = "final_schedule_mv",              file = "sql/materialized_views/final_schedule_mv.sql",              level = 1),
  list(name = "df_pts_poss_lineups_longer_mv",  file = "sql/materialized_views/df_pts_poss_longer.sql",            level = 1),
  list(name = "mv_lineup_totals_by_day",        file = "sql/materialized_views/sub_lineups_by_day.sql",            level = 2),
  list(name = "team_ppp_ratings_mv",            file = "sql/materialized_views/team_ppp_ratings_mv.sql",           level = 2),
  list(name = "onoff_default_mv",               file = "sql/materialized_views/onoff_mv.sql",                      level = 2),
  list(name = "player_onoff_by_game",           file = "sql/materialized_views/player_onoff_by_game.sql",          level = 3),
  list(name = "player_four_factors_by_game",    file = "sql/materialized_views/player_four_factors_by_game.sql",   level = 3),
  list(name = "lineup_four_factors_by_game",    file = "sql/materialized_views/lineup_four_factors_by_game.sql",   level = 3),
  list(name = "player_advanced_stats_mv",       file = "sql/materialized_views/player_advanced_stats_mv.sql",      level = 3),
  list(name = "team_four_factors_mv",           file = "sql/materialized_views/team_four_factors_mv.sql",          level = 4)
)

SCHEMA <- "basketball_test"

rebuild_all_mvs <- function(from_level = 1, skip = character(0)) {
  pg <- dbConnect(
    Postgres(),
    host     = Sys.getenv("PG_HOST"),
    port     = 5432L,
    dbname   = Sys.getenv("PG_DB"),
    user     = Sys.getenv("PG_USER"),
    password = Sys.getenv("PG_PASS"),
    sslmode  = "require"
  )
  on.exit(dbDisconnect(pg), add = TRUE)

  dbExecute(pg, sprintf("SET search_path TO %s, public;", SCHEMA))

  # Filter to requested levels and skip list
  targets <- Filter(function(mv) mv$level >= from_level && !(mv$name %in% skip), MV_REGISTRY)

  # Drop in reverse order (L4 → L1) to avoid cascade surprises
  cat("Dropping MVs...\n")
  for (mv in rev(targets)) {
    sql <- sprintf('DROP MATERIALIZED VIEW IF EXISTS %s.%s CASCADE;', SCHEMA, mv$name)
    cat(sprintf("  DROP %s ... ", mv$name))
    tryCatch({ dbExecute(pg, sql); cat("OK\n") },
             error = function(e) cat(sprintf("SKIP (%s)\n", conditionMessage(e))))
  }

  # Create in forward order (L1 → L4)
  cat("\nCreating MVs...\n")
  for (mv in targets) {
    cat(sprintf("  L%d: %s\n", mv$level, mv$name))

    lines <- readLines(mv$file, warn = FALSE)
    full_sql <- paste(lines, collapse = "\n")

    # Split on semicolons to separate CREATE from indexes
    parts <- strsplit(full_sql, ";")[[1]]
    parts <- trimws(parts)
    parts <- parts[nchar(parts) > 0]

    for (part in parts) {
      # Strip leading SQL comments
      clean <- gsub("^(\\s*--[^\n]*\n)+", "", part)
      clean <- trimws(clean)
      if (nchar(clean) == 0) next

      is_create <- grepl("^CREATE", clean, ignore.case = TRUE)
      label <- if (is_create) "CREATE" else sub("^.*(INDEX\\s+\\S+).*$", "\\1", clean)
      cat(sprintf("    %s ... ", label))
      tryCatch({
        dbExecute(pg, paste0(clean, ";"))
        cat("OK\n")
      }, error = function(e) cat(sprintf("ERROR: %s\n", conditionMessage(e))))
    }
  }

  # Verify row counts
  cat("\nRow counts:\n")
  for (mv in targets) {
    n <- tryCatch(
      dbGetQuery(pg, sprintf("SELECT count(*) AS n FROM %s.%s", SCHEMA, mv$name))$n,
      error = function(e) NA
    )
    cat(sprintf("  %-40s %s\n", mv$name, if (is.na(n)) "MISSING" else format(n, big.mark = ",")))
  }

  cat("\nDone.\n")
}
