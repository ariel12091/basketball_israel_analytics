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

# MV dependency order - each entry: list(name, sql_file, level)
MV_REGISTRY <- list(
  list(name = "final_schedule_mv",              file = "sql/materialized_views/final_schedule_mv.sql",              level = 1),
  list(name = "df_pts_poss_lineups_longer_mv",  file = "sql/materialized_views/df_pts_poss_longer.sql",            level = 1),
  list(name = "mv_lineup_totals_by_day",        file = "sql/materialized_views/sub_lineups_by_day.sql",            level = 2),
  list(name = "team_ppp_ratings_mv",            file = "sql/materialized_views/team_ppp_ratings_mv.sql",           level = 2),
  list(name = "onoff_default_mv",               file = "sql/materialized_views/onoff_mv.sql",                      level = 2),
  list(name = "team_metrics_by_game_mv",        file = "sql/materialized_views/team_metrics_by_game_mv.sql",       level = 2),
  list(name = "player_onoff_by_game",           file = "sql/materialized_views/player_onoff_by_game.sql",          level = 3),
  list(name = "player_four_factors_by_game",    file = "sql/materialized_views/player_four_factors_by_game.sql",   level = 3),
  list(name = "lineup_four_factors_by_game",    file = "sql/materialized_views/lineup_four_factors_by_game.sql",   level = 3),
  list(name = "player_advanced_stats_mv",       file = "sql/materialized_views/player_advanced_stats_mv.sql",      level = 3),
  list(name = "player_traditional_stats_mv",    file = "sql/materialized_views/player_traditional_stats_mv.sql",   level = 3),
  list(name = "team_metrics_rolling_mv",        file = "sql/materialized_views/team_metrics_rolling_mv.sql",       level = 3),
  list(name = "team_four_factors_mv",           file = "sql/materialized_views/team_four_factors_mv.sql",          level = 4)
)

SCHEMA <- "basketball_test"

extract_mv_name <- function(sql_file) {
  lines <- readLines(sql_file, warn = FALSE)
  txt <- paste(lines, collapse = "\n")
  m <- regexec("(?i)CREATE\\s+(OR\\s+REPLACE\\s+)?MATERIALIZED\\s+VIEW\\s+([a-zA-Z0-9_\\.]+)", txt, perl = TRUE)
  hit <- regmatches(txt, m)[[1]]
  if (!length(hit) || length(hit) < 3) return(NA_character_)
  full_name <- tolower(hit[3])
  parts <- strsplit(full_name, "\\.", fixed = FALSE)[[1]]
  tail(parts, 1)
}

validate_mv_registry <- function(registry = MV_REGISTRY) {
  reg_names <- vapply(registry, function(x) x$name, character(1))
  reg_files <- vapply(registry, function(x) x$file, character(1))

  missing_files <- reg_files[!file.exists(reg_files)]
  if (length(missing_files)) {
    stop(sprintf("Missing MV SQL files in MV_REGISTRY: %s", paste(missing_files, collapse = ", ")))
  }

  sql_files <- list.files("sql/materialized_views", pattern = "\\.sql$", full.names = TRUE)
  discovered <- lapply(sql_files, function(f) list(file = gsub("\\\\", "/", f), name = extract_mv_name(f)))
  discovered <- Filter(function(x) !is.na(x$name) && nzchar(x$name), discovered)

  disc_names <- vapply(discovered, function(x) x$name, character(1))
  disc_files <- vapply(discovered, function(x) x$file, character(1))

  dup_names <- unique(disc_names[duplicated(disc_names)])
  if (length(dup_names)) {
    stop(sprintf("Duplicate MV definitions found in sql/materialized_views: %s", paste(dup_names, collapse = ", ")))
  }

  missing_in_registry <- setdiff(disc_names, reg_names)
  if (length(missing_in_registry)) {
    stop(sprintf(
      "MV_REGISTRY is missing MV(s): %s. Add them to rebuild_all_mvs.R.",
      paste(missing_in_registry, collapse = ", ")
    ))
  }

  missing_in_sql <- setdiff(reg_names, disc_names)
  if (length(missing_in_sql)) {
    stop(sprintf(
      "MV_REGISTRY references MV(s) with no CREATE MATERIALIZED VIEW SQL file: %s",
      paste(missing_in_sql, collapse = ", ")
    ))
  }

  # Ensure each registry row points to the file that actually defines that MV name.
  disc_map <- setNames(disc_files, disc_names)
  mismatches <- reg_names[normalizePath(reg_files, winslash = "/", mustWork = TRUE) != normalizePath(disc_map[reg_names], winslash = "/", mustWork = TRUE)]
  if (length(mismatches)) {
    msg <- vapply(
      mismatches,
      function(nm) {
        reg_file <- reg_files[match(nm, reg_names)]
        sprintf("%s (registry=%s, actual=%s)", nm, reg_file, disc_map[[nm]])
      },
      character(1)
    )
    stop(sprintf("MV_REGISTRY file mapping mismatch: %s", paste(msg, collapse = "; ")))
  }

  invisible(TRUE)
}

rebuild_all_mvs <- function(from_level = 1, skip = character(0)) {
  validate_mv_registry(MV_REGISTRY)

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

  # Drop in reverse order (L4 -> L1) to avoid cascade surprises
  cat("Dropping MVs...\n")
  for (mv in rev(targets)) {
    sql <- sprintf('DROP MATERIALIZED VIEW IF EXISTS %s.%s CASCADE;', SCHEMA, mv$name)
    cat(sprintf("  DROP %s ... ", mv$name))
    tryCatch({ dbExecute(pg, sql); cat("OK\n") },
             error = function(e) cat(sprintf("SKIP (%s)\n", conditionMessage(e))))
  }

  # Create in forward order (L1 -> L4)
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
