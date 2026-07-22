# =============================================================================
# etl/run_data_quality_report.R
#
# Read-only daily data quality report for ETL/player identity risks.
#
# Usage:
#   Sys.setenv(APP_ENV = "test")
#   source("etl/run_data_quality_report.R")
#   run_data_quality_report()
#
# Or:
#   Rscript etl/run_data_quality_report.R
#
# Environment:
#   APP_ENV              "prod" uses basketball, anything else uses basketball_test
#   DQ_OUTPUT_DIR        default: etl/logs/data_quality
#   DQ_MAX_DETAIL_ROWS   default: 50 rows per markdown section
#   DQ_FAIL_ON_ERROR     true/false, default false
# =============================================================================

suppressPackageStartupMessages({
  library(DBI)
  library(RPostgres)
})

truthy <- function(x) {
  tolower(trimws(Sys.getenv(x, "false"))) %in% c("1", "true", "yes", "y")
}

script_path <- function() {
  file_arg <- grep("^--file=", commandArgs(FALSE), value = TRUE)
  if (length(file_arg)) {
    return(normalizePath(sub("^--file=", "", file_arg[[1]]), winslash = "/", mustWork = FALSE))
  }

  if (!is.null(sys.frames()[[1]]$ofile)) {
    return(normalizePath(sys.frames()[[1]]$ofile, winslash = "/", mustWork = FALSE))
  }

  NA_character_
}

repo_root <- function() {
  path <- script_path()
  if (!is.na(path) && nzchar(path)) {
    return(normalizePath(file.path(dirname(path), ".."), winslash = "/", mustWork = FALSE))
  }
  normalizePath(getwd(), winslash = "/", mustWork = FALSE)
}

quote_table <- function(con, schema, table) {
  as.character(DBI::dbQuoteIdentifier(con, DBI::Id(schema = schema, table = table)))
}

sql_string <- function(con, value) {
  as.character(DBI::dbQuoteString(con, value))
}

table_exists <- function(con, schema, table) {
  q <- sprintf(
    "SELECT 1
       FROM pg_catalog.pg_class c
       JOIN pg_catalog.pg_namespace n
         ON n.oid = c.relnamespace
      WHERE n.nspname = %s
        AND c.relname = %s
        AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
      LIMIT 1",
    sql_string(con, schema),
    sql_string(con, table)
  )
  nrow(DBI::dbGetQuery(con, q)) > 0
}

escape_md <- function(x) {
  x <- as.character(x)
  x[is.na(x)] <- ""
  x <- gsub("\\|", "\\\\|", x)
  x <- gsub("[\r\n]+", " ", x)
  x
}

markdown_table <- function(df, max_rows = 50L) {
  if (is.null(df) || !nrow(df)) return("No rows.")

  out <- utils::head(df, max_rows)
  out[] <- lapply(out, escape_md)
  header <- paste0("| ", paste(names(out), collapse = " | "), " |")
  sep <- paste0("| ", paste(rep("---", ncol(out)), collapse = " | "), " |")
  rows <- apply(out, 1L, function(row) paste0("| ", paste(row, collapse = " | "), " |"))
  suffix <- if (nrow(df) > max_rows) {
    sprintf("\n\nShowing %d of %d rows. See detail CSV for the full result.", max_rows, nrow(df))
  } else {
    ""
  }
  paste(c(header, sep, rows), collapse = "\n") |> paste0(suffix)
}

write_detail_csv <- function(df, output_dir, check_id) {
  if (is.null(df) || !nrow(df)) return("")
  detail_dir <- file.path(output_dir, "details")
  dir.create(detail_dir, recursive = TRUE, showWarnings = FALSE)
  path <- file.path(detail_dir, paste0(check_id, ".csv"))
  utils::write.csv(df, path, row.names = FALSE, na = "")
  file.path("details", basename(path))
}

make_check_result <- function(
  check,
  status,
  details,
  detail_file = "",
  error_message = NA_character_,
  issue_count = NULL
) {
  row_count <- if (status %in% c("skipped", "not_automated")) NA_integer_ else nrow(details)
  if (is.null(issue_count)) {
    issue_count <- if (is.na(row_count)) NA_real_ else as.numeric(row_count)
  }
  summary <- data.frame(
    check_id = check$id,
    severity = check$severity,
    status = status,
    row_count = row_count,
    issue_count = issue_count,
    title = check$title,
    detail_file = detail_file,
    error_message = error_message,
    stringsAsFactors = FALSE
  )
  list(summary = summary, details = details)
}

run_sql_check <- function(con, schema, check, output_dir) {
  missing_tables <- check$required_tables[!vapply(check$required_tables, table_exists, logical(1), con = con, schema = schema)]
  if (length(missing_tables)) {
    details <- data.frame(
      note = sprintf("Skipped because required table(s) are missing: %s", paste(missing_tables, collapse = ", ")),
      stringsAsFactors = FALSE
    )
    detail_file <- write_detail_csv(details, output_dir, check$id)
    return(make_check_result(check, "skipped", details, detail_file))
  }

  tryCatch({
    details <- DBI::dbGetQuery(con, check$sql)
    issue_count <- if (!is.null(check$problem_count_col)) {
      if (!check$problem_count_col %in% names(details)) {
        stop(sprintf(
          "Check %s expected problem count column %s",
          check$id,
          check$problem_count_col
        ))
      }
      sum(suppressWarnings(as.numeric(details[[check$problem_count_col]])), na.rm = TRUE)
    } else {
      nrow(details)
    }
    status <- if (issue_count > 0) {
      if (identical(check$severity, "warning")) "warning" else "fail"
    } else {
      "pass"
    }
    detail_file <- write_detail_csv(details, output_dir, check$id)
    make_check_result(check, status, details, detail_file, issue_count = issue_count)
  }, error = function(e) {
    details <- data.frame(error = conditionMessage(e), stringsAsFactors = FALSE)
    detail_file <- write_detail_csv(details, output_dir, check$id)
    make_check_result(check, "query_error", details, detail_file, conditionMessage(e))
  })
}

run_r_check <- function(con, schema, root, check, output_dir) {
  tryCatch({
    details <- check$runner(con = con, schema = schema, root = root)
    status_override <- attr(details, "dq_status", exact = TRUE)
    if (identical(status_override, "skipped")) {
      detail_file <- write_detail_csv(details, output_dir, check$id)
      return(make_check_result(check, "skipped", details, detail_file))
    }

    issue_count <- if (!is.null(check$problem_count_col)) {
      if (!check$problem_count_col %in% names(details)) {
        stop(sprintf(
          "Check %s expected problem count column %s",
          check$id,
          check$problem_count_col
        ))
      }
      sum(suppressWarnings(as.numeric(details[[check$problem_count_col]])), na.rm = TRUE)
    } else {
      nrow(details)
    }
    status <- if (issue_count > 0) {
      if (identical(check$severity, "warning")) "warning" else "fail"
    } else {
      "pass"
    }
    detail_file <- write_detail_csv(details, output_dir, check$id)
    make_check_result(check, status, details, detail_file, issue_count = issue_count)
  }, error = function(e) {
    details <- data.frame(error = conditionMessage(e), stringsAsFactors = FALSE)
    detail_file <- write_detail_csv(details, output_dir, check$id)
    make_check_result(check, "query_error", details, detail_file, conditionMessage(e))
  })
}

cold_storage_snapshot_details <- function(root) {
  cold_dir <- file.path(root, "exports", "cold")
  required_files <- file.path(
    cold_dir,
    paste0(c("actions_clean", "possessions", "pws", "stints"), ".parquet")
  )
  missing_files <- required_files[!file.exists(required_files)]
  if (length(missing_files) || !requireNamespace("arrow", quietly = TRUE)) {
    note <- if (length(missing_files)) {
      sprintf(
        "Skipped because cold-storage file(s) are missing: %s",
        paste(basename(missing_files), collapse = ", ")
      )
    } else {
      "Skipped because the arrow package is unavailable."
    }
    out <- data.frame(note = note, stringsAsFactors = FALSE)
    attr(out, "dq_status") <- "skipped"
    return(out)
  }

  read_cols <- function(name, columns) {
    out <- arrow::read_parquet(
      file.path(cold_dir, paste0(name, ".parquet")),
      mmap = FALSE
    )
    out[, columns, drop = FALSE]
  }
  key_string <- function(df, columns) {
    do.call(paste, c(df[columns], sep = "\r"))
  }
  count_by_game <- function(game_id, issue_type, detail) {
    if (!length(game_id)) return(NULL)
    counts <- as.data.frame(table(game_id), stringsAsFactors = FALSE)
    names(counts) <- c("game_id", "affected_rows")
    counts$game_id <- suppressWarnings(as.integer(as.character(counts$game_id)))
    counts$affected_rows <- as.numeric(counts$affected_rows)
    counts$issue_type <- issue_type
    counts$detail <- detail
    counts[, c("issue_type", "game_id", "affected_rows", "detail")]
  }

  actions <- read_cols("actions_clean", c("game_id", "id"))
  possessions <- read_cols("possessions", c("game_id", "id"))
  pws <- read_cols(
    "pws",
    c(
      "game_id", "id", "team_id", "segment_id", "final_start_id",
      "final_end_id", "lineup_hash_offense", "lineup_hash_defense",
      "team_id_defense"
    )
  )
  stints <- read_cols(
    "stints",
    c(
      "game_id", "team_id", "segment_id", "final_start_id",
      "final_end_id", "lineup_hash_offense", "lineup_hash_defense",
      "team_id_defense"
    )
  )

  action_key <- key_string(actions, c("game_id", "id"))
  possession_key <- key_string(possessions, c("game_id", "id"))
  issues <- list(
    count_by_game(
      actions$game_id[!action_key %in% possession_key],
      "action_missing_from_possessions",
      "Cleaned action key is absent from the cold possessions snapshot."
    ),
    count_by_game(
      possessions$game_id[!possession_key %in% action_key],
      "possession_missing_from_actions",
      "Possession key is absent from the cold cleaned-actions snapshot."
    )
  )

  mapping_cols <- c(
    "game_id", "team_id", "segment_id", "final_start_id", "final_end_id",
    "lineup_hash_offense", "lineup_hash_defense", "team_id_defense"
  )
  valid_pws <- stats::complete.cases(pws[mapping_cols])
  valid_stints <- stats::complete.cases(stints[mapping_cols])
  pws_mapping_key <- key_string(pws[valid_pws, , drop = FALSE], mapping_cols)
  stint_mapping_key <- unique(key_string(stints[valid_stints, , drop = FALSE], mapping_cols))
  missing_stint_mapping <- !pws_mapping_key %in% stint_mapping_key
  issues[[length(issues) + 1L]] <- count_by_game(
    pws$game_id[valid_pws][missing_stint_mapping],
    "pws_mapping_missing_from_stints",
    "PWS row references a segment/lineup mapping absent from the cold stints snapshot."
  )

  invalid_pws_segment <- !is.na(pws$segment_id) & pws$segment_id <= 0
  issues[[length(issues) + 1L]] <- count_by_game(
    pws$game_id[invalid_pws_segment],
    "nonpositive_pws_segment_id",
    "Cold PWS row has a non-positive segment_id."
  )
  invalid_stint_segment <- !is.na(stints$segment_id) & stints$segment_id <= 0
  issues[[length(issues) + 1L]] <- count_by_game(
    stints$game_id[invalid_stint_segment],
    "nonpositive_stint_segment_id",
    "Cold stint row has a non-positive segment_id."
  )

  pws_key <- key_string(pws, c("game_id", "id", "team_id"))
  duplicate_pws <- duplicated(pws_key) | duplicated(pws_key, fromLast = TRUE)
  issues[[length(issues) + 1L]] <- count_by_game(
    pws$game_id[duplicate_pws],
    "duplicate_pws_storage_key",
    "Cold PWS contains duplicate (game_id, id, team_id) storage keys."
  )

  issues <- Filter(Negate(is.null), issues)
  if (!length(issues)) {
    return(data.frame(
      issue_type = character(),
      game_id = integer(),
      affected_rows = numeric(),
      detail = character(),
      stringsAsFactors = FALSE
    ))
  }
  out <- do.call(rbind, issues)
  out[order(out$issue_type, out$game_id), , drop = FALSE]
}

build_checks <- function(con, schema) {
  fr <- quote_table(con, schema, "full_rosters")
  ll <- quote_table(con, schema, "lineups_lookup")
  llo <- quote_table(con, schema, "lineups_lookup_on")
  sl <- quote_table(con, schema, "sub_lineups")
  sls <- quote_table(con, schema, "sub_lineups_stats")
  onoff <- quote_table(con, schema, "onoff_default_mv")
  pas <- quote_table(con, schema, "player_advanced_stats_mv")
  aliases <- quote_table(con, schema, "player_id_aliases")
  overrides <- quote_table(con, schema, "player_id_game_overrides")
  identity_map <- quote_table(con, schema, "player_identity_map")
  identity_compat <- quote_table(con, schema, "player_identity_aliases_v")
  identity_resolved <- quote_table(con, schema, "resolved_player_identity_v")
  processed <- quote_table(con, schema, "etl_processed_games")
  actions <- quote_table(con, schema, "actions_clean")
  df_long <- quote_table(con, schema, "df_pts_poss_lineups_longer_mv")
  pff <- quote_table(con, schema, "player_four_factors_by_game")
  tmg <- quote_table(con, schema, "team_metrics_by_game_mv")
  lff <- quote_table(con, schema, "lineup_four_factors_by_game")

  list(
    list(
      id = "A_same_player_id_multiple_roster_names",
      title = "Same team-season-player ID has multiple roster names",
      severity = "error",
      purpose = "First identity check. Catches reused provider IDs or bad canonical merges.",
      required_tables = c("full_rosters"),
      sql = sprintf(
        "WITH roster_base AS (
           SELECT
             game_year,
             team_id,
             player_id,
             game_id,
             btrim(concat_ws(' ', firstname, lastname)) AS full_name
           FROM %s
           WHERE player_id IS NOT NULL
             AND btrim(concat_ws(' ', firstname, lastname)) <> ''
         ),
         roster_name_grain AS (
           SELECT
             game_year,
             team_id,
             player_id,
             game_id,
             upper(regexp_replace(regexp_replace(full_name, '\\.\\s+', '.', 'g'), '\\s+', ' ', 'g')) AS normalized_name
           FROM roster_base
           WHERE full_name ~ '[A-Za-z]'
           GROUP BY game_year, team_id, player_id, game_id, normalized_name
         ),
         name_sets AS (
           SELECT
             game_year,
             team_id,
             player_id,
             normalized_name,
             string_agg(game_id::text, ',' ORDER BY game_id) AS games
           FROM roster_name_grain
           GROUP BY game_year, team_id, player_id, normalized_name
         )
         SELECT
           game_year,
           team_id,
           player_id,
           count(*)::int AS distinct_names,
           string_agg(normalized_name || ' [' || games || ']', ' | ' ORDER BY normalized_name) AS name_games
         FROM name_sets
         GROUP BY game_year, team_id, player_id
         HAVING count(*) > 1
         ORDER BY game_year DESC, team_id, player_id",
        fr
      )
    ),
    list(
      id = "B_same_roster_name_multiple_player_ids",
      title = "Same roster name has multiple player IDs in one team-season",
      severity = "warning",
      purpose = "Catches real players split across multiple provider IDs before a season alias exists.",
      required_tables = c("full_rosters"),
      sql = sprintf(
        "WITH roster_names AS (
           SELECT
             game_year,
             team_id,
             player_id,
             upper(regexp_replace(regexp_replace(btrim(concat_ws(' ', firstname, lastname)), '\\.\\s+', '.', 'g'), '\\s+', ' ', 'g')) AS player_name,
             string_agg(DISTINCT game_id::text, ',' ORDER BY game_id::text) AS games
           FROM %s
           WHERE player_id IS NOT NULL
             AND btrim(concat_ws(' ', firstname, lastname)) <> ''
             AND btrim(concat_ws(' ', firstname, lastname)) ~ '[A-Za-z]'
           GROUP BY game_year, team_id, player_id, player_name
         )
         SELECT
           game_year,
           team_id,
           player_name,
           count(DISTINCT player_id)::int AS distinct_player_ids,
           string_agg(DISTINCT player_id::text, ',' ORDER BY player_id::text) AS player_ids,
           string_agg(player_id::text || ' [' || games || ']', ' | ' ORDER BY player_id) AS id_games
         FROM roster_names
         GROUP BY game_year, team_id, player_name
         HAVING count(DISTINCT player_id) > 1
         ORDER BY game_year DESC, team_id, player_name",
        fr
      )
    ),
    list(
      id = "C_active_correction_residue_game_scoped_tables",
      title = "Active correction residue remains in game-scoped tables",
      severity = "error",
      purpose = "Verifies active season aliases and game overrides were applied in roster and lineup base rows.",
      required_tables = c("full_rosters", "lineups_lookup", "player_id_aliases", "player_id_game_overrides"),
      sql = sprintf(
        "WITH active_corrections AS (
           SELECT NULL::int AS game_id, game_year, team_id, alias_player_id, canonical_player_id, 'season'::text AS correction_scope
             FROM %s
            WHERE active
           UNION ALL
           SELECT game_id, game_year, team_id, alias_player_id, canonical_player_id, 'game'::text AS correction_scope
             FROM %s
            WHERE active
         ),
         hits AS (
           SELECT
             'full_rosters'::text AS source_table,
             a.correction_scope,
             fr.game_year,
             fr.team_id,
             fr.player_id AS alias_player_id,
             a.canonical_player_id,
             fr.game_id
           FROM %s fr
           JOIN active_corrections a
             ON a.game_year = fr.game_year
            AND a.team_id = fr.team_id
            AND a.alias_player_id = fr.player_id
            AND (a.game_id IS NULL OR a.game_id = fr.game_id)
           UNION ALL
           SELECT
             'lineups_lookup'::text AS source_table,
             a.correction_scope,
             ll.game_year,
             ll.team_id,
             ll.player_id AS alias_player_id,
             a.canonical_player_id,
             ll.game_id
           FROM %s ll
           JOIN active_corrections a
             ON a.game_year = ll.game_year
            AND a.team_id = ll.team_id
            AND a.alias_player_id = ll.player_id
            AND (a.game_id IS NULL OR a.game_id = ll.game_id)
         )
         SELECT
           source_table,
           correction_scope,
           game_year,
           team_id,
           alias_player_id,
           canonical_player_id,
           count(*)::int AS rows,
           count(DISTINCT game_id)::int AS games,
           string_agg(DISTINCT game_id::text, ',' ORDER BY game_id::text) AS game_ids
         FROM hits
         GROUP BY source_table, correction_scope, game_year, team_id, alias_player_id, canonical_player_id
         ORDER BY source_table, game_year DESC, team_id, alias_player_id",
        aliases, overrides, fr, ll
      )
    ),
    list(
      id = "D_active_game_overrides_without_canonical_roster_row",
      title = "Active game overrides have no canonical roster row",
      severity = "warning",
      purpose = "Catches stale overrides, wrong game IDs, wrong team IDs, or upstream roster changes.",
      required_tables = c("full_rosters", "player_id_game_overrides"),
      sql = sprintf(
        "SELECT
           o.game_id,
           o.game_year,
           o.team_id,
           o.alias_player_id,
           o.canonical_player_id,
           o.player_name,
           count(fr.player_id)::int AS matching_roster_rows
         FROM %s o
         LEFT JOIN %s fr
           ON fr.game_id = o.game_id
          AND fr.game_year = o.game_year
          AND fr.team_id = o.team_id
          AND fr.player_id = o.canonical_player_id
         WHERE o.active
         GROUP BY o.game_id, o.game_year, o.team_id, o.alias_player_id, o.canonical_player_id, o.player_name
         HAVING count(fr.player_id) = 0
         ORDER BY o.game_year DESC, o.team_id, o.game_id",
        overrides, fr
      )
    ),
    list(
      id = "E_aggregate_names_not_roster_valid",
      title = "App aggregate names are not roster-valid full-name pairs",
      severity = "error",
      purpose = "Catches hybrid display names such as a first name from one player and last name from another.",
      required_tables = c("full_rosters", "onoff_default_mv", "player_advanced_stats_mv"),
      sql = sprintf(
        "WITH roster_pairs AS (
           SELECT DISTINCT
             game_year,
             team_id,
             player_id,
             upper(regexp_replace(regexp_replace(btrim(concat_ws(' ', firstname, lastname)), '\\.\\s+', '.', 'g'), '\\s+', ' ', 'g')) AS normalized_name
           FROM %s
           WHERE player_id IS NOT NULL
             AND btrim(concat_ws(' ', firstname, lastname)) <> ''
         ),
         aggregate_names AS (
           SELECT
             'onoff_default_mv'::text AS source_table,
             \"Year\"::int AS game_year,
             team_id,
             player_id,
             btrim(concat_ws(' ', \"First Name\", \"Last Name\")) AS aggregate_name,
             upper(regexp_replace(regexp_replace(btrim(concat_ws(' ', \"First Name\", \"Last Name\")), '\\.\\s+', '.', 'g'), '\\s+', ' ', 'g')) AS normalized_name
           FROM %s
           UNION ALL
           SELECT
             'player_advanced_stats_mv'::text AS source_table,
             game_year,
             team_id,
             player_id,
             btrim(concat_ws(' ', firstname, lastname)) AS aggregate_name,
             upper(regexp_replace(regexp_replace(btrim(concat_ws(' ', firstname, lastname)), '\\.\\s+', '.', 'g'), '\\s+', ' ', 'g')) AS normalized_name
           FROM %s
         )
         SELECT
           a.source_table,
           a.game_year,
           a.team_id,
           a.player_id,
           a.aggregate_name
         FROM aggregate_names a
         LEFT JOIN roster_pairs r
           ON r.game_year = a.game_year
          AND r.team_id = a.team_id
          AND r.player_id = a.player_id
          AND r.normalized_name = a.normalized_name
         WHERE btrim(a.aggregate_name) <> ''
           AND r.player_id IS NULL
         ORDER BY a.source_table, a.game_year DESC, a.team_id, a.player_id",
        fr, onoff, pas
      )
    ),
    list(
      id = "F_lineup_derivative_active_alias_residue",
      title = "Lineup derivative tables still contain active season alias IDs",
      severity = "error",
      purpose = "Catches stale lineup hashes and sub-lineup aggregates after a player correction backfill.",
      required_tables = c("lineups_lookup_on", "sub_lineups", "sub_lineups_stats", "player_id_aliases"),
      sql = sprintf(
        "WITH season_aliases AS (
           SELECT game_year, team_id, alias_player_id, canonical_player_id
             FROM %s
            WHERE active
         ),
         hits AS (
           SELECT
             'lineups_lookup_on'::text AS source_table,
             llo.game_year,
             llo.team_id,
             a.alias_player_id,
             a.canonical_player_id,
             count(*)::int AS rows
           FROM %s llo
           JOIN season_aliases a
             ON a.game_year = llo.game_year
            AND a.team_id = llo.team_id
            AND a.alias_player_id = llo.player_id
           GROUP BY llo.game_year, llo.team_id, a.alias_player_id, a.canonical_player_id
           UNION ALL
           SELECT
             'sub_lineups'::text AS source_table,
             s.game_year,
             s.team_id,
             a.alias_player_id,
             a.canonical_player_id,
             count(*)::int AS rows
           FROM %s s
           JOIN season_aliases a
             ON a.game_year = s.game_year
            AND a.team_id = s.team_id
            AND s.player_ids && ARRAY[a.alias_player_id]::int4[]
           GROUP BY s.game_year, s.team_id, a.alias_player_id, a.canonical_player_id
           UNION ALL
           SELECT
             'sub_lineups_stats'::text AS source_table,
             ss.game_year,
             ss.team_id,
             a.alias_player_id,
             a.canonical_player_id,
             count(*)::int AS rows
           FROM %s ss
           JOIN season_aliases a
             ON a.game_year = ss.game_year
            AND a.team_id = ss.team_id
            AND ss.player_ids && ARRAY[a.alias_player_id]::int4[]
           GROUP BY ss.game_year, ss.team_id, a.alias_player_id, a.canonical_player_id
         )
         SELECT *
         FROM hits
         ORDER BY source_table, game_year DESC, team_id, alias_player_id",
        aliases, llo, sl, sls
      )
    ),
    list(
      id = "G_cleaned_action_duplicate_ids",
      title = "Cleaned actions have duplicate action IDs",
      severity = "error",
      purpose = "Catches duplicate (game_id, id) rows if a source-feed conflict reaches the cleaned table.",
      required_tables = c("actions_clean"),
      sql = sprintf(
        "SELECT
           game_id,
           id,
           count(*)::int AS rows
         FROM %s
         GROUP BY game_id, id
         HAVING count(*) > 1
         ORDER BY game_id, id",
        actions
      )
    ),
    list(
      id = "H_base_loaded_games_missing_processed_marker",
      title = "Base-loaded games are missing etl_processed_games marker",
      severity = "error",
      purpose = "Catches partial publication gaps where base rows were inserted but the game was not marked processed.",
      required_tables = c("full_rosters", "lineups_lookup", "etl_processed_games"),
      sql = sprintf(
        "WITH base_loaded AS (
           SELECT
             game_id,
             max(game_year)::int AS game_year,
             sum(roster_rows)::bigint AS roster_rows,
             sum(lineup_rows)::bigint AS lineup_rows
           FROM (
             SELECT game_id, max(game_year) AS game_year, count(*)::bigint AS roster_rows, 0::bigint AS lineup_rows
               FROM %s
              GROUP BY game_id
             UNION ALL
             SELECT game_id, max(game_year) AS game_year, 0::bigint AS roster_rows, count(*)::bigint AS lineup_rows
               FROM %s
              GROUP BY game_id
           ) x
           GROUP BY game_id
         )
         SELECT
           b.game_id,
           b.game_year,
           b.roster_rows,
           b.lineup_rows
         FROM base_loaded b
         LEFT JOIN %s p
           ON p.game_id = b.game_id
         WHERE p.game_id IS NULL
           AND (b.roster_rows > 0 OR b.lineup_rows > 0)
         ORDER BY b.game_year DESC, b.game_id",
        fr, ll, processed
      )
    ),
    list(
      id = "I_processed_games_missing_base_rows",
      title = "Processed games are missing non-purged base rows",
      severity = "error",
      purpose = "Catches processed markers that would cause ETL to skip games even though roster or lineup base rows are absent.",
      required_tables = c("etl_processed_games", "full_rosters", "lineups_lookup"),
      sql = sprintf(
        "WITH
         fr AS (
           SELECT game_id, count(*)::int AS rows
             FROM %s
            GROUP BY game_id
         ),
         ll AS (
           SELECT game_id, count(*)::int AS rows
             FROM %s
            GROUP BY game_id
         )
         SELECT
           p.game_id,
           p.game_year,
           coalesce(fr.rows, 0) AS full_rosters_rows,
           coalesce(ll.rows, 0) AS lineups_lookup_rows
         FROM %s p
         LEFT JOIN fr ON fr.game_id = p.game_id
         LEFT JOIN ll ON ll.game_id = p.game_id
         WHERE coalesce(fr.rows, 0) = 0
            OR coalesce(ll.rows, 0) = 0
         ORDER BY p.game_year DESC, p.game_id",
        fr, ll, processed
      )
    ),
    list(
      id = "J_processed_base_games_missing_downstream_game_rows",
      title = "Processed base-loaded games are missing downstream game-grain rows",
      severity = "error",
      purpose = "Catches games marked processed before the game-grain refresh chain completed.",
      required_tables = c("etl_processed_games", "full_rosters", "lineups_lookup", "df_pts_poss_lineups_longer_mv", "player_four_factors_by_game", "team_metrics_by_game_mv"),
      sql = sprintf(
        "WITH
         base_loaded AS (
           SELECT
             game_id,
             max(full_rosters_rows)::int AS full_rosters_rows,
             max(lineups_lookup_rows)::int AS lineups_lookup_rows
           FROM (
             SELECT game_id, count(*)::int AS full_rosters_rows, 0::int AS lineups_lookup_rows
               FROM %s
              GROUP BY game_id
             UNION ALL
             SELECT game_id, 0::int AS full_rosters_rows, count(*)::int AS lineups_lookup_rows
               FROM %s
              GROUP BY game_id
           ) x
           GROUP BY game_id
         ),
         df AS (
           SELECT game_id, count(*)::int AS rows
             FROM %s
            GROUP BY game_id
         ),
         pff AS (
           SELECT game_id, count(*)::int AS rows
             FROM %s
            GROUP BY game_id
         ),
         tmg AS (
           SELECT game_id, count(*)::int AS rows
             FROM %s
            GROUP BY game_id
         )
         SELECT
           p.game_id,
           p.game_year,
           b.full_rosters_rows,
           b.lineups_lookup_rows,
           coalesce(df.rows, 0) AS df_pts_poss_lineups_longer_rows,
           coalesce(pff.rows, 0) AS player_four_factors_rows,
           coalesce(tmg.rows, 0) AS team_metrics_rows
         FROM %s p
         JOIN base_loaded b
           ON b.game_id = p.game_id
         LEFT JOIN df ON df.game_id = p.game_id
         LEFT JOIN pff ON pff.game_id = p.game_id
         LEFT JOIN tmg ON tmg.game_id = p.game_id
         WHERE b.full_rosters_rows > 0
           AND b.lineups_lookup_rows > 0
           AND (
             coalesce(df.rows, 0) = 0
             OR coalesce(pff.rows, 0) = 0
             OR coalesce(tmg.rows, 0) = 0
           )
         ORDER BY p.game_year DESC, p.game_id",
        fr, ll, df_long, pff, tmg, processed
      )
    ),
    list(
      id = "K_app_aggregate_duplicate_keys",
      title = "App aggregate tables have duplicate player keys",
      severity = "error",
      purpose = "Keeps app-facing player surfaces unique at their expected key grain.",
      required_tables = c("onoff_default_mv", "player_advanced_stats_mv"),
      sql = sprintf(
        "SELECT *
         FROM (
           SELECT
             'onoff_default_mv'::text AS source_table,
             '(Year, team_id, player_id)'::text AS key_columns,
             count(*)::int AS duplicate_groups
           FROM (
             SELECT \"Year\", team_id, player_id
               FROM %s
              GROUP BY \"Year\", team_id, player_id
             HAVING count(*) > 1
           ) d
           UNION ALL
           SELECT
             'player_advanced_stats_mv'::text AS source_table,
             '(game_year, team_id, player_id)'::text AS key_columns,
             count(*)::int AS duplicate_groups
           FROM (
             SELECT game_year, team_id, player_id
               FROM %s
              GROUP BY game_year, team_id, player_id
             HAVING count(*) > 1
           ) d
         ) x
         WHERE duplicate_groups > 0
         ORDER BY source_table",
        onoff, pas
      )
    ),
    list(
      id = "L_raw_pbp_duplicate_action_ids",
      title = "Raw PBP duplicate action IDs",
      severity = "todo",
      purpose = "Needs a pre-clean raw payload check. This report records the gap so it is not forgotten.",
      required_tables = character(0),
      sql = NA_character_,
      not_automated_note = paste(
        "Not automated in this script yet.",
        "The check must run against raw provider payloads before clean_actions(),",
        "because actions_clean and cold storage only contain cleaned rows.",
        "Known case: game 381 duplicated action IDs 3810375 through 3810385 at the Q2/Q3 boundary."
      )
    ),
    list(
      id = "M_identity_dictionary_mapping_ambiguities",
      title = "Player identity dictionary has ambiguous or conflicting mappings",
      severity = "error",
      purpose = "Ensures one active mapping per source context and verifies game overrides resolve to the canonical roster identity.",
      required_tables = c("player_identity_map"),
      sql = sprintf(
        "WITH problems AS (
           SELECT
             'duplicate_active_season_mapping'::text AS problem,
             NULL::int AS game_id,
             game_year,
             team_id,
             source_player_id,
             count(*)::int AS rows,
             NULL::text AS detail
           FROM %s
           WHERE active
             AND game_id IS NULL
           GROUP BY game_year, team_id, source_player_id
           HAVING count(*) > 1
           UNION ALL
           SELECT
             'duplicate_active_game_mapping'::text AS problem,
             game_id,
             max(game_year)::int AS game_year,
             team_id,
             source_player_id,
             count(*)::int AS rows,
             NULL::text AS detail
           FROM %s
           WHERE active
             AND game_id IS NOT NULL
           GROUP BY game_id, team_id, source_player_id
           HAVING count(*) > 1
           UNION ALL
           SELECT
             'game_mapping_canonical_identity_conflict'::text AS problem,
             g.game_id,
             g.game_year,
             g.team_id,
             g.source_player_id,
             1::int AS rows,
             'game identity_id=' || g.identity_id::text ||
               ', canonical season identity_id=' || c.identity_id::text AS detail
           FROM %s g
           JOIN %s c
             ON c.active
            AND c.provider = g.provider
            AND c.game_id IS NULL
            AND c.game_year = g.game_year
            AND c.team_id = g.team_id
            AND c.source_player_id = g.canonical_player_id
            AND c.canonical_player_id = g.canonical_player_id
           WHERE g.active
             AND g.game_id IS NOT NULL
             AND g.identity_id <> c.identity_id
         )
         SELECT *
         FROM problems
         ORDER BY problem, game_year, team_id, source_player_id, game_id",
        identity_map, identity_map, identity_map, identity_map
      )
    ),
    list(
      id = "N_identity_compatibility_missing_legacy_corrections",
      title = "Identity compatibility view is missing active legacy corrections",
      severity = "error",
      purpose = "Guarantees the current ETL receives the same alias-to-canonical corrections after switching to the dictionary.",
      required_tables = c(
        "player_id_aliases",
        "player_id_game_overrides",
        "player_identity_aliases_v"
      ),
      sql = sprintf(
        "WITH corrections AS (
           SELECT
             NULL::int AS game_id,
             game_year,
             team_id,
             alias_player_id,
             canonical_player_id,
             'season'::text AS correction_scope
           FROM %s
           WHERE active
           UNION ALL
           SELECT
             game_id,
             game_year,
             team_id,
             alias_player_id,
             canonical_player_id,
             'game'::text AS correction_scope
           FROM %s
           WHERE active
         )
         SELECT c.*
         FROM corrections c
         LEFT JOIN %s v
           ON v.game_id IS NOT DISTINCT FROM c.game_id
          AND v.game_year = c.game_year
          AND v.team_id = c.team_id
          AND v.alias_player_id = c.alias_player_id
          AND v.canonical_player_id = c.canonical_player_id
          AND v.correction_scope = c.correction_scope
         WHERE v.alias_player_id IS NULL
         ORDER BY c.game_year DESC, c.team_id, c.alias_player_id, c.game_id",
        aliases, overrides, identity_compat
      )
    ),
    list(
      id = "O_identity_unresolved_source_contexts",
      title = "Roster source contexts are unresolved by the identity dictionary",
      severity = "warning",
      purpose = "Surfaces roster identities that fell back to provider source IDs instead of a stable dictionary identity.",
      required_tables = c("resolved_player_identity_v"),
      sql = sprintf(
        "SELECT
           game_year,
           team_id,
           source_player_id,
           count(DISTINCT game_id)::int AS games,
           string_agg(DISTINCT game_id::text, ',' ORDER BY game_id::text) AS game_ids,
           max(display_name) AS display_name
         FROM %s
         WHERE resolution_scope = 'source'
         GROUP BY game_year, team_id, source_player_id
         ORDER BY game_year DESC, team_id, source_player_id",
        identity_resolved
      )
    ),
    list(
      id = "P0_source_placeholder_roster_identities",
      title = "Source rosters contain placeholder player identities",
      severity = "warning",
      purpose = "Keeps unusable provider identities visible even when they are excluded from app-facing aggregates.",
      required_tables = c("full_rosters"),
      problem_count_col = "placeholder_identities",
      sql = sprintf(
        "WITH placeholders AS (
           SELECT
             game_year,
             team_id,
             player_id,
             max(btrim(concat_ws(' ', firstname, lastname))) AS player_name,
             count(DISTINCT game_id)::int AS games,
             string_agg(DISTINCT game_id::text, ',' ORDER BY game_id::text) AS game_ids
           FROM %s
           WHERE upper(btrim(concat_ws(' ', firstname, lastname))) IN
                 ('NEW NEW', 'UNKNOWN UNKNOWN', 'TEST TEST')
           GROUP BY game_year, team_id, player_id
         )
         SELECT
           *,
           1::bigint AS placeholder_identities
         FROM placeholders
         ORDER BY game_year DESC, team_id, player_id",
        fr
      )
    ),
    list(
      id = "P_app_invalid_or_nonparticipant_player_rows",
      title = "App player aggregates contain non-participating placeholder identities",
      severity = "warning",
      purpose = "Reports provider placeholder identities visible in app-facing aggregates. They did not participate and do not affect scoring, possessions, lineups, or minutes.",
      required_tables = c("onoff_default_mv", "player_advanced_stats_mv"),
      problem_count_col = "invalid_rows",
      sql = sprintf(
        "WITH onoff_counts AS (
           SELECT count(*)::bigint AS total_rows
           FROM %s
         ),
         advanced_counts AS (
           SELECT count(*)::bigint AS total_rows
           FROM %s
         ),
         placeholder_keys AS (
           SELECT \"Year\"::int AS game_year, team_id, player_id
           FROM %s
           WHERE upper(btrim(concat_ws(' ', \"First Name\", \"Last Name\"))) IN
                 ('NEW NEW', 'UNKNOWN UNKNOWN', 'TEST TEST')

           UNION

           SELECT game_year, team_id, player_id
           FROM %s
           WHERE upper(btrim(concat_ws(' ', firstname, lastname))) IN
                 ('NEW NEW', 'UNKNOWN UNKNOWN', 'TEST TEST')
         ),
         no_on_floor_keys AS (
           SELECT \"Year\"::int AS game_year, team_id, player_id
           FROM %s
           WHERE coalesce(\"ON Poss\", 0) = 0
             AND coalesce(minutes, 0) = 0
             AND coalesce(\"OFF Poss\", 0) > 0
         )
         SELECT
           'app_player_aggregates'::text AS source_table,
           o.total_rows,
           a.total_rows AS advanced_total_rows,
           (SELECT count(*) FROM placeholder_keys)::bigint AS placeholder_rows,
           (SELECT count(*) FROM no_on_floor_keys)::bigint AS no_on_floor_rows,
           (SELECT count(*) FROM placeholder_keys)::bigint AS invalid_rows,
           round(
             100.0 * (SELECT count(*) FROM placeholder_keys) /
               nullif(o.total_rows, 0),
             4
           ) AS invalid_pct
         FROM onoff_counts o
         CROSS JOIN advanced_counts a",
        onoff, pas, onoff, pas, onoff
      )
    ),
    list(
      id = "P1_reviewed_data_quality_exceptions",
      title = "Reviewed non-actionable data-quality exceptions",
      severity = "warning",
      purpose = "Keeps reviewed source and administrative exceptions visible without treating them as unresolved ETL defects.",
      required_tables = c("schedule"),
      problem_count_col = "reviewed_exceptions",
      sql = paste0(
        "SELECT *
         FROM (
           VALUES
             (157, 'starter_context', 'Period-boundary substitution declarations have no opponent starter context; no statistical impact.', 1::bigint),
             (184, 'early_termination', 'Game ended early; short event timeline and derived score coverage are expected.', 1::bigint),
             (380, 'early_termination_official_result', 'Game ended early; schedule stores the official 20-1 result while played events reconstruct to 74-66.', 1::bigint)
         ) AS x(game_id, exception_type, reason, reviewed_exceptions)
         ORDER BY game_id"
      )
    ),
    list(
      id = "Q_persisted_rows_without_lineup_match",
      title = "Persisted event-team rows have no five-player lineup match",
      severity = "error",
      purpose = "Counts df_pts_poss_lineups_longer rows whose game/team/lineup hash cannot resolve to an ON-floor five-player lineup.",
      required_tables = c("df_pts_poss_lineups_longer_mv", "lineups_lookup"),
      problem_count_col = "unmatched_rows",
      sql = sprintf(
        "WITH lineup_keys AS (
           SELECT
             game_id,
             team_id,
             lineup_hash,
             count(DISTINCT player_id) FILTER (WHERE is_on_verdict = 1)::int AS players_on
           FROM %s
           WHERE lineup_hash IS NOT NULL
           GROUP BY game_id, team_id, lineup_hash
         ),
         row_quality AS (
           SELECT
             d.game_id,
             count(*)::bigint AS total_rows,
             count(*) FILTER (
               WHERE lk.lineup_hash IS NULL
                  OR lk.players_on <> 5
             )::bigint AS unmatched_rows
           FROM %s d
           LEFT JOIN lineup_keys lk
             ON lk.game_id = d.game_id
            AND lk.team_id = d.team_id
            AND lk.lineup_hash = d.lineup_hash
           GROUP BY d.game_id
         ),
         totals AS (
           SELECT
             sum(total_rows)::bigint AS overall_total_rows,
             sum(unmatched_rows)::bigint AS overall_unmatched_rows
           FROM row_quality
         ),
         affected_totals AS (
           SELECT
             sum(total_rows)::bigint AS affected_total_rows,
             sum(unmatched_rows)::bigint AS affected_unmatched_rows
           FROM row_quality
           WHERE unmatched_rows > 0
         )
         SELECT
           r.game_id,
           r.total_rows,
           r.unmatched_rows,
           round(100.0 * r.unmatched_rows / nullif(r.total_rows, 0), 4) AS unmatched_pct,
           t.overall_total_rows,
           t.overall_unmatched_rows,
           round(
             100.0 * t.overall_unmatched_rows / nullif(t.overall_total_rows, 0),
             4
           ) AS overall_unmatched_pct,
           a.affected_total_rows,
           a.affected_unmatched_rows,
           round(
             100.0 * a.affected_unmatched_rows / nullif(a.affected_total_rows, 0),
             4
           ) AS affected_games_unmatched_pct
         FROM row_quality r
         CROSS JOIN totals t
         CROSS JOIN affected_totals a
         WHERE r.unmatched_rows > 0
         ORDER BY r.unmatched_rows DESC, r.game_id",
        ll, df_long
      )
    ),
    list(
      id = "R_invalid_lineup_player_counts",
      title = "Lineup states do not contain exactly five distinct ON players",
      severity = "error",
      purpose = "Counts lineup-state rows with missing hashes or player counts other than five.",
      required_tables = c("lineups_lookup"),
      problem_count_col = "invalid_states",
      sql = sprintf(
        "WITH states AS (
           SELECT
             game_id,
             team_id,
             id,
             max(lineup_hash) AS lineup_hash,
             max(n_on)::int AS reported_n_on,
             count(*) FILTER (WHERE is_on_verdict = 1)::int AS on_rows,
             count(DISTINCT player_id) FILTER (WHERE is_on_verdict = 1)::int AS distinct_on_players
           FROM %s
           GROUP BY game_id, team_id, id
         ),
         quality AS (
           SELECT
             *,
             (
               lineup_hash IS NULL
               OR reported_n_on IS DISTINCT FROM 5
               OR on_rows <> 5
               OR distinct_on_players <> 5
             ) AS invalid
           FROM states
         ),
         totals AS (
           SELECT
             count(*)::bigint AS overall_total_states,
             count(*) FILTER (WHERE invalid)::bigint AS overall_invalid_states
           FROM quality
         )
         SELECT
           q.game_id,
           q.team_id,
           count(*)::bigint AS total_states,
           count(*) FILTER (WHERE q.invalid)::bigint AS invalid_states,
           round(
             100.0 * count(*) FILTER (WHERE q.invalid) / nullif(count(*), 0),
             4
           ) AS invalid_pct,
           min(q.reported_n_on) FILTER (WHERE q.invalid) AS min_reported_n_on,
           max(q.reported_n_on) FILTER (WHERE q.invalid) AS max_reported_n_on,
           t.overall_total_states,
           t.overall_invalid_states,
           round(
             100.0 * t.overall_invalid_states / nullif(t.overall_total_states, 0),
             4
           ) AS overall_invalid_pct
         FROM quality q
         CROSS JOIN totals t
         GROUP BY
           q.game_id,
           q.team_id,
           t.overall_total_states,
           t.overall_invalid_states
         HAVING count(*) FILTER (WHERE q.invalid) > 0
         ORDER BY invalid_states DESC, q.game_id, q.team_id",
        ll
      )
    ),
    list(
      id = "S_invalid_starter_counts",
      title = "Statistical rows are missing valid starter context",
      severity = "warning",
      purpose = "Validates base lineup starter counts and statistical offense/defense rows. Administrative rows without event ownership are excluded.",
      required_tables = c("lineups_lookup", "df_pts_poss_lineups_longer_mv"),
      problem_count_col = "invalid_rows",
      sql = sprintf(
        "WITH lineup_states AS (
           SELECT
             game_id,
             team_id,
             id,
             min(num_starters)::int AS min_starters,
             max(num_starters)::int AS max_starters
           FROM %s
           GROUP BY game_id, team_id, id
         ),
         counts AS (
           SELECT
             'lineups_lookup_states'::text AS source_table,
             count(*)::bigint AS total_rows,
             count(*) FILTER (
               WHERE min_starters IS NULL
                  OR max_starters IS NULL
                  OR min_starters <> max_starters
                  OR min_starters < 0
                  OR max_starters > 5
             )::bigint AS invalid_rows,
             count(DISTINCT game_id) FILTER (
               WHERE min_starters IS NULL
                  OR max_starters IS NULL
                  OR min_starters <> max_starters
                  OR min_starters < 0
                  OR max_starters > 5
             )::int AS affected_games,
             string_agg(DISTINCT game_id::text, ',' ORDER BY game_id::text)
               FILTER (
                 WHERE min_starters IS NULL
                    OR max_starters IS NULL
                    OR min_starters <> max_starters
                    OR min_starters < 0
                    OR max_starters > 5
               ) AS affected_game_ids
           FROM lineup_states

           UNION ALL

           SELECT
             'df_pts_poss_lineups_longer_mv'::text,
             count(*)::bigint,
             count(*) FILTER (
               WHERE type_lineup IN ('offense', 'defense')
                 AND (
                      num_starters IS NULL
                   OR own_starters IS NULL
                   OR opp_starters IS NULL
                   OR num_starters NOT BETWEEN 0 AND 5
                   OR own_starters NOT BETWEEN 0 AND 5
                   OR opp_starters NOT BETWEEN 0 AND 5
                 )
             )::bigint,
             count(DISTINCT game_id) FILTER (
               WHERE type_lineup IN ('offense', 'defense')
                 AND (
                      num_starters IS NULL
                   OR own_starters IS NULL
                   OR opp_starters IS NULL
                   OR num_starters NOT BETWEEN 0 AND 5
                   OR own_starters NOT BETWEEN 0 AND 5
                   OR opp_starters NOT BETWEEN 0 AND 5
                 )
             )::int,
             string_agg(DISTINCT game_id::text, ',' ORDER BY game_id::text)
               FILTER (
                 WHERE type_lineup IN ('offense', 'defense')
                   AND (
                        num_starters IS NULL
                     OR own_starters IS NULL
                     OR opp_starters IS NULL
                     OR num_starters NOT BETWEEN 0 AND 5
                     OR own_starters NOT BETWEEN 0 AND 5
                     OR opp_starters NOT BETWEEN 0 AND 5
                   )
               )
           FROM %s
         )
         SELECT
           source_table,
           total_rows,
           invalid_rows,
           round(100.0 * invalid_rows / nullif(total_rows, 0), 4) AS invalid_pct,
           affected_games,
           affected_game_ids
         FROM counts
         ORDER BY source_table",
        ll, df_long
      )
    ),
    list(
      id = "T_invalid_team_minutes",
      title = "App-equivalent team minutes differ materially from official duration",
      severity = "error",
      purpose = "Sums canonical lineup-boundary segment durations and flags team-games more than one minute above or below official regulation/OT duration. Reviewed early terminations remain in P1 instead.",
      required_tables = c("df_pts_poss_lineups_longer_mv"),
      problem_count_col = "invalid_team_games",
      sql = sprintf(
        "WITH segment_times AS (
           SELECT
             game_id,
             team_id,
             lineup_hash,
             segment_id,
             max(segment_seconds)::numeric AS segment_seconds
           FROM %s
           WHERE game_id NOT IN (184, 380)
             AND lineup_hash IS NOT NULL
             AND segment_id IS NOT NULL
             AND segment_seconds IS NOT NULL
           GROUP BY game_id, team_id, lineup_hash, segment_id
         ),
         team_minutes AS (
           SELECT
             game_id,
             team_id,
             coalesce(sum(segment_seconds), 0)::numeric / 60.0 AS minutes
           FROM segment_times
           GROUP BY game_id, team_id
         ),
         quarter_counts AS (
           SELECT game_id, max(quarter)::int AS max_quarter
           FROM %s
           GROUP BY game_id
         ),
         quality AS (
           SELECT
             tm.game_id,
             tm.team_id,
             tm.minutes,
             (40 + greatest(coalesce(q.max_quarter, 4) - 4, 0) * 5)::numeric AS expected_minutes
           FROM team_minutes tm
           LEFT JOIN quarter_counts q USING (game_id)
         ),
         marked AS (
           SELECT
             *,
             (
               minutes < 0
               OR abs(minutes - expected_minutes) > 1.0
             ) AS invalid
           FROM quality
         ),
         totals AS (
           SELECT
             count(*)::bigint AS overall_total_team_games,
             count(*) FILTER (WHERE invalid)::bigint AS overall_invalid_team_games
           FROM marked
         )
         SELECT
           m.game_id,
           m.team_id,
           round(m.minutes::numeric, 3) AS minutes,
           m.expected_minutes,
           round((m.minutes - m.expected_minutes)::numeric, 3) AS minute_difference,
           1::bigint AS invalid_team_games,
           t.overall_total_team_games,
           t.overall_invalid_team_games,
           round(
             100.0 * t.overall_invalid_team_games /
               nullif(t.overall_total_team_games, 0),
             4
           ) AS overall_invalid_pct
         FROM marked m
         CROSS JOIN totals t
         WHERE m.invalid
         ORDER BY abs(m.minutes - m.expected_minutes) DESC, m.game_id, m.team_id",
        df_long, df_long
      )
    ),
    list(
      id = "U_invalid_lineup_metric_values",
      title = "Lineup game rows contain invalid counts or minutes",
      severity = "error",
      purpose = "Counts lineup rows with negative values, makes above attempts, invalid starter counts, or negative minutes.",
      required_tables = c("lineup_four_factors_by_game"),
      problem_count_col = "invalid_rows",
      sql = sprintf(
        "SELECT
           count(*)::bigint AS total_rows,
           count(*) FILTER (
             WHERE num_starters IS NULL
                OR num_starters NOT BETWEEN 0 AND 5
                OR coalesce(total_points, 0) < 0
                OR coalesce(total_poss, 0) < 0
                OR coalesce(ts_poss_count, 0) < 0
                OR coalesce(oreb_count, 0) < 0
                OR coalesce(oreb_opportunities, 0) < 0
                OR coalesce(tov_count, 0) < 0
                OR coalesce(total_ft_attempts, 0) < 0
                OR coalesce(total_fga, 0) < 0
                OR coalesce(total_fgm, 0) < 0
                OR coalesce(total_fg3_made, 0) < 0
                OR coalesce(total_fgm, 0) > coalesce(total_fga, 0)
                OR coalesce(total_fg3_made, 0) > coalesce(total_fgm, 0)
                OR coalesce(minutes, 0) < 0
           )::bigint AS invalid_rows,
           round(
             100.0 * count(*) FILTER (
               WHERE num_starters IS NULL
                  OR num_starters NOT BETWEEN 0 AND 5
                  OR coalesce(total_points, 0) < 0
                  OR coalesce(total_poss, 0) < 0
                  OR coalesce(ts_poss_count, 0) < 0
                  OR coalesce(oreb_count, 0) < 0
                  OR coalesce(oreb_opportunities, 0) < 0
                  OR coalesce(tov_count, 0) < 0
                  OR coalesce(total_ft_attempts, 0) < 0
                  OR coalesce(total_fga, 0) < 0
                  OR coalesce(total_fgm, 0) < 0
                  OR coalesce(total_fg3_made, 0) < 0
                  OR coalesce(total_fgm, 0) > coalesce(total_fga, 0)
                  OR coalesce(total_fg3_made, 0) > coalesce(total_fgm, 0)
                  OR coalesce(minutes, 0) < 0
             ) / nullif(count(*), 0),
             4
           ) AS invalid_pct
         FROM %s",
        lff
      )
    ),
    list(
      id = "V_team_game_score_reconciliation",
      title = "Team-game scores do not reconcile across schedule and derived totals",
      severity = "error",
      purpose = "Compares schedule scores with reconstructed traditional points, lineup offense points, and opponent defense points.",
      required_tables = c("team_metrics_by_game_mv", "etl_processed_games"),
      problem_count_col = "invalid_team_games",
      sql = sprintf(
        "WITH quality AS (
           SELECT
             tm.game_id,
             tm.team_id,
             tm.team_name,
             tm.team_score,
             tm.opp_score,
             tm.pts AS traditional_points,
             tm.off_points_raw AS lineup_offense_points,
             tm.def_points_raw AS lineup_defense_points,
             CASE
               WHEN tm.pts IS NULL
                 OR tm.off_points_raw IS NULL
                 OR tm.def_points_raw IS NULL
               THEN 'missing_derived_data'
               ELSE 'score_mismatch'
             END AS failure_type,
             (
               tm.pts IS DISTINCT FROM tm.team_score
               OR tm.off_points_raw IS DISTINCT FROM tm.team_score
               OR tm.def_points_raw IS DISTINCT FROM tm.opp_score
             ) AS invalid
           FROM %s tm
           JOIN %s ep
             ON ep.game_id = tm.game_id
           WHERE tm.game_id NOT IN (184, 380)
         ),
         totals AS (
           SELECT
             count(*)::bigint AS overall_total_team_games,
             count(*) FILTER (WHERE invalid)::bigint AS overall_invalid_team_games
           FROM quality
         )
         SELECT
           q.game_id,
           q.team_id,
           q.team_name,
           q.team_score,
           q.traditional_points,
           q.lineup_offense_points,
           q.opp_score,
           q.lineup_defense_points,
           q.failure_type,
           1::bigint AS invalid_team_games,
           t.overall_total_team_games,
           t.overall_invalid_team_games,
           round(
             100.0 * t.overall_invalid_team_games /
               nullif(t.overall_total_team_games, 0),
             4
           ) AS overall_invalid_pct
         FROM quality q
         CROSS JOIN totals t
         WHERE q.invalid
         ORDER BY q.game_id, q.team_id",
        tmg, processed
      )
    ),
    list(
      id = "W_team_game_possession_reconciliation",
      title = "Team-game offense and defense possessions do not reconcile",
      severity = "error",
      purpose = "Verifies each team's offense possessions equal its opponent's defense possessions and vice versa.",
      required_tables = c("team_metrics_by_game_mv"),
      problem_count_col = "invalid_team_games",
      sql = sprintf(
        "WITH quality AS (
           SELECT
             a.game_id,
             a.team_id,
             a.team_name,
             a.opp_team_id,
             a.opp_team_name,
             a.off_poss_raw AS team_off_poss,
             a.def_poss_raw AS team_def_poss,
             b.def_poss_raw AS opponent_def_poss,
             b.off_poss_raw AS opponent_off_poss,
             (
               b.team_id IS NULL
               OR a.off_poss_raw IS DISTINCT FROM b.def_poss_raw
               OR a.def_poss_raw IS DISTINCT FROM b.off_poss_raw
             ) AS invalid
           FROM %s a
           LEFT JOIN %s b
             ON b.game_id = a.game_id
            AND b.team_id = a.opp_team_id
         ),
         totals AS (
           SELECT
             count(*)::bigint AS overall_total_team_games,
             count(*) FILTER (WHERE invalid)::bigint AS overall_invalid_team_games
           FROM quality
         )
         SELECT
           q.game_id,
           q.team_id,
           q.team_name,
           q.opp_team_id,
           q.opp_team_name,
           q.team_off_poss,
           q.opponent_def_poss,
           q.team_def_poss,
           q.opponent_off_poss,
           1::bigint AS invalid_team_games,
           t.overall_total_team_games,
           t.overall_invalid_team_games,
           round(
             100.0 * t.overall_invalid_team_games /
               nullif(t.overall_total_team_games, 0),
             4
           ) AS overall_invalid_pct
         FROM quality q
         CROSS JOIN totals t
         WHERE q.invalid
         ORDER BY q.game_id, q.team_id",
        tmg, tmg
      )
    ),
    list(
      id = "X_player_minute_conservation",
      title = "Player minutes do not conserve reconstructed team lineup minutes",
      severity = "error",
      purpose = "Sums player ON-floor minutes and compares them with five times the reconstructed team lineup minutes. Official-duration completeness is checked separately.",
      required_tables = c(
        "lineups_lookup",
        "df_pts_poss_lineups_longer_mv",
        "team_metrics_by_game_mv",
        "etl_processed_games"
      ),
      problem_count_col = "invalid_team_games",
      sql = sprintf(
        "WITH lineup_map AS (
           SELECT DISTINCT
             game_id,
             team_id,
             lineup_hash,
             player_id
           FROM %s
           WHERE coalesce(is_on_verdict, 0)::int = 1
             AND lineup_hash IS NOT NULL
         ),
         segment_times AS (
           SELECT
             game_id,
             team_id,
             lineup_hash,
             segment_id,
             max(segment_seconds)::numeric / 60.0 AS lineup_minutes
           FROM %s
           WHERE lineup_hash IS NOT NULL
             AND segment_id IS NOT NULL
             AND segment_seconds IS NOT NULL
           GROUP BY game_id, team_id, lineup_hash, segment_id
         ),
         team_segment_minutes AS (
           SELECT
             game_id,
             team_id,
             sum(lineup_minutes)::numeric AS team_lineup_minutes
           FROM segment_times
           GROUP BY game_id, team_id
         ),
         player_game_minutes AS (
           SELECT
             st.game_id,
             lm.team_id,
             lm.player_id,
             sum(st.lineup_minutes)::numeric AS player_minutes
           FROM segment_times st
           JOIN lineup_map lm
             ON lm.game_id = st.game_id
            AND lm.team_id = st.team_id
            AND lm.lineup_hash = st.lineup_hash
           GROUP BY st.game_id, lm.team_id, lm.player_id
         ),
         team_player_minutes AS (
           SELECT
             game_id,
             team_id,
             sum(player_minutes)::numeric AS actual_player_minutes,
             count(*) FILTER (WHERE player_minutes > 0)::int AS players_with_minutes
           FROM player_game_minutes
           GROUP BY game_id, team_id
         ),
         quality AS (
           SELECT
             tm.game_id,
             tm.team_id,
             tm.team_name,
             coalesce(pm.actual_player_minutes, 0)::numeric AS actual_player_minutes,
             (5 * coalesce(sm.team_lineup_minutes, 0))::numeric AS expected_player_minutes,
             coalesce(pm.players_with_minutes, 0)::int AS players_with_minutes,
             (
               pm.game_id IS NULL
               OR sm.game_id IS NULL
               OR abs(
                 coalesce(pm.actual_player_minutes, 0) -
                   5 * coalesce(sm.team_lineup_minutes, 0)
               ) > 0.1
             ) AS invalid
           FROM %s tm
           JOIN %s ep
             ON ep.game_id = tm.game_id
           LEFT JOIN team_segment_minutes sm
             ON sm.game_id = tm.game_id
            AND sm.team_id = tm.team_id
           LEFT JOIN team_player_minutes pm
             ON pm.game_id = tm.game_id
            AND pm.team_id = tm.team_id
         ),
         totals AS (
           SELECT
             count(*)::bigint AS overall_total_team_games,
             count(*) FILTER (WHERE invalid)::bigint AS overall_invalid_team_games
           FROM quality
         )
         SELECT
           q.game_id,
           q.team_id,
           q.team_name,
           round(q.actual_player_minutes, 2) AS actual_player_minutes,
           q.expected_player_minutes,
           round(q.actual_player_minutes - q.expected_player_minutes, 2) AS minute_difference,
           q.players_with_minutes,
           1::bigint AS invalid_team_games,
           t.overall_total_team_games,
           t.overall_invalid_team_games,
           round(
             100.0 * t.overall_invalid_team_games /
               nullif(t.overall_total_team_games, 0),
             4
           ) AS overall_invalid_pct
         FROM quality q
         CROSS JOIN totals t
         WHERE q.invalid
         ORDER BY abs(q.actual_player_minutes - q.expected_player_minutes) DESC,
                  q.game_id,
                  q.team_id",
        ll, df_long, tmg, processed
      )
    ),
    list(
      id = "Y_ot_period_start_lineup_coverage",
      title = "Overtime periods do not begin with a valid five-player lineup",
      severity = "error",
      purpose = "Requires every persisted team/OT period to have a valid five-player lineup state at the 05:00 boundary.",
      required_tables = c("lineups_lookup"),
      problem_count_col = "invalid_periods",
      sql = sprintf(
        "WITH states AS (
           SELECT
             game_id,
             team_id,
             quarter,
             id,
             end_quarter_seconds_remaining,
             max(lineup_hash) AS lineup_hash,
             count(DISTINCT player_id)
               FILTER (WHERE coalesce(is_on_verdict, 0)::int = 1)::int AS players_on
           FROM %s
           WHERE quarter >= 5
           GROUP BY
             game_id,
             team_id,
             quarter,
             id,
             end_quarter_seconds_remaining
         ),
         period_quality AS (
           SELECT
             game_id,
             team_id,
             quarter,
             max(end_quarter_seconds_remaining)
               FILTER (WHERE lineup_hash IS NOT NULL AND players_on = 5)
               AS first_valid_clock,
             count(*) FILTER (
               WHERE lineup_hash IS NOT NULL AND players_on = 5
             )::int AS valid_states
           FROM states
           GROUP BY game_id, team_id, quarter
         ),
         marked AS (
           SELECT
             *,
             (
               first_valid_clock IS NULL
               OR first_valid_clock < 299
             ) AS invalid
           FROM period_quality
         ),
         totals AS (
           SELECT
             count(*)::bigint AS overall_total_periods,
             count(*) FILTER (WHERE invalid)::bigint AS overall_invalid_periods
           FROM marked
         )
         SELECT
           m.game_id,
           m.team_id,
           m.quarter,
           m.first_valid_clock,
           m.valid_states,
           1::bigint AS invalid_periods,
           t.overall_total_periods,
           t.overall_invalid_periods,
           round(
             100.0 * t.overall_invalid_periods /
               nullif(t.overall_total_periods, 0),
             4
           ) AS overall_invalid_pct
         FROM marked m
         CROSS JOIN totals t
         WHERE m.invalid
         ORDER BY m.game_id, m.quarter, m.team_id",
        ll
      )
    ),
    list(
      id = "Z_ot_event_player_lineup_mismatches",
      title = "Overtime event players are absent from their team's attached lineup",
      severity = "warning",
      purpose = "Flags OT action-player assignments that disagree with the persisted five-player lineup; same-clock provider ordering may explain some rows.",
      required_tables = c(
        "full_rosters",
        "lineups_lookup",
        "df_pts_poss_lineups_longer_mv"
      ),
      problem_count_col = "unmatched_events",
      sql = sprintf(
        "WITH roster_map AS (
           SELECT DISTINCT game_id, player_id, team_id
           FROM %s
           WHERE player_id IS NOT NULL
         ),
         lineup_map AS (
           SELECT DISTINCT game_id, team_id, lineup_hash, player_id
           FROM %s
           WHERE coalesce(is_on_verdict, 0)::int = 1
             AND lineup_hash IS NOT NULL
         ),
         ot_events AS (
           SELECT DISTINCT
             d.game_id,
             d.id,
             d.quarter,
             d.player_id,
             r.team_id
           FROM %s d
           JOIN roster_map r
             ON r.game_id = d.game_id
            AND r.player_id = d.player_id
           WHERE d.quarter >= 5
             AND d.player_id IS NOT NULL
             AND d.type IN (
               'shot', 'freeThrow', 'rebound', 'assist', 'steal',
               'block', 'turnover', 'foul', 'foul-drawn', 'deflection'
             )
         ),
         attached AS (
           SELECT
             e.game_id,
             e.id,
             e.quarter,
             e.team_id,
             e.player_id,
             d.lineup_hash,
             (lm.player_id IS NOT NULL) AS player_on
           FROM ot_events e
           LEFT JOIN %s d
             ON d.game_id = e.game_id
            AND d.id = e.id
            AND d.team_id = e.team_id
           LEFT JOIN lineup_map lm
             ON lm.game_id = d.game_id
            AND lm.team_id = d.team_id
            AND lm.lineup_hash = d.lineup_hash
            AND lm.player_id = e.player_id
         ),
         per_event AS (
           SELECT
             game_id,
             id,
             quarter,
             team_id,
             player_id,
             bool_or(player_on) AS player_on,
             max(lineup_hash) AS lineup_hash
           FROM attached
           GROUP BY game_id, id, quarter, team_id, player_id
         ),
         quality AS (
           SELECT *
           FROM per_event
           WHERE NOT coalesce(player_on, FALSE)
         ),
         totals AS (
           SELECT
             (SELECT count(*) FROM per_event)::bigint AS overall_total_events,
             count(*)::bigint AS overall_unmatched_events
           FROM quality
         )
         SELECT
           q.game_id,
           q.id AS event_id,
           q.quarter,
           q.team_id,
           q.player_id,
           q.lineup_hash,
           1::bigint AS unmatched_events,
           t.overall_total_events,
           t.overall_unmatched_events,
           round(
             100.0 * t.overall_unmatched_events /
               nullif(t.overall_total_events, 0),
             4
           ) AS overall_unmatched_pct
         FROM quality q
         CROSS JOIN totals t
         ORDER BY q.game_id, q.quarter, q.id, q.team_id",
        fr, ll, df_long, df_long
      )
    ),
    list(
      id = "AA_material_clock_order_anomalies",
      title = "Material game-clock or period-order anomalies reach the app table",
      severity = "error",
      purpose = "Flags quarter regressions, within-quarter backward clock jumps above 24 seconds, and clock values outside their legal period range. Small provider-order jitter is reported separately.",
      required_tables = c("df_pts_poss_lineups_longer_mv"),
      problem_count_col = "invalid_games",
      sql = sprintf(
        "WITH action_grain AS (
           SELECT
             game_id,
             id,
             max(quarter)::int AS quarter,
             max(end_game_seconds_remaining)::numeric AS game_clock
           FROM %s
           GROUP BY game_id, id
         ),
         ordered AS (
           SELECT
             a.*,
             lag(id) OVER (PARTITION BY game_id ORDER BY id) AS prev_id,
             lag(quarter) OVER (PARTITION BY game_id ORDER BY id) AS prev_quarter,
             lag(game_clock) OVER (PARTITION BY game_id ORDER BY id) AS prev_clock
           FROM action_grain a
         ),
         game_quality AS (
           SELECT
             game_id,
             count(*) FILTER (
               WHERE prev_quarter IS NOT NULL AND quarter < prev_quarter
             )::bigint AS quarter_regression_rows,
             count(*) FILTER (
               WHERE quarter = prev_quarter AND game_clock - prev_clock > 24
             )::bigint AS reversal_gt24_rows,
             max(game_clock - prev_clock) FILTER (
               WHERE quarter = prev_quarter
             ) AS max_reversal_seconds,
             count(*) FILTER (
               WHERE quarter IS NULL
                  OR quarter < 1
                  OR game_clock IS NULL
                  OR (
                    quarter BETWEEN 1 AND 4
                    AND game_clock NOT BETWEEN
                      (4 - quarter) * 600 AND (5 - quarter) * 600
                  )
                  OR (quarter >= 5 AND game_clock NOT BETWEEN 0 AND 300)
             )::bigint AS out_of_range_rows
           FROM ordered
           GROUP BY game_id
         ),
         marked AS (
           SELECT
             *,
             (
               quarter_regression_rows > 0
               OR reversal_gt24_rows > 0
               OR out_of_range_rows > 0
             ) AS invalid
           FROM game_quality
         ),
         totals AS (
           SELECT
             count(*)::bigint AS overall_total_games,
             count(*) FILTER (WHERE invalid)::bigint AS overall_invalid_games
           FROM marked
         )
         SELECT
           m.game_id,
           m.quarter_regression_rows,
           m.reversal_gt24_rows,
           m.max_reversal_seconds,
           m.out_of_range_rows,
           1::bigint AS invalid_games,
           t.overall_total_games,
           t.overall_invalid_games,
           round(
             100.0 * t.overall_invalid_games /
               nullif(t.overall_total_games, 0),
             4
           ) AS overall_invalid_pct
         FROM marked m
         CROSS JOIN totals t
         WHERE m.invalid
         ORDER BY
           m.quarter_regression_rows DESC,
           m.max_reversal_seconds DESC NULLS LAST,
           m.game_id",
        df_long
      )
    ),
    list(
      id = "AB_clock_order_jitter",
      title = "Non-trivial within-quarter clock-order jitter",
      severity = "warning",
      purpose = "Reports backward game-clock jumps above five and up to 24 seconds. One-to-five-second reversals are treated as common provider ordering/rounding noise.",
      required_tables = c("df_pts_poss_lineups_longer_mv"),
      problem_count_col = "reversal_rows",
      sql = sprintf(
        "WITH action_grain AS (
           SELECT
             game_id,
             id,
             max(quarter)::int AS quarter,
             max(end_game_seconds_remaining)::numeric AS game_clock
           FROM %s
           GROUP BY game_id, id
         ),
         ordered AS (
           SELECT
             a.*,
             lag(id) OVER (PARTITION BY game_id ORDER BY id) AS prev_id,
             lag(quarter) OVER (PARTITION BY game_id ORDER BY id) AS prev_quarter,
             lag(game_clock) OVER (PARTITION BY game_id ORDER BY id) AS prev_clock
           FROM action_grain a
         ),
         quality AS (
           SELECT
             game_id,
             count(*) FILTER (
               WHERE quarter = prev_quarter
                 AND game_clock - prev_clock > 5
                 AND game_clock - prev_clock <= 24
             )::bigint AS reversal_rows,
             max(game_clock - prev_clock) FILTER (
               WHERE quarter = prev_quarter
                 AND game_clock - prev_clock > 5
                 AND game_clock - prev_clock <= 24
             ) AS max_reversal_seconds,
             string_agg(
               format('%%s->%%s', prev_id, id),
               ',' ORDER BY id
             ) FILTER (
               WHERE quarter = prev_quarter
                 AND game_clock - prev_clock > 5
                 AND game_clock - prev_clock <= 24
             ) AS action_transitions
           FROM ordered
           GROUP BY game_id
         )
         SELECT
           game_id,
           reversal_rows,
           max_reversal_seconds,
           action_transitions
         FROM quality
         WHERE reversal_rows > 0
         ORDER BY max_reversal_seconds DESC, game_id",
        df_long
      )
    ),
    list(
      id = "AC_missing_regulation_period_coverage",
      title = "Regulation periods are missing from the app event table",
      severity = "error",
      purpose = "Requires each persisted game/team to contain event rows labeled for regulation quarters 1-4. Reviewed early terminations 184 and 380 remain documented in P1.",
      required_tables = c("df_pts_poss_lineups_longer_mv"),
      problem_count_col = "invalid_periods",
      sql = sprintf(
        "WITH game_teams AS (
           SELECT DISTINCT game_id, team_id
           FROM %s
           WHERE game_id NOT IN (184, 380)
             AND team_id IS NOT NULL
         ),
         required_periods AS (
           SELECT
             gt.game_id,
             gt.team_id,
             q.quarter
           FROM game_teams gt
           CROSS JOIN generate_series(1, 4) AS q(quarter)
         ),
         period_rows AS (
           SELECT
             game_id,
             team_id,
             quarter,
             count(DISTINCT id)::bigint AS action_rows
           FROM %s
           WHERE quarter BETWEEN 1 AND 4
           GROUP BY game_id, team_id, quarter
         ),
         missing AS (
           SELECT
             rp.game_id,
             rp.team_id,
             rp.quarter,
             coalesce(pr.action_rows, 0)::bigint AS action_rows
           FROM required_periods rp
           LEFT JOIN period_rows pr
             ON pr.game_id = rp.game_id
            AND pr.team_id = rp.team_id
            AND pr.quarter = rp.quarter
           WHERE coalesce(pr.action_rows, 0) = 0
         ),
         totals AS (
           SELECT
             (SELECT count(*) FROM required_periods)::bigint AS overall_required_periods,
             count(*)::bigint AS overall_invalid_periods
           FROM missing
         )
         SELECT
           m.game_id,
           m.team_id,
           m.quarter AS missing_quarter,
           m.action_rows,
           1::bigint AS invalid_periods,
           t.overall_required_periods,
           t.overall_invalid_periods,
           round(
             100.0 * t.overall_invalid_periods /
               nullif(t.overall_required_periods, 0),
             4
           ) AS overall_invalid_pct
         FROM missing m
         CROSS JOIN totals t
         ORDER BY m.game_id, m.team_id, m.quarter",
        df_long, df_long
      )
    ),
    list(
      id = "AD_clutch_clock_exposure",
      title = "Clock-order defects can change clutch-filter membership",
      severity = "error",
      purpose = "Flags Q4 action order that moves back outside the selected five-minute window or regresses from Q4/overtime to an earlier period. Missing Q4 coverage is handled separately.",
      required_tables = c("df_pts_poss_lineups_longer_mv"),
      problem_count_col = "exposed_rows",
      sql = sprintf(
        "WITH action_grain AS (
           SELECT
             game_id,
             id,
             max(quarter)::int AS quarter,
             max(end_game_seconds_remaining)::numeric AS game_clock
           FROM %s
           GROUP BY game_id, id
         ),
         ordered AS (
           SELECT
             a.*,
             lag(id) OVER (PARTITION BY game_id ORDER BY id) AS prev_id,
             lag(quarter) OVER (PARTITION BY game_id ORDER BY id) AS prev_quarter,
             lag(game_clock) OVER (PARTITION BY game_id ORDER BY id) AS prev_clock
           FROM action_grain a
         ),
         exposed AS (
           SELECT
             *,
             CASE
               WHEN quarter = 4 AND prev_quarter = 4
                 AND prev_clock <= 300 AND game_clock > 300
               THEN 'q4_reentered_outside_five_minutes'
               WHEN prev_quarter >= 4 AND quarter < prev_quarter
               THEN 'period_regressed_out_of_clutch_scope'
             END AS exposure_type
           FROM ordered
         )
         SELECT
           game_id,
           prev_id,
           id AS action_id,
           prev_quarter,
           quarter,
           prev_clock,
           game_clock,
           exposure_type,
           1::bigint AS exposed_rows
         FROM exposed
         WHERE exposure_type IS NOT NULL
         ORDER BY game_id, action_id",
        df_long
      )
    ),
    list(
      id = "AE_duplicate_persisted_action_stint_keys",
      title = "Persisted actions map to multiple app rows for one team",
      severity = "error",
      purpose = "Requires one df_pts_poss_lineups_longer row per (game, team, action ID). Extra rows would duplicate points, shots, and possession endings in downstream aggregates.",
      required_tables = c("df_pts_poss_lineups_longer_mv"),
      problem_count_col = "extra_rows",
      sql = sprintf(
        "WITH duplicated AS (
           SELECT
             game_id,
             team_id,
             id,
             count(*)::bigint AS row_copies,
             count(DISTINCT segment_id)::int AS distinct_segments,
             count(DISTINCT lineup_hash)::int AS distinct_lineups
           FROM %s
           GROUP BY game_id, team_id, id
           HAVING count(*) > 1
         )
         SELECT
           game_id,
           team_id,
           id AS action_id,
           row_copies,
           distinct_segments,
           distinct_lineups,
           row_copies - 1 AS extra_rows
         FROM duplicated
         ORDER BY row_copies DESC, game_id, team_id, id",
        df_long
      )
    ),
    list(
      id = "AF_invalid_persisted_segment_ids",
      title = "Persisted app rows have missing or non-positive segment IDs",
      severity = "error",
      purpose = "Protects segment-level minute aggregation from collapsed or missing stint identifiers.",
      required_tables = c("df_pts_poss_lineups_longer_mv"),
      problem_count_col = "invalid_rows",
      sql = sprintf(
        "WITH quality AS (
           SELECT
             game_id,
             team_id,
             count(*)::bigint AS total_rows,
             count(*) FILTER (
               WHERE segment_id IS NULL OR segment_id <= 0
             )::bigint AS invalid_rows,
             min(segment_id)::int AS min_segment_id,
             max(segment_id)::int AS max_segment_id
           FROM %s
           GROUP BY game_id, team_id
         )
         SELECT
           game_id,
           team_id,
           total_rows,
           invalid_rows,
           min_segment_id,
           max_segment_id,
           round(100.0 * invalid_rows / nullif(total_rows, 0), 4) AS invalid_pct
         FROM quality
         WHERE invalid_rows > 0
         ORDER BY invalid_rows DESC, game_id, team_id",
        df_long
      )
    ),
    list(
      id = "AH_canonical_segment_timing",
      title = "Canonical segment timing is complete and conserves game coverage",
      severity = "error",
      purpose = "Requires canonical event/segment clocks on every persisted segment row, one nonnegative duration per segment, and team segment totals within five seconds of the retained game timeline.",
      required_tables = c("df_pts_poss_lineups_longer_mv"),
      problem_count_col = "affected_rows",
      sql = sprintf(
        "WITH row_quality AS (
           SELECT
             game_id,
             team_id,
             count(*) FILTER (
               WHERE segment_id IS NOT NULL
                 AND (
                   event_elapsed_seconds IS NULL
                   OR clock_regression_seconds IS NULL
                   OR segment_start_elapsed_seconds IS NULL
                   OR segment_end_elapsed_seconds IS NULL
                   OR segment_seconds IS NULL
                 )
             )::bigint AS missing_timing_rows,
             max(event_elapsed_seconds)::numeric AS game_end_elapsed_seconds
           FROM %s
           GROUP BY game_id, team_id
         ),
         segment_quality AS (
           SELECT
             game_id,
             team_id,
             lineup_hash,
             segment_id,
             count(DISTINCT segment_seconds)::int AS distinct_durations,
             min(segment_seconds)::numeric AS segment_seconds,
             min(segment_start_elapsed_seconds)::numeric AS segment_start,
             min(segment_end_elapsed_seconds)::numeric AS segment_end
           FROM %s
           WHERE lineup_hash IS NOT NULL
             AND segment_id IS NOT NULL
           GROUP BY game_id, team_id, lineup_hash, segment_id
         ),
         team_segments AS (
           SELECT
             game_id,
             team_id,
             count(*) FILTER (
               WHERE distinct_durations <> 1
                  OR segment_seconds IS NULL
                  OR segment_seconds < 0
                  OR segment_end < segment_start
             )::bigint AS invalid_segments,
             coalesce(sum(segment_seconds), 0)::numeric AS total_segment_seconds
           FROM segment_quality
           GROUP BY game_id, team_id
         ),
         quality AS (
           SELECT
             r.game_id,
             r.team_id,
             r.missing_timing_rows,
             coalesce(s.invalid_segments, 0)::bigint AS invalid_segments,
             coalesce(s.total_segment_seconds, 0)::numeric AS total_segment_seconds,
             r.game_end_elapsed_seconds,
             abs(
               coalesce(s.total_segment_seconds, 0) -
               coalesce(r.game_end_elapsed_seconds, 0)
             )::numeric AS conservation_difference_seconds
           FROM row_quality r
           LEFT JOIN team_segments s USING (game_id, team_id)
         )
         SELECT
           game_id,
           team_id,
           missing_timing_rows,
           invalid_segments,
           total_segment_seconds,
           game_end_elapsed_seconds,
           conservation_difference_seconds,
           (
             missing_timing_rows + invalid_segments +
             CASE WHEN conservation_difference_seconds > 5 THEN 1 ELSE 0 END
           )::bigint AS affected_rows
         FROM quality
         WHERE missing_timing_rows > 0
            OR invalid_segments > 0
            OR conservation_difference_seconds > 5
         ORDER BY affected_rows DESC, game_id, team_id",
        df_long, df_long
      )
    ),
    list(
      id = "AG_cold_storage_snapshot_consistency",
      title = "Cold-storage Parquets form a consistent latest-game snapshot",
      severity = "warning",
      purpose = "Checks action/possession key parity, PWS-to-stint mapping integrity, storage-key uniqueness, and positive segment IDs. Failures affect offline audits and restoration rather than the live app.",
      required_tables = character(),
      problem_count_col = "affected_rows",
      runner = function(con, schema, root) {
        cold_storage_snapshot_details(root)
      }
    )
  )
}

run_data_quality_report <- function(
  output_dir = Sys.getenv("DQ_OUTPUT_DIR", file.path("etl", "logs", "data_quality")),
  max_detail_rows = as.integer(Sys.getenv("DQ_MAX_DETAIL_ROWS", "50")),
  fail_on_error = truthy("DQ_FAIL_ON_ERROR")
) {
  root <- repo_root()
  env_file <- file.path(root, "etl", ".Renviron")
  if (file.exists(env_file)) {
    readRenviron(env_file)
  }

  app_env <- Sys.getenv("APP_ENV", "test")
  schema <- if (identical(app_env, "prod")) "basketball" else "basketball_test"
  output_dir <- if (grepl("^[A-Za-z]:|^/", output_dir)) output_dir else file.path(root, output_dir)
  output_dir <- normalizePath(output_dir, winslash = "/", mustWork = FALSE)
  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  detail_dir <- file.path(output_dir, "details")
  if (dir.exists(detail_dir)) {
    stale_detail_files <- list.files(detail_dir, pattern = "\\.csv$", full.names = TRUE)
    if (length(stale_detail_files)) unlink(stale_detail_files)
  }

  max_detail_rows <- if (is.finite(max_detail_rows) && max_detail_rows > 0) max_detail_rows else 50L

  con <- DBI::dbConnect(
    RPostgres::Postgres(),
    host = Sys.getenv("PG_HOST"),
    port = as.integer(Sys.getenv("PG_PORT", "6543")),
    dbname = Sys.getenv("PG_DB"),
    user = Sys.getenv("PG_USER"),
    password = Sys.getenv("PG_PASS"),
    sslmode = Sys.getenv("PG_SSLMODE", "require")
  )
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  started_at <- Sys.time()
  stamp <- format(started_at, "%Y%m%d_%H%M%S")
  checks <- build_checks(con, schema)

  results <- lapply(checks, function(check) {
    if (is.function(check$runner)) {
      return(run_r_check(con, schema, root, check, output_dir))
    }
    if (!is.na(check$sql[[1]])) {
      return(run_sql_check(con, schema, check, output_dir))
    }

    details <- data.frame(note = check$not_automated_note, stringsAsFactors = FALSE)
    detail_file <- write_detail_csv(details, output_dir, check$id)
    make_check_result(check, "not_automated", details, detail_file)
  })

  summary_df <- do.call(rbind, lapply(results, `[[`, "summary"))
  summary_path <- file.path(output_dir, paste0("summary_", stamp, ".csv"))
  utils::write.csv(summary_df, summary_path, row.names = FALSE, na = "")

  detail_sections <- unlist(lapply(seq_along(checks), function(i) {
    check <- checks[[i]]
    result <- results[[i]]
    c(
      sprintf("## %s: %s", check$id, check$title),
      "",
      sprintf("Severity: `%s`  ", check$severity),
      sprintf("Status: `%s`  ", result$summary$status[[1]]),
      sprintf("Detail rows: `%s`  ", ifelse(is.na(result$summary$row_count[[1]]), "n/a", result$summary$row_count[[1]])),
      sprintf("Issue count: `%s`", ifelse(is.na(result$summary$issue_count[[1]]), "n/a", result$summary$issue_count[[1]])),
      "",
      check$purpose,
      "",
      if (nzchar(result$summary$detail_file[[1]])) {
        sprintf("Detail CSV: `%s`", result$summary$detail_file[[1]])
      } else {
        "Detail CSV: none"
      },
      "",
      markdown_table(result$details, max_detail_rows),
      ""
    )
  }))

  has_error_rows <- any(summary_df$severity == "error" & summary_df$status %in% c("fail", "query_error"))
  has_warning_rows <- any(summary_df$severity == "warning" & summary_df$status == "warning")
  overall_status <- if (has_error_rows) "FAIL" else if (has_warning_rows) "WARNING" else "PASS"

  report_lines <- c(
    "# Data Quality Report",
    "",
    sprintf("Run time: `%s`", format(started_at, "%Y-%m-%d %H:%M:%S %Z")),
    sprintf("APP_ENV: `%s`", app_env),
    sprintf("Schema: `%s`", schema),
    sprintf("Overall status: `%s`", overall_status),
    sprintf("Fail on error: `%s`", fail_on_error),
    "",
    "## Summary",
    "",
    markdown_table(summary_df[, c("check_id", "severity", "status", "row_count", "issue_count", "title", "detail_file")], max_rows = nrow(summary_df)),
    "",
    "## Checks",
    "",
    detail_sections
  )

  report_path <- file.path(output_dir, paste0("data_quality_report_", stamp, ".md"))
  writeLines(report_lines, report_path, useBytes = TRUE)
  latest_path <- file.path(output_dir, "latest.md")
  writeLines(report_lines, latest_path, useBytes = TRUE)

  message(sprintf("Data quality report written: %s", report_path))
  message(sprintf("Data quality latest report: %s", latest_path))
  message(sprintf("Data quality summary CSV: %s", summary_path))
  message(sprintf("Overall status: %s", overall_status))

  result <- list(
    status = overall_status,
    has_error_rows = has_error_rows,
    has_warning_rows = has_warning_rows,
    report_path = report_path,
    latest_path = latest_path,
    summary_path = summary_path,
    summary = summary_df
  )

  if (fail_on_error && has_error_rows) {
    quit(status = 2, save = "no")
  }

  invisible(result)
}

if (!truthy("DQ_NO_AUTORUN") &&
    identical(environment(), globalenv()) &&
    !interactive()) {
  run_data_quality_report()
}
