# =============================================================================
# euroleague/scripts/run_euro_data_quality_report.R
#
# Read-only data quality report for the isolated `euroleague` schema.
#
# Adapted from etl/run_data_quality_report.R. The engine below is a verbatim
# copy of that file's generic machinery, kept inside marked blocks so it can
# later be extracted into a shared file as a mechanical move. Only lines marked
# `# EUROLEAGUE:` differ. Do not edit the copied block for any other reason --
# fix the Israeli original and re-copy.
#
# Check IDs A..AJ mirror the Israeli catalogue so the two reports can be read
# side by side. Checks with no EuroLeague analogue stay in the catalogue as
# `not_automated` rather than being dropped. N1..N9 are EuroLeague-native.
#
# Usage:
#   Rscript euroleague/scripts/run_euro_data_quality_report.R
#
# Or:
#   Sys.setenv(DQ_NO_AUTORUN = "true")
#   source("euroleague/scripts/run_euro_data_quality_report.R")
#   run_data_quality_report()
#
# Environment:
#   DQ_OUTPUT_DIR                default: euroleague/logs/data_quality
#   DQ_MAX_DETAIL_ROWS           default: 50 rows per markdown section
#   DQ_FAIL_ON_ERROR             true/false, default false
#   EURO_DQ_STORAGE_BUDGET_MB    default: 500, used by check N9
# =============================================================================

# --- BEGIN verbatim copy of etl/run_data_quality_report.R engine (part 1) ----
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
# --- END verbatim copy (part 1) ----------------------------------------------

euro_storage_footprint_details <- function(con, schema, root) {
  budget_mb <- suppressWarnings(as.numeric(Sys.getenv("EURO_DQ_STORAGE_BUDGET_MB", "500")))
  if (!is.finite(budget_mb) || budget_mb <= 0) budget_mb <- 500

  df <- DBI::dbGetQuery(con, sprintf(
    "SELECT c.relname AS relation,
            CASE c.relkind WHEN 'r' THEN 'table' WHEN 'm' THEN 'materialized_view' ELSE c.relkind::text END AS kind,
            pg_total_relation_size(c.oid) AS total_bytes,
            pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size
       FROM pg_catalog.pg_class c
       JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = %s
        AND c.relkind IN ('r', 'm')
      ORDER BY pg_total_relation_size(c.oid) DESC",
    sql_string(con, schema)
  ))

  if (!nrow(df)) {
    return(data.frame(
      relation = character(),
      kind = character(),
      total_size = character(),
      share_pct = numeric(),
      over_budget = numeric(),
      stringsAsFactors = FALSE
    ))
  }

  total_bytes <- sum(as.numeric(df$total_bytes))
  budget_bytes <- budget_mb * 1024 * 1024

  out <- data.frame(
    relation = c("(schema total)", df$relation),
    kind = c(sprintf("budget %.0f MB", budget_mb), df$kind),
    total_size = c(
      DBI::dbGetQuery(con, sprintf("SELECT pg_size_pretty(%s::bigint) AS s", format(total_bytes, scientific = FALSE)))$s,
      df$total_size
    ),
    share_pct = round(100 * c(total_bytes, as.numeric(df$total_bytes)) / total_bytes, 2),
    over_budget = c(as.numeric(total_bytes > budget_bytes), rep(0, nrow(df))),
    stringsAsFactors = FALSE
  )
  out
}

build_checks <- function(con, schema) {
  q <- function(table) quote_table(con, schema, table)

  fr    <- q("full_rosters")
  pls   <- q("players")
  sch   <- q("schedule")
  act   <- q("actions")
  araw  <- q("actions_raw")
  atc   <- q("action_team_context_actions")
  mseg  <- q("matchup_segments_actions")
  pffg  <- q("player_four_factors_by_game")
  tffg  <- q("team_four_factors_by_game")
  ltbg  <- q("lineup_totals_by_game")
  psabg <- q("player_stats_actions_by_game")
  tbox  <- q("team_boxscores")
  gqa   <- q("game_qa")
  lrun  <- q("load_runs")
  recon <- q("reconciliation_metrics")
  qinc  <- q("qa_incidents")
  onoff <- q("player_onoff_default_mv")
  adv   <- q("player_advanced_stats_mv")
  trad  <- q("player_traditional_stats_mv")

  # Publication state is keyed (load_run_id, game_id); a game's current state is
  # its highest load run.  Every publication check reads through this.
  latest_qa <- sprintf(
    "SELECT DISTINCT ON (game_id) * FROM %s ORDER BY game_id, load_run_id DESC",
    gqa
  )

  # The provider keeps PERIOD = 5 for every overtime (see migration 015), so raw
  # actions.period undercounts multi-overtime games.  action_team_context_actions
  # already carries the effective period, which is what game duration must use.
  endpoint_vocabulary <- paste(
    "'made_field_goal'", "'miss_defensive_rebound'", "'turnover'",
    "'ordinary_ft_trip_final_make'", "'final_ft_miss_defensive_rebound'",
    "'blocked_shot_defensive_rebound'", "'and_one_final_ft'", "'period_end_miss'",
    "'period_end_offensive_rebound'", "'compound_penalty_offense_resolved'",
    "'period_end_blocked_miss'", "'made_basket_dead_ball_ft'", "'final_ft_miss_end'",
    sep = ", "
  )

  list(
    list(
      id = "A_same_player_id_multiple_roster_names",
      title = "Same season-team-player ID has multiple roster names",
      severity = "warning",
      purpose = paste(
        "One internal player ID carrying two roster names. Severity is deliberately",
        "lower than the Israeli counterpart: there, one ID with two names signals a",
        "provider re-mint. Here internal player_id is assigned per provider_player_id",
        "and stays stable, so this is provider name inconsistency (reordered surname and",
        "given name, or a spelling change) and does not split aggregates. Verified",
        "2026-08-19: lineup arrays use the roster spelling for the game they belong to",
        "and resolve to the correct player, so the variants do not corrupt lineups.",
        "What they do break is any join written on a single canonical name -- check Z",
        "had exactly that bug and reported 258 false positives before it was fixed to",
        "match against the game's own roster spellings. Check B carries the error",
        "severity, because that is the direction that splits a player's season."
      ),
      required_tables = c("full_rosters", "schedule"),
      sql = sprintf(
        "SELECT s.competition,
                s.season,
                f.team_id,
                f.player_id,
                COUNT(DISTINCT f.source_player_name) AS name_count,
                STRING_AGG(DISTINCT f.source_player_name, ' | ' ORDER BY f.source_player_name) AS roster_names,
                COUNT(*) AS roster_rows
           FROM %s f
           JOIN %s s ON s.game_id = f.game_id
          GROUP BY 1, 2, 3, 4
         HAVING COUNT(DISTINCT f.source_player_name) > 1
          ORDER BY 5 DESC, 1, 2, 3, 4",
        fr, sch
      )
    ),

    list(
      id = "B_same_roster_name_multiple_player_ids",
      title = "Same roster name has multiple player IDs in one season and team",
      severity = "error",
      purpose = paste(
        "One person on one team in one season must resolve to one internal player ID.",
        "Two IDs behind one name split that person's season aggregates across two rows",
        "in every app tab. This is the provider re-mint the Israeli project built its",
        "identity dictionary to catch, and the euroleague schema has no such dictionary,",
        "so nothing downstream repairs it. Raised above the Israeli severity for that",
        "reason."
      ),
      required_tables = c("full_rosters", "schedule"),
      sql = sprintf(
        "SELECT s.competition,
                s.season,
                f.team_id,
                UPPER(BTRIM(f.source_player_name)) AS roster_name,
                COUNT(DISTINCT f.player_id) AS player_id_count,
                STRING_AGG(DISTINCT f.player_id::text, ' | ') AS player_ids,
                COUNT(*) AS roster_rows
           FROM %s f
           JOIN %s s ON s.game_id = f.game_id
          WHERE f.source_player_name IS NOT NULL
            AND BTRIM(f.source_player_name) <> ''
          GROUP BY 1, 2, 3, 4
         HAVING COUNT(DISTINCT f.player_id) > 1
          ORDER BY 5 DESC, 1, 2, 3, 4",
        fr, sch
      )
    ),

    list(
      id = "C_active_correction_residue_game_scoped_tables",
      title = "Active correction residue remains in game-scoped tables",
      severity = "todo",
      purpose = paste(
        "No EuroLeague analogue. The Israeli check reads player_id_aliases and",
        "player_id_game_overrides; the euroleague schema has no correction layer, so a",
        "decided correction has nowhere to be recorded. Tracked as a known gap."
      ),
      required_tables = character(0),
      sql = NA_character_,
      not_automated_note = "No correction layer exists in the euroleague schema (no alias or game-override relation)."
    ),

    list(
      id = "D_active_game_overrides_without_canonical_roster_row",
      title = "Active game overrides have no canonical roster row",
      severity = "todo",
      purpose = "No EuroLeague analogue: there is no player_id_game_overrides relation.",
      required_tables = character(0),
      sql = NA_character_,
      not_automated_note = "No game-override relation exists in the euroleague schema."
    ),

    list(
      id = "E_aggregate_player_ids_not_identity_resolved",
      title = "App aggregate player IDs do not resolve to a players dimension row",
      severity = "error",
      purpose = paste(
        "Every player ID published to the app must resolve to a row in the players",
        "dimension, otherwise the tab renders a statistic with no name behind it."
      ),
      required_tables = c("player_onoff_default_mv", "player_advanced_stats_mv", "player_traditional_stats_mv", "players"),
      sql = sprintf(
        "SELECT 'player_onoff_default_mv' AS source_relation,
                m.competition, m.game_year::integer AS game_year, m.team_id, m.player_id
           FROM %s m
           LEFT JOIN %s p ON p.player_id = m.player_id
          WHERE p.player_id IS NULL
          UNION ALL
         SELECT 'player_advanced_stats_mv',
                m.competition, m.game_year::integer, m.team_id, m.player_id
           FROM %s m
           LEFT JOIN %s p ON p.player_id = m.player_id
          WHERE p.player_id IS NULL
          UNION ALL
         SELECT 'player_traditional_stats_mv',
                m.competition, m.game_year::integer, m.team_id, m.player_id
           FROM %s m
           LEFT JOIN %s p ON p.player_id = m.player_id
          WHERE p.player_id IS NULL
          ORDER BY 1, 2, 3, 4, 5",
        onoff, pls, adv, pls, trad, pls
      )
    ),

    list(
      id = "F_lineup_derivative_active_alias_residue",
      title = "Lineup derivative tables still contain active season alias IDs",
      severity = "todo",
      purpose = "No EuroLeague analogue: no alias relation, and lineup identity is rebuilt per load run.",
      required_tables = character(0),
      sql = NA_character_,
      not_automated_note = "No alias relation exists in the euroleague schema."
    ),

    list(
      id = "G_canonical_action_duplicate_keys",
      title = "Canonical actions have duplicate event keys",
      severity = "error",
      purpose = paste(
        "(game_id, source_event_order) is the canonical event key. A duplicate would",
        "double-count every metric derived from that event."
      ),
      required_tables = c("actions"),
      sql = sprintf(
        "SELECT game_id, source_event_order, COUNT(*) AS row_count
           FROM %s
          GROUP BY 1, 2
         HAVING COUNT(*) > 1
          ORDER BY 3 DESC, 1, 2",
        act
      )
    ),

    list(
      id = "H_loaded_games_missing_publication_marker",
      title = "Loaded games have no game_qa publication row",
      severity = "error",
      purpose = paste(
        "game_qa is the only publication marker this schema has. A game with canonical",
        "actions but no QA row cannot be asked 'is it currently published and clean',",
        "which is the gap the Israeli project closed with etl_processed_games."
      ),
      required_tables = c("actions", "game_qa", "schedule"),
      sql = sprintf(
        "SELECT b.game_id, s.competition, s.season, s.gamecode, s.round_number, s.phase,
                (SELECT COUNT(*) FROM %s a WHERE a.game_id = b.game_id) AS action_rows
           FROM (SELECT DISTINCT game_id FROM %s) b
           LEFT JOIN %s s ON s.game_id = b.game_id
          WHERE NOT EXISTS (SELECT 1 FROM %s g WHERE g.game_id = b.game_id)
          ORDER BY 1",
        act, act, sch, gqa
      )
    ),

    list(
      id = "I_published_games_missing_base_rows",
      title = "Games published clear are missing canonical base rows",
      severity = "error",
      purpose = "A game whose latest load run reports publication_status = 'clear' must have canonical actions and a roster.",
      required_tables = c("game_qa", "actions", "full_rosters"),
      sql = sprintf(
        "WITH latest AS (%s)
         SELECT l.game_id, l.load_run_id, l.publication_status,
                (SELECT COUNT(*) FROM %s a WHERE a.game_id = l.game_id) AS action_rows,
                (SELECT COUNT(*) FROM %s f WHERE f.game_id = l.game_id) AS roster_rows
           FROM latest l
          WHERE l.publication_status = 'clear'
            AND (NOT EXISTS (SELECT 1 FROM %s a WHERE a.game_id = l.game_id)
              OR NOT EXISTS (SELECT 1 FROM %s f WHERE f.game_id = l.game_id))
          ORDER BY 1",
        latest_qa, act, fr, act, fr
      )
    ),

    list(
      id = "J_loaded_games_missing_downstream_rows",
      title = "Base-loaded games are missing downstream game-grain rows",
      severity = "error",
      purpose = paste(
        "Every game with canonical actions must also reach the derived facts the app",
        "reads: the event/team-perspective fact, canonical segments, player and team",
        "four factors, and lineup totals. A base load that stopped short leaves the app",
        "silently missing a game rather than reporting an error."
      ),
      required_tables = c(
        "actions", "action_team_context_actions", "matchup_segments_actions",
        "player_four_factors_by_game", "team_four_factors_by_game", "lineup_totals_by_game"
      ),
      sql = sprintf(
        "WITH latest AS (%s),
              base AS (SELECT DISTINCT game_id FROM %s)
         SELECT b.game_id, s.competition, s.season, s.gamecode, s.round_number, s.phase,
                l.load_run_id, l.publication_status,
                EXISTS (SELECT 1 FROM %s x WHERE x.game_id = b.game_id) AS has_action_context,
                EXISTS (SELECT 1 FROM %s x WHERE x.game_id = b.game_id) AS has_segments,
                EXISTS (SELECT 1 FROM %s x WHERE x.game_id = b.game_id) AS has_player_four_factors,
                EXISTS (SELECT 1 FROM %s x WHERE x.game_id = b.game_id) AS has_team_four_factors,
                EXISTS (SELECT 1 FROM %s x WHERE x.game_id = b.game_id) AS has_lineup_totals
           FROM base b
           LEFT JOIN %s s ON s.game_id = b.game_id
           LEFT JOIN latest l ON l.game_id = b.game_id
          WHERE NOT (EXISTS (SELECT 1 FROM %s x WHERE x.game_id = b.game_id)
                 AND EXISTS (SELECT 1 FROM %s x WHERE x.game_id = b.game_id)
                 AND EXISTS (SELECT 1 FROM %s x WHERE x.game_id = b.game_id)
                 AND EXISTS (SELECT 1 FROM %s x WHERE x.game_id = b.game_id)
                 AND EXISTS (SELECT 1 FROM %s x WHERE x.game_id = b.game_id))
          ORDER BY 1",
        latest_qa, act,
        atc, mseg, pffg, tffg, ltbg,
        sch,
        atc, mseg, pffg, tffg, ltbg
      )
    ),

    list(
      id = "K_app_aggregate_duplicate_keys",
      title = "App aggregate materialized views have duplicate player keys",
      severity = "error",
      purpose = "Each app aggregate must hold one row per competition, season, team, and player.",
      required_tables = c("player_onoff_default_mv", "player_advanced_stats_mv", "player_traditional_stats_mv"),
      sql = sprintf(
        "SELECT 'player_onoff_default_mv' AS source_relation, competition, game_year::integer AS game_year,
                team_id, player_id, COUNT(*) AS row_count
           FROM %s GROUP BY 1, 2, 3, 4, 5 HAVING COUNT(*) > 1
          UNION ALL
         SELECT 'player_advanced_stats_mv', competition, game_year::integer, team_id, player_id, COUNT(*)
           FROM %s GROUP BY 1, 2, 3, 4, 5 HAVING COUNT(*) > 1
          UNION ALL
         SELECT 'player_traditional_stats_mv', competition, game_year::integer, team_id, player_id, COUNT(*)
           FROM %s GROUP BY 1, 2, 3, 4, 5 HAVING COUNT(*) > 1
          ORDER BY 6 DESC, 1, 2, 3, 4, 5",
        onoff, adv, trad
      )
    ),

    list(
      id = "L_raw_pbp_duplicate_play_numbers",
      title = "Raw PBP has duplicate provider play numbers within a game",
      severity = "warning",
      purpose = paste(
        "The provider play number should identify an event within a game. Duplicates are",
        "not automatically a defect - the parser inserts synthetic rows - but they are",
        "the first place to look when raw and canonical counts diverge."
      ),
      required_tables = c("actions_raw"),
      sql = sprintf(
        "SELECT game_id, provider_play_number, COUNT(*) AS row_count
           FROM %s
          WHERE provider_play_number IS NOT NULL
          GROUP BY 1, 2
         HAVING COUNT(*) > 1
          ORDER BY 3 DESC, 1, 2",
        araw
      )
    ),

    list(
      id = "M_identity_dictionary_mapping_ambiguities",
      title = "Player identity dictionary has ambiguous or conflicting mappings",
      severity = "todo",
      purpose = "No EuroLeague analogue: there is no player_identity_map.",
      required_tables = character(0),
      sql = NA_character_,
      not_automated_note = "No identity dictionary exists in the euroleague schema. Checks A and B are the current substitute."
    ),

    list(
      id = "N_identity_compatibility_missing_legacy_corrections",
      title = "Identity compatibility view is missing active legacy corrections",
      severity = "todo",
      purpose = "No EuroLeague analogue: there is no identity compatibility view.",
      required_tables = character(0),
      sql = NA_character_,
      not_automated_note = "No identity compatibility view exists in the euroleague schema."
    ),

    list(
      id = "O_identity_unresolved_source_contexts",
      title = "Roster source contexts are unresolved by the identity dictionary",
      severity = "todo",
      purpose = "No EuroLeague analogue: there is no resolved_player_identity_v.",
      required_tables = character(0),
      sql = NA_character_,
      not_automated_note = "No identity resolution view exists in the euroleague schema."
    ),

    list(
      id = "P0_source_placeholder_roster_identities",
      title = "Source rosters contain placeholder player identities",
      severity = "warning",
      purpose = "A roster row with no player ID or a placeholder name cannot be attributed and pollutes every player aggregate built from it.",
      required_tables = c("full_rosters"),
      sql = sprintf(
        "SELECT f.game_id, f.team_id, f.player_id, f.source_player_name, f.jersey_number, f.roster_source
           FROM %s f
          WHERE f.player_id IS NULL
             OR f.source_player_name IS NULL
             OR BTRIM(f.source_player_name) = ''
             OR UPPER(BTRIM(f.source_player_name)) IN ('UNKNOWN', 'N/A', 'NA', 'PLAYER', '-')
          ORDER BY 1, 2, 3",
        fr
      )
    ),

    list(
      id = "P_app_aggregates_without_roster_participation",
      title = "App player aggregates contain players with no roster row that season",
      severity = "warning",
      purpose = paste(
        "A player published for a team and season must appear on that team's roster in",
        "at least one game of that season. A row that does not is either a stale",
        "aggregate or a mis-attributed identity."
      ),
      required_tables = c("player_onoff_default_mv", "full_rosters", "schedule"),
      sql = sprintf(
        "SELECT m.competition, m.game_year, m.team_id, m.player_id, p.display_name
           FROM %s m
           LEFT JOIN %s p ON p.player_id = m.player_id
          WHERE NOT EXISTS (
                  SELECT 1
                    FROM %s f
                    JOIN %s s ON s.game_id = f.game_id
                   WHERE f.player_id = m.player_id
                     AND f.team_id = m.team_id
                     AND s.season = m.game_year)
          ORDER BY 1, 2, 3, 4",
        onoff, pls, fr, sch
      )
    ),

    list(
      id = "P1_reviewed_data_quality_exceptions",
      title = "Reviewed non-actionable data-quality exceptions",
      severity = "warning",
      purpose = "Incidents already triaged and closed. Listed so a closed incident stays visible rather than disappearing from the report.",
      required_tables = c("qa_incidents"),
      sql = sprintf(
        "SELECT category, severity, status, rule_code, COUNT(*) AS incident_count,
                MIN(created_at) AS first_seen, MAX(resolved_at) AS last_resolved
           FROM %s
          WHERE status IN ('resolved', 'accepted', 'waived', 'closed')
          GROUP BY 1, 2, 3, 4
          ORDER BY 5 DESC",
        qinc
      )
    ),

    list(
      id = "Q_context_rows_without_five_player_lineup",
      title = "Event/team-perspective rows do not carry two five-player lineups",
      severity = "error",
      purpose = paste(
        "Every persisted event/team row must name exactly five players on each side.",
        "Anything else makes lineup attribution for that event meaningless."
      ),
      required_tables = c("action_team_context_actions"),
      sql = sprintf(
        "SELECT game_id, team_id, type_lineup,
                CARDINALITY(own_lineup) AS own_lineup_size,
                CARDINALITY(opp_lineup) AS opp_lineup_size,
                COUNT(*) AS row_count
           FROM %s
          WHERE own_lineup IS NULL
             OR opp_lineup IS NULL
             OR CARDINALITY(own_lineup) <> 5
             OR CARDINALITY(opp_lineup) <> 5
          GROUP BY 1, 2, 3, 4, 5
          ORDER BY 6 DESC",
        atc
      )
    ),

    list(
      id = "R_invalid_lineup_player_counts",
      title = "Lineup states do not contain five distinct players",
      severity = "error",
      purpose = "A lineup naming the same player twice has fewer than five people on the floor and inflates that player's exposure.",
      required_tables = c("action_team_context_actions", "matchup_segments_actions"),
      sql = sprintf(
        "SELECT 'action_team_context_actions' AS source_relation, game_id, team_id,
                ARRAY_TO_STRING(own_lineup, ' | ') AS own_lineup, COUNT(*) AS row_count
           FROM %s
          WHERE CARDINALITY(own_lineup) <> (SELECT COUNT(DISTINCT e) FROM UNNEST(own_lineup) e)
          GROUP BY 1, 2, 3, 4
          UNION ALL
         SELECT 'matchup_segments_actions', game_id, team_id,
                ARRAY_TO_STRING(own_lineup, ' | '), COUNT(*)
           FROM %s
          WHERE CARDINALITY(own_lineup) <> (SELECT COUNT(DISTINCT e) FROM UNNEST(own_lineup) e)
          GROUP BY 1, 2, 3, 4
          ORDER BY 5 DESC, 1, 2, 3",
        atc, mseg
      )
    ),

    list(
      id = "S_invalid_starter_context",
      title = "Starter context is missing or out of domain",
      severity = "warning",
      purpose = paste(
        "Each team starts five players, and the starter counts carried on the event fact",
        "must stay within 0-5. Starter context drives the num_starters filters on every",
        "EuroLeague tab."
      ),
      required_tables = c("full_rosters", "action_team_context_actions"),
      sql = sprintf(
        "SELECT r.issue_type, r.game_id, r.team_id, r.observed_value
           FROM (SELECT 'roster_starter_count'::text AS issue_type, f.game_id, f.team_id,
                        COUNT(*) FILTER (WHERE f.is_starter) AS observed_value
                   FROM %s f
                  GROUP BY 2, 3) r
          WHERE r.observed_value <> 5
          UNION ALL
         SELECT 'context_starter_domain', a.game_id, a.team_id, COUNT(*)
           FROM %s a
          WHERE a.own_starters IS NULL OR a.own_starters < 0 OR a.own_starters > 5
             OR a.opp_starters IS NULL OR a.opp_starters < 0 OR a.opp_starters > 5
          GROUP BY 1, 2, 3
          ORDER BY 1, 2, 3",
        fr, atc
      )
    ),

    list(
      id = "T_invalid_team_minutes",
      title = "Canonical team minutes differ from official game duration",
      severity = "error",
      purpose = paste(
        "Summed canonical segment seconds per team must equal 40 minutes plus 5 minutes",
        "per overtime. Overtime count comes from the effective period on the event fact,",
        "because the provider keeps PERIOD = 5 for every overtime (migration 015).",
        "This is the Israeli T-class invariant and the basis of every pace figure."
      ),
      required_tables = c("action_team_context_actions", "matchup_segments_actions"),
      sql = sprintf(
        "WITH duration AS (
                SELECT game_id,
                       MAX(period) AS last_effective_period,
                       2400 + 300 * GREATEST(MAX(period) - 4, 0) AS expected_seconds
                  FROM %s
                 GROUP BY 1),
              segments AS (
                SELECT game_id, team_id, SUM(segment_seconds) AS segment_seconds
                  FROM %s
                 GROUP BY 1, 2)
         SELECT g.game_id, g.team_id, d.last_effective_period, d.expected_seconds,
                g.segment_seconds,
                ROUND(g.segment_seconds - d.expected_seconds, 3) AS difference_seconds
           FROM segments g
           JOIN duration d ON d.game_id = g.game_id
          WHERE ABS(g.segment_seconds - d.expected_seconds) > 1
          ORDER BY ABS(g.segment_seconds - d.expected_seconds) DESC",
        atc, mseg
      )
    ),

    list(
      id = "U_invalid_lineup_metric_values",
      title = "Lineup game rows contain impossible counts",
      severity = "error",
      purpose = "Negative counts, negative floor time, or makes exceeding attempts are arithmetic impossibilities, not judgement calls.",
      required_tables = c("lineup_totals_by_game"),
      sql = sprintf(
        "SELECT game_id, team_id, lineup_key, type_lineup, possessions, points, seconds,
                fgm, fga, fg2_made, fg2_att, fg3_made, fg3_att,
                CASE WHEN possessions < 0 OR points < 0 OR seconds < 0 THEN 'negative_value'
                     ELSE 'made_exceeds_attempted' END AS issue_type
           FROM %s
          WHERE possessions < 0 OR points < 0 OR seconds < 0
             OR fgm > fga OR fg2_made > fg2_att OR fg3_made > fg3_att
          ORDER BY 1, 2, 3",
        ltbg
      )
    ),

    list(
      id = "U1_lineup_denominator_anomalies",
      title = "Lineup game rows have possessions without floor time, or rebounds without opportunities",
      severity = "warning",
      purpose = paste(
        "Two softer signals kept separate from the hard impossibilities in U. A lineup",
        "credited possessions with zero recorded seconds is a timing attribution gap. An",
        "offensive rebound with no recorded opportunity means the rebound was credited",
        "off a miss the OREB denominator does not count, which is a definitional edge",
        "rather than a corrupt row."
      ),
      required_tables = c("lineup_totals_by_game"),
      sql = sprintf(
        "SELECT game_id, team_id, lineup_key, type_lineup, possessions, points, seconds,
                orebounds, oreb_opportunities,
                CASE WHEN orebounds > oreb_opportunities THEN 'orebounds_exceed_opportunities'
                     ELSE 'possessions_without_floor_time' END AS issue_type
           FROM %s
          WHERE orebounds > oreb_opportunities
             OR (seconds = 0 AND possessions > 0)
          ORDER BY 10, 1, 2",
        ltbg
      )
    ),

    list(
      id = "V_team_game_score_reconciliation",
      title = "Team-game scores do not reconcile across derived facts, box score, and schedule",
      severity = "error",
      purpose = paste(
        "Points summed from the derived team four-factor fact must equal the official",
        "team box score, which must equal the schedule result. Three independent sources",
        "agreeing is the strongest evidence the possession engine is attributing scoring",
        "to the right team."
      ),
      required_tables = c("team_four_factors_by_game", "team_boxscores", "schedule"),
      sql = sprintf(
        "WITH ff AS (SELECT game_id, team_id, SUM(off_pts) AS derived_points
                       FROM %s GROUP BY 1, 2)
         SELECT ff.game_id, ff.team_id, ff.derived_points, tb.points AS boxscore_points,
                CASE WHEN tb.is_home THEN s.home_points ELSE s.away_points END AS schedule_points
           FROM ff
           JOIN %s tb ON tb.game_id = ff.game_id AND tb.team_id = ff.team_id
           JOIN %s s ON s.game_id = ff.game_id
          WHERE ff.derived_points <> tb.points
             OR tb.points <> CASE WHEN tb.is_home THEN s.home_points ELSE s.away_points END
          ORDER BY 1, 2",
        tffg, tbox, sch
      )
    ),

    list(
      id = "W_team_game_possession_reconciliation",
      title = "Team-game offense and defense possessions do not reconcile",
      severity = "error",
      purpose = "One team's offensive possessions in a game are by definition its opponent's defensive possessions. Any gap means the possession engine lost or duplicated a possession.",
      required_tables = c("team_four_factors_by_game"),
      sql = sprintf(
        "WITH ff AS (SELECT game_id, team_id, SUM(off_poss) AS off_poss, SUM(def_poss) AS def_poss
                       FROM %s GROUP BY 1, 2)
         SELECT a.game_id, a.team_id, b.team_id AS opponent_team_id,
                a.off_poss, b.def_poss AS opponent_def_poss,
                a.off_poss - b.def_poss AS difference
           FROM ff a
           JOIN ff b ON b.game_id = a.game_id AND b.team_id <> a.team_id
          WHERE a.off_poss <> b.def_poss
          ORDER BY ABS(a.off_poss - b.def_poss) DESC",
        tffg
      )
    ),

    list(
      id = "X_player_minute_conservation",
      title = "Player minutes do not conserve reconstructed team lineup minutes",
      severity = "error",
      purpose = paste(
        "The five players on the floor each accrue the same wall-clock time, so the sum",
        "of a team's ON minutes in a game must be exactly five times its lineup minutes.",
        "This is the Israeli X-class invariant and the sharpest test of the lineup",
        "reconstruction."
      ),
      required_tables = c("matchup_segments_actions", "player_four_factors_by_game"),
      sql = sprintf(
        "WITH team_minutes AS (
                SELECT game_id, team_id, SUM(segment_seconds) / 60.0 AS team_minutes
                  FROM %s GROUP BY 1, 2),
              player_minutes AS (
                SELECT game_id, team_id, SUM(onoff_minutes) AS player_minutes_on
                  FROM %s
                 WHERE is_on_key = 1 AND type_lineup = 'offense'
                 GROUP BY 1, 2)
         SELECT t.game_id, t.team_id,
                ROUND(t.team_minutes, 3) AS team_minutes,
                ROUND(COALESCE(p.player_minutes_on, 0), 3) AS player_minutes_on,
                ROUND(COALESCE(p.player_minutes_on, 0) - 5 * t.team_minutes, 3) AS difference
           FROM team_minutes t
           LEFT JOIN player_minutes p ON p.game_id = t.game_id AND p.team_id = t.team_id
          WHERE ABS(COALESCE(p.player_minutes_on, 0) - 5 * t.team_minutes) > 0.02
          ORDER BY ABS(COALESCE(p.player_minutes_on, 0) - 5 * t.team_minutes) DESC",
        mseg, pffg
      )
    ),

    list(
      id = "Y_overtime_segment_coverage",
      title = "Overtime events are not covered by a canonical lineup segment",
      severity = "error",
      purpose = paste(
        "Overtime is where lineup reconstruction historically breaks, because the",
        "provider does not restate the on-court five. Every overtime event must still",
        "resolve to a canonical segment for its team."
      ),
      required_tables = c("action_team_context_actions", "matchup_segments_actions"),
      sql = sprintf(
        "SELECT a.game_id, a.team_id, a.period, COUNT(*) AS uncovered_rows
           FROM %s a
          WHERE a.period > 4
            AND (a.segment_id IS NULL
                 OR NOT EXISTS (SELECT 1 FROM %s m
                                 WHERE m.game_id = a.game_id
                                   AND m.team_id = a.team_id
                                   AND m.segment_id = a.segment_id))
          GROUP BY 1, 2, 3
          ORDER BY 4 DESC",
        atc, mseg
      )
    ),

    list(
      id = "Z_event_players_absent_from_lineup",
      title = "Acting players are absent from their team's attached lineup",
      severity = "warning",
      purpose = paste(
        "A player who records an offensive action for his own team must be one of the",
        "five in the lineup attached to that event. Offenders are lineup reconstruction",
        "errors: the possession is attributed to a five that did not include the actor.",
        "",
        "Lineups are text arrays of player names, and the provider spells some players",
        "two ways (check A). A lineup legitimately uses the spelling from the roster of",
        "the game it belongs to, which is not always players.display_name. Matching on",
        "display_name alone therefore reports a player as absent when he is present",
        "under his other spelling: that bug produced 258 false positives out of 425",
        "before this check also matched against the game's own roster names."
      ),
      required_tables = c("action_team_context_actions", "players", "full_rosters"),
      sql = sprintf(
        "SELECT a.game_id, a.team_id, a.period, a.source_event_order, a.play_type,
                p.display_name AS acting_player,
                ARRAY_TO_STRING(a.own_lineup, ' | ') AS attached_lineup
           FROM %s a
           JOIN %s p ON p.player_id = a.action_player_id
          WHERE a.type_lineup = 'offense'
            AND a.event_team_id = a.team_id
            AND NOT (p.display_name = ANY (a.own_lineup))
            AND NOT EXISTS (
                  SELECT 1
                    FROM %s f
                   WHERE f.game_id = a.game_id
                     AND f.player_id = a.action_player_id
                     AND f.source_player_name = ANY (a.own_lineup))
          ORDER BY 1, 4",
        atc, pls, fr
      )
    ),

    list(
      id = "AA_material_clock_order_anomalies",
      title = "Material game-clock regressions reach the event fact",
      severity = "error",
      purpose = paste(
        "Within a period, elapsed seconds must not decrease as event order advances.",
        "A regression beyond two seconds is a genuine ordering defect and can move an",
        "event across a clutch-window boundary."
      ),
      required_tables = c("action_team_context_actions"),
      sql = sprintf(
        "WITH ordered AS (
                SELECT game_id, team_id, period, source_event_order, event_elapsed_seconds,
                       LAG(event_elapsed_seconds) OVER (
                         PARTITION BY game_id, team_id, period ORDER BY source_event_order
                       ) AS previous_seconds
                  FROM %s)
         SELECT game_id, team_id, period, source_event_order,
                previous_seconds, event_elapsed_seconds,
                ROUND(previous_seconds - event_elapsed_seconds, 3) AS regression_seconds
           FROM ordered
          WHERE previous_seconds IS NOT NULL
            AND event_elapsed_seconds < previous_seconds - 2
          ORDER BY 7 DESC",
        atc
      )
    ),

    list(
      id = "AB_clock_order_jitter",
      title = "Non-trivial within-period clock-order jitter",
      severity = "warning",
      purpose = "Regressions of two seconds or less. Usually whole-second provider rounding rather than a real ordering defect, but a rising count is worth noticing.",
      required_tables = c("action_team_context_actions"),
      sql = sprintf(
        "WITH ordered AS (
                SELECT game_id, team_id, period, source_event_order, event_elapsed_seconds,
                       LAG(event_elapsed_seconds) OVER (
                         PARTITION BY game_id, team_id, period ORDER BY source_event_order
                       ) AS previous_seconds
                  FROM %s)
         SELECT game_id, team_id, period, source_event_order,
                previous_seconds, event_elapsed_seconds,
                ROUND(previous_seconds - event_elapsed_seconds, 3) AS regression_seconds
           FROM ordered
          WHERE previous_seconds IS NOT NULL
            AND event_elapsed_seconds < previous_seconds
            AND event_elapsed_seconds >= previous_seconds - 2
          ORDER BY 7 DESC",
        atc
      )
    ),

    list(
      id = "AC_missing_regulation_period_coverage",
      title = "Regulation periods are missing from the event fact",
      severity = "error",
      purpose = "Every team-game must have events in all four regulation periods. A missing period silently shortens that team's game.",
      required_tables = c("action_team_context_actions"),
      sql = sprintf(
        "WITH coverage AS (
                SELECT game_id, team_id,
                       COUNT(DISTINCT period) FILTER (WHERE period BETWEEN 1 AND 4) AS regulation_periods,
                       MAX(period) AS last_period
                  FROM %s
                 GROUP BY 1, 2)
         SELECT game_id, team_id, regulation_periods, last_period
           FROM coverage
          WHERE regulation_periods <> 4
          ORDER BY 3, 1, 2",
        atc
      )
    ),

    list(
      id = "AD_clutch_clock_and_margin_domain",
      title = "Clutch filter inputs are out of domain",
      severity = "error",
      purpose = paste(
        "The clutch readers filter on regulation_seconds_remaining and the pre-event",
        "margin carried by the player action fact. A value outside its domain changes",
        "clutch membership rather than producing an error."
      ),
      required_tables = c("player_stats_actions_by_game"),
      sql = sprintf(
        "SELECT game_id, team_id, source_event_order, is_overtime,
                regulation_seconds_remaining, pre_margin, pre_abs_margin, pre_status,
                CASE WHEN pre_abs_margin <> ABS(pre_margin) THEN 'margin_inconsistent'
                     WHEN pre_status NOT IN (-1, 0, 1) THEN 'status_out_of_domain'
                     ELSE 'regulation_clock_out_of_domain' END AS issue_type
           FROM %s
          WHERE pre_abs_margin <> ABS(pre_margin)
             OR pre_status NOT IN (-1, 0, 1)
             OR (NOT is_overtime
                 AND (regulation_seconds_remaining IS NULL
                      OR regulation_seconds_remaining < 0
                      OR regulation_seconds_remaining > 2400))
          ORDER BY 1, 3",
        psabg
      )
    ),

    list(
      id = "AE_duplicate_persisted_action_team_keys",
      title = "Persisted actions map to multiple rows for one team",
      severity = "error",
      purpose = "(game_id, source_event_order, team_id) is the grain of the event/team-perspective fact. A duplicate double-counts that event for that team.",
      required_tables = c("action_team_context_actions"),
      sql = sprintf(
        "SELECT game_id, source_event_order, team_id, COUNT(*) AS row_count
           FROM %s
          GROUP BY 1, 2, 3
         HAVING COUNT(*) > 1
          ORDER BY 4 DESC, 1, 2, 3",
        atc
      )
    ),

    list(
      id = "AF_invalid_persisted_segment_ids",
      title = "Event rows have missing, non-positive, or orphaned segment IDs",
      severity = "error",
      purpose = "Every event row must point at a canonical segment for its team, or its floor time cannot be attributed to a lineup.",
      required_tables = c("action_team_context_actions", "matchup_segments_actions"),
      sql = sprintf(
        "SELECT a.game_id, a.team_id, a.period,
                CASE WHEN a.segment_id IS NULL THEN 'null_segment_id'
                     WHEN a.segment_id <= 0 THEN 'non_positive_segment_id'
                     ELSE 'segment_absent_from_canonical_table' END AS issue_type,
                COUNT(*) AS row_count
           FROM %s a
          WHERE a.segment_id IS NULL
             OR a.segment_id <= 0
             OR NOT EXISTS (SELECT 1 FROM %s m
                             WHERE m.game_id = a.game_id
                               AND m.team_id = a.team_id
                               AND m.segment_id = a.segment_id)
          GROUP BY 1, 2, 3, 4
          ORDER BY 5 DESC",
        atc, mseg
      )
    ),

    list(
      id = "AG_cold_storage_snapshot_consistency",
      title = "Cold-storage Parquets form a consistent latest-game snapshot",
      severity = "todo",
      purpose = "No EuroLeague analogue: the euroleague schema keeps every relation hot and exports nothing to Parquet. See N9 for the storage consequence.",
      required_tables = character(0),
      sql = NA_character_,
      not_automated_note = "No cold storage exists for the euroleague schema."
    ),

    list(
      id = "AH_canonical_segment_timing",
      title = "Canonical segments are contiguous, half-open, and self-consistent",
      severity = "error",
      purpose = paste(
        "Segments are half-open [start_event_order, end_event_order_exclusive) and must",
        "tile a team's game without gap or overlap, with stored duration equal to the",
        "clock difference. This is the single invariant every minutes and pace figure",
        "rests on."
      ),
      required_tables = c("matchup_segments_actions"),
      sql = sprintf(
        "WITH ordered AS (
                SELECT game_id, team_id, segment_id,
                       start_event_order, end_event_order_exclusive,
                       start_elapsed_seconds, end_elapsed_seconds, segment_seconds,
                       LAG(end_event_order_exclusive) OVER w AS previous_end_order,
                       LAG(end_elapsed_seconds) OVER w AS previous_end_seconds
                  FROM %s
                WINDOW w AS (PARTITION BY game_id, team_id ORDER BY start_event_order))
         SELECT game_id, team_id, segment_id, start_event_order, end_event_order_exclusive,
                previous_end_order, start_elapsed_seconds, end_elapsed_seconds,
                previous_end_seconds, segment_seconds,
                CASE WHEN end_event_order_exclusive <= start_event_order THEN 'non_positive_event_range'
                     WHEN segment_seconds < 0 THEN 'negative_duration'
                     WHEN ROUND(segment_seconds, 3) <> ROUND(end_elapsed_seconds - start_elapsed_seconds, 3) THEN 'duration_mismatch'
                     WHEN previous_end_order IS NOT NULL AND previous_end_order <> start_event_order THEN 'event_order_discontinuity'
                     ELSE 'clock_discontinuity' END AS issue_type
           FROM ordered
          WHERE end_event_order_exclusive <= start_event_order
             OR segment_seconds < 0
             OR ROUND(segment_seconds, 3) <> ROUND(end_elapsed_seconds - start_elapsed_seconds, 3)
             OR (previous_end_order IS NOT NULL AND previous_end_order <> start_event_order)
             OR (previous_end_seconds IS NOT NULL AND ROUND(previous_end_seconds, 3) <> ROUND(start_elapsed_seconds, 3))
          ORDER BY 1, 2, 4",
        mseg
      )
    ),

    list(
      id = "AI_team_game_minute_mirror",
      title = "Opposing teams do not accrue equal canonical minutes",
      severity = "error",
      purpose = "Both teams are on the floor for the same wall clock. Unequal segment totals mean one side's reconstruction lost or gained time.",
      required_tables = c("matchup_segments_actions"),
      sql = sprintf(
        "WITH segments AS (SELECT game_id, team_id, SUM(segment_seconds) AS segment_seconds
                             FROM %s GROUP BY 1, 2)
         SELECT a.game_id, a.team_id, b.team_id AS opponent_team_id,
                a.segment_seconds, b.segment_seconds AS opponent_seconds,
                ROUND(a.segment_seconds - b.segment_seconds, 3) AS difference_seconds
           FROM segments a
           JOIN segments b ON b.game_id = a.game_id AND b.team_id <> a.team_id
          WHERE ABS(a.segment_seconds - b.segment_seconds) > 0.001
          ORDER BY ABS(a.segment_seconds - b.segment_seconds) DESC",
        mseg
      )
    ),

    list(
      id = "AJ_free_throw_trip_annotation",
      title = "Free-throw trip annotation is incomplete or out of domain",
      severity = "error",
      purpose = paste(
        "TS% and the OREB denominator both depend on free throws being grouped into",
        "trips. A free-throw row without a trip id, or a points value outside 0-3,",
        "breaks that grouping. Named for the fields it reads, not for a progress",
        "fraction the EuroLeague schema does not store."
      ),
      required_tables = c("action_team_context_actions"),
      sql = sprintf(
        "SELECT game_id, team_id, source_event_order, play_type, ft_attempts,
                synthetic_ft_trip_id, ft_reverse_order, points,
                CASE WHEN ft_attempts > 0 AND synthetic_ft_trip_id IS NULL THEN 'free_throw_without_trip_id'
                     WHEN ft_attempts < 0 THEN 'negative_ft_attempts'
                     WHEN points < 0 OR points > 3 THEN 'points_out_of_domain'
                     ELSE 'non_positive_ft_reverse_order' END AS issue_type
           FROM %s
          WHERE (ft_attempts > 0 AND synthetic_ft_trip_id IS NULL)
             OR ft_attempts < 0
             OR points < 0 OR points > 3
             OR (ft_reverse_order IS NOT NULL AND ft_reverse_order < 1)
          ORDER BY 1, 3",
        atc
      )
    ),

    list(
      id = "N1_publication_blocked_games",
      title = "Games whose latest load run is blocked from publication",
      severity = "error",
      purpose = paste(
        "publication_status = 'blocked' means the schema's own fail-closed gates",
        "rejected the game. Its facts are in the read layer regardless, because the",
        "season aggregates do not filter on QA status."
      ),
      required_tables = c("game_qa", "schedule"),
      sql = sprintf(
        "WITH latest AS (%s)
         SELECT l.game_id, s.competition, s.season, s.gamecode, s.round_number, s.phase,
                l.load_run_id, l.publication_status,
                l.possession_structural_status, l.possession_review_status,
                l.lineup_structure_valid, l.lineup_invalid_actor_rows,
                l.boxscore_metrics_exact, l.score_progression_exact, l.score_progression_reconciled,
                l.total_possessions, l.possession_difference, l.same_team_transitions,
                l.unresolved_ft_rows, l.duplicate_endpoint_incidents, l.missing_parent_targets
           FROM latest l
           LEFT JOIN %s s ON s.game_id = l.game_id
          WHERE l.publication_status = 'blocked'
          ORDER BY 1",
        latest_qa, sch
      )
    ),

    list(
      id = "N2_publication_review_games",
      title = "Games whose latest load run is held for review",
      severity = "warning",
      purpose = paste(
        "publication_status = 'review' means at least one gate wants a human look.",
        "These games are already inside every season aggregate, so the count matters:",
        "a large share means the read layer is mostly unreviewed data."
      ),
      required_tables = c("game_qa", "schedule"),
      sql = sprintf(
        "WITH latest AS (%s)
         SELECT l.game_id, s.competition, s.season, s.gamecode, s.round_number, s.phase,
                l.load_run_id, l.possession_review_status,
                l.lineup_structure_valid, l.lineup_invalid_actor_rows,
                l.boxscore_metrics_exact, l.score_progression_exact, l.score_progression_reconciled,
                l.possession_difference, l.same_team_transitions,
                l.provisional_ft_rows, l.unresolved_ft_rows,
                l.duplicate_endpoint_incidents, l.missing_parent_targets
           FROM latest l
           LEFT JOIN %s s ON s.game_id = l.game_id
          WHERE l.publication_status = 'review'
          ORDER BY 1",
        latest_qa, sch
      )
    ),

    list(
      id = "N3_reconciliation_metric_mismatches",
      title = "PBP-derived team metrics do not match the official box score",
      severity = "error",
      purpose = paste(
        "reconciliation_metrics compares every derived team counting stat with the",
        "official box score. A mismatch on the latest load run for a game is a parser",
        "or attribution defect in that specific metric."
      ),
      required_tables = c("reconciliation_metrics", "schedule"),
      sql = sprintf(
        "SELECT r.game_id, s.competition, s.season, s.gamecode, s.round_number,
                r.team_id, r.metric, r.pbp_value, r.official_value, r.difference, r.load_run_id
           FROM %s r
           LEFT JOIN %s s ON s.game_id = r.game_id
          WHERE NOT r.matches
            AND r.load_run_id = (SELECT MAX(r2.load_run_id) FROM %s r2 WHERE r2.game_id = r.game_id)
          ORDER BY ABS(r.difference) DESC, r.game_id, r.metric",
        recon, sch, recon
      )
    ),

    list(
      id = "N4_open_qa_incidents",
      title = "Unresolved QA incidents",
      severity = "warning",
      purpose = "Incidents the loader raised and nobody closed. An empty result can also mean the incident recorder is not being written to, which N1 and N2 will contradict.",
      required_tables = c("qa_incidents"),
      sql = sprintf(
        "SELECT category, severity, status, rule_code, COUNT(*) AS incident_count,
                COUNT(DISTINCT game_id) AS games,
                MIN(created_at) AS first_seen, MAX(created_at) AS last_seen
           FROM %s
          WHERE status IS DISTINCT FROM 'resolved'
          GROUP BY 1, 2, 3, 4
          ORDER BY 5 DESC",
        qinc
      )
    ),

    list(
      id = "N5_load_runs_not_completed",
      title = "Load runs that did not complete, or completed with failures",
      severity = "error",
      purpose = paste(
        "A run left in running, partial, or failed state has published some games and",
        "not others. A run stuck in 'running' with no completed_at is the operational",
        "equivalent of a lock nobody released."
      ),
      required_tables = c("load_runs"),
      sql = sprintf(
        "SELECT load_run_id, competition, season, status,
                requested_games, successful_games, failed_games,
                started_at, completed_at, package_name, package_version, collector_version
           FROM %s
          WHERE status <> 'completed' OR failed_games > 0
          ORDER BY load_run_id",
        lrun
      )
    ),

    list(
      id = "N6_unconfirmed_action_grouping",
      title = "Canonical actions whose parser grouping is not confirmed",
      severity = "warning",
      purpose = paste(
        "grouping_status records how confident the parser is that an event was attached",
        "to the right possession. Provisional and unresolved rows are the population",
        "from which possession-boundary defects are drawn."
      ),
      required_tables = c("actions"),
      problem_count_col = "action_rows",
      sql = sprintf(
        "SELECT a.grouping_status, a.game_id,
                MIN(a.grouping_confidence_pct) AS min_confidence_pct,
                COUNT(*) AS action_rows
           FROM %s a
          WHERE a.grouping_status IS DISTINCT FROM 'confirmed'
          GROUP BY 1, 2
          ORDER BY 4 DESC, 1, 2",
        act
      )
    ),

    list(
      id = "N7_scheduled_games_not_loaded",
      title = "Games the schedule reports as played have no canonical actions",
      severity = "error",
      purpose = paste(
        "The schedule relation is populated from the same load as the games, so this",
        "check cannot see games never collected at all. It catches a schedule row that",
        "survived a load whose PBP did not."
      ),
      required_tables = c("schedule", "actions"),
      sql = sprintf(
        "SELECT s.game_id, s.competition, s.season, s.gamecode, s.round_number, s.phase,
                s.status, s.scheduled_at, s.first_seen_load_run_id, s.last_seen_load_run_id
           FROM %s s
          WHERE s.status = 'played'
            AND NOT EXISTS (SELECT 1 FROM %s a WHERE a.game_id = s.game_id)
          ORDER BY 1",
        sch, act
      )
    ),

    list(
      id = "N8_possession_endpoint_vocabulary",
      title = "Possession endpoints carry an unrecognised reason",
      severity = "warning",
      purpose = paste(
        "Every possession-ending action is annotated with why it ended. A reason outside",
        "the reviewed vocabulary means the parser emitted a category nobody has audited,",
        "which is how an unnoticed possession-counting change arrives."
      ),
      required_tables = c("actions"),
      sql = sprintf(
        "SELECT COALESCE(a.endpoint_reason, '(null)') AS endpoint_reason,
                COUNT(*) AS action_rows,
                COUNT(DISTINCT a.game_id) AS games
           FROM %s a
          WHERE a.end_possession
            AND (a.endpoint_reason IS NULL OR a.endpoint_reason NOT IN (%s))
          GROUP BY 1
          ORDER BY 2 DESC",
        act, endpoint_vocabulary
      )
    ),

    list(
      id = "N9_schema_storage_footprint",
      title = "EuroLeague schema storage against the instance budget",
      severity = "warning",
      purpose = paste(
        "The euroleague schema shares one Supabase instance with the Israeli schema and",
        "has no cold storage, so its growth spends the shared budget. Override the",
        "threshold with EURO_DQ_STORAGE_BUDGET_MB (default 500)."
      ),
      required_tables = character(0),
      sql = NA_character_,
      problem_count_col = "over_budget",
      runner = function(con, schema, root) euro_storage_footprint_details(con, schema, root)
    )
  )
}

# --- BEGIN verbatim copy of etl/run_data_quality_report.R engine (part 2) ----
run_data_quality_report <- function(
  # EUROLEAGUE: repo_root() resolves to the euroleague/ directory for this
  # script, so the default output path is relative to it rather than the repo.
  output_dir = Sys.getenv("DQ_OUTPUT_DIR", file.path("logs", "data_quality")),
  max_detail_rows = as.integer(Sys.getenv("DQ_MAX_DETAIL_ROWS", "50")),
  fail_on_error = truthy("DQ_FAIL_ON_ERROR")
) {
  root <- repo_root()
  # EUROLEAGUE: credentials live at the repository root, one level above root.
  env_file <- file.path(root, "..", "etl", ".Renviron")
  if (file.exists(env_file)) {
    readRenviron(env_file)
  }

  app_env <- Sys.getenv("APP_ENV", "test")
  # EUROLEAGUE: the shadow schema is not APP_ENV-dependent.
  schema <- "euroleague"
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
    # EUROLEAGUE: report title.
    "# EuroLeague Data Quality Report",
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
# --- END verbatim copy (part 2) ----------------------------------------------
