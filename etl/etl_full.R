# =============================================================================
# etl/etl_full.R — Full automated ETL pipeline
#
# Orchestrates:
#   Phase 1: Setup + connection
#   Phase 2: Base table ETL (etl_update)
#   Phase 3: Sub-lineup generation
#   Phase 4: MV refresh (10 MVs, 4 dependency levels)
#   Phase 5: Sub-lineup stats refresh
#   Phase 6: Validation (per-game row count checks)
#
# Usage:
#   Sys.setenv(APP_ENV = "test")
#   source("etl/etl_full.R")
#   etl_full()                    # auto-detect new games
#   etl_full(dry_run = TRUE)      # preview without writes
#   etl_full(game_ids = c(12345)) # specific games
# =============================================================================

# ─── Sub-lineup helper functions (extracted from etl_lineups.R) ──────────────

make_subs_for_one <- function(lineup_df, ks = c(2, 3, 4)) {
  lineup_hash <- unique(lineup_df$lineup_hash)
  team_id     <- unique(lineup_df$team_id)
  game_year   <- unique(lineup_df$game_year)

  ids <- sort(unique(as.integer(lineup_df$player_id)))
  out <- list()

  for (k in ks) {
    if (length(ids) < k) next

    m <- t(combn(ids, k))

    sub_ids <- apply(
      m, 1L,
      function(v) paste(sort(as.integer(v)), collapse = "_")
    )

    sub_hash <- vapply(
      sub_ids,
      digest::digest,
      FUN.VALUE = character(1L),
      algo = "md5",
      USE.NAMES = FALSE
    )

    out[[length(out) + 1L]] <- tibble::tibble(
      lineup_hash     = lineup_hash,
      team_id         = team_id,
      game_year       = game_year,
      sub_lineup_id   = sub_ids,
      sub_lineup_hash = sub_hash,
      sub_size        = rep.int(k, nrow(m))
    )
  }

  if (!length(out)) {
    return(tibble::tibble(
      lineup_hash     = character(0),
      team_id         = integer(0),
      game_year       = integer(0),
      sub_lineup_id   = character(0),
      sub_lineup_hash = character(0),
      sub_size        = integer(0)
    ))
  }

  dplyr::bind_rows(out)
}

build_sub_lineups_all <- function(players_df, ks = c(2, 3, 4)) {
  if (!nrow(players_df)) {
    return(tibble::tibble(
      lineup_hash     = character(0),
      team_id         = integer(0),
      game_year       = integer(0),
      sub_lineup_id   = character(0),
      sub_lineup_hash = character(0),
      sub_size        = integer(0)
    ))
  }

  groups <- players_df %>%
    dplyr::group_by(lineup_hash, team_id, game_year) %>%
    dplyr::group_split()

  purrr::map_dfr(groups, make_subs_for_one, ks = ks)
}

# ─── Logging ─────────────────────────────────────────────────────────────────

setup_logging <- function() {
  log_dir <- file.path("etl", "logs")
  dir.create(log_dir, recursive = TRUE, showWarnings = FALSE)

  timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
  log_file  <- file.path(log_dir, paste0("etl_full_", timestamp, ".log"))

  # Return a logging function that captures the file path
  log_fn <- function(msg, level = "INFO") {
    line <- sprintf("[%s] %s: %s", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), level, msg)
    message(line)
    cat(line, "\n", file = log_file, append = TRUE)
  }

  list(log_msg = log_fn, log_file = log_file)
}

# ---- App meta: last successful ETL time ----
ensure_app_meta <- function(pg, schema) {
  DBI::dbExecute(
    pg,
    sprintf(
      'CREATE TABLE IF NOT EXISTS "%s"."app_meta" (key text PRIMARY KEY, value text NOT NULL, updated_at timestamptz NOT NULL DEFAULT now())',
      schema
    )
  )
}

set_last_success <- function(pg, schema, ts = Sys.time()) {
  ensure_app_meta(pg, schema)
  DBI::dbExecute(
    pg,
    sprintf(
      'INSERT INTO "%s"."app_meta"(key, value, updated_at) VALUES ($1, $2, now()) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()',
      schema
    ),
    params = list("etl_full_last_success", format(ts, "%Y-%m-%d %H:%M:%S"))
  )
}

# ─── Main pipeline ───────────────────────────────────────────────────────────

etl_full <- function(game_ids = NULL, dry_run = FALSE) {

  # =========================================================================
  # Phase 1: Setup + Connection
  # =========================================================================

  logger  <- setup_logging()
  log_msg <- logger$log_msg

  log_msg(sprintf("ETL Full pipeline started (dry_run = %s)", dry_run))
  log_msg(sprintf("Log file: %s", logger$log_file))

  # Load environment variables
  env_file <- file.path("etl", ".Renviron")
  if (file.exists(env_file)) {
    readRenviron(env_file)
    log_msg(sprintf("Loaded env from %s", env_file))
  } else {
    log_msg(sprintf("No .Renviron found at %s — using existing env vars", env_file), "WARN")
  }

  # Source etl_onoff.R to get pg connection, SCHEMA, and all helper functions.
  # The file has a bare etl_update() call at the bottom, so we parse and
  # evaluate only the definitions (everything up to the end of etl_update's
  # closing brace, excluding the "Usage" execution block).
  log_msg("Sourcing etl/etl_onoff.R (definitions only) ...")
  etl_lines <- readLines("etl/etl_onoff.R")
  # Strip the trailing execution block: lines after "# Usage"
  usage_marker <- grep("^#\\s*Usage", etl_lines)
  if (length(usage_marker)) {
    # Cut 2 lines before (the "# ===" separator line) to be safe
    cut_at <- usage_marker[1] - 2
    etl_lines <- etl_lines[seq_len(cut_at)]
  }
  eval(parse(text = etl_lines), envir = .GlobalEnv)
  log_msg(sprintf("Schema: %s (APP_ENV = %s)", SCHEMA, APP_ENV))

  # Ensure cleanup on exit
  on.exit({
    if (exists("pg") && DBI::dbIsValid(pg)) {
      log_msg("Closing database connection")
      DBI::dbDisconnect(pg)
    }
  }, add = TRUE)

  overall_start <- proc.time()

  # =========================================================================
  # Phase 2: Base Table ETL
  # =========================================================================

  log_msg("─── Phase 2: Base Table ETL ───")

  processed_ids <- tryCatch({
    # Fetch schedule
    sched_df <- fetch_israel_schedule() %>%
      dplyr::filter(score_team1 > 0)

    log_msg(sprintf("Schedule fetched: %d games with scores", nrow(sched_df)))

    # Determine which games to process
    if (is.null(game_ids)) {
      existing <- DBI::dbGetQuery(
        pg, sprintf('SELECT DISTINCT game_id FROM "%s"."actions_clean"', SCHEMA)
      )
      ids <- setdiff(sched_df$game_id, existing$game_id) |> sort() |> unique()
    } else {
      ids <- sort(unique(as.integer(game_ids)))
    }

    # Build schedule subset for requested IDs, with DB fallback when IDs are not
    # present in the external schedule feed (common for cup games).
    sched_subset <- dplyr::semi_join(
      sched_df,
      tibble::tibble(game_id = ids),
      by = "game_id"
    )
    missing_in_feed <- setdiff(ids, sched_subset$game_id)
    if (length(missing_in_feed)) {
      missing_csv <- paste(as.integer(missing_in_feed), collapse = ",")
      db_subset <- DBI::dbGetQuery(
        pg,
        sprintf(
          paste0(
            'SELECT game_id, game_year, game_date, gn, game_type, team1, team2, ',
            'team_name_eng_1, team_name_eng_2, score_team1, score_team2 ',
            'FROM "%s"."schedule" WHERE game_id IN (%s)'
          ),
          SCHEMA, missing_csv
        )
      )
      if (nrow(db_subset)) {
        db_subset$game_date <- as.Date(db_subset$game_date)
        db_subset$pbp_url <- paste0(
          "https://stats.segevstats.com/realtimestat_heb/get_team_action.php?game_id=",
          db_subset$game_id
        )
        db_subset$box_url <- paste0(
          "https://stats.segevstats.com/realtimestat_heb/get_team_score.php?game_id=",
          db_subset$game_id
        )
        sched_subset <- dplyr::bind_rows(sched_subset, db_subset) |>
          dplyr::distinct(game_id, .keep_all = TRUE)
        log_msg(sprintf(
          "Added %d game(s) from %s.schedule fallback (missing from feed)",
          nrow(db_subset), SCHEMA
        ))
      }
      still_missing <- setdiff(ids, sched_subset$game_id)
      if (length(still_missing)) {
        log_msg(sprintf(
          "Requested IDs still missing from both feed and %s.schedule: %s",
          SCHEMA, paste(still_missing, collapse = ", ")
        ), "WARN")
      }
    }

    if (!length(ids)) {
      log_msg("No new games to process")
      ids
    } else {

    log_msg(sprintf("Games to process: %d (%s)", length(ids), paste(ids, collapse = ", ")))
    ensure_pws_num_starters_cols(pg, SCHEMA)

    if (dry_run) {
      log_msg("[DRY RUN] Would run etl_update() for the above games")
      ids
    } else {
      # Assign game_ids + sched_subset to global env so etl_update() can see them
      # (etl_update references these as free variables)
      assign("game_ids", ids, envir = .GlobalEnv)

      assign("sched_subset", sched_subset, envir = .GlobalEnv)

      t0 <- proc.time()

      # Upsert schedule
      upsert_by_like(pg, SCHEMA, "schedule", sched_subset)
      log_msg(sprintf("  schedule: %d rows upserted", nrow(sched_subset)))

      # Fetch PBP only for rows with usable URLs.
      fetchable_sched <- sched_subset |>
        dplyr::filter(!is.na(pbp_url), nzchar(pbp_url))
      skipped_pbp <- setdiff(sched_subset$game_id, fetchable_sched$game_id)
      if (length(skipped_pbp)) {
        log_msg(sprintf(
          "Skipping %d game(s) without pbp_url: %s",
          length(skipped_pbp), paste(skipped_pbp, collapse = ", ")
        ), "WARN")
      }
      if (!nrow(fetchable_sched)) {
        stop("No fetchable games (all requested rows missing pbp_url).", call. = FALSE)
      }
      pbps <- purrr::map2(fetchable_sched$game_id, fetchable_sched$pbp_url, fetch_game_pbp)
      log_msg(sprintf("Fetched PBP for %d games", length(pbps)))

      # actions_clean
      actions_df <- purrr::map(pbps, clean_actions) |> purrr::list_rbind()
      upsert_by_like(pg, SCHEMA, "actions_clean", actions_df)
      log_msg(sprintf("  actions_clean: %d rows upserted", nrow(actions_df)))

      # subs
      subs_df <- actions_df %>%
        dplyr::filter(type == "substitution") |>
        dplyr::mutate(
          parameters_player_in = dplyr::if_else(!is.na(parameters_player_in), player_id, NA),
          parameters_player_out = dplyr::if_else(!is.na(parameters_player_out), player_id, NA)
        )
      upsert_by_like(pg, SCHEMA, "subs", subs_df)
      log_msg(sprintf("  subs: %d rows upserted", nrow(subs_df)))

      # full_rosters
      box_sched <- fetchable_sched |>
        dplyr::mutate(
          box_url = dplyr::if_else(
            is.na(box_url) | box_url == "",
            paste0(
              "https://stats.segevstats.com/realtimestat_heb/get_team_score.php?game_id=",
              game_id
            ),
            box_url
          )
        )
      boxes <- purrr::map2(box_sched$game_id, box_sched$box_url, fetch_game_box)
      game_year_map <- fetchable_sched |>
        dplyr::select(game_id, game_year) |>
        dplyr::distinct() |>
        dplyr::mutate(game_year = as.integer(game_year))
      roster_df <- purrr::map(pbps, extract_roster) |>
        purrr::list_rbind() |>
        dplyr::rename_with(tolower) |>
        dplyr::left_join(game_year_map, by = "game_id")
      starters_df <- purrr::map(boxes, extract_starters) |>
        purrr::list_rbind() |>
        dplyr::rename_with(tolower)
      roster_df <- roster_df |>
        dplyr::left_join(starters_df, by = c("game_id", "team_id", "player_id")) |>
        dplyr::mutate(starter = dplyr::coalesce(as.logical(starter), FALSE))
      roster_df <- enrich_roster_names_from_existing(pg, SCHEMA, roster_df)
      upsert_by_like(pg, SCHEMA, "full_rosters", roster_df)
      log_msg(sprintf("  full_rosters: %d rows upserted", nrow(roster_df)))

      # possessions
      actions_tbl <- dplyr::tbl(pg, dbplyr::in_schema(SCHEMA, "actions_clean")) |>
        dplyr::filter(game_id %in% fetchable_sched$game_id)
      poss_stage <- compute_possessions(actions_tbl) |>
        dplyr::collect() |>
        dplyr::rename(quarter = quarter.x) |>
        dplyr::select(-quarter.y)
      upsert_by_like(pg, SCHEMA, "possessions", poss_stage)
      log_msg(sprintf("  possessions: %d rows upserted", nrow(poss_stage)))

      # lineups_lookup
      df_lineups_df <- compute_lineups_lookup(pg) |>
        dplyr::filter(game_id %in% fetchable_sched$game_id) |>
        dplyr::collect()
      upsert_by_like(pg, SCHEMA, "lineups_lookup", df_lineups_df)
      log_msg(sprintf("  lineups_lookup: %d rows upserted", nrow(df_lineups_df)))
      lineup_starters <- df_lineups_df %>%
        dplyr::select(game_id, team_id, lineup_hash, num_starters) %>%
        dplyr::distinct(game_id, team_id, lineup_hash, .keep_all = TRUE)

      # stints
      stints_df <- compute_stints(pg) |>
        dplyr::filter(game_id %in% fetchable_sched$game_id) |>
        dplyr::collect() %>%
        dplyr::select(
          team_id_offense, game_id, final_start_seg, final_end_seg,
          segment_id, lineup_hash_offense, lineup_hash_defense, team_id_defense,
          q_bucket, final_start_id, final_end_id
        ) %>%
        dplyr::rename(team_id = team_id_offense)
      upsert_by_like(pg, SCHEMA, "stints", stints_df)
      log_msg(sprintf("  stints: %d rows upserted", nrow(stints_df)))

      # pws (possessions-within-stints)
      by <- dplyr::join_by(
        team_id, game_id, q_bucket,
        between(id, final_start_id, final_end_id, bounds = "[)")
      )
      pws_stage <- dplyr::left_join(
        poss_stage %>% dplyr::mutate(q_bucket = dplyr::if_else(quarter < 5, 0L, quarter)),
        stints_df,
        by
      )
      pws_stage <- pws_stage %>%
        dplyr::left_join(
          lineup_starters %>%
            dplyr::rename(
              lineup_hash_offense = lineup_hash,
              num_starters_offense = num_starters
            ),
          by = c("game_id", "team_id", "lineup_hash_offense")
        ) %>%
        dplyr::left_join(
          lineup_starters %>%
            dplyr::rename(
              team_id_defense = team_id,
              lineup_hash_defense = lineup_hash,
              num_starters_defense = num_starters
            ),
          by = c("game_id", "team_id_defense", "lineup_hash_defense")
        )
      upsert_by_like(pg, SCHEMA, "pws", pws_stage)
      log_msg(sprintf("  pws: %d rows upserted", nrow(pws_stage)))

      DBI::dbExecute(pg, sprintf('ANALYZE "%s"."pws";', SCHEMA))

      elapsed <- (proc.time() - t0)["elapsed"]
      log_msg(sprintf("Phase 2 complete. %d games processed in %.1fs", length(ids), elapsed))

      ids
    }
    }
  }, error = function(e) {
    log_msg(sprintf("Phase 2 FAILED: %s", conditionMessage(e)), "ERROR")
    stop("Base ETL failed — aborting pipeline.", call. = FALSE)
  })

  # Guardrail: verify key tables have rows
  for (tbl_name in c("schedule", "actions_clean", "lineups_lookup", "pws")) {
    cnt <- DBI::dbGetQuery(
      pg, sprintf('SELECT count(*) AS n FROM "%s"."%s"', SCHEMA, tbl_name)
    )$n
    if (cnt == 0) {
      log_msg(sprintf("GUARDRAIL: %s.%s has 0 rows!", SCHEMA, tbl_name), "ERROR")
    } else {
      log_msg(sprintf("  %s: %s total rows", tbl_name, format(cnt, big.mark = ",")))
    }
  }

  if (!length(processed_ids)) {
    log_msg("No new games â€” skipping Phases 3-6")
  } else {
    # =========================================================================
    # Phase 3: Sub-Lineup Generation
    # =========================================================================

  log_msg("─── Phase 3: Sub-Lineup Generation ───")

  tryCatch({
    t0 <- proc.time()

    # Pull ON lineups for processed games
    lineups_src <- dplyr::tbl(pg, dbplyr::in_schema(SCHEMA, "lineups_lookup"))

    src_rows <- lineups_src %>%
      dplyr::filter(game_id %in% !!processed_ids, is_on_verdict == 1) %>%
      dplyr::select(lineup_hash, team_id, player_id, game_year) %>%
      dplyr::distinct() %>%
      dplyr::arrange(lineup_hash, player_id) %>%
      dplyr::collect()

    log_msg(sprintf("  ON lineup rows from processed games: %d", nrow(src_rows)))

    if (nrow(src_rows) == 0) {
      log_msg("  No ON lineups found — skipping sub-lineup generation")
    } else {
      # Anti-join against existing lineups_lookup_on
      ll_on <- dplyr::tbl(pg, dbplyr::in_schema(SCHEMA, "lineups_lookup_on"))

      existing_rows <- ll_on %>%
        dplyr::filter(
          lineup_hash %in% !!unique(src_rows$lineup_hash),
          game_year   %in% !!unique(src_rows$game_year)
        ) %>%
        dplyr::select(lineup_hash, team_id, player_id, game_year) %>%
        dplyr::distinct() %>%
        dplyr::collect()

      new_rows_on <- dplyr::anti_join(
        src_rows, existing_rows,
        by = c("lineup_hash", "team_id", "player_id", "game_year")
      )

      log_msg(sprintf("  New ON lineup rows: %d (existing: %d)", nrow(new_rows_on), nrow(existing_rows)))

      if (nrow(new_rows_on) == 0) {
        log_msg("  All lineups already in lineups_lookup_on — skipping")
      } else if (dry_run) {
        log_msg(sprintf("[DRY RUN] Would insert %d rows into lineups_lookup_on", nrow(new_rows_on)))

        # Build sub-lineups to report count
        sub_lineups_table <- build_sub_lineups_all(new_rows_on, ks = c(2, 3, 4))
        log_msg(sprintf("[DRY RUN] Would generate %d sub-lineup rows", nrow(sub_lineups_table)))
      } else {
        # Insert into lineups_lookup_on
        DBI::dbWriteTable(
          pg,
          DBI::Id(schema = SCHEMA, table = "lineups_lookup_on"),
          new_rows_on,
          append = TRUE, row.names = FALSE
        )
        log_msg(sprintf("  Inserted %d rows into lineups_lookup_on", nrow(new_rows_on)))

        # Build sub-lineups
        sub_lineups_table <- build_sub_lineups_all(new_rows_on, ks = c(2, 3, 4))
        log_msg(sprintf("  Generated %d sub-lineup combinations", nrow(sub_lineups_table)))

        if (nrow(sub_lineups_table) > 0) {
          sub_line <- sub_lineups_table %>%
            dplyr::mutate(created_at = Sys.time()) %>%
            dplyr::rename(num_lineup = sub_size, lineup_id = sub_lineup_id) %>%
            dplyr::select(
              team_id, lineup_hash, sub_lineup_hash,
              lineup_id, num_lineup, game_year, created_at
            )

          DBI::dbWriteTable(
            pg,
            DBI::Id(schema = SCHEMA, table = "sub_lineups"),
            sub_line,
            append = TRUE, row.names = FALSE
          )
          log_msg(sprintf("  Inserted %d rows into sub_lineups", nrow(sub_line)))
        }
      }
    }

    elapsed <- (proc.time() - t0)["elapsed"]
    log_msg(sprintf("Phase 3 complete in %.1fs", elapsed))

  }, error = function(e) {
    log_msg(sprintf("Phase 3 FAILED: %s — continuing to Phase 4", conditionMessage(e)), "ERROR")
  })

  # =========================================================================
  # Phase 4: MV Refresh (10 MVs, 4 dependency levels)
  # =========================================================================

  log_msg("─── Phase 4: MV Refresh ───")

  mv_levels <- list(
    list(level = 1, mvs = c("final_schedule_mv", "df_pts_poss_lineups_longer_mv")),
    list(level = 2, mvs = c("mv_lineup_totals_by_day", "team_ppp_ratings_mv", "onoff_default_mv")),
    list(level = 3, mvs = c("player_onoff_by_game", "player_four_factors_by_game",
                            "lineup_four_factors_by_game", "player_advanced_stats_mv")),
    list(level = 4, mvs = c("team_four_factors_mv"))
  )

  if (dry_run) {
    for (lv in mv_levels) {
      for (mv in lv$mvs) {
        log_msg(sprintf("[DRY RUN] Would refresh: %s (level %d)", mv, lv$level))
      }
    }
  } else {
    tryCatch({
      t0 <- proc.time()

      DBI::dbBegin(pg)
      DBI::dbExecute(pg, sprintf("SET LOCAL search_path TO %s, public;", SCHEMA))
      log_msg(sprintf("  search_path set to %s, public (within transaction)", SCHEMA))

      for (lv in mv_levels) {
        for (mv in lv$mvs) {
          mv_t0 <- proc.time()

          sql <- sprintf("REFRESH MATERIALIZED VIEW %s;", mv)
          DBI::dbExecute(pg, sql)

          cnt <- DBI::dbGetQuery(pg, sprintf("SELECT count(*) AS n FROM %s;", mv))$n
          mv_elapsed <- (proc.time() - mv_t0)["elapsed"]
          log_msg(sprintf("  [L%d] %s refreshed — %s rows (%.1fs)",
                          lv$level, mv, format(cnt, big.mark = ","), mv_elapsed))
        }
      }

      DBI::dbCommit(pg)

      elapsed <- (proc.time() - t0)["elapsed"]
      log_msg(sprintf("Phase 4 complete. All 10 MVs refreshed in %.1fs", elapsed))

    }, error = function(e) {
      log_msg(sprintf("Phase 4 FAILED on MV refresh: %s", conditionMessage(e)), "ERROR")
      try(DBI::dbRollback(pg), silent = TRUE)
      log_msg("  Transaction rolled back — MV state unchanged", "ERROR")
    })
  }

  # =========================================================================
  # Phase 5: Sub-Lineup Stats Refresh
  # =========================================================================

  log_msg("─── Phase 5: Sub-Lineup Stats Refresh ───")

  if (dry_run) {
    if (length(processed_ids)) {
      log_msg(sprintf(
        "[DRY RUN] Would call refresh_sub_lineups_stats_for_games() for %d game(s): %s",
        length(processed_ids), paste(processed_ids, collapse = ", ")
      ))
    } else {
      log_msg("[DRY RUN] Would call refresh_sub_lineups_stats()")
    }
  } else {
    tryCatch({
      t0 <- proc.time()

      before_cnt <- DBI::dbGetQuery(
        pg, sprintf('SELECT count(*) AS n FROM "%s"."sub_lineups_stats"', SCHEMA)
      )$n
      log_msg(sprintf("  sub_lineups_stats before: %s rows", format(before_cnt, big.mark = ",")))

      DBI::dbExecute(
        pg, sprintf('SET search_path TO %s, public;', SCHEMA)
      )

      # Prefer incremental refresh for processed game IDs when function exists.
      incr_exists <- DBI::dbGetQuery(
        pg,
        "SELECT EXISTS (
           SELECT 1
           FROM pg_proc p
           JOIN pg_namespace n ON n.oid = p.pronamespace
           WHERE n.nspname = $1
             AND p.proname = 'refresh_sub_lineups_stats_for_games'
         ) AS ok",
        params = list(SCHEMA)
      )$ok[[1]]

      if (length(processed_ids) && isTRUE(incr_exists)) {
        touched <- DBI::dbGetQuery(
          pg,
          "SELECT refresh_sub_lineups_stats_for_games($1::int4[]) AS n",
          params = list(as.integer(processed_ids))
        )$n[[1]]
        log_msg(sprintf(
          "  Used incremental refresh for %d game(s); touched %s sub-lineup rows",
          length(processed_ids), format(as.integer(touched), big.mark = ",")
        ))
      } else {
        if (length(processed_ids) && !isTRUE(incr_exists)) {
          log_msg("  Incremental refresh function not found; falling back to full refresh", "WARN")
        }
        DBI::dbExecute(pg, "SELECT refresh_sub_lineups_stats();")
        log_msg("  Used full refresh_sub_lineups_stats()")
      }

      after_cnt <- DBI::dbGetQuery(
        pg, sprintf('SELECT count(*) AS n FROM "%s"."sub_lineups_stats"', SCHEMA)
      )$n

      elapsed <- (proc.time() - t0)["elapsed"]
      log_msg(sprintf("  sub_lineups_stats after: %s rows (%.1fs)",
                      format(after_cnt, big.mark = ","), elapsed))
      delta <- after_cnt - before_cnt
      log_msg(sprintf("Phase 5 complete. Delta: %s%d rows",
                      if (delta >= 0) "+" else "", as.integer(delta)))

    }, error = function(e) {
      log_msg(sprintf("Phase 5 FAILED: %s — continuing to Phase 6", conditionMessage(e)), "ERROR")
    })
  }

  # =========================================================================
  # Phase 6: Validation (per-game row count checks)
  # =========================================================================

  log_msg("─── Phase 6: Validation ───")

  tryCatch({
    checks <- list(
      list(table = "actions_clean",   min_rows = 50),
      list(table = "full_rosters",    min_rows = 10),
      list(table = "possessions",     min_rows = 50),
      list(table = "lineups_lookup",  min_rows = 1),
      list(table = "pws",            min_rows = 50)
    )

    warn_count <- 0
    minute_floor_warn <- 39.0

    for (gid in processed_ids) {
      for (chk in checks) {
        cnt <- DBI::dbGetQuery(
          pg,
          sprintf('SELECT count(*) AS n FROM "%s"."%s" WHERE game_id = $1', SCHEMA, chk$table),
          params = list(as.integer(gid))
        )$n

        if (cnt < chk$min_rows) {
          log_msg(sprintf("  game %d: %s has %d rows (expected >= %d)",
                          gid, chk$table, cnt, chk$min_rows), "WARN")
          warn_count <- warn_count + 1
        }
      }

      # Team-minute integrity check (deduped timeline seconds):
      #  - warn if under 40 minutes
      #  - warn if over 40 minutes without OT
      minute_rows <- DBI::dbGetQuery(
        pg,
        sprintf(
          "WITH per_second AS (
             SELECT DISTINCT
               game_id,
               team_id,
               end_game_seconds_remaining
             FROM \"%s\".\"df_pts_poss_lineups_longer_mv\"
             WHERE game_id = $1
               AND type_lineup = 'offense'
               AND end_game_seconds_remaining IS NOT NULL
           ),
           stitched AS (
             SELECT
               game_id,
               team_id,
               end_game_seconds_remaining,
               LAG(end_game_seconds_remaining) OVER (
                 PARTITION BY game_id, team_id
                 ORDER BY end_game_seconds_remaining DESC
               ) AS prev_egr
             FROM per_second
           ),
           team_minutes AS (
             SELECT
               game_id,
               team_id,
               COALESCE(SUM(prev_egr - end_game_seconds_remaining), 0) / 60.0 AS minutes
             FROM stitched
             WHERE prev_egr IS NOT NULL
             GROUP BY game_id, team_id
           ),
           qtr AS (
             SELECT game_id, MAX(quarter)::int AS max_quarter
             FROM \"%s\".\"df_pts_poss_lineups_longer_mv\"
             WHERE game_id = $1
             GROUP BY game_id
           )
           SELECT
             tm.team_id,
             tm.minutes,
             COALESCE(qtr.max_quarter, 4) AS max_quarter,
             (COALESCE(qtr.max_quarter, 4) > 4) AS has_ot
           FROM team_minutes tm
           LEFT JOIN qtr USING (game_id)
           ORDER BY tm.team_id",
          SCHEMA, SCHEMA
        ),
        params = list(as.integer(gid))
      )

      if (!nrow(minute_rows)) {
        log_msg(sprintf("  game %d: minute integrity check has no team rows", gid), "WARN")
        warn_count <- warn_count + 1
      } else {
        for (i in seq_len(nrow(minute_rows))) {
          team_id_i <- minute_rows$team_id[[i]]
          mins_i    <- as.numeric(minute_rows$minutes[[i]])
          has_ot_i  <- isTRUE(minute_rows$has_ot[[i]])

          if (!is.finite(mins_i)) {
            log_msg(sprintf(
              "  game %d team %d: minute integrity check returned invalid minutes",
              gid, team_id_i
            ), "WARN")
            warn_count <- warn_count + 1
          } else if (mins_i < minute_floor_warn) {
            log_msg(sprintf(
              "  game %d team %d: minutes %.1f < %.1f",
              gid, team_id_i, mins_i, minute_floor_warn
            ), "WARN")
            warn_count <- warn_count + 1
          } else if (mins_i > 40 && !has_ot_i) {
            log_msg(sprintf(
              "  game %d team %d: minutes %.1f > 40.0 with no OT flag",
              gid, team_id_i, mins_i
            ), "WARN")
            warn_count <- warn_count + 1
          }
        }
      }
    }

    if (warn_count == 0) {
      log_msg(sprintf("  All %d games passed validation checks", length(processed_ids)))
    } else {
      log_msg(sprintf("  %d warning(s) across %d games", warn_count, length(processed_ids)), "WARN")
    }

    log_msg("Phase 6 complete")

  }, error = function(e) {
    log_msg(sprintf("Phase 6 FAILED: %s", conditionMessage(e)), "WARN")
  })

  }

  # =========================================================================
  # Summary
  # =========================================================================

  overall_elapsed <- (proc.time() - overall_start)["elapsed"]
  log_msg(sprintf("═══ ETL Full pipeline finished in %.1fs ═══", overall_elapsed))
  log_msg(sprintf("Log saved to: %s", logger$log_file))

  if (!dry_run) {
    tryCatch({
      set_last_success(pg, SCHEMA)
      log_msg("Recorded last_success timestamp in app_meta")
    }, error = function(e) {
      log_msg(sprintf("Failed to record last_success timestamp: %s", conditionMessage(e)), "WARN")
    })
  }

  invisible(list(
    game_ids = processed_ids,
    log_file = logger$log_file,
    dry_run  = dry_run
  ))
}
