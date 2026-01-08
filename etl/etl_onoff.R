# =========================
# pgfig / pgnection
# =========================
library(DBI); library(RPostgres)
library(dplyr); library(dbplyr); library(tidyr)
library(purrr); library(jsonlite); library(lubridate)
library(janitor); library(digest); library(stringr)
library(pool)

pg <- DBI::dbConnect(drv = Postgres(), 
                     host = Sys.getenv("PG_HOST"),
                     port = as.integer(Sys.getenv("PG_PORT", "6543")),  # pooled port recommended
                     dbname = Sys.getenv("PG_DB"),
                     user = Sys.getenv("PG_USER"),
                     password = Sys.getenv("PG_PASS"),
                     sslmode = Sys.getenv("PG_SSLMODE", "require"))

APP_ENV     <- Sys.getenv("APP_ENV", "test")     # "prod" or "test"
PROD_SCHEMA <- "basketball"
TEST_SCHEMA <- "basketball_test"
SCHEMA      <- if (APP_ENV == "prod") PROD_SCHEMA else TEST_SCHEMA

pg <- dbPool(
  drv = Postgres(),
  host = Sys.getenv("PG_HOST"),
  port = as.integer(Sys.getenv("PG_PORT", "6543")),
  dbname = Sys.getenv("PG_DB"),
  user = Sys.getenv("PG_USER"),
  password = Sys.getenv("PG_PASS"),
  sslmode = Sys.getenv("PG_SSLMODE", "require"),
  minSize = 0, maxSize = as.integer(Sys.getenv("POOL_MAX", "3")),
  idleTimeout = 15000
)
DBI::dbExecute(pg, paste0("SET search_path TO ", DBI::dbQuoteIdentifier(pg, "SCHEMA"), ", public;"))

# =========================
# Schema discovery & cloning (tables + constraints + indexes)
# =========================
table_exists <- function(pg, schema, table) {
  nrow(DBI::dbGetQuery(pg,
                       "SELECT 1 FROM information_schema.tables WHERE table_schema=$1 AND table_name=$2",
                       params = list(schema, table)
  )) > 0
}
get_table_cols <- function(pg, schema, table) {
  DBI::dbGetQuery(pg,
                  "SELECT column_name
       FROM information_schema.columns
      WHERE table_schema=$1 AND table_name=$2
      AND is_generated = 'NEVER'
      ORDER BY ordinal_position",
                  params = list(schema, table)
  )$column_name
}
get_pk_cols <- function(pg, schema, table) {
  DBI::dbGetQuery(pg,
                  "SELECT kcu.column_name
       FROM information_schema.table_constraints tc
       JOIN information_schema.key_column_usage kcu
         ON tc.constraint_name = kcu.constraint_name
        AND tc.table_schema     = kcu.table_schema
      WHERE tc.constraint_type = 'PRIMARY KEY'
        AND tc.table_schema=$1
        AND tc.table_name=$2
      ORDER BY kcu.ordinal_position",
                  params = list(schema, table)
  )$column_name
}
ensure_schema <- function(pg, schema) {
  DBI::dbExecute(pg, sprintf('CREATE SCHEMA IF NOT EXISTS "%s";', schema))
}
list_base_tables <- function(pg, schema) {
  DBI::dbGetQuery(pg, "
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = $1
      AND table_type   = 'BASE TABLE'
    ORDER BY table_name;", params = list(schema)
  )$table_name
}

# Create table in target schema by cloning structure from source (no data)
clone_table_like <- function(pg, from_schema, to_schema, table) {
  if (!table_exists(pg, from_schema, table)) return(invisible(FALSE))
  if (table_exists(pg, to_schema, table))   return(invisible(TRUE))
  DBI::dbExecute(pg, sprintf(
    'CREATE TABLE "%s"."%s" (LIKE "%s"."%s" INCLUDING ALL);',
    to_schema, table, from_schema, table
  ))
  TRUE
}

# Copy PK/UNIQUE/CHECK/FK constraints from source -> target (skip if already present)
clone_constraints_from <- function(pg, from_schema, to_schema, table) {
  src <- DBI::dbGetQuery(pg, "
    SELECT c.pgname, pg_get_constraintdef(c.oid) AS pgdef, c.pgtype
    FROM pg_constraint c
    JOIN pg_namespace n ON n.oid = c.pgnamespace
    JOIN pg_class     cl ON cl.oid = c.pgrelid
    WHERE n.nspname = $1 AND cl.relname = $2
    ORDER BY c.pgtype DESC, c.pgname;",
                         params = list(from_schema, table)
  )
  if (!nrow(src)) return(invisible())
  
  tgt_existing <- DBI::dbGetQuery(pg, "
    SELECT c.pgname
    FROM pg_constraint c
    JOIN pg_namespace n ON n.oid = c.pgnamespace
    JOIN pg_class     cl ON cl.oid = c.pgrelid
    WHERE n.nspname = $1 AND cl.relname = $2;",
                                  params = list(to_schema, table)
  )$pgname
  
  for (i in seq_len(nrow(src))) {
    pgname <- src$pgname[i]
    pgdef  <- src$pgdef[i]
    if (pgname %in% tgt_existing) next
    # Recreate constraint on target
    sql <- sprintf('ALTER TABLE "%s"."%s" ADD constraint %s %s;',
                   to_schema, table, pgname, pgdef)
    DBI::dbExecute(pg, sql)
  }
  invisible()
}

# Copy indexes from source schema to target schema for given table
clone_indexes_from <- function(pg, from_schema, to_schema, table) {
  idx <- DBI::dbGetQuery(pg, "
    SELECT indexname, indexdef
    FROM pg_indexes
    WHERE schemaname = $1 AND tablename = $2;",
                         params = list(from_schema, table)
  )
  if (!nrow(idx)) return(invisible())
  for (i in seq_len(nrow(idx))) {
    def <- idx$indexdef[i]
    # rewrite ON schema.table to target schema.table (quoted or not)
    def2 <- sub(
      sprintf("\\bON\\s+(\"?%s\"?)\\.((\"?%s\"?))\\b", from_schema, table),
      sprintf('ON "%s"."%s"', to_schema, table),
      def, perl = TRUE
    )
    # make creation idempotent
    def2 <- sub("^CREATE UNIQUE INDEX", "CREATE UNIQUE INDEX IF NOT EXISTS", def2)
    def2 <- sub("^CREATE INDEX",        "CREATE INDEX IF NOT EXISTS",        def2)
    DBI::dbExecute(pg, def2)
  }
  invisible()
}

# Bootstrap test schema from prod (all current tables + constraints + indexes)
bootstrap_test_schema <- function(pg, prod_schema, test_schema, copy_indexes = TRUE) {
  ensure_schema(pg, test_schema)
  tbls <- list_base_tables(pg, prod_schema)
  for (t in tbls) {
    created <- clone_table_like(pg, prod_schema, test_schema, t)
    # Always ensure constraints and indexes parity (in case prod changed later)
    clone_constraints_from(pg, prod_schema, test_schema, t)
    if (copy_indexes) clone_indexes_from(pg, prod_schema, test_schema, t)
  }
}

if (APP_ENV != "prod") bootstrap_test_schema(pg, PROD_SCHEMA, TEST_SCHEMA, copy_indexes = TRUE)

# Ensure a table exists in current SCHEMA; if missing in test, clone from prod (with constraints + indexes)
ensure_table_like_from_prod <- function(pg, schema, prod_schema, table, clone_indexes = TRUE) {
  if (table_exists(pg, schema, table)) return(invisible(FALSE))  # nothing created now
  if (schema == prod_schema) stop(sprintf("Missing %s.%s in prod.", schema, table))
  if (!table_exists(pg, prod_schema, table)) {
    stop(sprintf("Source table %s.%s does not exist in prod.", prod_schema, table))
  }
  clone_table_like(pg, prod_schema, schema, table)
  clone_constraints_from(pg, prod_schema, schema, table)
  if (clone_indexes) clone_indexes_from(pg, prod_schema, schema, table)
  invisible(TRUE)  # created now
}

# =========================
# Stage-and-upsert (schema-driven; needs PK)
# =========================
upsert_by_like <- function(pg, schema = SCHEMA, table, df) {
  stopifnot(is.data.frame(df))
  # auto-create missing test table (and constraints + indexes) from prod
  ensure_table_like_from_prod(pg, schema, TEST_SCHEMA, table, clone_indexes = TRUE)
  
  if (!table_exists(pg, schema, table)) stop(sprintf("Target %s.%s does not exist.", schema, table))
  cols <- get_table_cols(pg, schema, table); if (!length(cols)) stop("No columns found.")
  pk   <- get_pk_cols(pg, schema, table);   if (!length(pk))   stop(sprintf("No PRIMARY KEY on %s.%s", schema, table))
  
  # Align df to DB columns (add missing as NA, drop extras, reorder)
  missing <- setdiff(cols, names(df)); if (length(missing)) df[missing] <- NA
  df <- df[, cols, drop = FALSE]
  
  stage <- sprintf("__stage_%s_%s", table, as.integer(as.numeric(Sys.time())))
  q_stage  <- DBI::SQL(sprintf('"%s"."%s"', schema, stage))
  q_target <- DBI::SQL(sprintf('"%s"."%s"', schema, table))
  
  DBI::dbExecute(pg, sprintf(
    'CREATE TABLE "%s"."%s" (LIKE "%s"."%s" INCLUDING ALL);',
    schema, stage, schema, table
  ))
  DBI::dbWriteTable(pg, name = q_stage, value = df, append = TRUE, row.names = FALSE)
  
  cols_csv    <- paste(sprintf('"%s"', cols), collapse = ", ")
  pk_csv      <- paste(sprintf('"%s"', pk),   collapse = ", ")
  updates_csv <- paste(sprintf('"%s" = EXCLUDED."%s"', setdiff(cols, pk), setdiff(cols, pk)), collapse = ", ")
  
  sql <- sprintf("
    INSERT INTO %s (%s)
    SELECT %s FROM %s
    ON CONFLICT (%s) DO UPDATE SET %s;",
                 q_target, cols_csv, cols_csv, q_stage, pk_csv, updates_csv
  )
  
  DBI::dbExecute(pg, "BEGIN;")
  tryCatch({
    DBI::dbExecute(pg, sql)
    DBI::dbExecute(pg, sprintf('DROP TABLE %s;', q_stage))
    DBI::dbExecute(pg, "COMMIT;")
  }, error = function(e) {
    DBI::dbExecute(pg, "ROLLBACK;")
    try(DBI::dbExecute(pg, sprintf('DROP TABLE IF EXISTS %s;', q_stage)), silent = TRUE)
    stop(e)
  })
}

# =========================
# Migrations (safe, additive)
# =========================
run_migrations <- function(pg, schema = SCHEMA) {
  DBI::dbExecute(pg, sprintf('ALTER TABLE "%s"."schedule" ADD COLUMN IF NOT EXISTS season TEXT;', schema))
  DBI::dbExecute(pg, sprintf('ALTER TABLE "%s"."schedule" ADD COLUMN IF NOT EXISTS competition TEXT;', schema))
  DBI::dbExecute(pg, sprintf('CREATE INDEX IF NOT EXISTS idx_%s_schedule_season_comp ON "%s"."schedule"(season, competition);', schema, schema))
}
run_migrations(pg, SCHEMA)

# =========================
# Fetch & normalize JSON (archived link per your request)
# =========================
fetch_israel_schedule <- function() {
  raw <- jsonlite::read_json("https://basket.co.il/pbp/json/games_all.json?1752150490942")
  sched <- purrr::map(raw[[1]]$games, as.data.frame) |> list_rbind()
  sched |>
    mutate(
      game_id   = as.integer(ExternalID),
      game_date = suppressWarnings(lubridate::dmy(game_date_txt)),
      pbp_url   = paste0("https://stats.segevstats.com/realtimestat_heb/get_team_action.php?game_id=", ExternalID),
      box_url   = paste0("https://stats.segevstats.com/realtimestat_heb/get_team_score.php?game_id=", ExternalID)
    )
}
fetch_game_pbp <- function(game_id, pbp_url) {
  jsonlite::read_json(pbp_url, simplifyVector = TRUE)
}



clean_actions <- function(pbp) {
  a <- tibble::as_tibble(pbp$result$actions) |>
    tidyr::unnest(parameters, names_sep = "_") |>
    janitor::clean_names()
  
  game_id_val <- pbp$result$gameInfo$gameId
  
  a |>
    filter(type != "clock") |>
    mutate(
      game_id = as.integer(game_id_val),
      end_quarter_seconds_remaining = lubridate::period_to_seconds(lubridate::ms(quarter_time)),
      end_game_seconds_remaining = dplyr::if_else(
        quarter < 5,
        end_quarter_seconds_remaining + (4 - quarter) * 600,
        as.numeric(end_quarter_seconds_remaining)
      ),
      parameters_points = dplyr::if_else(
        parameters_made == "made" & type == "freeThrow", 1L,
        dplyr::coalesce(parameters_points, NA_integer_)
      ),
      team_score = dplyr::if_else(parameters_made == "made", parameters_points, NA_integer_),
      id = as.integer(id),
      parent_action_id = dplyr::if_else(parent_action_id == 0L, id, parent_action_id),
      team_id = as.integer(team_id),
      player_id = as.integer(player_id),
      end_game_seconds_remaining = as.numeric(end_game_seconds_remaining)
    ) %>%
    group_by(game_id, team_id, id) %>%
    mutate(row_num_user = row_number(user_time)) %>%
    filter(row_num_user == 1)
}




extract_roster <- function(pbp) {
  gi <- pbp$result$gameInfo
  if (is.null(gi$homeTeam) || is.null(gi$awayTeam)) return(tibble::tibble())
  home <- tibble::as_tibble(gi$homeTeam$players) |>
    mutate(team_id = gi$homeTeam$id, game_id = gi$gameId,
           team_name = gi$homeTeam$name, team_name_local = gi$homeTeam$nameLocal)
  away <- tibble::as_tibble(gi$awayTeam$players) |>
    mutate(team_id = gi$awayTeam$id, game_id = gi$gameId,
           team_name = gi$awayTeam$name, team_name_local = gi$awayTeam$nameLocal)
  dplyr::bind_rows(away, home) |>
    rename(player_id = id) |>
    mutate(across(c(game_id, team_id, player_id), as.integer)) |>
    distinct(game_id, team_id, player_id, .keep_all = TRUE)
}

# =========================
# Core computations (no transmute)
# =========================
compute_possessions <- function(actions_tbl) {
  poss0 <- actions_tbl |>
    mutate(
      pct_ft = round(parameters_free_throw_number / NULLIF(parameters_free_throws_awarded, 0), 2),
      team_score = case_when(parameters_made == "made" ~ parameters_points, TRUE ~ NULL)
    ) |>
    mutate(q_bucket = if_else(quarter < 5, 0L, quarter)) %>%
    group_by(game_id) |>
    dbplyr::window_order(id) |>
    ungroup()
  
  base <- poss0 |>
    group_by(game_id, parent_action_id) |>
    dbplyr::window_order(id, quarter, desc(end_game_seconds_remaining), user_time) |>
    mutate(
      next_type      = lead(type),
      next_paramtype = lead(parameters_type),
      end_poss = case_when(
        type == "shot"      & parameters_made == "made"             ~ TRUE,
        next_paramtype == "defensive" & next_type == "rebound"      ~ TRUE,
        type == "freeThrow" & parameters_made == "made" & pct_ft==1 ~ TRUE,
        type == "turnover"                                           ~ TRUE,
        TRUE                                                         ~ FALSE
      ),
      sum_poss_poss = sum(if_else(end_poss, 1L, 0L),                            na.rm = TRUE),
      sum_block     = sum(if_else(parameters_made == "blocked",   1L, 0L),      na.rm = TRUE),
      sum_tech      = sum(if_else(parameters_type == "technical",  1L, 0L),      na.rm = TRUE),
      final_end_poss_base = case_when(
        sum_poss_poss >= 2 & id == parent_action_id ~ NULL,
        sum_block >= 1                               ~ lead(end_poss),
        sum_tech  >= 1                               ~ NULL,
        TRUE ~ end_poss
      )
    ) |>
    ungroup() |>
    select(-next_type, -next_paramtype)
  
  eoq_targets <- poss0 |>
    group_by(game_id, quarter) |>
    dbplyr::window_order(id) |>
    mutate(
      next_paramtype = lead(parameters_type),
      prev_id        = lag(id),
      prev_parent    = lag(parent_action_id),
      id_to_mark = case_when(
        next_paramtype == "end-of-quarter" & type == "shot"    & parameters_made == "missed"    ~ id,
        next_paramtype == "end-of-quarter" & type == "rebound" & parameters_type == "offensive" ~ id,
        next_paramtype == "end-of-quarter" & type == "block" & prev_parent == parent_action_id  ~ prev_id,
        TRUE ~ NA_integer_
      )
    ) |>
    filter(!is.na(id_to_mark)) |>
    distinct(game_id, id = id_to_mark) |>
    mutate(eoq_override = TRUE) |>
    ungroup()
  
  base |>
    left_join(eoq_targets, by = c("game_id", "id")) |>
    mutate(final_end_poss = case_when(
      eoq_override ~ TRUE,
      TRUE         ~ final_end_poss_base
    )) |>
    select(-eoq_override, -final_end_poss_base)
}

poss_stage %>%
  #filter(game_id %in% sample(poss_stage$game_id, 1)) %>%
  arrange(id) %>%
  relocate(c(type, final_end_poss), .after = parameters_type) %>%
  group_by(game_id, team_id) %>%
  summarise(total_pts = sum(team_score, na.rm = TRUE),
            total_poss = sum(final_end_poss, na.rm  = TRUE ),
            ppp = total_pts / total_poss) %>%
  arrange(game_id) %>%
  left_join(full_rosters %>% collect() %>%
              distinct(team_id, game_id, team_name), by = c("team_id", "game_id")) %>%
  filter(total_poss > 0) %>%
  ggplot(aes(x=ppp)) + geom_histogram()
view()



full_rosters <- tbl(pg, dbplyr::in_schema(SCHEMA, "full_rosters"))

schedule <- tbl(pg, dbplyr::in_schema(SCHEMA, "schedule"))


subs_df <- actions_df %>%
  filter(type == "substitution")

compute_lineups_lookup <- function(pg) {
  actions <- tbl(pg, in_schema(SCHEMA, "actions_clean")) |> filter(game_id %in% sched_subset$game_id)
  subs <- actions |> filter(type == "substitution") |>    mutate(parameters_player_in = if_else(!is.na(parameters_player_in), player_id, NA), 
                                                                 parameters_player_out = if_else(!is.na(parameters_player_out), player_id, NA))
  full_rosters <- tbl(pg, dbplyr::in_schema(SCHEMA, "full_rosters"))
  
  
  full_rosters |>
    distinct(player_id, game_id, team_id, game_year) |>
    inner_join(subs, by = c("team_id", "game_id")) |>
    mutate(
      is_on = case_when(
        player_id.x == parameters_player_in  ~ 1,
        player_id.x == parameters_player_out ~ 0,
        TRUE ~ NA_real_
      )
    ) |>
    group_by(game_id, player_id.x) |>
    dbplyr::window_order(id, quarter, desc(end_game_seconds_remaining), user_time) |>
    fill(is_on, .direction = "down") |>
    mutate(.ord = row_number()) |>
    ungroup() |>
    slice_max(
      order_by = .ord, n = 1, with_ties = FALSE,
      by = c(game_id, quarter, quarter_time, end_game_seconds_remaining,
             player_id.x, team_id)
    ) |>
    rename(player_id = player_id.x) |>
    mutate(
      is_on_verdict = is_on) |>
    
    mutate(
      lineup_id = dbplyr::sql("
    string_agg(
      CASE WHEN is_on_verdict = 1 THEN player_id::text END,
      '_'
    ) OVER (
      PARTITION BY game_id, team_id, quarter, end_game_seconds_remaining
      ORDER BY player_id::bigint
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )
  "),
      n_on = dbplyr::sql("
    sum(CASE WHEN is_on_verdict = 1 THEN 1 ELSE 0 END)
    OVER (
      PARTITION BY game_id, team_id, quarter, id
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )
  ")
    ) |>
    mutate(lineup_hash = dbplyr::sql("md5(lineup_id)"))
}



compute_lineup_dims <- function(pg) {
  ll <- tbl(pg, in_schema(SCHEMA, "lineups_lookup"))
  list(
    dim     = ll |> distinct(lineup_hash, lineup_id),
    players = ll |> filter(is_on_verdict == TRUE) |> distinct(lineup_hash, player_id)
  )
}

compute_stints <- function(pg) {
  poss <- tbl(pg, in_schema(SCHEMA, "possessions"))
  ll   <- tbl(pg, in_schema(SCHEMA, "lineups_lookup"))
  
  lineups_segments <- ll |>
    distinct(id, game_id, team_id, quarter, quarter_time,
             end_game_seconds_remaining, lineup_id, lineup_hash) |>
    group_by(game_id, team_id) |>
    dbplyr::window_order(quarter, desc(end_game_seconds_remaining), id) |>
    #arrange(quarter, desc(end_game_seconds_remaining), id) |>
    left_join(
      poss |>
        select(id, game_id, quarter) |>
        group_by(quarter, game_id) |>
        summarise(last_id = max(id), .groups = "drop") |>
        filter(quarter >= 4) |>
        ungroup(),
      by = c("game_id", "quarter") 
    ) |>
    mutate(
      start_segment = end_game_seconds_remaining,
      end_segment   = case_when(lead(quarter) > quarter & lead(quarter) >= 5 ~ 0,
                                TRUE ~ coalesce(lead(end_game_seconds_remaining), 0)),
      start_id = id,
      end_id   = coalesce(lead(id), last_id)
    ) |>
    ungroup() |>
    select(-last_id)
  
  stints_stage <- lineups_segments |>
    ungroup() |>
    dbplyr::window_order(quarter, desc(start_segment)) |>
    #arrange(quarter, desc(start_segment)) |>
    inner_join(lineups_segments, by = c("game_id"), suffix = c("_offense","_defense")) |>
    filter(team_id_offense != team_id_defense,
           (quarter_offense < 5 & quarter_defense < 5) |
             (quarter_offense >= 5 & quarter_defense >= 5 & quarter_offense == quarter_defense)) |>
    mutate(
      final_start_seg = pmin(start_segment_offense, start_segment_defense),
      final_end_seg   = pmax(end_segment_offense, end_segment_defense),
      final_start_id  = pmax(start_id_offense, start_id_defense),
      final_end_id    = pmin(end_id_offense, end_id_defense),
      quarter_min     = pmin(quarter_offense, quarter_defense),
      q_bucket        = if_else(quarter_min < 5, 0L, quarter_min)
    ) |>
    filter(final_end_id > final_start_id) |>
    group_by(game_id, final_start_id, final_end_id) |>
    mutate(segment_id = dbplyr::sql("
      DENSE_RANK() OVER (PARTITION BY game_id ORDER BY final_start_id, final_end_id)
    ")) |>
    ungroup()
}

# =========================
# Orchestrator
# =========================
etl_update <- function(game_ids = NULL, season = NULL, competition = NULL) {
  # schedule (with optional season / competition)
  sched_df <- fetch_israel_schedule() %>%
    filter(score_team1 > 0)
  
  
  if (!is.null(season))      sched_df$season <- season
  if (!is.null(competition)) sched_df$competition <- competition
  if (!"season" %in% names(sched_df))      sched_df$season <- NA_character_
  if (!"competition" %in% names(sched_df)) sched_df$competition <- NA_character_
  upsert_by_like(pg, SCHEMA, "schedule", sched_subset)
  
  # which games to process
  if (is.null(game_ids)) {
    existing <- dbGetQuery(pg, sprintf('SELECT DISTINCT game_id FROM "%s"."actions_clean"', SCHEMA))
    ids <- setdiff(sched_df$game_id, existing$game_id) |> sort() |> unique()
  } else {
    ids <- sort(unique(as.integer(game_id)))
  }
  if (!length(ids)) { message("Nothing to do."); return(invisible()) }
  
  DBI::dbExecute(pg, "BEGIN;")
  on.exit(try(DBI::dbExecute(pg, "ROLLBACK;"), silent = TRUE), add = TRUE)
  
  
  # fetch PBP JSON
  sched_subset <- dplyr::semi_join(sched_df, tibble::tibble(game_id = ids), by = "game_id")
  pbps <- purrr::map2(sched_subset$game_id, sched_subset$pbp_url, fetch_game_pbp)
  
  pbps <- purrr::map2(new_games$game_id, new_games$pbp_link, fetch_game_pbp)
  
  # actions_clean
  actions_df <- purrr::map(pbps, clean_actions) |> list_rbind()
  
  
  upsert_by_like(pg, SCHEMA, "actions_clean", actions_df)
  
  
  # subs
  subs_df <- actions_df %>% filter(type == "substitution") |> 
    mutate(parameters_player_in = if_else(!is.na(parameters_player_in), player_id, NA), 
           parameters_player_out = if_else(!is.na(parameters_player_out), player_id, NA))
  upsert_by_like(pg, SCHEMA, "subs", subs_df)
  
  subs_df %>%
    select(parameters_player_in)
  # full_rosters
  roster_df <- purrr::map(pbps, extract_roster) |> list_rbind() |> rename_with(tolower) %>%
    mutate(game_year = 2026)
  
  upsert_by_like(pg, SCHEMA, "full_rosters", roster_df)
  
  # possessions
  actions_tbl <- tbl(pg, dbplyr::in_schema(SCHEMA, "actions_clean")) |> filter(game_id %in% sched_subset$game_id)
  poss_stage  <- compute_possessions(actions_tbl) |> collect() |> rename(quarter = quarter.x) |> 
    select(-quarter.y) 
  
  upsert_by_like(pg, SCHEMA, "possessions", poss_stage)
  
  # lineups_lookup
  df_lineups_df <- compute_lineups_lookup(pg) |> filter(game_id %in% sched_subset$game_id) |> collect()
  upsert_by_like(pg, SCHEMA, "lineups_lookup", df_lineups_df)
  
  
  # lineup_dim / lineup_players for affected
  affected_hashes <- unique(na.omit(df_lineups_df$lineup_hash))
  if (length(affected_hashes)) {
    dims <- compute_lineup_dims(pg)
    dim_df     <- dims$dim    |> collect() |> filter(lineup_hash %in% affected_hashes)
    players_df <- dims$players |> collect() |> filter(lineup_hash %in% affected_hashes)
    if (nrow(dim_df))     upsert_by_like(pg, SCHEMA, "lineup_dim",     dim_df)
    if (nrow(players_df)) upsert_by_like(pg, SCHEMA, "lineup_players", players_df)
    
    # sub-lineups (2/3/4)
    compute_sub_lineups_from_lineups_lookup(pg, affected_hashes, ks = c(2,3,4))
  }
  
  # stints
  stints_df <- compute_stints(pg) |> filter(game_id %in% sched_subset$game_id) |> collect() %>% 
    select(team_id_offense, game_id, final_start_seg, final_end_seg,
           segment_id, lineup_hash_offense, lineup_hash_defense, team_id_defense, q_bucket,
           final_start_id, final_end_id) %>%
    rename(team_id = team_id_offense)
  
  upsert_by_like(pg, SCHEMA, "stints", stints_df)
  
  by <- join_by(team_id, game_id, q_bucket, between(id, final_start_id, final_end_id, bounds = "[)"))
  
  pws_stage <- left_join(poss_stage %>%
                           mutate(q_bucket = if_else(quarter < 5, 0L, quarter)), stints_df, by)
  
  
  upsert_by_like(pg, SCHEMA, "pws", pws_stage)
  
  pws_stage %>%
    select(game_id)
  
  sched_subset$game_id
  
  # ppp long
  ppp_df <- compute_ppp_long(pg) |> filter(game_id %in% ids) |> collect()
  upsert_by_like(pg, SCHEMA, "df_pts_poss_lineups_longer", ppp_df)
  
  DBI::dbExecute(pg, "ANALYZE;")
  DBI::dbExecute(pg, "COMMIT;")
  on.exit(NULL, add = FALSE)
  message(sprintf("✅ %s ETL done for games: %s", toupper(APP_ENV), paste(ids, collapse = ", ")))
}

# =========================
# Usage
# =========================
Sys.setenv(APP_ENV = "test"); etl_update()
# Sys.setenv(APP_ENV = "prod"); etl_update()
#etl_update(c(62436), season = "2024/25", competition = "Israeli League")
