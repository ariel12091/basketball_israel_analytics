# Read-only verification for the ETL-refreshed Team Hub / Compare ratings cache.

if (file.exists("etl/.Renviron")) readRenviron("etl/.Renviron")

suppressPackageStartupMessages({
  library(DBI)
  library(RPostgres)
})

db_config <- function(default_port) {
  list(
    host = Sys.getenv("PG_HOST"),
    port = as.integer(Sys.getenv("PG_PORT", as.character(default_port))),
    dbname = Sys.getenv("PG_DB"),
    user = Sys.getenv("PG_USER"),
    password = Sys.getenv("PG_PASS"),
    sslmode = Sys.getenv("PG_SSLMODE", "require")
  )
}

connect_with <- function(config, port = config$port) {
  dbConnect(
    Postgres(),
    host = config$host,
    port = as.integer(port),
    dbname = config$dbname,
    user = config$user,
    password = config$password,
    sslmode = config$sslmode,
    connect_timeout = 15L
  )
}

etl_config <- db_config(5432L)
etl_con <- connect_with(etl_config, port = 5432L)
on.exit(dbDisconnect(etl_con), add = TRUE)

cache <- dbGetQuery(
  etl_con,
  "SELECT *
     FROM basketball_test.team_ratings_preset_cache
    ORDER BY game_year, preset_variant, team_id"
)
if (!nrow(cache)) stop("team_ratings_preset_cache is empty")

variant_calls <- c(
  starters_hi = paste0(
    "SELECT * FROM basketball_test.get_team_ratings_dynamic(",
    "%d, p_num_starters_off_min := 3, p_num_starters_off_max := 5)"
  ),
  starters_lo = paste0(
    "SELECT * FROM basketball_test.get_team_ratings_dynamic(",
    "%d, p_num_starters_off_min := 0, p_num_starters_off_max := 2)"
  ),
  clutch = paste0(
    "SELECT * FROM basketball_test.get_team_ratings_dynamic(",
    "%d, p_max_margin := 5, p_max_time_remaining := 300)"
  ),
  last10 = paste0(
    "SELECT * FROM basketball_test.get_team_ratings_dynamic(",
    "%d, p_last_n_games := 10)"
  ),
  top4 = paste0(
    "SELECT * FROM basketball_test.get_team_ratings_dynamic(",
    "%d, p_opp_rank_side := 'top', p_opp_rank_n := 4, ",
    "p_opp_rank_metric := 'net')"
  ),
  bottom4 = paste0(
    "SELECT * FROM basketball_test.get_team_ratings_dynamic(",
    "%d, p_opp_rank_side := 'bottom', p_opp_rank_n := 4, ",
    "p_opp_rank_metric := 'net')"
  )
)

rating_cols <- setdiff(names(cache), c("preset_variant", "refreshed_at"))
mismatches <- character(0)
for (year in sort(unique(cache$game_year))) {
  for (variant in names(variant_calls)) {
    expected <- dbGetQuery(etl_con, sprintf(variant_calls[[variant]], year))
    expected <- expected[order(expected$team_id), rating_cols, drop = FALSE]
    actual <- cache[
      cache$game_year == year & cache$preset_variant == variant,
      rating_cols,
      drop = FALSE
    ]
    actual <- actual[order(actual$team_id), , drop = FALSE]
    rownames(expected) <- NULL
    rownames(actual) <- NULL
    comparison <- all.equal(
      actual,
      expected,
      check.attributes = FALSE,
      tolerance = 1e-9
    )
    if (!isTRUE(comparison)) {
      mismatches <- c(
        mismatches,
        sprintf("%d/%s: %s", year, variant, paste(comparison, collapse = "; "))
      )
    }
  }
}
if (length(mismatches)) {
  stop("Cache parity failed:\n", paste(mismatches, collapse = "\n"))
}

duplicate_groups <- dbGetQuery(
  etl_con,
  "SELECT count(*)::int AS n
     FROM (
       SELECT game_year, preset_variant, team_id
       FROM basketball_test.team_ratings_preset_cache
       GROUP BY 1,2,3
       HAVING count(*) > 1
     ) d"
)$n[[1]]
if (duplicate_groups != 0L) stop("Cache contains duplicate keys")

print(
  dbGetQuery(
    etl_con,
    "SELECT game_year, preset_variant, count(*)::int AS rows
       FROM basketball_test.team_ratings_preset_cache
      GROUP BY 1,2
      ORDER BY 1,2"
  )
)
message("Dynamic parity: OK")

if (!file.exists("app/.Renviron")) stop("app/.Renviron is required for role verification")
readRenviron("app/.Renviron")
app_config <- db_config(6543L)
app_con <- connect_with(app_config)
on.exit(dbDisconnect(app_con), add = TRUE)

privileges <- dbGetQuery(
  app_con,
  "SELECT
     current_user AS role,
     has_table_privilege(
       current_user,
       'basketball_test.team_ratings_preset_cache',
       'SELECT'
     ) AS can_select,
     has_table_privilege(
       current_user,
       'basketball_test.team_ratings_preset_cache',
       'INSERT'
     ) AS can_insert,
     has_table_privilege(
       current_user,
       'basketball_test.team_ratings_preset_cache',
       'UPDATE'
     ) AS can_update,
     has_table_privilege(
       current_user,
       'basketball_test.team_ratings_preset_cache',
       'DELETE'
     ) AS can_delete,
     has_function_privilege(
       current_user,
       'basketball_test.refresh_team_ratings_preset_cache_for_games(integer[])',
       'EXECUTE'
     ) AS can_refresh"
)
print(privileges)
if (!isTRUE(privileges$can_select[[1]]) ||
    isTRUE(privileges$can_insert[[1]]) ||
    isTRUE(privileges$can_update[[1]]) ||
    isTRUE(privileges$can_delete[[1]]) ||
    isTRUE(privileges$can_refresh[[1]])) {
  stop("app_readonly privilege verification failed")
}

selected_year <- max(cache$game_year)
read_times <- replicate(
  5,
  system.time(
    dbGetQuery(
      app_con,
      "SELECT *
         FROM basketball_test.team_ratings_preset_cache
        WHERE game_year = $1::int4
        ORDER BY preset_variant, rank_net_rtg",
      params = list(as.integer(selected_year))
    )
  )[["elapsed"]]
)
message(
  sprintf(
    "App-role cache read (%d, %d rows): median %.3fs; runs %s",
    selected_year,
    sum(cache$game_year == selected_year),
    stats::median(read_times),
    paste(sprintf("%.3fs", read_times), collapse = ", ")
  )
)
