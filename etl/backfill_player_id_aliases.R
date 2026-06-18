# Scoped retroactive rebuild for canonical player_id aliases.
#
# Usage:
#   Sys.setenv(APP_ENV = "test")
#   source("etl/backfill_player_id_aliases.R")
#   backfill_player_id_aliases(dry_run = TRUE)   # inspect affected game_ids
#   backfill_player_id_aliases(dry_run = FALSE)  # rebuild affected game_ids, incremental stats refresh

backfill_player_id_aliases <- function(
    dry_run = TRUE,
    seed_defaults = TRUE,
    game_ids = NULL,
    force_full_sub_lineup_stats = FALSE
) {
  env_file <- file.path("etl", ".Renviron")
  if (file.exists(env_file)) readRenviron(env_file)

  suppressPackageStartupMessages({
    library(DBI)
    library(RPostgres)
    library(dplyr)
    library(tibble)
  })

  source("etl/player_id_aliases.R")
  source("etl/player_identity_dictionary.R")

  app_env <- Sys.getenv("APP_ENV", "test")
  schema <- if (identical(app_env, "prod")) "basketball" else "basketball_test"

  pg <- DBI::dbConnect(
    drv = RPostgres::Postgres(),
    host = Sys.getenv("PG_HOST"),
    port = as.integer(Sys.getenv("PG_PORT", "6543")),
    dbname = Sys.getenv("PG_DB"),
    user = Sys.getenv("PG_USER"),
    password = Sys.getenv("PG_PASS"),
    sslmode = Sys.getenv("PG_SSLMODE", "require")
  )
  on.exit({
    if (DBI::dbIsValid(pg)) DBI::dbDisconnect(pg)
  }, add = TRUE)

  ensure_player_id_corrections_tables(pg, schema)
  if (isTRUE(seed_defaults)) {
    n_seeded <- seed_default_player_id_aliases(pg, schema)
    n_override_seeded <- seed_default_player_id_game_overrides(pg, schema)
    message(sprintf("Seeded/updated %d default season alias mapping(s) in %s.player_id_aliases", n_seeded, schema))
    message(sprintf("Seeded/updated %d default game override mapping(s) in %s.player_id_game_overrides", n_override_seeded, schema))
  }
  identity_counts <- sync_player_identity_dictionary(pg, schema)
  message(sprintf(
    "Identity dictionary synced: %d identities, %d active mappings, %d active corrections",
    identity_counts$identities[[1]],
    identity_counts$active_mappings[[1]],
    identity_counts$active_corrections[[1]]
  ))

  aliases <- load_player_id_aliases(pg, schema)
  if (!nrow(aliases)) {
    message(sprintf("No active player ID corrections found in %s", schema))
    return(invisible(list(game_ids = integer(0), dry_run = dry_run)))
  }

  affected_ids <- if (is.null(game_ids)) {
    affected_player_alias_game_ids(pg, schema)
  } else {
    sort(unique(as.integer(game_ids)))
  }
  affected_ids <- affected_ids[is.finite(affected_ids)]

  message(sprintf("Active player ID corrections: %d", nrow(aliases)))
  message(sprintf("Affected games: %d", length(affected_ids)))
  if (length(affected_ids)) {
    message(paste(affected_ids, collapse = ", "))
  }

  if (!length(affected_ids)) {
    return(invisible(list(game_ids = affected_ids, dry_run = dry_run)))
  }

  # Close this connection before etl_full opens its own managed connection.
  DBI::dbDisconnect(pg)

  source("etl/etl_full.R")
  result <- etl_full(
    game_ids = affected_ids,
    dry_run = dry_run,
    force_full_sub_lineup_stats = force_full_sub_lineup_stats
  )

  invisible(list(
    game_ids = affected_ids,
    dry_run = dry_run,
    etl_result = result
  ))
}

if (identical(Sys.getenv("RUN_PLAYER_ALIAS_BACKFILL"), "1")) {
  dry_run_env <- tolower(Sys.getenv("DRY_RUN", "true"))
  backfill_player_id_aliases(dry_run = !(dry_run_env %in% c("false", "0", "no")))
}
