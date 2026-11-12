# Add linked_basket_player_ids / basket_person_id to basket_player_registration
# (sql/migrations/2026-11-12_basket_registration_links.sql) and fill the links
# from the cached profile pages -- no requests. basket_person_id is left NULL
# for etl/basket_person_ids.R.
#
# Gates: every registration with a cached profile gets an array, and every
# link whose two ends are registrations joins the same Hebrew name and date of
# birth.
#
# Dry run by default: one transaction, every gate, then ROLLBACK.
# CONFIRM_BASKET_LINKS=1 commits.

confirm_apply <- identical(Sys.getenv("CONFIRM_BASKET_LINKS", "0"), "1")
source("scripts/backfill_basket_shooting_history.R")
readRenviron("etl/.Renviron")
con <- DBI::dbConnect(RPostgres::Postgres(), host = Sys.getenv("PG_HOST"), port = 5432L,
  dbname = Sys.getenv("PG_DB"), user = Sys.getenv("PG_USER"),
  password = Sys.getenv("PG_PASS"), sslmode = "require", bigint = "numeric")
on.exit(DBI::dbDisconnect(con))
q <- function(sql) DBI::dbGetQuery(con, sql)
gate <- function(ok, what) {
  cat(if (ok) "ok  " else "FAIL", what, "\n")
  if (!ok) stop(what)
}
cache_dir <- "exports/cache/basket_shooting_history"

cat("mode:", if (confirm_apply) "APPLY (commits)" else "DRY RUN (rolls back)", "\n")
DBI::dbExecute(con, "BEGIN")
ok <- FALSE
tryCatch({
  DBI::dbExecute(con, "SET LOCAL lock_timeout = '5s'")
  DBI::dbExecute(con, "SET LOCAL statement_timeout = '120s'")
  DBI::dbExecute(con, paste(readLines("sql/migrations/2026-11-12_basket_registration_links.sql",
    warn = FALSE, encoding = "UTF-8"), collapse = "\n"), immediate = TRUE)

  regs <- q(paste("SELECT game_year, basket_player_id, source FROM", BASKET_REGISTRATION_TABLE))
  regs$path <- profile_cache_path(cache_dir, regs$game_year, regs$basket_player_id)
  cached <- regs[file.exists(regs$path), ]
  cached$linked_basket_player_ids <- lapply(seq_len(nrow(cached)), function(i)
    parse_basket_player_links(read_downloaded_html(cached$path[i]), cached$basket_player_id[i]))
  written <- write_basket_links(con, cached)
  gate(written == nrow(cached), sprintf("%d of %d cached profiles written", written, nrow(cached)))

  bad <- q(paste(
    "SELECT r.game_year, r.basket_player_id, r.player_name, r.date_of_birth,",
    "       o.game_year AS other_year, o.basket_player_id AS other_id, o.player_name AS other_name,",
    "       o.date_of_birth AS other_dob",
    "FROM", BASKET_REGISTRATION_TABLE, "r",
    "CROSS JOIN LATERAL unnest(r.linked_basket_player_ids) AS l(id)",
    "JOIN", BASKET_REGISTRATION_TABLE, "o ON o.basket_player_id = l.id",
    "WHERE r.player_name IS DISTINCT FROM o.player_name OR r.date_of_birth IS DISTINCT FROM o.date_of_birth"))
  joined <- q(paste(
    "SELECT count(*)::integer AS n FROM", BASKET_REGISTRATION_TABLE, "r",
    "CROSS JOIN LATERAL unnest(r.linked_basket_player_ids) AS l(id)",
    "JOIN", BASKET_REGISTRATION_TABLE, "o ON o.basket_player_id = l.id"))$n
  if (nrow(bad)) print(bad)
  gate(nrow(bad) == 0, sprintf("all %d links between registrations join the same name and date of birth", joined))

  print(q(paste(
    "SELECT game_year, source, count(*) FILTER (WHERE cardinality(linked_basket_player_ids) > 0) AS with_links,",
    "count(*) FILTER (WHERE cardinality(linked_basket_player_ids) = 0) AS no_links,",
    "count(*) FILTER (WHERE linked_basket_player_ids IS NULL) AS unread",
    "FROM", BASKET_REGISTRATION_TABLE, "GROUP BY 1, 2 ORDER BY 1, 2")))
  ok <- TRUE
}, error = function(e) cat("FAILED:", conditionMessage(e), "\n"))

if (ok && confirm_apply) { DBI::dbExecute(con, "COMMIT"); cat("COMMITTED\n") } else { DBI::dbExecute(con, "ROLLBACK"); cat("ROLLED BACK\n") }
if (!ok) quit(status = 1)
