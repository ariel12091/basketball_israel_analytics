# Pins sql/migrations/2026-11-03_and_one_possession_targets.sql against the and-one rule in
# compute_possessions() (etl/possession_rules.R). Read-only: duckdb in memory plus SELECTs on
# PostgreSQL (port 5432); no table, no temp table, no write.
#
#   Rscript scripts/pin_and_one_targets.R
#
# The previous rule is read from the fixed commit below (the special-foul rule, before the and-one
# change), so "the ends the new rule removed" has a fixed reference. Three passes:
#   1. fixtures: previous rule on duckdb -> rows the SQL reads (its rows CTE swapped for VALUES) ->
#      its targets must equal old ends minus new ends, and the new rule must add none.
#   2. archive: the same comparison on exports/cold/actions_clean.parquet, 2026 and 2027 games, with the
#      targets of the real SQL on the stored view for the same games.
#   3. PostgreSQL: the new rule itself run on PostgreSQL (the ETL's engine) on the fixtures and on one
#      full archived 2027 game, as dplyr::tbl(con, sql("... VALUES ...")): equal to duckdb. A game over
#      60 seconds is a stop (the ETL role's statement limit is 2 minutes).
# Exits 1 on any difference.

OLD_RULE_SHA <- "dfd4667"

suppressPackageStartupMessages({ library(dplyr); library(dbplyr); library(DBI); library(RPostgres) })
say <- function(...) cat(sprintf(...), "\n", sep = "")
failures <- character(0)
fail <- function(...) { failures <<- c(failures, sprintf(...)); say("  FAIL: %s", failures[[length(failures)]]) }
secs <- function(t0) proc.time()[["elapsed"]] - t0

source("etl/possession_rules.R")
new_rule <- compute_possessions
old_file <- tempfile(fileext = ".R")
writeLines(system2("git", c("show", paste0(OLD_RULE_SHA, ":etl/possession_rules.R")), stdout = TRUE), old_file)
old_env <- new.env(parent = globalenv())
sys.source(old_file, envir = old_env)
old_rule <- old_env$compute_possessions
say("Previous rule: %s (%s)", OLD_RULE_SHA, system2("git", c("rev-parse", OLD_RULE_SHA), stdout = TRUE))
source("etl/tests/helper-possession-fixtures.R")

readRenviron("etl/.Renviron")
pg <- dbConnect(Postgres(), host = Sys.getenv("PG_HOST"), port = 5432L, dbname = Sys.getenv("PG_DB"),
  user = Sys.getenv("PG_USER"), password = Sys.getenv("PG_PASS"),
  sslmode = Sys.getenv("PG_SSLMODE", "require"), bigint = "numeric")
on.exit(dbDisconnect(pg), add = TRUE)
dbExecute(pg, "SET default_transaction_read_only = on")
duck <- DBI::dbConnect(duckdb::duckdb())
on.exit(DBI::dbDisconnect(duck, shutdown = TRUE), add = TRUE)

RULE_COLS <- c(game_id = "int4", id = "int4", parent_action_id = "int4", quarter = "int4",
  end_game_seconds_remaining = "float8", user_time = "text", team_id = "int4", type = "text",
  parameters_type = "text", parameters_made = "text", parameters_points = "int4",
  parameters_free_throw_number = "int4", parameters_free_throws_awarded = "int4")

# Ids whose final_end_poss is TRUE.
end_ids <- function(out) sort(out$id[out$final_end_poss %in% TRUE])
key <- function(game_id, id) paste(game_id, id)

duck_rule <- function(rule, rows) {
  nm <- paste0("a", sample.int(1e9, 1))
  dplyr::copy_to(duck, rows, nm, overwrite = TRUE)
  dplyr::collect(rule(dplyr::tbl(duck, nm)))
}

# A SELECT ... FROM (VALUES ...) with every cell cast, so NULLs carry a type.
values_sql <- function(rows, cols) {
  cell <- function(v, type) {
    if (is.na(v)) return(sprintf("NULL::%s", type))
    if (type == "text") sprintf("'%s'::text", gsub("'", "''", v, fixed = TRUE))
    else if (type == "bool") sprintf("%s::bool", if (isTRUE(v)) "TRUE" else "FALSE")
    else sprintf("%s::%s", format(v, scientific = FALSE, digits = 15), type)
  }
  lines <- vapply(seq_len(nrow(rows)), function(i) {
    paste0("(", paste(vapply(names(cols), function(nm) cell(rows[[nm]][[i]], cols[[nm]]), ""), collapse = ", "), ")")
  }, "")
  sprintf("SELECT * FROM (VALUES %s) AS v(%s)", paste(lines, collapse = ",\n"), paste(names(cols), collapse = ", "))
}

pg_rule <- function(rule, rows) {
  sql <- values_sql(rows[names(RULE_COLS)], RULE_COLS)
  dplyr::collect(rule(dplyr::tbl(pg, dbplyr::sql(sql))))
}

# The targets SQL with its rows CTE replaced by the given rows (as the view would hold them).
TARGETS_SQL <- paste(readLines("sql/migrations/2026-11-03_and_one_possession_targets.sql"), collapse = "\n")
VIEW_COLS <- c(game_id = "int4", id = "int4", parent_action_id = "int4", quarter = "int4", type = "text",
  parameters_type = "text", parameters_made = "text", team_id = "int4", final_end_poss = "bool")
targets_on_rows <- function(view_rows) {
  swapped <- sub("-- rows:begin.*-- rows:end",
    sprintf("WITH rows AS (%s),", values_sql(view_rows[names(VIEW_COLS)], VIEW_COLS)), TARGETS_SQL)
  stopifnot(!identical(swapped, TARGETS_SQL), !grepl("ANY($1", swapped, fixed = TRUE))
  dbGetQuery(pg, swapped)
}
view_rows_from <- function(rows, old_out) {
  r <- as.data.frame(rows)
  r$final_end_poss <- key(r$game_id, r$id) %in% key(old_out$game_id, old_out$id[old_out$final_end_poss %in% TRUE])
  r
}

compare <- function(label, old_ids, new_ids, target_ids) {
  removed <- setdiff(old_ids, new_ids); added <- setdiff(new_ids, old_ids)
  ok <- identical(sort(removed), sort(target_ids)) && length(added) == 0L
  say("  %-34s removed %-4d added %-3d SQL targets %-4d %s", label, length(removed), length(added),
      length(target_ids), if (ok) "ok" else "DIFFERENT")
  if (!ok) fail("%s: removed {%s} added {%s} targets {%s}", label, paste(removed, collapse = ","),
                paste(added, collapse = ","), paste(target_ids, collapse = ","))
}

say("Pass 1: fixtures (duckdb; SQL on PostgreSQL VALUES)")
dev_ends <- list()
for (nm in names(POSSESSION_FIXTURES)) {
  fx <- POSSESSION_FIXTURES[[nm]]
  old_out <- duck_rule(old_rule, fx$rows); new_out <- duck_rule(new_rule, fx$rows)
  dev_ends[[nm]] <- end_ids(new_out)
  compare(nm, end_ids(old_out), end_ids(new_out), targets_on_rows(view_rows_from(fx$rows, old_out))$id)
  if (!identical(end_ids(new_out), fx$ends)) fail("%s: new rule ends differ from the fixture's expectation", nm)
}

say("Pass 2: archive (duckdb, 2026 and 2027 games)")
games <- dbGetQuery(pg, "SELECT DISTINCT game_id, game_year FROM basketball_test.final_schedule_mv WHERE game_year IN (2026, 2027)")
parq <- "exports/cold/actions_clean.parquet"
in_archive <- DBI::dbGetQuery(duck, sprintf("SELECT DISTINCT game_id FROM read_parquet('%s')", parq))$game_id
games <- games[games$game_id %in% in_archive, ]
say("  %d archived games (%s)", nrow(games), paste(sprintf("%d: %d", as.integer(names(table(games$game_year))), table(games$game_year)), collapse = ", "))
arch_sql <- sprintf("SELECT %s FROM read_parquet('%s') WHERE game_id IN (%s)",
  paste(names(RULE_COLS), collapse = ", "), parq, paste(games$game_id, collapse = ","))
arch <- DBI::dbGetQuery(duck, arch_sql)
t0 <- proc.time()[["elapsed"]]
old_a <- duck_rule(old_rule, arch); new_a <- duck_rule(new_rule, arch)
say("  both rules on %d rows: %.1fs (%.2fs per game for the pair)", nrow(arch), secs(t0), secs(t0) / nrow(games))
removed <- setdiff(key(old_a$game_id, old_a$id)[old_a$final_end_poss %in% TRUE], key(new_a$game_id, new_a$id)[new_a$final_end_poss %in% TRUE])
added <- setdiff(key(new_a$game_id, new_a$id)[new_a$final_end_poss %in% TRUE], key(old_a$game_id, old_a$id)[old_a$final_end_poss %in% TRUE])
tg <- dbGetQuery(pg, TARGETS_SQL, params = list("{2026,2027}"))
tg <- tg[tg$game_id %in% games$game_id, ]
tkeys <- key(tg$game_id, tg$id)
say("  ends removed %d, added %d, SQL targets in these games %d (2026: %d, 2027: %d)", length(removed), length(added), length(tkeys),
    sum(tg$game_id %in% games$game_id[games$game_year == 2026]), sum(tg$game_id %in% games$game_id[games$game_year == 2027]))
if (length(added)) fail("archive: the new rule added %d ends", length(added))
if (!setequal(removed, tkeys)) fail("archive: removed minus targets {%s}, targets minus removed {%s}",
  paste(setdiff(removed, tkeys), collapse = ","), paste(setdiff(tkeys, removed), collapse = ","))
if (setequal(removed, tkeys) && !length(added)) say("  archive pass: removed set equals the SQL targets, none added")

say("Pass 3: the new rule on PostgreSQL")
t0 <- proc.time()[["elapsed"]]
for (nm in names(POSSESSION_FIXTURES)) {
  fx <- POSSESSION_FIXTURES[[nm]]
  got <- end_ids(pg_rule(new_rule, fx$rows))
  if (!identical(got, fx$ends) || !identical(got, dev_ends[[nm]])) fail("PostgreSQL fixture %s: ends {%s}, expected {%s}", nm, paste(got, collapse = ","), paste(fx$ends, collapse = ","))
}
say("  %d fixtures on PostgreSQL: %s (%.1fs)", length(POSSESSION_FIXTURES),
    if (!length(failures)) "all match" else "see FAIL lines", secs(t0))
g <- tg$game_id[tg$game_id %in% games$game_id[games$game_year == 2027]][1]
rows_g <- arch[arch$game_id == g, ]
t0 <- proc.time()[["elapsed"]]
pg_g <- pg_rule(new_rule, rows_g)
took <- secs(t0)
duck_ids <- end_ids(new_a[new_a$game_id == g, ])
say("  full 2027 game %d (%d rows, holds %d targets): %d ends on PostgreSQL, %d on duckdb, %.1fs", g, nrow(rows_g),
    sum(tg$game_id == g), length(end_ids(pg_g)), length(duck_ids), took)
if (!identical(end_ids(pg_g), duck_ids)) fail("PostgreSQL ends differ from duckdb in game %d", g)
if (took > 60) fail("game %d took %.1fs on PostgreSQL (over 60s; the ETL limit is 120s)", g, took)

if (length(failures)) { say("PIN FAILED (%d)", length(failures)); quit(status = 1L) }
say("PIN OK")
