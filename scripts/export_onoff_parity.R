# Read-only. Writes one row per difference between the stored on/off views and their readers for a
# season, so a change to possession ends can be compared against a baseline:
#
#   Rscript scripts/export_onoff_parity.R --year 2025 --out <file.csv>
#
# The same three comparisons as parity() in scripts/repair_onoff_d23.R, with its sample and tolerances:
#   summary                          onoff_default_mv vs onoff_compute(full season), on the players with
#                                    "ON Poss" and "OFF Poss" of 20 or more on each side;
#                                    tolerance .03 for pr_* columns, .1 for minutes, 1e-9 otherwise
#   four_factors_compute             player_advanced_stats_mv vs four_factors_compute(year), 1e-9
#   four_factors_dashboard_compute   player_advanced_stats_mv vs four_factors_dashboard_compute(year), 1e-9
# Columns: team_id, player_id, source, column, view_value, reader_value. A player held by one side only
# is a row with column "player set" and the other side's value NA; a value missing on one side only
# (the nightly pin counts it as an infinite gap) is a row with NA on that side.

args <- commandArgs(trailingOnly = TRUE)
arg_value <- function(flag) {
  i <- match(flag, args)
  if (is.na(i) || i == length(args)) NULL else args[[i + 1L]]
}
year <- as.integer(arg_value("--year"))
out <- arg_value("--out")
stopifnot("--year <season> --out <file.csv> are required" = length(year) == 1L && !is.na(year) && year >= 2025L && !is.null(out))

suppressPackageStartupMessages({ library(DBI); library(RPostgres) })
readRenviron("etl/.Renviron")
con <- dbConnect(Postgres(), host = Sys.getenv("PG_HOST"), port = 5432L, dbname = Sys.getenv("PG_DB"),
  user = Sys.getenv("PG_USER"), password = Sys.getenv("PG_PASS"), sslmode = Sys.getenv("PG_SSLMODE", "require"),
  connect_timeout = 15L, bigint = "numeric")
on.exit(dbDisconnect(con), add = TRUE)
invisible(dbExecute(con, "SET default_transaction_read_only = on"))
invisible(dbExecute(con, "SET search_path TO basketball_test, public"))
invisible(dbExecute(con, "SET statement_timeout = '120s'"))

# The nightly pin's own keying (app/tests/testthat/test-player-reader-paths-db.R), kept per player here.
keyed <- function(d) { d$.key <- paste(as.integer(d$team_id), as.integer(d$player_id)); d }

differences <- function(view, reader, source, tolerance) {
  a <- keyed(view); b <- keyed(reader)
  both <- intersect(a$.key, b$.key)
  x <- a[match(both, a$.key), ]; y <- b[match(both, b$.key), ]
  cols <- setdiff(intersect(names(a), names(b)), c(".key", "team_id", "player_id", "game_year", "Year"))
  cols <- Filter(function(cc) is.numeric(x[[cc]]) && is.numeric(y[[cc]]), cols)
  rows <- lapply(cols, function(cc) {
    u <- x[[cc]]; v <- y[[cc]]
    bad <- is.na(u) != is.na(v) | (!is.na(u) & !is.na(v) & abs(u - v) > tolerance(cc))
    if (!any(bad)) return(NULL)
    data.frame(team_id = x$team_id[bad], player_id = x$player_id[bad], source = source, column = cc,
               view_value = u[bad], reader_value = v[bad], stringsAsFactors = FALSE)
  })
  only <- function(d, keep, side) {
    if (!length(keep)) return(NULL)
    d <- d[d$.key %in% keep, ]
    data.frame(team_id = d$team_id, player_id = d$player_id, source = source, column = "player set",
               view_value = if (side == "view") 1 else NA_real_, reader_value = if (side == "reader") 1 else NA_real_,
               stringsAsFactors = FALSE)
  }
  rows <- c(rows, list(only(a, setdiff(a$.key, b$.key), "view"), only(b, setdiff(b$.key, a$.key), "reader")))
  rows <- Filter(Negate(is.null), rows)
  cat(sprintf("  %-32s %d players matched, %d view only, %d reader only, %d differences\n", source, length(both),
      length(setdiff(a$.key, b$.key)), length(setdiff(b$.key, a$.key)), sum(vapply(rows, nrow, 1L))))
  if (length(rows)) do.call(rbind, rows) else NULL
}

cat(sprintf("on/off parity, season %d\n", year))
view <- dbGetQuery(con, sprintf('SELECT * FROM onoff_default_mv WHERE "Year" = %d', year))
reader <- dbGetQuery(con, sprintf("SELECT * FROM onoff_compute('%d-09-01'::date, '%d-07-01'::date, NULL::text, 0, 0, -1e9, '%d')",
                                  year - 1L, year, year))
# which(): a row with a missing possession count is not sampled (indexing by NA would add an all-NA row).
sampled <- function(d) d[which(d[["ON Poss"]] >= 20 & d[["OFF Poss"]] >= 20), ]
summary_tol <- function(cc) if (grepl("^pr_", cc)) .03 else if (cc == "minutes") .1 else 1e-9
res <- list(differences(sampled(view), sampled(reader), "summary", summary_tol))
adv <- dbGetQuery(con, sprintf("SELECT * FROM player_advanced_stats_mv WHERE game_year = %d", year))
for (fn in c("four_factors_compute", "four_factors_dashboard_compute"))
  res[[fn]] <- differences(adv, dbGetQuery(con, sprintf("SELECT * FROM %s(%d)", fn, year)), fn, function(cc) 1e-9)

res <- Filter(Negate(is.null), res)
all_rows <- if (length(res)) do.call(rbind, res) else
  data.frame(team_id = integer(0), player_id = integer(0), source = character(0), column = character(0),
             view_value = numeric(0), reader_value = numeric(0))
all_rows <- all_rows[order(all_rows$source, all_rows$team_id, all_rows$player_id, all_rows$column), ]
utils::write.csv(all_rows, out, row.names = FALSE, na = "")
cat(sprintf("%d difference rows, %d teams, %d columns -> %s\n", nrow(all_rows), length(unique(all_rows$team_id)),
    length(unique(all_rows$column)), out))
