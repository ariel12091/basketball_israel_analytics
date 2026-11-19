# Player headshots in the database (docs/shared_definitions.md, "Player
# height, position and headshot"; 2026-11-19): basket_player_registration
# gets photo_url, and player_display_names_v appends it as its last column, so
# the web app reads Israeli headshots as it reads heights (it used to read
# web/src/data/images.json, from scripts/export_web_images.R).
#
#   Rscript scripts/apply_basket_registration_photo.R
#
# 1. ALTER TABLE ... ADD COLUMN photo_url (a basket.co.il URL or NULL); ALTER
#    keeps the table's grants.
# 2. The view, rebuilt from its LIVE definition with exact edits (as
#    scripts/apply_player_display_names_size.R did): a `photo` CTE (verified
#    registrations, one photo per person-season) joined on identity and
#    season only -- not on team, unlike the names and heights, because a
#    headshot is the person's: a player who changed teams has it on both
#    (images.json did the same; joined on team, 27 such 2025-26 rows had
#    none). photo_url is appended last; CREATE OR REPLACE VIEW keeps
#    app_readonly's grant.
# 3. The backfill: every cached team page and profile of every season
#    (basket_cached_photos(), scripts/basket_rosters.R), written by
#    write_basket_photos(). The twice-weekly Basket refresh keeps it current.
#
# Dry run by default: one transaction, every gate, then ROLLBACK.
# CONFIRM_BASKET_PHOTO=1 commits. Either way the DDL that ran is written to
# sql/migrations/2026-11-19_basket_registration_photo.sql.
#
# Gates: the view's existing columns are unchanged (row fingerprint);
# app_readonly keeps SELECT; per season the view has at least 95% as many
# players with a photo as images.json had (applied 2026-11-19: 247 / 293 /
# 215 against 247 / 293 / 213, every URL equal; images.json was then deleted,
# so a re-run skips this gate).

source("scripts/basket_rosters.R")
confirm_apply <- identical(Sys.getenv("CONFIRM_BASKET_PHOTO", "0"), "1")
readRenviron(Sys.getenv("ETL_RENVIRON", "etl/.Renviron"))
suppressPackageStartupMessages({ library(DBI); library(RPostgres) })
con <- dbConnect(Postgres(), host = Sys.getenv("PG_HOST"), port = 5432L,
  dbname = Sys.getenv("PG_DB"), user = Sys.getenv("PG_USER"),
  password = Sys.getenv("PG_PASS"), sslmode = "require", bigint = "numeric")
q <- function(sql) dbGetQuery(con, sql)
x <- function(sql) invisible(dbExecute(con, sql))
view <- "basketball_test.player_display_names_v"
cache_dir <- Sys.getenv("BASKET_CACHE_DIR", file.path("exports", "cache", "basket_shooting_history"))
cols <- function() q(sprintf(
  "SELECT attname FROM pg_attribute WHERE attrelid = '%s'::regclass AND attnum > 0 AND NOT attisdropped ORDER BY attnum", view))$attname
fingerprint <- function(c) q(sprintf(
  "SELECT count(*) AS n, md5(string_agg(md5(row(%s)::text), '' ORDER BY game_year, team_id, player_id)) AS h FROM %s",
  paste(c, collapse = ","), view))

# images.json's photo count per season: the bar the backfill must reach.
json_counts <- local({
  path <- file.path("web", "src", "data", "images.json")
  if (!file.exists(path)) return(NULL)
  photos <- jsonlite::fromJSON(path, simplifyVector = FALSE)$photos
  vapply(photos, length, integer(1))
})

cat("mode:", if (confirm_apply) "APPLY (commits)" else "DRY RUN (rolls back)", "\n")
x("BEGIN")
ok <- FALSE
tryCatch({
  x("SET LOCAL lock_timeout = '5s'")
  x("SET LOCAL statement_timeout = '120s'")
  before <- cols()
  if ("photo_url" %in% before) stop("view already has photo_url")
  fp_before <- fingerprint(before)
  def <- q(sprintf("SELECT pg_get_viewdef('%s'::regclass, true) AS d", view))$d

  edit <- function(text, pattern, replacement, what) {
    hits <- gregexpr(pattern, text, perl = TRUE)[[1]]
    if (length(hits) != 1 || hits[1] < 0) stop("expected exactly one match for ", what)
    sub(pattern, replacement, text, perl = TRUE)
  }
  def <- edit(def,
    "(GROUP BY b\\.game_year, d\\.team_id_rosters, b\\.identity_id\\s*\\))(\\s+SELECT p\\.game_year)",
    paste0("\\1, photo AS (\n         SELECT r.game_year,\n            r.identity_id,\n",
           "            min(r.photo_url) AS photo_url\n",
           "           FROM basketball_test.basket_player_registration r\n",
           "          WHERE r.identity_match_status = 'verified'::text AND r.photo_url IS NOT NULL\n",
           "          GROUP BY r.game_year, r.identity_id\n        )\\2"),
    "the end of the basket CTE")
  def <- edit(def,
    "(END AS position_en)(\\s+FROM pairs p)",
    "\\1,\n        CASE\n            WHEN p.identities = 1 THEN ph.photo_url\n            ELSE NULL::text\n        END AS photo_url\\2",
    "the final select")
  def <- edit(def,
    "(\\s+LEFT JOIN basketball_test\\.player_identities pi)",
    "\n     LEFT JOIN photo ph ON p.identities = 1 AND ph.game_year = p.game_year AND ph.identity_id = p.identity_id\\1",
    "the identity join")
  alter <- paste(
    "ALTER TABLE basketball_test.basket_player_registration",
    "ADD COLUMN photo_url text",
    "CONSTRAINT basket_player_registration_photo_url_check",
    "CHECK (photo_url LIKE 'https://basket.co.il/%');")
  comment <- paste(
    "COMMENT ON COLUMN basketball_test.basket_player_registration.photo_url IS",
    "'Headshot: the team-page roster card, else the profile page (scripts/basket_rosters.R write_basket_photos). Never blanked by a refresh.';")
  ddl <- paste0("CREATE OR REPLACE VIEW ", view, " AS\n", sub(";\\s*$", "", def), ";\n")
  writeLines(c(
    "-- Generated by scripts/apply_basket_registration_photo.R (2026-11-19):",
    "-- basket_player_registration.photo_url, appended to player_display_names_v",
    "-- (rebuilt from its live definition) as its last column.",
    "", alter, comment, "", ddl), "sql/migrations/2026-11-19_basket_registration_photo.sql", useBytes = TRUE)
  x(alter)
  x(comment)
  x(ddl)

  after <- cols()
  cat("columns before:", length(before), " after:", length(after), "\n")
  if (!identical(after, c(before, "photo_url"))) stop("columns are not a pure append: ", paste(after, collapse = ","))
  fp_after <- fingerprint(before)
  if (!identical(fp_before$n, fp_after$n) || !identical(fp_before$h, fp_after$h))
    stop("the existing columns changed (row fingerprint differs)")
  cat("existing columns unchanged: row fingerprint identical,", fp_after$n, "rows\n")
  grant <- q(sprintf("SELECT has_table_privilege('app_readonly', '%s', 'SELECT') AS g", view))$g
  if (!isTRUE(grant)) stop("app_readonly lost SELECT on the view")
  cat("app_readonly SELECT: TRUE\n")

  seasons <- q("SELECT DISTINCT game_year FROM basketball_test.basket_player_registration ORDER BY 1")$game_year
  photos <- basket_cached_photos(cache_dir, seasons)
  cat(sprintf("cached photos: %d (%d team cards, %d profiles)\n", nrow(photos),
              sum(photos$photo_source == "roster"), sum(photos$photo_source == "profile")))
  set <- write_basket_photos(con, photos)
  cat("registrations given a photo:", set, "\n")

  cov <- q(sprintf(paste(
    "SELECT game_year, count(*) FILTER (WHERE display_name IS NOT NULL) AS players,",
    "count(photo_url) AS with_photo FROM %s GROUP BY 1 ORDER BY 1"), view))
  cov$images_json <- if (is.null(json_counts)) NA_integer_ else
    unname(json_counts[as.character(cov$game_year)])
  print(cov, row.names = FALSE)
  short <- cov[!is.na(cov$images_json) & cov$with_photo < 0.95 * cov$images_json, , drop = FALSE]
  if (nrow(short)) stop("fewer photos than images.json in season(s) ", paste(short$game_year, collapse = ", "))
  ok <- TRUE
}, error = function(e) cat("FAILED:", conditionMessage(e), "\n"))

if (ok && confirm_apply) { x("COMMIT"); cat("COMMITTED\n") } else { x("ROLLBACK"); cat("ROLLED BACK\n") }
dbDisconnect(con)
if (!ok) quit(status = 1)
