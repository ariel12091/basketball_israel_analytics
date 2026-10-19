# Coordinate-noise sensitivity for the "RA share predicts eFG/TS" result
# (shotgen_zone_regression.R). Provider coords are a ~0.15-unit click grid
# with two magnet cells (3.2% of shots) and dunk-calibrated error ~±1 unit.
# Variants of the SAME v2 rule (only the flagged ingredient changes):
#   A  baseline v2 (verified identical to classify_shot_zone)
#   B  magnet spots excluded (is_magnet_spot cells dropped from FGA)
#   C  dunk/allyhoop tags forced to RA (their true location is the rim)
#   D  rim radius 1.25 (true-FIBA RA)
#   E  rim radius 1.69 (calibration upper band)
#   F  B + C combined
# For each: team-season shares -> off_efg / off_ppp ~ ra + paint + c3 + ab3
# + factor(yr) [baseline mid]; report the RA-share coefficient.
suppressMessages({
  library(arrow); library(sf); library(dplyr)
  library(DBI); library(RPostgres)
})
source("scripts/analysis/fiba_court_zones.R")

out <- file.path(Sys.getenv("SCRATCH"), "shotgen_zone_sensitivity_out.txt")
sink(out)
options(width = 200)

acts <- read_parquet("exports/cold/actions_clean.parquet",
  col_select = c("game_id", "id", "type", "parameters_type", "parameters_points",
                 "parameters_made", "parameters_coord_x", "parameters_coord_y")) |>
  distinct(game_id, id, .keep_all = TRUE) |>
  filter(type == "shot", !is.na(parameters_coord_x)) |>
  mutate(x = parameters_coord_x / 100, y = parameters_coord_y / 100)

readRenviron("app/.Renviron")
con <- dbConnect(RPostgres::Postgres(),
  host=Sys.getenv("PG_HOST"), port=as.integer(Sys.getenv("PG_PORT")),
  dbname=Sys.getenv("PG_DB"), user=Sys.getenv("PG_USER"),
  password=Sys.getenv("PG_PASS"), sslmode=Sys.getenv("PG_SSLMODE"),
  connect_timeout=15L, bigint="numeric")
mv_shots <- dbGetQuery(con, "
  SELECT d.game_id, d.id, d.team_id, s.game_year AS yr
  FROM basketball_test.df_pts_poss_lineups_longer_mv d
  JOIN basketball_test.schedule s ON s.game_id = d.game_id
  WHERE d.type = 'shot' AND d.type_lineup = 'offense'")
outc <- dbGetQuery(con, "
  SELECT r.game_year AS yr, r.team_id, r.off_ppp, f.off_efg, f.off_ts
  FROM basketball_test.team_ppp_ratings_mv r
  JOIN basketball_test.team_four_factors_mv f
    ON f.game_year = r.game_year AND f.team_id = r.team_id")
dbDisconnect(con)

sz <- acts |> inner_join(mv_shots, by = c("game_id", "id"))

# v2 rule with the rim radius as a parameter; identical to classify_shot_zone
# at rim_r = rim_zone_radius (verified below)
classify_var <- function(x, y, points, rim_r) {
  r <- sqrt((x - cx)^2 + (y - hoop_center_y)^2)
  dplyr::case_when(
    points == 3 & y <= corner3_height ~ "c3",
    points == 3                       ~ "ab3",
    r <= rim_r                                     ~ "ra",
    abs(x - cx) <= key_width / 2 & y <= key_height ~ "paint_nonra",
    .default = "mid"
  )
}
base_committed <- classify_shot_zone(sz$x, sz$y, sz$parameters_points)
base_committed <- dplyr::case_when(
  base_committed %in% c("Left Corner 3", "Right Corner 3") ~ "c3",
  base_committed == "Above Break 3" ~ "ab3",
  base_committed == "Rim (r<=1.45)" ~ "ra",
  base_committed == "In The Paint (Non-Rim)" ~ "paint_nonra",
  .default = "mid")
cat("== INVARIANT: variant classifier vs committed classify_shot_zone ==\n")
cat("mismatches at rim_r = 1.45:",
    sum(base_committed != classify_var(sz$x, sz$y, sz$parameters_points,
                                       rim_zone_radius)), "(expect 0)\n\n")

run_variant <- function(dd, label) {
  tz <- dd |>
    group_by(yr, team_id) |>
    summarise(fga = n(),
              ra_share  = 100 * sum(zg == "ra") / n(),
              paint_share = 100 * sum(zg == "paint_nonra") / n(),
              mid_share = 100 * sum(zg == "mid") / n(),
              c3_share  = 100 * sum(zg == "c3") / n(),
              ab3_share = 100 * sum(zg == "ab3") / n(), .groups = "drop") |>
    inner_join(outc, by = c("yr", "team_id"))
  row <- c()
  for (o in c("off_efg", "off_ppp")) {
    m <- summary(lm(reformulate(c("ra_share", "paint_share", "c3_share",
                                  "ab3_share", "factor(yr)"), o), tz))
    row <- c(row, round(coef(m)["ra_share", c(1, 4)], 3))
  }
  data.frame(variant = label, lg_ra_share = round(100 * sum(dd$zg == "ra") / nrow(dd), 1),
             efg_coef = row[1], efg_p = row[2], ppp_coef = row[3], ppp_p = row[4])
}

sz$zg <- classify_var(sz$x, sz$y, sz$parameters_points, rim_zone_radius)
res <- run_variant(sz, "A v2 baseline (r=1.45)")

szB <- sz |> filter(!is_magnet_spot(x, y))
res <- rbind(res, run_variant(szB, "B magnet spots excluded"))

szC <- sz |> mutate(zg = ifelse(parameters_type %in% c("dunk", "allyhoop"), "ra", zg))
res <- rbind(res, run_variant(szC, "C dunks forced to RA"))

szD <- sz |> mutate(zg = classify_var(x, y, parameters_points, 1.25))
res <- rbind(res, run_variant(szD, "D rim radius 1.25"))

szE <- sz |> mutate(zg = classify_var(x, y, parameters_points, 1.69))
res <- rbind(res, run_variant(szE, "E rim radius 1.69"))

szF <- szB |> mutate(zg = ifelse(parameters_type %in% c("dunk", "allyhoop"), "ra", zg))
res <- rbind(res, run_variant(szF, "F magnet-excl + dunks->RA"))

cat("== RA-SHARE COEFFICIENT ACROSS VARIANTS (team level, n=28) ==\n")
cat("(coef = per +1pp RA share, mid baseline; league RA share for context)\n\n")
print(res, row.names = FALSE)

cat("\n== magnet-cell composition (who do we drop in B?) ==\n")
mg <- sz |> filter(is_magnet_spot(x, y))
cat("magnet shots:", nrow(mg), sprintf("(%.1f%% of all)", 100 * nrow(mg) / nrow(sz)), "\n")
print(mg |> count(parameters_type, sort = TRUE) |> as.data.frame(), row.names = FALSE)
cat("\nmagnet share by team-season (top 6 — operator habit check):\n")
print(sz |> group_by(yr, team_id) |>
  summarise(magnet_pct = round(100 * mean(is_magnet_spot(x, y)), 1), .groups = "drop") |>
  arrange(desc(magnet_pct)) |> head(6) |> as.data.frame(), row.names = FALSE)

sink()
cat("done\n")
