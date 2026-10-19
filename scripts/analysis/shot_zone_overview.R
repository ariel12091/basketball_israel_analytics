# Comprehensive shot overview by court zone: joins parquet coordinates+zones
# to the live MV (game_id, id), league zone table, corner-3 vs above-break-3.
suppressMessages({
  library(arrow); library(sf); library(dplyr)
  library(DBI); library(RPostgres)
})
source("scripts/analysis/fiba_court_zones.R")

out <- file.path(Sys.getenv("SCRATCH"), "shot_zone_overview_out.txt")
sink(out)
options(width = 220)

acts <- read_parquet(
  "exports/cold/actions_clean.parquet",
  col_select = c("game_id", "id", "parent_action_id", "type", "parameters_type",
                 "parameters_points", "parameters_made",
                 "parameters_coord_x", "parameters_coord_y")
) |> distinct(game_id, id, .keep_all = TRUE)

shots <- acts |> filter(type == "shot")
assisted_keys <- acts |>
  filter(type == "assist", !is.na(parent_action_id)) |>
  distinct(game_id, parent_action_id) |>
  rename(id = parent_action_id) |>
  mutate(assisted = TRUE)

readRenviron("app/.Renviron")
con <- dbConnect(RPostgres::Postgres(),
  host=Sys.getenv("PG_HOST"), port=as.integer(Sys.getenv("PG_PORT")),
  dbname=Sys.getenv("PG_DB"), user=Sys.getenv("PG_USER"),
  password=Sys.getenv("PG_PASS"), sslmode=Sys.getenv("PG_SSLMODE"),
  connect_timeout=15L, bigint="numeric")
yr_map <- dbGetQuery(con, "SELECT DISTINCT game_id, game_year FROM basketball_test.schedule")
mv_shots <- dbGetQuery(con, "
  SELECT game_id, id FROM basketball_test.df_pts_poss_lineups_longer_mv
  WHERE type = 'shot' AND type_lineup = 'offense'")
dbDisconnect(con)

cat("== 1. PARQUET <-> MV JOIN CHECK (key: game_id, id) ==\n")
mv_shots$in_mv <- TRUE
m <- full_join(shots |> select(game_id, id) |> mutate(in_pq = TRUE),
               mv_shots, by = c("game_id", "id"))
cat("parquet shots:", sum(!is.na(m$in_pq)),
    " MV shots:", sum(!is.na(m$in_mv)),
    " matched:", sum(!is.na(m$in_pq) & !is.na(m$in_mv)), "\n")
only_mv <- m |> filter(is.na(in_pq)) |> count(game_id, sort = TRUE)
cat("MV-only shots come from", nrow(only_mv), "games; top:\n")
print(head(as.data.frame(only_mv), 5), row.names = FALSE)

# zone classification (v2: points-first, radial rim disc)
sc <- shots |>
  filter(!is.na(parameters_coord_x), !is.na(parameters_coord_y)) |>
  mutate(x = parameters_coord_x / 100, y = parameters_coord_y / 100,
         shot_zone_basic = classify_shot_zone(x, y, parameters_points)) |>
  left_join(yr_map, by = "game_id") |>
  left_join(assisted_keys, by = c("game_id", "id")) |>
  mutate(assisted = !is.na(assisted),
         made = parameters_made == "made")

cat("\n== 2. LEAGUE ZONE OVERVIEW (both seasons; blocked = miss; FG pts only) ==\n")
zov <- sc |>
  group_by(shot_zone_basic) |>
  summarise(att = n(),
            freq_pct = round(100 * n() / nrow(sc), 1),
            fg_pct = round(100 * mean(made), 1),
            pps = round(sum(parameters_points[made]) / n(), 3),
            ast_pct_of_makes = round(100 * sum(assisted & made) / sum(made), 1),
            .groups = "drop") |>
  arrange(desc(att))
print(as.data.frame(zov), row.names = FALSE)

cat("\nby season (fg / pps per zone):\n")
zs <- sc |>
  group_by(game_year, shot_zone_basic) |>
  summarise(att = n(), fg_pct = round(100 * mean(made), 1),
            pps = round(sum(parameters_points[made]) / n(), 3), .groups = "drop") |>
  arrange(shot_zone_basic, game_year)
print(as.data.frame(zs), row.names = FALSE)

cat("\n== 3. VALIDATED 3PT: CORNER vs ABOVE BREAK ==\n")
cat("(3pt-tagged shots only; zone from coordinates)\n")
t3 <- sc |>
  filter(parameters_type == "jump-shot", parameters_points == 3) |>
  mutate(zone3 = ifelse(shot_zone_basic %in% c("Left Corner 3", "Right Corner 3"),
                        "Corner 3", "Above Break 3"))
t3_sum <- t3 |>
  group_by(zone3) |>
  summarise(att = n(), share_of_3s = round(100 * n() / nrow(t3), 1),
            fg_pct = round(100 * mean(made), 1),
            pps = round(3 * mean(made), 3),
            ast_pct_of_makes = round(100 * sum(assisted & made) / sum(made), 1),
            .groups = "drop") |>
  arrange(desc(att))
print(as.data.frame(t3_sum), row.names = FALSE)

cat("\nleft vs right corner:\n")
lr <- t3 |>
  filter(shot_zone_basic %in% c("Left Corner 3", "Right Corner 3")) |>
  group_by(shot_zone_basic) |>
  summarise(att = n(), fg_pct = round(100 * mean(made), 1), .groups = "drop")
print(as.data.frame(lr), row.names = FALSE)

cat("\ncorner vs above-break by season:\n")
t3y <- t3 |>
  filter(zone3 != "(inside arc / other)") |>
  group_by(game_year, zone3) |>
  summarise(att = n(), fg_pct = round(100 * mean(made), 1), .groups = "drop")
print(as.data.frame(t3y), row.names = FALSE)

write.csv(zov, file.path("output", "shot_zone_overview.csv"), row.names = FALSE)
write.csv(t3_sum, file.path("output", "three_pt_corner_split.csv"), row.names = FALSE)
cat("\n-> output/shot_zone_overview.csv, output/three_pt_corner_split.csv\n")

sink()
cat("done\n")
