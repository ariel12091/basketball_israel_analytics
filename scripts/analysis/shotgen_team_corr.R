# Team-level: does shot mix (layup/dunk/3pt/corner-3 shares) correlate with
# offensive rating? Reuses layup_team_fixed.R keys; rating from team_ppp_ratings_mv.
suppressMessages({
  library(DBI); library(RPostgres); library(dplyr)
  library(arrow); library(sf)
})
source("scripts/analysis/fiba_court_zones.R")

out <- file.path(Sys.getenv("SCRATCH"), "gen_team_corr_out.txt")
sink(out)
options(width = 240)

readRenviron("app/.Renviron")
con <- dbConnect(RPostgres::Postgres(),
  host=Sys.getenv("PG_HOST"), port=as.integer(Sys.getenv("PG_PORT")),
  dbname=Sys.getenv("PG_DB"), user=Sys.getenv("PG_USER"),
  password=Sys.getenv("PG_PASS"), sslmode=Sys.getenv("PG_SSLMODE"),
  connect_timeout=15L, bigint="numeric")

mix <- dbGetQuery(con, "
  WITH tn AS (
    SELECT team_id, game_year, MIN(team_name) AS team_name
    FROM basketball_test.full_rosters GROUP BY team_id, game_year
  )
  SELECT s.game_year AS yr, tn.team_name, d.team_id,
         SUM(d.team_score) AS pts,
         SUM(d.final_end_poss::int) AS poss,
         COUNT(*) FILTER (WHERE d.type = 'shot') AS fga,
         COUNT(*) FILTER (WHERE d.type = 'shot' AND d.parameters_type = 'lay-up') AS layup_att,
         COUNT(*) FILTER (WHERE d.type = 'shot' AND d.parameters_type IN ('dunk','allyhoop')) AS dunk_att,
         COUNT(*) FILTER (WHERE d.type = 'shot' AND d.parameters_points = 3) AS fg3_att,
         COUNT(*) FILTER (WHERE d.type = 'shot' AND d.parameters_points = 2
                            AND d.parameters_type = 'jump-shot') AS mid_att,
         SUM(CASE WHEN d.type = 'shot' AND d.parameters_type = 'lay-up'
                   AND d.parameters_made = 'made' THEN d.parameters_points ELSE 0 END) AS layup_pts,
         SUM(CASE WHEN d.type = 'shot' AND d.parameters_type IN ('dunk','allyhoop')
                   AND d.parameters_made = 'made' THEN d.parameters_points ELSE 0 END) AS dunk_pts,
         SUM(CASE WHEN d.type = 'shot' AND d.parameters_points = 3
                   AND d.parameters_made = 'made' THEN 3 ELSE 0 END) AS fg3_pts,
         SUM(CASE WHEN d.type = 'shot' AND d.parameters_points = 2 AND d.parameters_type = 'jump-shot'
                   AND d.parameters_made = 'made' THEN 2 ELSE 0 END) AS mid_pts
  FROM basketball_test.df_pts_poss_lineups_longer_mv d
  JOIN basketball_test.schedule s ON s.game_id = d.game_id
  JOIN tn ON tn.team_id = d.team_id AND tn.game_year = s.game_year
  WHERE d.type_lineup = 'offense'
  GROUP BY 1, 2, 3")

rtg <- dbGetQuery(con, "
  SELECT game_year AS yr, team_id, off_ppp
  FROM basketball_test.team_ppp_ratings_mv")

ff <- dbGetQuery(con, "
  SELECT game_year AS yr, team_id, off_efg, off_ts
  FROM basketball_test.team_four_factors_mv")

mv_shots <- dbGetQuery(con, "
  SELECT game_id, id, team_id
  FROM basketball_test.df_pts_poss_lineups_longer_mv
  WHERE type = 'shot' AND type_lineup = 'offense'")
yr_map <- dbGetQuery(con, "SELECT DISTINCT game_id, game_year FROM basketball_test.schedule")
dbDisconnect(con)

d <- mix |>
  left_join(rtg, by = c("yr", "team_id")) |>
  left_join(ff,  by = c("yr", "team_id")) |>
  mutate(off_rtg_calc = round(100 * pts / poss, 1),
         layup_share = 100 * layup_att / fga,
         dunk_share  = 100 * dunk_att / fga,
         rim_share   = layup_share + dunk_share,
         fg3_share   = 100 * fg3_att / fga,
         mid_share   = 100 * mid_att / fga,
         good_share  = rim_share + fg3_share,
         rim_per100  = 100 * (layup_att + dunk_att) / poss,
         fg3_per100  = 100 * fg3_att / poss,
         good_per100 = rim_per100 + fg3_per100)

cat("== INVARIANT CHECKS ==\n")
cat("team-seasons:", nrow(d), " (expect 28; per yr:", paste(table(d$yr), collapse="/"), ")\n")
cat("max |off_ppp(MV) - off_rtg(recomputed)|:",
    max(abs(d$off_ppp - d$off_rtg_calc), na.rm = TRUE), "\n")
cat("league rim share (expect ~46-48):",
    round(100 * sum(d$layup_att + d$dunk_att) / sum(d$fga), 1), "\n\n")

# league per-type PPS (per season) -> expected PPS of mix (selection only)
lg <- d |> group_by(yr) |>
  summarise(pps_layup = sum(layup_pts)/sum(layup_att),
            pps_dunk  = sum(dunk_pts)/sum(dunk_att),
            pps_fg3   = sum(fg3_pts)/sum(fg3_att),
            pps_mid   = sum(mid_pts)/sum(mid_att), .groups="drop")
cat("== LEAGUE PPS BY TYPE (FG pts only, blocked = miss) ==\n")
print(as.data.frame(lg), row.names = FALSE)

d <- d |> left_join(lg, by = "yr") |>
  mutate(xpps_mix = (layup_share*pps_layup + dunk_share*pps_dunk +
                     fg3_share*pps_fg3 + mid_share*pps_mid) / 100)

# corner 3 from parquet coords (v2 zones); team via MV join on (game_id,id)
acts <- read_parquet("exports/cold/actions_clean.parquet",
  col_select = c("game_id","id","type","parameters_points",
                 "parameters_coord_x","parameters_coord_y")) |>
  distinct(game_id, id, .keep_all = TRUE) |>
  filter(type == "shot", !is.na(parameters_coord_x))
c3 <- acts |>
  inner_join(mv_shots, by = c("game_id","id")) |>
  left_join(yr_map, by = "game_id") |>
  mutate(x = parameters_coord_x/100, y = parameters_coord_y/100,
         zone = classify_shot_zone(x, y, parameters_points)) |>
  group_by(team_id, yr = game_year) |>
  summarise(pq_fga = n(),
            pq_fg3 = sum(parameters_points == 3),
            c3_att = sum(zone %in% c("Left Corner 3","Right Corner 3")),
            .groups = "drop") |>
  mutate(c3_share_fga = 100 * c3_att / pq_fga,
         c3_share_3s  = 100 * c3_att / pq_fg3)
d <- d |> left_join(c3, by = c("team_id","yr"))
cat("\ncorner-3 league share of 3PA (expect ~9-10 under v2):",
    round(100 * sum(d$c3_att)/sum(d$pq_fg3), 1), "\n")

cat("\n== TEAM TABLE (sorted by off_ppp) ==\n")
print(d |> arrange(desc(off_ppp)) |>
  transmute(yr, team_name, off_ppp, poss,
            layup_share = round(layup_share,1), dunk_share = round(dunk_share,1),
            rim_share = round(rim_share,1), fg3_share = round(fg3_share,1),
            mid_share = round(mid_share,1), good_share = round(good_share,1),
            good_per100 = round(good_per100,1),
            c3_share_3s = round(c3_share_3s,1), xpps = round(xpps_mix,3)) |>
  as.data.frame(), row.names = FALSE)

preds <- c("layup_share","dunk_share","rim_share","fg3_share","mid_share",
           "good_share","rim_per100","fg3_per100","good_per100",
           "c3_share_fga","c3_share_3s","xpps_mix")
outcomes <- c("off_ppp","off_efg","off_ts")
cor_tbl <- function(dd, label) {
  cat("\n--", label, "(n =", nrow(dd), ") --\n")
  res <- t(sapply(preds, function(p) {
    unlist(lapply(outcomes, function(o) {
      ok <- is.finite(dd[[p]]) & is.finite(dd[[o]])
      setNames(round(cor(dd[[p]][ok], dd[[o]][ok]), 2), o)
    }))
  }))
  print(res)
}
cat("\n== PEARSON r: predictor (rows) vs outcome (cols) ==\n")
cor_tbl(d, "pooled (raw, both seasons)")
dm <- d |> group_by(yr) |> mutate(across(all_of(c(preds, outcomes)),
        ~ .x - mean(.x, na.rm=TRUE))) |> ungroup()
cor_tbl(dm, "within-season (demeaned)")
for (y in sort(unique(d$yr))) cor_tbl(d |> filter(yr == y), paste("season", y))

cat("\n== OLS: off_ppp ~ rim_share + fg3_share + factor(yr) ==\n")
print(summary(lm(off_ppp ~ rim_share + fg3_share + factor(yr), data = d)))
cat("\n== OLS: off_ppp ~ xpps_mix + factor(yr) ==\n")
print(summary(lm(off_ppp ~ xpps_mix + factor(yr), data = d)))

write.csv(d, file.path("output", "shotgen_team_2025_2026.csv"), row.names = FALSE)
sink()
cat("done\n")
