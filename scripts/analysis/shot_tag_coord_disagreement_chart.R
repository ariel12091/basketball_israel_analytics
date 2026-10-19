# Shot chart: where the coordinate click DISAGREES with the shot-type tag.
#   Panel A: jump-shot 3pt — scoreboard says 3 points, click plots inside the arc
#   Panel B: lay-up — tag says interior, click plots outside paint-wide (rim disc + key)
# Agreement mass drawn as slate grid spots (alpha ~ count); disagreement in amber
# (sized by count). Coords are a ~0.15-unit click grid, so spots stack — aggregate
# per distinct spot instead of jittering.
# Output: docs/shot_tag_vs_coord_disagreement.png + stats to $SCRATCH.
suppressMessages({
  library(arrow); library(dplyr); library(ggplot2); library(sf)
})
source("scripts/analysis/fiba_court_zones.R")

out <- file.path(Sys.getenv("SCRATCH"), "shot_tag_coord_disagreement_out.txt")
sink(out)
options(width = 220)

shots <- read_parquet(
  "exports/cold/actions_clean.parquet",
  col_select = c("game_id", "id", "type", "parameters_type", "parameters_points",
                 "parameters_made", "parameters_coord_x", "parameters_coord_y")
) |>
  filter(type == "shot") |>
  distinct(game_id, id, .keep_all = TRUE) |>
  filter(!is.na(parameters_coord_x), !is.na(parameters_coord_y)) |>
  mutate(x = parameters_coord_x / 100, y = parameters_coord_y / 100,
         made = parameters_made == "made")
cat("total deduped shots with coords:", nrow(shots), "\n")

# --- Panel A: 3pt-tagged shots whose click lands in 2pt territory ---
# Below the corner break the 3pt boundary is the vertical side lines;
# above it, the fitted arc (r = 9.10 from the rim).
p3 <- shots |>
  filter(parameters_type == "jump-shot", parameters_points == 3) |>
  mutate(r = sqrt((x - cx)^2 + (y - hoop_center_y)^2),
         disagree = ifelse(
           y <= three_point_side_height,
           x > three_point_side_offset & x < width - three_point_side_offset,
           r < three_point_radius))

# --- Panel B: lay-ups whose click lands outside paint-wide ---
lay <- shots |>
  filter(parameters_type == "lay-up") |>
  mutate(zone = classify_shot_zone(x, y, parameters_points),
         disagree = !zone %in% c("Rim (r<=1.45)", "In The Paint (Non-Rim)"))

stat_line <- function(df) {
  n <- sum(df$disagree)
  sprintf("%s of %s (%.1f%%) disagree · FG on those: %.1f%% (vs %.1f%% agreeing)",
          format(n, big.mark = ","), format(nrow(df), big.mark = ","),
          100 * n / nrow(df),
          100 * mean(df$made[df$disagree]), 100 * mean(df$made[!df$disagree]))
}
cat("\nPanel A (3pt-tagged inside arc): ", stat_line(p3), "\n")
cat("Panel B (lay-ups outside paint): ", stat_line(lay), "\n")

cat("\n3pt disagreement depth inside the arc (units inside boundary, arc region only):\n")
p3 |> filter(disagree, y > three_point_side_height) |>
  summarise(n = n(),
            q50 = median(three_point_radius - r),
            q90 = quantile(three_point_radius - r, .9),
            max = max(three_point_radius - r)) |>
  as.data.frame() |> print(row.names = FALSE)

cat("\nlay-up disagreement zones:\n")
lay |> filter(disagree) |> count(zone) |> as.data.frame() |> print(row.names = FALSE)

# --- Assemble plot data (facet labels carry the stats) ---
lab_a <- paste0("JUMP-SHOT 3PT — click inside the arc\n", stat_line(p3))
lab_b <- paste0("LAY-UP — click outside the paint\n", stat_line(lay))
pts <- bind_rows(p3 |> mutate(panel = lab_a), lay |> mutate(panel = lab_b)) |>
  count(panel, x, y, disagree)

col_surface <- "#0F172A"; col_lines <- "#3d4a61"
col_text <- "#E5E7EB"; col_muted <- "#94a3b8"
col_mass <- "#64748b"; col_disagree <- "#e8a435"

p <- plot_court(list(court = col_surface, lines = col_lines, text = col_text)) +
  geom_point(data = filter(pts, !disagree), aes(x, y, alpha = n),
             color = col_mass, size = 0.9, stroke = 0) +
  scale_alpha_continuous(range = c(0.10, 0.55), trans = "sqrt", guide = "none") +
  geom_point(data = filter(pts, disagree), aes(x, y, size = n),
             shape = 21, fill = col_disagree, color = col_surface,
             stroke = 0.25, alpha = 0.9) +
  scale_size_area(max_size = 4.5, guide = "none") +
  facet_wrap(~panel) +
  labs(
    title = "Where the click disagrees with the shot-type tag",
    subtitle = paste0("amber = tag-coordinate disagreement (sized by shots per grid spot) · ",
                      "slate = agreement mass (alpha scales with count)"),
    caption = paste0("Seasons 2025–26, ", format(nrow(shots), big.mark = ","),
                     " shots (actions_clean.parquet, deduped) · ",
                     "coords are a ~0.15-unit click grid · 2-vs-3 uses scoreboard points — ",
                     "amber 3s are real 3s with a misplaced click")
  ) +
  theme(
    plot.title    = element_text(color = col_text, size = 16, face = "bold",
                                 hjust = 0.5, margin = margin(b = 4)),
    plot.subtitle = element_text(color = col_muted, size = 10, hjust = 0.5,
                                 margin = margin(b = 10)),
    plot.caption  = element_text(color = col_muted, size = 8, hjust = 0.5,
                                 margin = margin(t = 8)),
    strip.text    = element_text(color = col_text, size = 10, lineheight = 1.2,
                                 margin = margin(b = 6)),
    plot.margin   = margin(12, 16, 8, 16)
  )

ggsave("docs/shot_tag_vs_coord_disagreement.png", p,
       width = 13, height = 7.8, dpi = 150, bg = col_surface)
cat("\n-> docs/shot_tag_vs_coord_disagreement.png\n")
sink()
cat("done\n")
