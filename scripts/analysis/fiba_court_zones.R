# ============================================================
# FIBA half-court geometry + basic shot zones (sf)
# Cleaned from the original exploratory script; arcs built with
# a deterministic angle-sequence helper instead of st_crop.
#
# Provider coordinate system (Segev PBP): parameters_coord_x/y are
# centimeters-ish; divide by 100. Rim sits at (7.5, 1.4). The court
# is NOT true FIBA scale — the 3pt arc fits at ~9.10 (true FIBA is
# 6.75), so use the TUNED dimensions below, matched to the data
# (see docs/court_tuned_910_210_ra145.png).
#
# Usage: source() this file, then use court_sf, basic_polys,
#        plot_court(), classify shots via sf::st_join(shots_sf, basic_polys).
# ============================================================

library(sf)

court_themes <- list(
  light = list(court = "#F7F3E8", lines = "#1A1A1A", text = "#1A1A1A"),
  dark  = list(court = "#0F172A", lines = "#E5E7EB", text = "#E5E7EB")
)

# --- Dimensions (provider units / 100; tuned to the data) ---
line_thick <- 0.05
width      <- 15
height     <- 28 / 2
key_height <- 5.8
key_width  <- 4.9
key_radius <- 1.8
backboard_width  <- 1.8
backboard_thick  <- 0.1
backboard_offset <- 1.2
hoop_radius   <- 0.45 / 2
hoop_center_y <- 1.4
rim_thick   <- 0.02
neck_length <- hoop_center_y - (backboard_offset + hoop_radius + rim_thick)

three_point_radius      <- 9.10   # tuned (original guess: 9.34)
three_point_side_offset <- 2.10
restricted_area_radius  <- 1.45   # tuned (true FIBA: 1.25)

three_point_side_height <-
  sqrt(three_point_radius^2 - (three_point_side_offset - width / 2)^2) + hoop_center_y

cx <- width / 2   # rim x

# --- Helpers ---
arc_pts <- function(cx, cy, r, from, to, n = 240) {
  t <- seq(from, to, length.out = n)
  cbind(cx + r * cos(t), cy + r * sin(t))
}
ring <- function(m) st_polygon(list(rbind(m, m[1, ])))
# donut ring for a line of given thickness (outer boundary, inner hole)
band <- function(outer, inner) st_polygon(list(rbind(outer, outer[1, ]),
                                               rbind(inner, inner[1, ])))

# --- Court features (lines drawn as thin filled bands) ---
half_court <- band(
  rbind(c(-line_thick, -line_thick), c(-line_thick, height + line_thick),
        c(width + line_thick, height + line_thick), c(width + line_thick, -line_thick)),
  rbind(c(0, 0), c(0, height), c(width, height), c(width, 0))
)

key <- band(
  rbind(c(cx - key_width / 2, 0), c(cx - key_width / 2, key_height),
        c(cx + key_width / 2, key_height), c(cx + key_width / 2, 0)),
  rbind(c(cx - key_width / 2 + line_thick, 0),
        c(cx - key_width / 2 + line_thick, key_height - line_thick),
        c(cx + key_width / 2 - line_thick, key_height - line_thick),
        c(cx + key_width / 2 - line_thick, 0))
)

backboard <- ring(rbind(
  c(cx - backboard_width / 2, backboard_offset - backboard_thick),
  c(cx - backboard_width / 2, backboard_offset),
  c(cx + backboard_width / 2, backboard_offset),
  c(cx + backboard_width / 2, backboard_offset - backboard_thick)
))

neck <- ring(rbind(
  c(cx - line_thick / 2, backboard_offset),
  c(cx - line_thick / 2, backboard_offset + neck_length),
  c(cx + line_thick / 2, backboard_offset + neck_length),
  c(cx + line_thick / 2, backboard_offset)
))

hoop <- band(arc_pts(cx, hoop_center_y, hoop_radius + rim_thick, 0, 2 * pi),
             arc_pts(cx, hoop_center_y, hoop_radius, 0, 2 * pi))

key_circle <- band(arc_pts(cx, key_height, key_radius, 0, pi),
                   arc_pts(cx, key_height, key_radius - line_thick, 0, pi))

half_circle <- band(arc_pts(cx, height, key_radius, pi, 2 * pi),
                    arc_pts(cx, height, key_radius - line_thick, pi, 2 * pi))

# 3pt line: two verticals + arc, as one band
t_right <- atan2(three_point_side_height - hoop_center_y, (width - three_point_side_offset) - cx)
t_left  <- atan2(three_point_side_height - hoop_center_y, three_point_side_offset - cx)
# arc runs from left endpoint over the top to right endpoint (t_left > pi/2 > t_right)
three_outer <- rbind(
  c(three_point_side_offset, 0),
  arc_pts(cx, hoop_center_y, three_point_radius, t_left, t_right),
  c(width - three_point_side_offset, 0)
)
three_inner <- rbind(
  c(three_point_side_offset + line_thick, 0),
  arc_pts(cx, hoop_center_y, three_point_radius - line_thick, t_left, t_right),
  c(width - (three_point_side_offset + line_thick), 0)
)
three_point_line <- band(three_outer, three_inner)

# Restricted area line: uprights from backboard to rim height + semicircle
ra_line_outer <- rbind(
  c(cx - restricted_area_radius - line_thick, backboard_offset),
  c(cx - restricted_area_radius - line_thick, hoop_center_y),
  arc_pts(cx, hoop_center_y, restricted_area_radius + line_thick, pi, 0),
  c(cx + restricted_area_radius + line_thick, backboard_offset)
)
ra_line_inner <- rbind(
  c(cx - restricted_area_radius, backboard_offset),
  c(cx - restricted_area_radius, hoop_center_y),
  arc_pts(cx, hoop_center_y, restricted_area_radius, pi, 0),
  c(cx + restricted_area_radius, backboard_offset)
)
restricted_area_line <- band(ra_line_outer, ra_line_inner)

make_court_sf <- function() {
  st_sf(
    description = c("half_court", "key", "hoop", "backboard", "neck",
                    "key_circle", "three_point_line", "half_circle",
                    "restricted_area"),
    geom = st_sfc(half_court, key, hoop, backboard, neck,
                  key_circle, three_point_line, half_circle,
                  restricted_area_line)
  )
}
court_sf <- make_court_sf()

# ============================
# Basic shot zones (filled polygons, exhaustive inside the court)
# ============================
zone_ra <- ring(rbind(
  c(cx - restricted_area_radius, backboard_offset),
  c(cx - restricted_area_radius, hoop_center_y),
  arc_pts(cx, hoop_center_y, restricted_area_radius, pi, 0),
  c(cx + restricted_area_radius, backboard_offset)
))

zone_key <- ring(rbind(
  c(cx - key_width / 2, 0), c(cx - key_width / 2, key_height),
  c(cx + key_width / 2, key_height), c(cx + key_width / 2, 0)
))
zone_paint_non_ra <- st_difference(zone_key, zone_ra)

zone_right_corner_3 <- ring(rbind(
  c(0, 0), c(0, three_point_side_height),
  c(three_point_side_offset, three_point_side_height), c(three_point_side_offset, 0)
))
zone_left_corner_3 <- ring(rbind(
  c(width - three_point_side_offset, 0), c(width - three_point_side_offset, three_point_side_height),
  c(width, three_point_side_height), c(width, 0)
))

zone_mid_range <- st_difference(
  ring(rbind(
    c(three_point_side_offset, 0),
    c(three_point_side_offset, three_point_side_height),
    arc_pts(cx, hoop_center_y, three_point_radius, t_left, t_right),
    c(width - three_point_side_offset, three_point_side_height),
    c(width - three_point_side_offset, 0)
  )),
  zone_key
)

zone_above_break_3 <- st_difference(
  ring(rbind(c(0, three_point_side_height), c(0, height),
             c(width, height), c(width, three_point_side_height))),
  ring(rbind(
    c(three_point_side_offset, three_point_side_height),
    arc_pts(cx, hoop_center_y, three_point_radius, t_left, t_right),
    c(width - three_point_side_offset, three_point_side_height)
  ))
)

basic_polys <- st_sf(
  shot_zone_basic = c("Above Break 3", "Left Corner 3", "Right Corner 3",
                      "Mid-Range", "Restricted Area", "In The Paint (Non-RA)"),
  geom = st_sfc(zone_above_break_3, zone_left_corner_3, zone_right_corner_3,
                zone_mid_range, zone_ra, zone_paint_non_ra)
)

# ============================
# Analysis-grade zone classification (v2) — preferred over st_join(basic_polys)
# for stats. Rationale (coordinate calibration, 2026-07-12):
#  - coords are on a ~0.15-unit click grid (3,674 distinct spots) — no continuous distance
#  - parameters_points is scoreboard truth: 2-vs-3 never uses coordinates
#  - corner-vs-arc needs only y (+ outer-strip x); no arc radius dependence
#  - rim zone = radial disc around rim (7.5, 1.4), NOT the painted RA polygon
#    (the polygon's y>=1.2 lower edge misclassifies under-rim clicks)
# ============================
rim_zone_radius <- 1.45
# Corner 3 = 3pt shot below the full-width horizontal line tangent to the top
# of the restricted-area arc (y = rim y + RA radius). User-defined cut,
# 2026-07-14 — replaces the corner-break-height + outer-strip rule.
corner3_height <- hoop_center_y + restricted_area_radius   # 2.85

classify_shot_zone <- function(x, y, points) {
  r <- sqrt((x - cx)^2 + (y - hoop_center_y)^2)
  dplyr::case_when(
    points == 3 & y <= corner3_height & x < cx ~ "Right Corner 3",
    points == 3 & y <= corner3_height          ~ "Left Corner 3",
    points == 3                                ~ "Above Break 3",
    r <= rim_zone_radius                                ~ "Rim (r<=1.45)",
    abs(x - cx) <= key_width / 2 & y <= key_height      ~ "In The Paint (Non-Rim)",
    .default = "Mid-Range"
  )
}

# Operator "magnet" spots — two grid cells holding ~3% of all shots
# (suspected default clicks; both classify as rim-zone, flag for sensitivity checks)
is_magnet_spot <- function(x, y) {
  (x == 7.5 & y == 2.8) | (x == 7.5 & y == 2.24)
}

# ==============
# Plotting helper
# ==============
plot_court <- function(court_theme = court_themes$light) {
  ggplot2::ggplot() +
    ggplot2::geom_sf(data = court_sf, fill = court_theme$lines, color = court_theme$lines) +
    ggplot2::theme_void() +
    ggplot2::theme(
      text = ggplot2::element_text(color = court_theme$text),
      plot.background  = ggplot2::element_rect(fill = court_theme$court, color = court_theme$court),
      panel.background = ggplot2::element_rect(fill = court_theme$court, color = court_theme$court),
      legend.position  = "none"
    )
}
