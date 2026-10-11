# Read-only, on-demand Team Insights. Rates always follow aggregation of counts.
insight_counts <- c("poss", "pts", "fgm", "fga", "fg3m", "tov", "oreb", "oreb_opp", "fta", "minutes")

insight_rate <- function(n, d) if (is.finite(d) && d > 0) unname(100 * n / d) else NA_real_

insight_summary <- function(d) {
  totals <- function(side) {
    x <- d[d$side == side, , drop = FALSE]
    setNames(vapply(insight_counts, function(k) sum(as.numeric(x[[k]]), na.rm = TRUE), numeric(1)), insight_counts)
  }
  o <- totals("offense"); a <- totals("defense")
  list(off = insight_rate(o["pts"], o["poss"]), def = insight_rate(a["pts"], a["poss"]),
       net = insight_rate(o["pts"], o["poss"]) - insight_rate(a["pts"], a["poss"]),
       off_poss = unname(o["poss"]), def_poss = unname(a["poss"]),
       games = length(unique(d$game_id[d$poss > 0])), minutes = unname(o["minutes"]),
       efg = insight_rate(o["fgm"] + .5 * o["fg3m"], o["fga"]),
       tov = insight_rate(o["tov"], o["poss"]),
       oreb = insight_rate(o["oreb"], o["oreb_opp"]), ftr = insight_rate(o["fta"], o["fga"]))
}

# Drop the SAME game from both disjoint groups; never average game ratings.
# This is a sensitivity check, not a confidence interval or significance test.
insight_sensitivity <- function(d, selected) {
  games <- unique(d$game_id)
  full <- insight_summary(d[selected, ])$net - insight_summary(d[!selected, ])$net
  if (length(games) < 3 || !is.finite(full)) return(list(status = "Insufficient games", range = c(NA_real_, NA_real_)))
  diffs <- vapply(games, function(g) {
    keep <- d$game_id != g
    insight_summary(d[selected & keep, ])$net - insight_summary(d[!selected & keep, ])$net
  }, numeric(1))
  if (any(!is.finite(diffs))) return(list(status = "Some exclusions lack a comparison", range = c(NA_real_, NA_real_)))
  list(status = if (full != 0 && all(sign(diffs) == sign(full))) "Direction survives each game exclusion" else "Direction changes or reaches zero",
       range = range(diffs))
}

insight_candidates <- function(d, minimum = 50) {
  ids <- unique(d$unit)
  rows <- lapply(ids, function(id) {
    a <- insight_summary(d[d$unit == id, ]); b <- insight_summary(d[d$unit != id, ])
    if (min(a$off_poss, a$def_poss, b$off_poss, b$def_poss) < minimum || a$games < 3 || b$games < 3 || !is.finite(a$net - b$net)) return(NULL)
    data.frame(unit = id, net = a$net, diff = a$net - b$net, minutes = a$minutes)
  })
  rows <- Filter(Negate(is.null), rows)
  if (!length(rows)) return(data.frame())
  do.call(rbind, rows)
}

insight_query <- function(comp) {
  if (identical(comp, "il")) {
    return("SELECT l.game_id::text, f.game_date, l.lineup_hash AS unit,
      l.type_lineup AS side, SUM(l.total_poss) AS poss, SUM(l.total_points) AS pts,
      SUM(l.total_fgm) AS fgm, SUM(l.total_fga) AS fga, SUM(l.total_fg3_made) AS fg3m,
      SUM(l.tov_count) AS tov, SUM(l.oreb_count) AS oreb, SUM(l.oreb_opportunities) AS oreb_opp,
      SUM(l.total_ft_attempts) AS fta, COALESCE(SUM(l.minutes), 0) AS minutes,
      COALESCE(n.player_names_str, 'Unmapped lineup') AS names,
      COALESCE(array_to_json(n.player_ids)::text, '[]') AS members,
      COALESCE(array_to_json(n.player_names)::text, '[]') AS member_names
      FROM basketball_test.lineup_four_factors_by_game l
      JOIN basketball_test.final_schedule_mv f ON f.game_id = l.game_id AND f.team_id = l.team_id
      LEFT JOIN basketball_test.sub_lineups_stats n ON n.sub_lineup_hash = l.lineup_hash
        AND n.team_id = l.team_id AND n.game_year = l.game_year AND n.num_lineup = 5
      WHERE l.game_year = $1::int4 AND l.team_id = $2::int4
      GROUP BY l.game_id, f.game_date, l.lineup_hash, l.type_lineup,
        n.player_names_str, n.player_ids, n.player_names")
  }
  "SELECT l.game_id::text, f.game_date, l.lineup_key AS unit, l.type_lineup AS side,
    SUM(l.possessions) AS poss, SUM(l.points) AS pts, SUM(l.fgm) AS fgm,
    SUM(l.fga) AS fga, SUM(l.fg3_made) AS fg3m, SUM(l.turnovers) AS tov,
    SUM(l.orebounds) AS oreb, SUM(l.oreb_opportunities) AS oreb_opp,
    SUM(l.ft_attempts) AS fta, COALESCE(SUM(l.seconds), 0) / 60.0 AS minutes,
    array_to_string(l.own_lineup, ', ') AS names,
    array_to_json(l.player_ids)::text AS members,
    (SELECT json_agg(COALESCE(p.display_name, '#' || u.pid::text) ORDER BY u.ord)::text
      FROM unnest(l.player_ids) WITH ORDINALITY u(pid, ord)
      LEFT JOIN euroleague.players p ON p.player_id = u.pid) AS member_names
    FROM euroleague.lineup_totals_by_game l
    JOIN euroleague.final_schedule_mv f ON f.game_id = l.game_id AND f.team_id = l.team_id
    WHERE l.game_year = $1::int4 AND l.team_id = $2::bigint AND l.competition = $3::text
    GROUP BY l.game_id, f.game_date, l.lineup_key, l.type_lineup, l.own_lineup, l.player_ids"
}

team_insights_entry_ui <- function() {
  div(class = "mb-4", style = "padding:18px;border:1px solid var(--ibpl-border);border-radius:12px;background:var(--ibpl-surface);",
      tags$span(style = "color:var(--ibpl-accent);font-size:.75rem;letter-spacing:.09em;", "NEW | TEAM INSIGHTS"),
      tags$h4(style = "margin:8px 0;", "Turn lineup numbers into questions worth testing"),
      tags$p(style = "color:var(--ibpl-text-muted);", "Compare five-player units, check sensitivity to individual games, and explore player staggering."),
      actionButton("open_team_insights", "Explore Team Insights", class = "btn btn-primary"))
}

insight_metric <- function(label, value) div(class = "ti-metric", tags$small(label), tags$strong(value))
insight_fmt <- function(x, suffix = "") if (is.finite(x)) paste0(sprintf("%.1f", x), suffix) else "---"
insight_sample <- function(x) sprintf("%d games | %.1f min | %.0f offense / %.0f defense possessions", x$games, x$minutes, x$off_poss, x$def_poss)

insight_card <- function(d, id, title) {
  chosen <- d$unit == id
  a <- insight_summary(d[chosen, ]); b <- insight_summary(d[!chosen, ])
  sensitivity <- insight_sensitivity(d, chosen)
  drivers <- c("eFG%" = a$efg - b$efg, "Turnover rate" = a$tov - b$tov,
               "Offensive rebound rate" = a$oreb - b$oreb, "FTA / FGA" = a$ftr - b$ftr)
  drivers <- drivers[is.finite(drivers)]
  drivers <- head(drivers[order(abs(drivers), decreasing = TRUE)], 2)
  div(class = "ti-card",
      tags$span(class = "ti-eyebrow", title),
      tags$h4(d$names[which(chosen)[1]]),
      div(class = "ti-metrics", insight_metric("Net / 100", insight_fmt(a$net)),
          insight_metric("Rest of team", insight_fmt(b$net)),
          insight_metric("Difference", sprintf("%+.1f", a$net - b$net))),
      tags$p(class = "ti-muted", paste("This unit:", insight_sample(a))),
      tags$p(class = "ti-muted", paste("Other units:", insight_sample(b))),
      tags$p("Offensive differences: ", paste(sprintf("%s %+.1f percentage points", names(drivers), drivers), collapse = "; "), ". These describe the split; they do not establish its cause."),
      div(class = "ti-check", tags$strong(sensitivity$status),
          if (all(is.finite(sensitivity$range))) tags$p(sprintf("Net difference after excluding each game: %+.1f to %+.1f per 100 possessions.", sensitivity$range[1], sensitivity$range[2]))),
      tags$p(style = "margin-bottom:0;", tags$strong("Review next: "),
             "Check opponent and teammate context, then review video before changing the rotation."))
}

server_team_insights <- function(input, output, session, shared) {
  context <- reactiveVal(NULL)
  loaded <- reactiveVal(NULL)
  observeEvent(input$open_team_insights, {
    comp <- input$league_select %||% "il"
    if (!comp %in% c("il", "E", "U")) comp <- "il"
    year <- if (comp == "il") shared$selected_game_year() else shared$euro$season()
    teams <- if (comp == "il") shared$teams_for_year_df() else shared$euro$teams_df()
    if (is.null(teams) || !nrow(teams)) {
      showNotification("No teams available for this season.", type = "message"); return()
    }
    ctx <- list(comp = comp, year = as.integer(year))
    context(ctx); loaded(NULL)
    choices <- setNames(as.character(teams$team_id), teams$team_name)
    home <- as.character(input$home_team %||% "")
    selected <- if (comp == "il" && length(home) == 1 && home %in% unname(choices)) home else unname(choices)[1]
    showModal(modalDialog(title = "Team Insights", size = "l", easyClose = TRUE,
      tags$style(HTML(".ti-controls{display:grid;grid-template-columns:2fr 1fr 1fr;gap:14px}.ti-controls .shiny-input-container,.ti-stagger .shiny-input-container{width:100%;min-width:0}.ti-controls>div,.ti-stagger>div{min-width:0}.ti-card{border:1px solid var(--ibpl-border,#504333);border-radius:12px;padding:20px;margin:16px 0}.ti-eyebrow{color:var(--ibpl-accent,#e8a435);font-size:.75rem;letter-spacing:.06em;text-transform:uppercase}.ti-metrics{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:10px;margin:16px 0}.ti-metric{display:flex;flex-direction:column;gap:5px}.ti-metric strong{font-size:1.6rem}.ti-muted{color:var(--ibpl-text-muted,#aaa);font-size:.85rem}.ti-check{border-left:3px solid var(--ibpl-accent,#e8a435);padding:10px 14px;background:rgba(232,164,53,.07);margin:14px 0}.ti-check p{margin:5px 0 0}.ti-card h4{font-size:1.05rem;line-height:1.5;overflow-wrap:anywhere}.ti-stagger{display:grid;grid-template-columns:1fr 1fr;gap:12px}.ti-stagger .ti-card{margin:0;padding:16px}@media(max-width:600px){.ti-controls,.ti-stagger{grid-template-columns:1fr}.ti-card{padding:14px}.ti-metric strong{font-size:1.3rem}.ti-metrics{gap:6px}}")),
      tags$p(class = "ti-muted", paste(if (comp == "il") "Israeli League" else EURO_COMPETITION_LABELS[[comp]],
        "--", season_label(ctx$year, ending_year = comp == "il"), "-- Preview")),
      div(class = "ti-controls",
          selectInput("ti_team", "Team", choices, selected = selected, selectize = FALSE),
          selectInput("ti_period", "Period", c("Full season" = "0", "Last 5 games" = "5", "Last 10 games" = "10"), selectize = FALSE),
          selectInput("ti_min", "Min. possessions per side", c("50" = "50", "100" = "100", "200" = "200"), selected = "50", selectize = FALSE)),
      actionButton("ti_run", "Generate insights", class = "btn btn-primary"),
      tags$p(class = "ti-muted", style = "margin-top:12px;", "Exploratory comparisons, without opponent or teammate adjustment. Three games and the possession minimum are screening rules, not proof of reliability. Scanning many lineups can produce chance extremes."),
      uiOutput("ti_findings"),
      footer = tagList(actionButton("ti_explore", "Open lineup explorer", class = "btn btn-outline-secondary"), modalButton("Close"))))
  }, ignoreInit = TRUE)

  observeEvent(input$ti_run, {
    ctx <- context(); req(ctx, input$ti_team)
    ctx$team <- as.character(input$ti_team)
    params <- list(ctx$year, ctx$team)
    if (ctx$comp != "il") params <- c(params, list(ctx$comp))
    result <- tryCatch(db_get_query(pg_pool, insight_query(ctx$comp), params = params), error = function(e) {
      app_log("team_insights", conditionMessage(e), session = session, level = "WARN")
      NULL
    })
    if (is.null(result)) {
      loaded(NULL); showNotification("Insights could not load. Try again shortly.", type = "error"); return()
    }
    result$game_date <- as.Date(result$game_date)
    result$member_ids <- lapply(result$members, function(x) as.character(jsonlite::fromJSON(x)))
    result$member_labels <- lapply(result$member_names, function(x) as.character(jsonlite::fromJSON(x)))
    ctx$data <- result; loaded(ctx)
  }, ignoreInit = TRUE)

  selected_data <- reactive({
    ctx <- loaded(); req(ctx, identical(ctx$team, as.character(input$ti_team)))
    d <- ctx$data
    n <- as.integer(input$ti_period %||% "0")
    if (n > 0 && nrow(d)) {
      games <- unique(d[order(d$game_date, d$game_id, decreasing = TRUE), c("game_id", "game_date")])
      d <- d[d$game_id %in% head(games$game_id, n), , drop = FALSE]
    }
    d
  })

  output$ti_findings <- renderUI({
    ctx <- loaded()
    if (is.null(ctx) || !identical(ctx$team, as.character(input$ti_team))) return(tags$p(class = "ti-check", "Select a team and generate its insights."))
    d <- selected_data()
    if (!nrow(d)) return(tags$p(class = "ti-check", "No lineup data available for this team and season."))
    candidates <- insight_candidates(d, as.numeric(input$ti_min))
    cards <- if (!nrow(candidates)) tags$p(class = "ti-check", "No five-player unit meets the sample rules. Try a longer period or a lower possession minimum.") else {
      ids <- unique(c(candidates$unit[which.max(candidates$diff)], candidates$unit[which.min(candidates$diff)], candidates$unit[which.max(candidates$minutes)]))
      lapply(seq_along(ids), function(i) insight_card(d, ids[i], c("Highest observed difference", "Lowest observed difference", "Most-used eligible unit")[match(ids[i], c(candidates$unit[which.max(candidates$diff)], candidates$unit[which.min(candidates$diff)], candidates$unit[which.max(candidates$minutes)]))]))
    }
    # Choices use aligned IDs/names, never display-name parsing or name identity.
    dictionary <- unlist(lapply(seq_len(nrow(d)), function(i) setNames(d$member_ids[[i]], d$member_labels[[i]])), use.names = TRUE)
    dictionary <- dictionary[!duplicated(unname(dictionary))]
    dictionary <- dictionary[order(names(dictionary))]
    tagList(tags$p(class = "ti-muted", sprintf("%d games with available lineup facts | %d of %d observed units meet the sample rules | %s to %s", length(unique(d$game_id)), nrow(candidates), length(unique(d$unit)), min(d$game_date), max(d$game_date))),
      tags$p(class = "ti-muted", "Coverage is not certified by this view; incomplete source games may affect these comparisons."),
      cards, tags$h4("Explore player staggering"),
      div(class = "ti-stagger", selectInput("ti_a", "Player A", dictionary, selectize = FALSE), selectInput("ti_b", "Player B", dictionary, selected = if (length(dictionary) > 1) unname(dictionary)[2] else unname(dictionary)[1], selectize = FALSE)),
      uiOutput("ti_stagger"))
  })

  output$ti_stagger <- renderUI({
    d <- selected_data(); req(input$ti_a, input$ti_b)
    if (input$ti_a == input$ti_b) return(tags$p(class = "ti-check", "Choose two different players."))
    mapped <- lengths(d$member_ids) == 5
    a <- vapply(d$member_ids, function(ids) input$ti_a %in% ids, logical(1))
    b <- vapply(d$member_ids, function(ids) input$ti_b %in% ids, logical(1))
    groups <- list("Both on" = a & b, "A on / B off" = a & !b, "A off / B on" = !a & b, "Both off" = !a & !b)
    tagList(if (any(!mapped)) tags$p(class = "ti-check", "Unmapped units are excluded from player comparisons."),
      div(class = "ti-stagger", lapply(names(groups), function(label) {
        s <- insight_summary(d[groups[[label]] & mapped, ])
        eligible <- min(s$off_poss, s$def_poss) >= as.numeric(input$ti_min) && s$games >= 3
        div(class = "ti-card", tags$strong(label),
            tags$p(style = "font-size:1.5rem;margin:10px 0;", if (eligible) paste("Net", insight_fmt(s$net)) else "Limited sample"),
            tags$p(class = "ti-muted", insight_sample(s)),
            if (eligible) tags$p(class = "ti-muted", paste("Offense", insight_fmt(s$off), "-- Defense", insight_fmt(s$def))))
      })),
      tags$p(class = "ti-muted", style = "margin-top:14px;", "Ratings are points per 100 possessions. These four groups have different teammates and opponents; their differences do not isolate either player's contribution."))
  })

  observeEvent(input$ti_explore, {
    ctx <- context(); req(ctx, input$ti_team)
    target <- if (ctx$comp == "il") "lineup_data" else "euro_lineups"
    shared$pending_nav(list(target = target, team_id = as.character(input$ti_team)))
    removeModal()
    updateTabsetPanel(session, "main_tabs", selected = target)
  }, ignoreInit = TRUE)
}
