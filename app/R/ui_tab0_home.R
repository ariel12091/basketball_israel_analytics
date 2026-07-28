# ui_tab0_home.R - Tab 0: Home / Landing Page

ui_tab0_home <- tabPanel(
  title = tags$span(tags$i(class = "bi bi-house-fill"), "Home"),
  value = "home",
  fluidPage(
    shared_head_tags(),

    div(
      style = "max-width: 760px; margin: 40px auto 0 auto; padding: 0 16px;",

      # Site title
      div(
        class = "text-center mb-4",
        tags$h2(
          style = "font-weight: 700; color: #e6edf3; margin-bottom: 4px;",
          "IBPL Analytics"
        ),
        tags$p(
          style = "color: #8b949e; font-size: .92rem; margin: 0;",
          "Your go-to site for Israel Basketball Premier League stats"
        )
      ),

      # Optional team selector
      div(
        class = "mb-4",
        selectizeInput(
          "home_team",
          label = NULL,
          choices = NULL,
          options = list(placeholder = "All teams", preload = "focus")
        )
      ),

      team_hub_ui(),

      # Row 1
      fluidRow(style = "align-items: stretch;",
        column(
          width = 6,
          tags$button(
            type = "button",
            class = "card bg-dark border-secondary mb-4 h-100 w-100 text-start p-0 home-nav-card js-shiny-event",
            `data-input-id` = "go_onoff",
            div(
              class = "card-body d-flex flex-column gap-2",
              tags$i(class = "bi bi-person-fill", style = "font-size: 2rem; color: #e8a435;"),
              tags$h5("Who is helping my team?", class = "card-title mb-1"),
              tags$small(class = "text-muted", "Player impact when on vs. off the court"),
              div(class = "mt-auto pt-2",
                tags$span(class = "text-warning small fw-semibold", "Go \u2192"))
            )
          )
        ),
        column(
          width = 6,
          tags$button(
            type = "button",
            class = "card bg-dark border-secondary mb-4 h-100 w-100 text-start p-0 home-nav-card js-shiny-event",
            `data-input-id` = "go_lineups",
            div(
              class = "card-body d-flex flex-column gap-2",
              tags$i(class = "bi bi-people-fill", style = "font-size: 2rem; color: #e8a435;"),
              tags$h5("Which lineups are working?", class = "card-title mb-1"),
              tags$small(class = "text-muted", "Best and worst 5-man units by possessions"),
              div(class = "mt-auto pt-2",
                tags$span(class = "text-warning small fw-semibold", "Go \u2192"))
            )
          )
        )
      ),

      # Row 2
      fluidRow(style = "align-items: stretch;",
        column(
          width = 6,
          tags$button(
            type = "button",
            class = "card bg-dark border-secondary mb-4 h-100 w-100 text-start p-0 home-nav-card js-shiny-event",
            `data-input-id` = "go_team",
            div(
              class = "card-body d-flex flex-column gap-2",
              tags$i(class = "bi bi-bar-chart-fill", style = "font-size: 2rem; color: #e8a435;"),
              tags$h5("How is my team performing?", class = "card-title mb-1"),
              tags$small(class = "text-muted", "Offense, defense, net rating vs. the league"),
              div(class = "mt-auto pt-2",
                tags$span(class = "text-warning small fw-semibold", "Go \u2192"))
            )
          )
        ),
        column(
          width = 6,
          tags$button(
            type = "button",
            class = "card bg-dark border-secondary mb-4 h-100 w-100 text-start p-0 home-nav-card js-shiny-event",
            `data-input-id` = "go_gamelogs",
            div(
              class = "card-body d-flex flex-column gap-2",
              tags$i(class = "bi bi-calendar-day-fill", style = "font-size: 2rem; color: #e8a435;"),
              tags$h5("What happened in last night's game?", class = "card-title mb-1"),
              tags$small(class = "text-muted", "Score, lineups, and stats by game"),
              div(class = "mt-auto pt-2",
                tags$span(class = "text-warning small fw-semibold", "Go \u2192"))
            )
          )
        )
      ),

      # Row 3
      fluidRow(style = "align-items: stretch;",
        column(
          width = 6,
          tags$button(
            type = "button",
            class = "card bg-dark border-secondary mb-4 h-100 w-100 text-start p-0 home-nav-card js-shiny-event",
            `data-input-id` = "go_playerstats",
            div(
              class = "card-body d-flex flex-column gap-2",
              tags$i(class = "bi bi-bar-chart-line", style = "font-size: 2rem; color: #e8a435;"),
              tags$h5("How are individual players performing?", class = "card-title mb-1"),
              tags$small(class = "text-muted", "Points, rebounds, assists, shooting splits per player"),
              div(class = "mt-auto pt-2",
                tags$span(class = "text-warning small fw-semibold", "Go \u2192"))
            )
          )
        ),
        column(
          width = 6,
          tags$button(
            type = "button",
            class = "card bg-dark border-secondary mb-4 h-100 w-100 text-start p-0 home-nav-card js-shiny-event",
            `data-input-id` = "go_compare",
            div(
              class = "card-body d-flex flex-column gap-2",
              tags$i(class = "bi bi-arrow-left-right", style = "font-size: 2rem; color: #e8a435;"),
              tags$h5("How do starters compare to the bench?", class = "card-title mb-1"),
              tags$small(class = "text-muted", "Compare any two situations side-by-side"),
              div(class = "mt-auto pt-2",
                tags$span(class = "text-warning small fw-semibold", "Go \u2192"))
            )
          )
        )
      )
    )
  )
)
