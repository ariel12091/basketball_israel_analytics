# ui_tab0_home.R - Tab 0: Home / Landing Page

ui_tab0_home <- tabPanel(
  title = tags$span(tags$i(class = "bi bi-house-fill"), "Home"),
  value = "home",
  fluidPage(
    shared_head_tags(),

    div(
      style = "max-width: 760px; margin: 40px auto 0 auto; padding: 0 16px;",

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

      # 2x2 question cards grid
      fluidRow(style = "align-items: stretch;",
        column(
          width = 6,
          div(
            class = "card bg-dark border-secondary mb-4 h-100",
            style = "cursor: default;",
            div(
              class = "card-body d-flex flex-column gap-2",
              tags$i(class = "bi bi-person-fill", style = "font-size: 2rem; color: #e8a435;"),
              tags$h5("Who is helping my team?", class = "card-title mb-1"),
              tags$small(
                class = "text-muted",
                "Player impact when on vs. off the court"
              ),
              div(
                class = "mt-auto pt-2",
                actionButton(
                  "go_onoff",
                  "Go \u2192",
                  class = "btn btn-outline-warning btn-sm"
                )
              )
            )
          )
        ),
        column(
          width = 6,
          div(
            class = "card bg-dark border-secondary mb-4 h-100",
            style = "cursor: default;",
            div(
              class = "card-body d-flex flex-column gap-2",
              tags$i(class = "bi bi-people-fill", style = "font-size: 2rem; color: #e8a435;"),
              tags$h5("Which lineups are working?", class = "card-title mb-1"),
              tags$small(
                class = "text-muted",
                "Best and worst 5-man units by possessions"
              ),
              div(
                class = "mt-auto pt-2",
                actionButton(
                  "go_lineups",
                  "Go \u2192",
                  class = "btn btn-outline-warning btn-sm"
                )
              )
            )
          )
        )
      ),

      fluidRow(style = "align-items: stretch;",
        column(
          width = 6,
          div(
            class = "card bg-dark border-secondary mb-4 h-100",
            style = "cursor: default;",
            div(
              class = "card-body d-flex flex-column gap-2",
              tags$i(class = "bi bi-bar-chart-fill", style = "font-size: 2rem; color: #e8a435;"),
              tags$h5("How is my team performing?", class = "card-title mb-1"),
              tags$small(
                class = "text-muted",
                "Offense, defense, net rating vs. the league"
              ),
              div(
                class = "mt-auto pt-2",
                actionButton(
                  "go_team",
                  "Go \u2192",
                  class = "btn btn-outline-warning btn-sm"
                )
              )
            )
          )
        ),
        column(
          width = 6,
          div(
            class = "card bg-dark border-secondary mb-4 h-100",
            style = "cursor: default;",
            div(
              class = "card-body d-flex flex-column gap-2",
              tags$i(class = "bi bi-calendar-day-fill", style = "font-size: 2rem; color: #e8a435;"),
              tags$h5("What happened in last night's game?", class = "card-title mb-1"),
              tags$small(
                class = "text-muted",
                "Score, lineups, and stats by game"
              ),
              div(
                class = "mt-auto pt-2",
                actionButton(
                  "go_gamelogs",
                  "Go \u2192",
                  class = "btn btn-outline-warning btn-sm"
                )
              )
            )
          )
        )
      ),

      # 5th card - full width
      fluidRow(
        column(
          width = 12,
          div(
            class = "card bg-dark border-secondary mb-4",
            style = "cursor: default;",
            div(
              class = "card-body d-flex align-items-center gap-3",
              tags$i(class = "bi bi-arrow-left-right", style = "font-size: 2rem; color: #e8a435;"),
              div(
                tags$h5("How do starters compare to the bench?", class = "card-title mb-1"),
                tags$small(class = "text-muted", "Compare any two situations side-by-side")
              ),
              div(
                class = "ms-auto",
                actionButton("go_compare", "Go \u2192", class = "btn btn-outline-warning btn-sm")
              )
            )
          )
        )
      )
    )
  )
)
