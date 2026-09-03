# ui_tab0_home.R - Tab 0: Home / Landing Page

# One builder for both leagues' Home cards. The blocks differ only in which tab
# each card opens, so the shape lives here and each block passes its own list;
# a card one league has and the other does not is one list entry, never a
# second copy of the markup.
#
# The card carries its own question and answer, so it is readable and clickable
# from the served HTML alone -- before the hub above it has queried anything.
# js-shiny-event keeps a click made in that window queued and replayed rather
# than dropped.
home_nav_cards <- function(items) {
  rows <- split(items, ceiling(seq_along(items) / 2))
  unname(lapply(rows, function(row) {
    div(
      class = "row home-nav-row",
      lapply(row, function(item) {
        column(
          width = 6,
          tags$button(
            type = "button",
            class = "home-nav-card js-shiny-event",
            `data-input-id` = item$input_id,
            tags$span(
              class = "home-nav-card-head",
              tags$i(class = paste("bi", item$icon), `aria-hidden` = "true"),
              tags$span(class = "home-nav-card-title", item$title)
            ),
            tags$span(class = "home-nav-card-sub", item$sub)
          )
        )
      })
    )
  }))
}

ui_tab0_home <- function() tabPanel(
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
          style = "font-weight: 700; color: var(--ibpl-text); margin-bottom: 4px;",
          "IBPL Analytics"
        ),
        tags$p(
          class = "league-only-il",
          style = "color: var(--ibpl-text-muted); font-size: .92rem; margin: 0;",
          "Your go-to site for Israel Basketball Premier League stats"
        ),
        tags$p(
          class = "league-only-el",
          style = "color: var(--ibpl-text-muted); font-size: .92rem; margin: 0;",
          "EuroLeague and EuroCup on/off, team and lineup stats"
        )
      ),

      # League chooser. Same three options as the navbar dropdown, in the same
      # order; both write that dropdown, so there is one code path. Picking one
      # filters the navbar to that league's tabs.
      div(
        class = "league-chooser",
        tags$button(
          type = "button", `data-league-btn` = "E",
          tags$span(class = "league-chooser-name", "EuroLeague"),
          tags$span(class = "league-chooser-sub", "On/off, lineups, team ratings, game logs, player stats")
        ),
        tags$button(
          type = "button", `data-league-btn` = "U",
          tags$span(class = "league-chooser-name", "EuroCup"),
          tags$span(class = "league-chooser-sub", "On/off, lineups, team ratings, game logs, player stats")
        ),
        tags$button(
          type = "button", `data-league-btn` = "il",
          tags$span(class = "league-chooser-name", "Israeli League"),
          tags$span(class = "league-chooser-sub", "Premier League — full stats suite")
        )
      ),

      # ---- EuroLeague ----
      # Same cards, same order, same icons and wording as the Israeli block
      # below -- only the tabs each one opens differ, and Player Stats does not
      # even differ there: it is one shared tab reading the league from
      # #league_select, so both blocks send go_playerstats and only one of the
      # two cards is ever visible. Compare is the only Israeli surface with no
      # EuroLeague counterpart, so it has no card here.
      div(
        class = "league-only-el",

        home_nav_cards(list(
          list(input_id = "go_euro_onoff",   icon = "bi-person-fill",
               title = "Who is helping my team?",
               sub   = "Player impact when on vs. off the court"),
          list(input_id = "go_euro_lineups", icon = "bi-people-fill",
               title = "Which lineups are working?",
               sub   = "Best and worst 5-man units by possessions"),
          list(input_id = "go_euro_team",    icon = "bi-bar-chart-fill",
               title = "How is my team performing?",
               sub   = "Offense, defense, net rating vs. the league"),
          list(input_id = "go_euro_gamelogs", icon = "bi-calendar-day-fill",
               title = "What happened in last night's game?",
               sub   = "Score, lineups, and stats by game"),
          list(input_id = "go_playerstats",  icon = "bi-bar-chart-line",
               title = "How are individual players performing?",
               sub   = "Points, rebounds, assists, shooting splits per player")
        )),

        tags$p(
          style = "color: var(--ibpl-text-dim); font-size: .8rem; text-align: center; margin-top: 4px;",
          "EuroLeague possessions come from a separate engine from the Israeli ",
          "league. The two are never ranked against each other."
        )
      ),

      # ---- Israeli league ----
      # Optional team selector
      div(
        class = "mb-4 home-team-controls league-only-il",
        div(
          class = "home-team-select",
          selectizeInput(
            "home_team",
            label = NULL,
            choices = with(
              static_team_roster(DEFAULT_GAME_YEAR),
              stats::setNames(as.character(team_id), team_name)
            ),
            selected = DEFAULT_HOME_TEAM_ID,
            options = list(placeholder = "All teams", preload = "focus")
          )
        ),
        div(
          class = "home-team-default",
          checkboxInput(
            "home_set_default",
            "Set as default",
            value = FALSE
          ),
          title = "Use this team on future visits"
        ),
        tags$script(HTML(
          "if (window.ibplApplyInitialHubTeamDefault) window.ibplApplyInitialHubTeamDefault();"
        ))
      ),

      div(
        class = "league-only-il",

        team_hub_ui(),

        home_nav_cards(list(
          list(input_id = "go_onoff",       icon = "bi-person-fill",
               title = "Who is helping my team?",
               sub   = "Player impact when on vs. off the court"),
          list(input_id = "go_lineups",     icon = "bi-people-fill",
               title = "Which lineups are working?",
               sub   = "Best and worst 5-man units by possessions"),
          list(input_id = "go_team",        icon = "bi-bar-chart-fill",
               title = "How is my team performing?",
               sub   = "Offense, defense, net rating vs. the league"),
          list(input_id = "go_gamelogs",    icon = "bi-calendar-day-fill",
               title = "What happened in last night's game?",
               sub   = "Score, lineups, and stats by game"),
          list(input_id = "go_playerstats", icon = "bi-bar-chart-line",
               title = "How are individual players performing?",
               sub   = "Points, rebounds, assists, shooting splits per player"),
          list(input_id = "go_compare",     icon = "bi-arrow-left-right",
               title = "How do starters compare to the bench?",
               sub   = "Compare any two situations side-by-side")
        ))
      )
    )
  )
)
