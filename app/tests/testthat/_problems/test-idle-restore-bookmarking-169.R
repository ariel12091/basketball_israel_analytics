# Extracted from test-idle-restore-bookmarking.R:169

# prequel ----------------------------------------------------------------------
fake_restore_session <- function(query_string) {
  ctx <- shiny:::RestoreContext$new(query_string)
  list(restoreContext = ctx, request = list(QUERY_STRING = query_string))
}

# test -------------------------------------------------------------------------
app_r_txt <- read_repo_txt("app.R")
tab3_txt <- read_repo_txt("R", "server_tab3.R")
tab4_txt <- read_repo_txt("R", "server_tab4.R")
tab5_txt <- read_repo_txt("R", "server_tab5_traditional.R")
tab7_txt <- read_repo_txt("R", "server_tab7_compare.R")
mod_txt  <- read_repo_txt("R", "mod_lineup_player_filter.R")
euro_global <- read_repo_txt("R", "global_euro.R")
euro_lineups <- read_repo_txt("R", "server_tab10_euro_lineups.R")
expect_false(grepl(
    'updateSelectizeInput(session, "teams", choices = team_choices, selected = character(0)',
    app_r_txt, fixed = TRUE
  ))
expect_match(app_r_txt, "update_restore_aware_selectize(", fixed = TRUE)
for (txt in list(tab3_txt, tab4_txt, tab5_txt, tab7_txt)) {
    expect_match(txt, "restore_aware_selection(", fixed = TRUE)
  }
expect_match(mod_txt, "restore_seed <- new.env(", fixed = TRUE)
expect_match(mod_txt, 'restored_input_value(session, "team")', fixed = TRUE)
expect_match(mod_txt, "selection_with_restore_seed(", fixed = TRUE)
expect_match(euro_global, "update_restore_aware_selectize(", fixed = TRUE)
expect_match(
    euro_global,
    'restore_once_selection(\n      session, "euro_game_year"',
    fixed = TRUE
  )
expect_match(euro_lineups, 'session, "euro_ld_opponents"', fixed = TRUE)
expect_match(euro_lineups, 'session, "euro_ld_phase"', fixed = TRUE)
expect_match(app_r_txt, "update_restore_aware_selectize(session, input, id, team_choices)", fixed = TRUE)
