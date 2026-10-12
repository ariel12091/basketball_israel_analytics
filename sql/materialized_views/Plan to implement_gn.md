 Plan to implement                                                                                                                                        │
│                                                                                                                                                          │
│ Plan: GN (Game Number) Filters                                                                                                                           │
│                                                                                                                                                          │
│ Context                                                                                                                                                  │
│                                                                                                                                                          │
│ Add GN-based game filters to all 4 tabs. Two mutually exclusive modes:                                                                                   │
│ 1. GN Range — between GN X and GN Y (like date range)                                                                                                    │
│ 2. Last X Games — per-team: each team's max GN minus user input                                                                                          │
│                                                                                                                                                          │
│ GN (gn) already exists in final_schedule_mv (line 8). All SQL functions already have a games_base/sched CTE that reads from this MV, so adding GN WHERE  │
│ clauses is straightforward.                                                                                                                              │
│                                                                                                                                                          │
│ ---                                                                                                                                                      │
│ Step 1: SQL Functions — Add 3 new params                                                                                                                 │
│                                                                                                                                                          │
│ Add to all 6 functions: p_min_gn INT DEFAULT NULL, p_max_gn INT DEFAULT NULL, p_last_n_games INT DEFAULT NULL                                            │
│                                                                                                                                                          │
│ For each function, add to its schedule-filtering CTE (NOT the opponent-ranking CTEs):                                                                    │
│                                                                                                                                                          │
│ AND (p_min_gn IS NULL OR fs.gn >= p_min_gn)                                                                                                              │
│ AND (p_max_gn IS NULL OR fs.gn <= p_max_gn)                                                                                                              │
│ AND (p_last_n_games IS NULL                                                                                                                              │
│      OR fs.gn >= (SELECT MAX(fs2.gn) - p_last_n_games + 1                                                                                                │
│                   FROM basketball_test.final_schedule_mv fs2                                                                                             │
│                   WHERE fs2.team_id = fs.team_id                                                                                                         │
│                     AND fs2.game_year = fs.game_year))                                                                                                   │
│                                                                                                                                                          │
│ Files & CTE locations:                                                                                                                                   │
│ ┌─────────────────────────────────────────────────┬─────────────┬──────────────────────────────────────────────────────────────────────────────┐         │
│ │                      File                       │ Param count │                                CTE to modify                                 │         │
│ ├─────────────────────────────────────────────────┼─────────────┼──────────────────────────────────────────────────────────────────────────────┤         │
│ │ sql/functions/onoff_compute.sql                 │ 14→17       │ sched CTE (line ~140)                                                        │         │
│ ├─────────────────────────────────────────────────┼─────────────┼──────────────────────────────────────────────────────────────────────────────┤         │
│ │ sql/functions/four_factors_compute.sql          │ 11→14       │ games_base CTE (line ~86)                                                    │         │
│ ├─────────────────────────────────────────────────┼─────────────┼──────────────────────────────────────────────────────────────────────────────┤         │
│ │ sql/functions/fetch_lineups_all.sql             │ 20→23       │ games_base in BOTH clutch path (line ~88) AND non-clutch path (need to find) │         │
│ ├─────────────────────────────────────────────────┼─────────────┼──────────────────────────────────────────────────────────────────────────────┤         │
│ │ sql/functions/fetch_lineups_four_factors.sql    │ 20→23       │ games_base in both paths                                                     │         │
│ ├─────────────────────────────────────────────────┼─────────────┼──────────────────────────────────────────────────────────────────────────────┤         │
│ │ sql/functions/get_team_ratings_dynamic.sql      │ 14→17       │ games_base CTE (line ~66)                                                    │         │
│ ├─────────────────────────────────────────────────┼─────────────┼──────────────────────────────────────────────────────────────────────────────┤         │
│ │ sql/functions/get_team_four_factors_dynamic.sql │ 14→17       │ games_base CTE                                                               │         │
│ └─────────────────────────────────────────────────┴─────────────┴──────────────────────────────────────────────────────────────────────────────┘         │
│ Fast path disabling in fetch_lineups_all: Add to fast path check (line ~52):                                                                             │
│ AND p_min_gn IS NULL AND p_max_gn IS NULL AND p_last_n_games IS NULL                                                                                     │
│                                                                                                                                                          │
│ DROP FUNCTION: Each file starts with a DROP with exact old signature. Update the DROP to match the old param count, then CREATE with the new params. The │
│  old DROP lines already exist — just verify they match the current deployed signatures.                                                                  │
│                                                                                                                                                          │
│ ---                                                                                                                                                      │
│ Step 2: UI — Add GN filter controls to all 4 tabs                                                                                                        │
│                                                                                                                                                          │
│ Inside each tab's "Game Filters" accordion_panel, add after the existing outcome filter:                                                                 │
│                                                                                                                                                          │
│ tags$hr(),                                                                                                                                               │
│ radioButtons("XX_gn_mode", "Game Number Filter",                                                                                                         │
│              choices = c("Off" = "off", "GN Range" = "range", "Last X Games" = "last"),                                                                  │
│              selected = "off", inline = TRUE),                                                                                                           │
│ conditionalPanel(                                                                                                                                        │
│   condition = "input.XX_gn_mode == 'range'",                                                                                                             │
│   fluidRow(                                                                                                                                              │
│     column(6, numericInput("XX_gn_min", "From GN", value = NA, min = 1, step = 1)),                                                                      │
│     column(6, numericInput("XX_gn_max", "To GN", value = NA, min = 1, step = 1))                                                                         │
│   )                                                                                                                                                      │
│ ),                                                                                                                                                       │
│ conditionalPanel(                                                                                                                                        │
│   condition = "input.XX_gn_mode == 'last'",                                                                                                              │
│   numericInput("XX_last_n", "Last N games", value = 5, min = 1, step = 1)                                                                                │
│ )                                                                                                                                                        │
│                                                                                                                                                          │
│ Prefix mapping: on_ (Tab 1), ld_ (Tab 2), tr_ (Tab 3), gl_ (Tab 4)                                                                                       │
│                                                                                                                                                          │
│ Files:                                                                                                                                                   │
│ - app/R/ui_tab1_onoff.R — inside the bslib::accordion_panel("Game Filters", ...) block (line ~43)                                                        │
│ - app/R/ui_tab2_lineup.R — inside bslib::accordion_panel("Game Filters", ...) (line ~37)                                                                 │
│ - app/R/ui_tab3_team.R — inside bslib::accordion_panel("Game Filters", ...) (line ~28)                                                                   │
│ - app/R/ui_tab4_gamelogs.R — inside bslib::accordion_panel("Game Filters", ...) (line ~31)                                                               │
│                                                                                                                                                          │
│ ---                                                                                                                                                      │
│ Step 3: Server — Extract GN params & pass to SQL                                                                                                         │
│                                                                                                                                                          │
│ Helper pattern (same for all tabs):                                                                                                                      │
│                                                                                                                                                          │
│ gn_mode <- input$XX_gn_mode %||% "off"                                                                                                                   │
│ min_gn <- if (identical(gn_mode, "range") && !is.na(input$XX_gn_min)) as.integer(input$XX_gn_min) else NA_integer_                                       │
│ max_gn <- if (identical(gn_mode, "range") && !is.na(input$XX_gn_max)) as.integer(input$XX_gn_max) else NA_integer_                                       │
│ last_n <- if (identical(gn_mode, "last") && !is.na(input$XX_last_n)) as.integer(input$XX_last_n) else NA_integer_                                        │
│                                                                                                                                                          │
│ Tab 1 (server_tab1.R):                                                                                                                                   │
│                                                                                                                                                          │
│ - run_onoff_compute_14 → run_onoff_compute_17: Add min_gn, max_gn, last_n_games params (positions 15-17)                                                 │
│ - run_four_factors_compute → add 3 params (positions 12-14)                                                                                              │
│ - fallback_needed() (line ~55): Add gn_mode != "off" to the trigger condition                                                                            │
│ - live_result_df() / live_ff_result_df(): Extract GN params, pass to functions                                                                           │
│ - ff_ranked_df(): Same — passes to both run_four_factors_compute and run_onoff_compute_14                                                                │
│ - bindEvent() lists: Add input$on_gn_mode, input$on_gn_min, input$on_gn_max, input$on_last_n                                                             │
│ - Reset handler (input$reset_defaults): Add reset for GN inputs                                                                                          │
│                                                                                                                                                          │
│ Tab 2 (server_tab2.R):                                                                                                                                   │
│                                                                                                                                                          │
│ - run_fetch_lineups_20 → run_fetch_lineups_23: Add 3 params (positions 21-23)                                                                            │
│ - run_fetch_lineups_ff_20 → same                                                                                                                         │
│ - ld_ff_ranked_df() / ld_summary_ranked_df(): Extract GN params, pass to functions                                                                       │
│ - ld_params(): Include GN params for the bindEvent() trigger                                                                                             │
│ - bindEvent() lists: Add GN inputs                                                                                                                       │
│ - Reset handler (input$ld_reset): Reset GN inputs                                                                                                        │
│                                                                                                                                                          │
│ Tab 3 (server_tab3.R):                                                                                                                                   │
│                                                                                                                                                          │
│ - run_team_ratings_dynamic → add 3 params (positions 15-17)                                                                                              │
│ - run_team_ff_dynamic → same                                                                                                                             │
│ - tr_params() (line ~47): Include GN params in the list                                                                                                  │
│ - tr_fallback_needed() (line ~80): Add has_gn <- !is.na(p$min_gn) || !is.na(p$max_gn) || !is.na(p$last_n_games)                                          │
│ - Reset handler (input$tr_reset): Reset GN inputs                                                                                                        │
│                                                                                                                                                          │
│ Tab 4 (server_tab4.R):                                                                                                                                   │
│                                                                                                                                                          │
│ - No SQL function changes — Tab 4 uses R-side filtering via gl_filtered_schedule()                                                                       │
│ - Add to gl_filtered_schedule() (after outcome filter, line ~97):                                                                                        │
│ # GN filter                                                                                                                                              │
│ gn_mode <- input$gl_gn_mode %||% "off"                                                                                                                   │
│ if (identical(gn_mode, "range")) {                                                                                                                       │
│   if (!is.na(input$gl_gn_min)) df <- df %>% filter(gn >= !!as.integer(input$gl_gn_min))                                                                  │
│   if (!is.na(input$gl_gn_max)) df <- df %>% filter(gn <= !!as.integer(input$gl_gn_max))                                                                  │
│ } else if (identical(gn_mode, "last")) {                                                                                                                 │
│   last_n <- as.integer(input$gl_last_n %||% 5)                                                                                                           │
│   if (!is.na(last_n)) {                                                                                                                                  │
│     max_gns <- df %>% group_by(team_id) %>% summarise(max_gn = max(gn, na.rm = TRUE), .groups = "drop")                                                  │
│     df <- df %>% inner_join(max_gns, by = "team_id") %>%                                                                                                 │
│       filter(gn >= max_gn - last_n + 1) %>% select(-max_gn)                                                                                              │
│   }                                                                                                                                                      │
│ }                                                                                                                                                        │
│ - Reset handler (input$gl_reset): Reset GN inputs                                                                                                        │
│                                                                                                                                                          │
│ ---                                                                                                                                                      │
│ Step 4: Deploy Script                                                                                                                                    │
│                                                                                                                                                          │
│ Create deploy_gn_filter.R:                                                                                                                               │
│ 1. Connect to Supabase port 5432 (DDL) using etl/.Renviron                                                                                               │
│ 2. For each of the 6 SQL function files:                                                                                                                 │
│   - Read the SQL file                                                                                                                                    │
│   - Execute the DROP + CREATE (the file already contains both)                                                                                           │
│ 3. No MV rebuild needed — gn is already in final_schedule_mv                                                                                             │
│                                                                                                                                                          │
│ ---                                                                                                                                                      │
│ Step 5: Reset handlers                                                                                                                                   │
│                                                                                                                                                          │
│ Each tab's reset handler must clear the GN filter:                                                                                                       │
│ updateRadioButtons(session, "XX_gn_mode", selected = "off")                                                                                              │
│ updateNumericInput(session, "XX_gn_min", value = NA)                                                                                                     │
│ updateNumericInput(session, "XX_gn_max", value = NA)                                                                                                     │
│ updateNumericInput(session, "XX_last_n", value = 5)                                                                                                      │
│                                                                                                                                                          │
│ ---                                                                                                                                                      │
│ Verification                                                                                                                                             │
│                                                                                                                                                          │
│ 1. SQL: After deploying, test each function with GN params:                                                                                              │
│ SELECT COUNT(*) FROM basketball_test.onoff_compute(                                                                                                      │
│   '2025-10-01','2026-07-01',NULL,0,0,-1e9,'2026',                                                                                                        │
│   NULL,NULL,'all','all',NULL,NULL,NULL,                                                                                                                  │
│   4, 9, NULL  -- min_gn=4, max_gn=9, last_n=NULL                                                                                                         │
│ );                                                                                                                                                       │
│ 2. App: Run locally, test each tab:                                                                                                                      │
│   - Set GN Range 4-9 → verify data narrows                                                                                                               │
│   - Set Last 5 games → verify per-team filtering                                                                                                         │
│   - Toggle between Off/Range/Last → verify mutual exclusivity                                                                                            │
│   - Reset → verify all GN inputs clear                                                                                                                   │
│   - Verify fallback_needed() triggers on Tabs 1 & 3 when GN filter active                                                                                │
│ 3. Tab 4: Verify GN filtering works R-side (no SQL function involved)                                                                                    │
╰──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯

● I'll implement the GN (Game Number) filters across all 4 tabs. Let me start by reading the key files to understand the current state.

● Let me read all the SQL function files and UI/server files in parallel.

● Read 2 files (ctrl+o to expand)

● Now let me read the remaining SQL files and UI/server files in parallel.

● Read 4 files (ctrl+o to expand)

● Good, now I have all 6 SQL files. Let me read the UI and server files in parallel.

● Read 4 files (ctrl+o to expand)
