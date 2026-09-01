server_path <- repo_file("R", "server_tab3.R")

read_server_contract <- function() {
  paste(readLines(server_path, warn = FALSE), collapse = "\n")
}

extract_contract_block <- function(source, start, end) {
  start_at <- regexpr(start, source, fixed = TRUE)[1]
  expect_gt(start_at, 0)
  remainder <- substring(source, start_at)
  end_at <- regexpr(end, remainder, fixed = TRUE)[1]
  expect_gt(end_at, 0)
  substring(remainder, 1, end_at - 1)
}

test_that("standard-clutch Four Factors and Minutes share one dashboard reactive", {
  source <- read_server_contract()
  dashboard <- extract_contract_block(
    source, "tr_ff_dashboard_data <- reactive({", "tr_ff_data <- reactive({"
  )
  four_factors <- extract_contract_block(
    source, "tr_ff_data <- reactive({", "tr_prev_data <- reactive({"
  )
  render <- extract_contract_block(
    source, "output$tr_table <- renderDT({", "output$tr_filter_chips <- renderUI({"
  )

  expect_match(dashboard, 'clutch_reader_kind\\(p\\), "dynamic"')
  expect_match(dashboard, "run_team_ff_dashboard_dynamic\\(pg_pool, p\\)")
  expect_match(four_factors, "tr_ff_dashboard_data\\(\\)")
  expect_match(render, 'identical\\(mode, "Four Factors"\\)')
  expect_match(render, "tr_ff_dashboard_data\\(\\)")
})

test_that("the shared reader does not broaden Summary or other filter routes", {
  source <- read_server_contract()
  summary <- extract_contract_block(
    source, "tr_data <- reactive({", "tr_ff_dashboard_data <- reactive({"
  )
  four_factors <- extract_contract_block(
    source, "tr_ff_data <- reactive({", "tr_prev_data <- reactive({"
  )

  expect_match(summary, "run_team_ratings_dynamic\\(")
  expect_false(grepl("ff_dashboard", summary, fixed = TRUE))
  expect_match(four_factors, "run_team_ff_dynamic\\(")
  expect_match(four_factors, 'clutch_reader_kind\\(p\\), "dynamic"')
  expect_equal(
    lengths(regmatches(source, gregexpr(
      "basketball_test.get_team_four_factors_dashboard_dynamic", source,
      fixed = TRUE
    ))),
    1L
  )
})
