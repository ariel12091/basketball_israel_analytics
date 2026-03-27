test_that("tab7 teams compare uses four-factor query for four-factor chips", {
  shiny::testServer(function(input, output, session) {
    server_tab7_compare(input, output, session, shared = make_shared())
  }, {
    session$setInputs(
      main_tabs = "compare",
      game_year = "2026",
      cmp_mode = "Teams"
    )
    session$flushReact()
    session$setInputs(cmp_metric = "off_ts")
    session$flushReact()

    expect_equal(output$cmp_summary_a_label, "TS%")
    expect_equal(output$cmp_summary_b_label, "TS%")
    expect_equal(output$cmp_summary_a, "54.8")
    expect_equal(output$cmp_summary_b, "54.8")
    expect_equal(output$cmp_summary_gap, "0.0")
  })
})

test_that("tab7 lineups compare uses four-factor query for four-factor chips", {
  shiny::testServer(function(input, output, session) {
    server_tab7_compare(input, output, session, shared = make_shared())
  }, {
    session$setInputs(
      main_tabs = "compare",
      game_year = "2026",
      cmp_mode = "Lineups"
    )
    session$flushReact()
    session$setInputs(cmp_metric = "off_oreb")
    session$flushReact()

    expect_equal(output$cmp_summary_a_label, "OREB%")
    expect_equal(output$cmp_summary_b_label, "OREB%")
    expect_equal(output$cmp_summary_a, "31.0")
    expect_equal(output$cmp_summary_b, "31.0")
    expect_equal(output$cmp_summary_gap, "0.0")
  })
})
