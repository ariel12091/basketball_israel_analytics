test_that("tab1 reset smoke", {
  shiny::testServer(function(input, output, session) {
    server_tab1(input, output, session, shared = make_shared())
  }, {
    session$setInputs(game_year = "2026", onoff_view_mode = "Summary")
    session$setInputs(reset_defaults = 1)
    session$flushReact()
    expect_true(TRUE)
  })
})

test_that("tab2 reset smoke", {
  shiny::testServer(function(input, output, session) {
    server_tab2(input, output, session, shared = make_shared())
  }, {
    session$setInputs(game_year = "2026", ld_view_mode = "Summary")
    session$setInputs(ld_reset = 1)
    session$flushReact()
    expect_true(TRUE)
  })
})

test_that("tab3 reset smoke", {
  shiny::testServer(function(input, output, session) {
    server_tab3(input, output, session, shared = make_shared())
  }, {
    session$setInputs(game_year = "2026", tr_view_mode = "Summary")
    session$setInputs(tr_reset = 1)
    session$flushReact()
    expect_true(TRUE)
  })
})

test_that("tab4 reset smoke", {
  shiny::testServer(function(input, output, session) {
    server_tab4(input, output, session, shared = make_shared())
  }, {
    session$setInputs(game_year = "2026", gl_view_mode = "Summary")
    session$setInputs(gl_reset = 1)
    session$flushReact()
    expect_true(TRUE)
  })
})

test_that("tab5 reset smoke", {
  shiny::testServer(function(input, output, session) {
    server_tab5_traditional(input, output, session, shared = make_shared())
  }, {
    session$setInputs(game_year = "2026", ts_display_mode = "Per Game")
    session$setInputs(ts_reset = 1)
    session$flushReact()
    expect_true(TRUE)
  })
})
