# CSV export: formula cells defused (docs/shared_definitions.md, section 3,
# "Exports"). A text cell whose first character is in CSV_FORMULA_PREFIXES
# and which is not a number is written with a leading single quote, so a
# spreadsheet never runs it as a formula. One rule, two surfaces: the API's
# CSV (csv_defuse_formula(), from api_respond()) and every Shiny CSV button
# (csv_export_button()'s body formatter, generated from the same constants).

test_that("the rule's constants", {
  expect_identical(CSV_FORMULA_PREFIXES, c("=", "+", "-", "@", "\t", "\r"))
  expect_true(is.character(CSV_NUMERIC_TEXT_PATTERN) && length(CSV_NUMERIC_TEXT_PATTERN) == 1L)
})

test_that("csv_defuse_formula() quotes formula text and leaves numbers and plain text", {
  x <- c("=1+1", "@x", "-x", "+x", "\tx", "\rx", "=", "plain", "79-83", "-5", "+1.5", "-3.2%", "-.5",
         "1e3", "", NA)
  expect_identical(
    csv_defuse_formula(x),
    c("'=1+1", "'@x", "'-x", "'+x", "'\tx", "'\rx", "'=", "plain", "79-83", "-5", "+1.5", "-3.2%", "-.5",
      "1e3", "", NA)
  )
  expect_identical(csv_defuse_formula(character(0)), character(0))
  # A factor is text too; numbers are returned as they are.
  expect_identical(csv_defuse_formula(factor(c("=x", "a"))), c("'=x", "a"))
  expect_identical(csv_defuse_formula(c(-5, 1)), c(-5, 1))
})

test_that("csv_export_button() carries the body formatter built from the shared constants", {
  b <- csv_export_button("x", now = as.POSIXct("2026-01-02 03:04:05", tz = "UTC"))[[1]]
  body <- b$exportOptions$format$body
  expect_s3_class(body, "JS_EVAL")
  src <- paste(as.character(body), collapse = "\n")
  expect_match(src, "^function\\(data, row, column, node\\)")
  expect_true(grepl("stripData", src, fixed = TRUE))
  expect_true(grepl(as.character(jsonlite::toJSON(CSV_FORMULA_PREFIXES)), src, fixed = TRUE))
  expect_true(grepl(CSV_NUMERIC_TEXT_PATTERN, src, fixed = TRUE))
  # The rest of the contract is unchanged.
  expect_equal(b$exportOptions$columns, ":visible")
})

test_that("the generated JS applies the same rule as csv_defuse_formula() (V8)", {
  skip_if_not_installed("V8")
  ctx <- V8::v8()
  # DataTables' stripData (strip HTML, trim, decode) is the identity here:
  # the inputs carry no markup.
  ctx$eval("var $ = { fn: { dataTable: { Buttons: { stripData: function (d) { return d; } } } } }; var jQuery = $;")
  ctx$eval(paste0("var fmt = ", as.character(csv_body_formatter_js()), ";"))
  x <- c("=1+1", "@x", "-x", "+x", "\tx", "\rx", "=", "plain", "79-83", "-5", "+1.5", "-3.2%", "-.5",
         "1e3", "", "--5", "-5-5", "@5")
  ctx$assign("xs", x)
  got <- ctx$get("xs.map(function (v) { return fmt(v, 0, 0, null); })")
  expect_identical(got, csv_defuse_formula(x))
  # Numbers and null pass through as they are.
  expect_identical(ctx$get("fmt(-5, 0, 0, null)"), -5L)
  expect_true(ctx$get("fmt(null, 0, 0, null) === null"))
  # HTML is stripped first: the formatter defuses the text a reader sees.
  ctx$eval("$.fn.dataTable.Buttons.stripData = function (d) { return typeof d === 'string' ? d.replace(/<[^>]*>/g, '') : d; };")
  expect_identical(ctx$get("fmt('<span>=HYPERLINK(1)</span>', 0, 0, null)"), "'=HYPERLINK(1)")
})

test_that("every Shiny CSV button goes through csv_export_button()", {
  for (f in c("server_tab3.R", "server_tab4.R", "server_tab5_traditional.R",
              "server_tab9_euro_team.R", "server_tab11_euro_gamelogs.R")) {
    src <- paste(readLines(repo_file("R", f), warn = FALSE), collapse = "\n")
    expect_false(grepl('extend = "csv"', src, fixed = TRUE), info = f)
    expect_true(grepl("csv_export_button(", src, fixed = TRUE), info = f)
  }
})
