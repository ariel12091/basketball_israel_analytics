# Coverage gate: every feature id in a "done" section of the API feature
# inventory must be named by a covers(...) call in some test file.
source(repo_file("..", "api", "coverage.R"), local = TRUE)

inv_path <- repo_file("..", "docs", "superpowers", "specs",
                      "2026-10-19-api-mcp-feature-inventory.md")
test_dirs <- c(repo_file("tests", "testthat"),
               repo_file("..", "api", "tests", "testthat"))

test_that("inventory has 109 unique ids", {
  ids <- inventory_ids(inv_path)
  expect_equal(length(ids), 109L)
  expect_false(anyDuplicated(ids) > 0L)
})

test_that("every id under a done prefix is covered", {
  ids <- inventory_ids(inv_path)
  missing <- uncovered_ids(ids, covered_ids(test_dirs), API_DONE_PREFIXES)
  if (length(missing)) message("Uncovered: ", paste(missing, collapse = ", "))
  expect_equal(missing, character())
})

test_that("the gate can fail", {
  tmp <- tempfile(fileext = ".md")
  writeLines(c("| ID | Feature |", "|---|---|", "| ZZ-01 | fake | UI |"), tmp)
  ids <- inventory_ids(tmp)
  expect_equal(ids, "ZZ-01")
  expect_equal(uncovered_ids(ids, covered_ids(test_dirs), "ZZ"), "ZZ-01")
})

test_that("covered_ids parses covers() calls, including c() and missing dirs", {
  d <- tempfile(); dir.create(d)
  writeLines(c('test_that("x", {', '  covers("AA-01", c("AA-02", "AA-03"))',
               '  # covers("AA-09")', '})'), file.path(d, "test-a.R"))
  expect_setequal(covered_ids(c(d, file.path(d, "nope"))),
                  c("AA-01", "AA-02", "AA-03"))
})

test_that("strict mode: nothing in the inventory is uncovered", {
  skip_if_not(Sys.getenv("IBPL_COVERAGE_STRICT") == "1")
  ids <- inventory_ids(inv_path)
  expect_equal(uncovered_ids(ids, covered_ids(test_dirs), unique(sub("-.*", "", ids))),
               character())
})
