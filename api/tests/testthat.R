# API test suite: Rscript -e "testthat::test_dir('api/tests/testthat')" from
# the repo root, or source this file from api/ (scripts/test_all.R does).
library(testthat)

test_dir("tests/testthat", reporter = "summary")
