# Read-only live acceptance checks using the existing app login.
if(file.exists('app/.Renviron')) readRenviron('app/.Renviron')
Sys.setenv(RUN_DB_TESTS='1',LC_ALL='C')
setwd('app/tests/testthat')
library(testthat)
source('helper-paths.R')
for(path in c('test-db-security-contracts.R','test-web-role-db.R','test-app-api-game-logs-db.R')) {
 result <- test_file(path,reporter='summary')
 outcomes <- as.data.frame(result)
 if(any(outcomes$failed>0 | outcomes$error | outcomes$skipped)) stop('Required proof failed or skipped: ',path)
}
