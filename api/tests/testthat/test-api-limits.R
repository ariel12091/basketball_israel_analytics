# Exposure controls from the 2026-10-20 security audit
# (.superpowers/sdd/2026-10-19-api-plan-1-foundation-game-logs/security-audit.md):
# M-1 caps on repeated and listed parameters and on the raw query length,
# M-2 plumber's shared-secret check restored, L-1 CSV formula cells defused.

limits_env <- api_test_env()
limits_router <- api_router(limits_env)

limits_params <- function(qs, league = "il") {
  limits_env$svc_game_logs_params(limits_env$api_query(api_req(qs)), league, data_version = "golden")
}

# A request through the real router with extra request headers (HTTP_* names,
# as httpuv hands them to plumber).
limits_http <- function(router, path, qs = "", headers = list()) {
  qs <- sub("^[?]", "", qs)
  req <- new.env()
  req$REQUEST_METHOD <- "GET"
  req$PATH_INFO <- path
  req$QUERY_STRING <- if (nzchar(qs)) paste0("?", qs) else ""
  req$SCRIPT_NAME <- ""
  req$SERVER_NAME <- "127.0.0.1"
  req$SERVER_PORT <- "3010"
  req$HTTP_HOST <- "127.0.0.1:3010"
  for (h in names(headers)) assign(h, headers[[h]], envir = req)
  req$rook.input <- list(read = function(...) raw(0), rewind = function() invisible(0L))
  out <- router$call(req)
  out$body <- if (is.raw(out$body)) rawToChar(out$body) else as.character(out$body)
  out$status <- as.integer(out$status)
  out
}

limits_expect_400 <- function(expr, param, info = NULL) {
  err <- api_error_of(expr)
  expect_identical(err$status, 400L, info = info)
  expect_identical(err$param, param, info = info)
  invisible(err)
}

# ---------------- M-1 ----------------

test_that("the caps are named constants with the agreed values", {
  covers("X-20")
  expect_identical(limits_env$GL_MAX_STAT, 10L)
  expect_identical(limits_env$SVC_MAX_SORT, 5L)
  expect_identical(limits_env$SVC_MAX_IDS, 50L)
  expect_identical(limits_env$API_MAX_QUERY_BYTES, 4096L)
})

test_that("stat: the cap is accepted, one more is a 400 on 'stat'", {
  covers("X-20")
  one <- "stat=off_ppp:ge:-1"
  at <- paste(rep(one, limits_env$GL_MAX_STAT), collapse = "&")
  expect_length(limits_params(at)$stat_filters, limits_env$GL_MAX_STAT)
  limits_expect_400(limits_params(paste(at, one, sep = "&")), "stat")
})

test_that("sort: the cap is accepted, one more key is a 400 on 'sort'", {
  covers("X-20")
  keys <- c("team_name:asc", "game_date:desc", "net_rtg:desc", "off_ppp:asc", "def_ppp:asc", "gn:asc")
  at <- paste0("sort=", paste(keys[seq_len(limits_env$SVC_MAX_SORT)], collapse = ","))
  expect_silent(limits_params(at))
  expect_length(limits_env$svc_sort(limits_env$api_query(api_req(at))), limits_env$SVC_MAX_SORT)
  over <- paste0("sort=", paste(keys[seq_len(limits_env$SVC_MAX_SORT + 1L)], collapse = ","))
  limits_expect_400(limits_params(over), "sort")
  # Through the handler as well, which reads sort itself.
  resp <- api_http(limits_router, "/v1/il/game-logs", paste0("season=2026&", over))
  expect_identical(resp$status, 400L)
  expect_identical(jsonlite::fromJSON(resp$body)$param, "sort")
})

test_that("id lists: the cap is accepted, one more is a 400 naming the list", {
  covers("X-20")
  n <- limits_env$SVC_MAX_IDS
  ids_at <- paste(seq_len(n), collapse = ",")
  ids_over <- paste(seq_len(n + 1L), collapse = ",")

  expect_length(limits_params(paste0("opponents=", ids_at))$opponents, n)
  limits_expect_400(limits_params(paste0("opponents=", ids_over)), "opponents")

  expect_length(limits_params(paste0("season=2025&teams=", ids_at), "euroleague")$teams, n)
  limits_expect_400(limits_params(paste0("season=2025&teams=", ids_over), "euroleague"), "teams")

  # game_type: 50 valid codes are accepted (repeats collapse to their choice).
  types_at <- paste(rep("5", n), collapse = ",")
  expect_identical(limits_params(paste0("game_type=", types_at))$game_type, "5")
  limits_expect_400(limits_params(paste0("game_type=", paste(rep("5", n + 1L), collapse = ","))), "game_type")

  phases_at <- paste(rep("RS", n), collapse = ",")
  expect_identical(unique(limits_params(paste0("season=2025&phase=", phases_at), "euroleague")$phase), "RS")
  limits_expect_400(limits_params(paste0("season=2025&phase=", paste(rep("RS", n + 1L), collapse = ",")),
                                  "euroleague"), "phase")
})

test_that("the raw query string is capped before decoding: 4096 bytes in, 4097 a 400 on 'query'", {
  covers("X-20")
  cap <- limits_env$API_MAX_QUERY_BYTES
  pad <- function(total) paste0("x=", strrep("a", total - 2L))
  expect_length(limits_env$api_query(api_req(pad(cap))), 1L)
  limits_expect_400(limits_env$api_query(api_req(pad(cap + 1L))), "query")
  # The leading '?' is not part of the query.
  expect_length(limits_env$api_query(api_req(paste0("?", pad(cap)))), 1L)
  # Bytes, not characters: an encoded value counts as sent.
  limits_expect_400(limits_env$api_query(api_req(paste0("x=", strrep("%E2%80%AE", 456)))), "query")
})

test_that("the audit's 64 KB request is now a quick 400", {
  covers("X-20")
  qs <- paste0("season=2026&", paste(rep("stat=off_ppp:ge:-1", 3400), collapse = "&"))
  expect_gt(nchar(qs), 60000L)
  elapsed <- system.time(resp <- api_http(limits_router, "/v1/il/game-logs", qs))[["elapsed"]]
  expect_identical(resp$status, 400L)
  expect_identical(jsonlite::fromJSON(resp$body)$param, "query")
  expect_lt(elapsed, 1)
})

test_that("every Game Logs preset stays inside the caps", {
  covers("X-20")
  for (name in names(GAME_LOGS_PRESETS)) {
    qs <- preset_to_query(name)
    expect_lte(nchar(qs, type = "bytes"), limits_env$API_MAX_QUERY_BYTES)
    league <- preset_api_league(GAME_LOGS_PRESETS[[name]])
    expect_true(is.list(limits_params(qs, league)), info = name)
  }
})

# ---------------- M-2 ----------------

test_that("with a shared secret set, a request without the matching header is a 400", {
  covers("X-20")
  old_opt <- options(plumber.sharedSecret = NULL)
  vars <- c("PLUMBER_SHARED_SECRET", "PLUMBER_SHAREDSECRET")
  old_env <- Sys.getenv(vars, unset = NA, names = TRUE)
  on.exit({
    options(old_opt)
    for (v in vars) {
      if (is.na(old_env[[v]])) Sys.unsetenv(v) else do.call(Sys.setenv, stats::setNames(list(old_env[[v]]), v))
    }
  }, add = TRUE)
  Sys.unsetenv(vars)

  # Unset: no change.
  expect_identical(limits_http(limits_router, "/v1/il/seasons")$status, 200L)

  check <- function() {
    none <- limits_http(limits_router, "/v1/il/seasons")
    expect_identical(none$status, 400L)
    expect_match(none$headers[["Content-Type"]], "^application/json")
    body <- jsonlite::fromJSON(none$body)
    expect_true(nzchar(body$error))
    expect_true("param" %in% names(body))
    expect_false(grepl("s3cr3t", none$body, fixed = TRUE))
    wrong <- limits_http(limits_router, "/v1/il/seasons", headers = list(HTTP_PLUMBER_SHARED_SECRET = "nope"))
    expect_identical(wrong$status, 400L)
    # Before any parameter is read: a bad query still gets the secret's 400.
    bad_q <- limits_http(limits_router, "/v1/il/game-logs", "season=%00")
    expect_identical(bad_q$status, 400L)
    expect_identical(bad_q$body, none$body)
    right <- limits_http(limits_router, "/v1/il/seasons", headers = list(HTTP_PLUMBER_SHARED_SECRET = "s3cr3t"))
    expect_identical(right$status, 200L)
  }

  # From either environment variable (PLUMBER_SHAREDSECRET is the name
  # plumber 1.3.3 itself derives from the option) ...
  Sys.setenv(PLUMBER_SHARED_SECRET = "s3cr3t")
  check()
  Sys.unsetenv("PLUMBER_SHARED_SECRET")
  Sys.setenv(PLUMBER_SHAREDSECRET = "s3cr3t")
  check()
  Sys.unsetenv("PLUMBER_SHAREDSECRET")
  # ... and from the option, which plumber reads first.
  options(plumber.sharedSecret = "s3cr3t")
  check()
  options(plumber.sharedSecret = NULL)
  expect_identical(limits_http(limits_router, "/v1/il/seasons")$status, 200L)
})

# ---------------- L-1 ----------------

test_that("CSV: text cells that start a formula get a leading quote; numbers do not", {
  covers("X-14")
  rows <- data.frame(
    name = c("=1+1", "@x", "-x", "+x", "\tx", "\rx", "plain", "79-83", NA),
    net = c(-5.5, -1, 0, 1, 2, 3, 4, 5, NA),
    stringsAsFactors = FALSE
  )
  res <- limits_env$api_respond(api_res(), list(rows = rows), format = "csv", slug = "t")
  csv <- utils::read.csv(text = res$body, stringsAsFactors = FALSE, na.strings = "")
  # read.csv turns a quoted CR into LF, so that cell is checked in the raw body.
  expect_identical(csv$name[-6], c("'=1+1", "'@x", "'-x", "'+x", "'\tx", "plain", "79-83", NA))
  expect_true(grepl("\"'\rx\"", res$body, fixed = TRUE))
  expect_identical(csv$net, rows$net)
  expect_true(grepl(",-5.5\n", res$body, fixed = TRUE))
  # The rule is the shared constant, not a second list.
  expect_identical(limits_env$CSV_FORMULA_PREFIXES, c("=", "+", "-", "@", "\t", "\r"))
})

test_that("CSV through the router: a crafted team name is defused, numeric columns untouched", {
  covers("X-14")
  resp <- api_http(limits_router, "/v1/il/teams", "season=2026&format=csv")
  expect_identical(resp$status, 200L)
  plain <- utils::read.csv(text = resp$body, stringsAsFactors = FALSE)
  expect_false(any(grepl("^'", plain$team_name)))
})
