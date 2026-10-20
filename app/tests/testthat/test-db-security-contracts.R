# Slice out one declaration block so an allowlist assertion cannot pass on a
# name that appears somewhere else in the file.
sql_block_between <- function(sql, start_marker, end_marker) {
  start <- regexpr(start_marker, sql, fixed = TRUE)
  expect_gt(start, 0L)
  rest <- substring(sql, start + attr(start, "match.length"))
  end <- regexpr(end_marker, rest, fixed = TRUE)
  expect_gt(end, 0L)
  substring(rest, 1L, end - 1L)
}

test_that("database security script does not grant browser-facing roles access", {
  sql <- read_repo_txt("..", "sql", "security", "enable_readonly_rls.sql")

  expect_match(sql, "REVOKE ALL PRIVILEGES ON SCHEMA %I FROM PUBLIC", fixed = TRUE)
  expect_match(sql, "ARRAY['anon', 'authenticated']", fixed = TRUE)
  expect_match(sql, "REVOKE ALL PRIVILEGES ON ALL TABLES", fixed = TRUE)
  expect_match(sql, "REVOKE ALL PRIVILEGES ON ALL FUNCTIONS", fixed = TRUE)

  expect_false(grepl("GRANT\\s+.+\\s+TO\\s+anon", sql, ignore.case = TRUE))
  expect_false(grepl("GRANT\\s+.+\\s+TO\\s+authenticated", sql, ignore.case = TRUE))
  expect_false(grepl("FOR SELECT TO anon", sql, fixed = TRUE))
  expect_false(grepl("FOR SELECT TO authenticated", sql, fixed = TRUE))
})

test_that("app role is read-only and function execution is allowlisted", {
  sql <- read_repo_txt("..", "sql", "security", "enable_readonly_rls.sql")

  expect_match(sql, "REVOKE CREATE ON SCHEMA %I FROM app_readonly", fixed = TRUE)
  expect_match(sql, "GRANT SELECT ON ALL TABLES IN SCHEMA %I TO app_readonly", fixed = TRUE)
  expect_match(sql, "REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA %I FROM app_readonly", fixed = TRUE)
  expect_match(sql, "p.proname = ANY(v_function_names)", fixed = TRUE)
  expect_match(sql, "FOR SELECT TO app_readonly USING (true)", fixed = TRUE)

  expected_functions <- c(
    "onoff_compute",
    "four_factors_compute",
    "four_factors_dashboard_compute",
    "fetch_lineups_csv_v2",
    "fetch_lineups_all",
    "fetch_lineups_four_factors_csv",
    "fetch_lineups_four_factors",
    "get_team_ratings_dynamic",
    "get_team_four_factors_dynamic",
    "get_team_four_factors_dashboard_dynamic",
    "get_team_traditional_dynamic",
    "get_team_minutes_dynamic",
    "get_player_traditional_dynamic",
    "get_player_traditional_from_games",
    "get_player_traditional_custom_clutch"
  )
  for (function_name in expected_functions) {
    expect_match(sql, sprintf("'%s'", function_name), fixed = TRUE)
  }
})

test_that("hardening covers the EuroLeague shadow schema on the same terms", {
  sql <- read_repo_txt("..", "sql", "security", "enable_readonly_rls.sql")

  expect_match(sql, "'euroleague'", fixed = TRUE)

  # The EuroLeague app API, and only it.
  euro_functions <- c(
    "onoff_compute",
    "four_factors_compute",
    "four_factors_dashboard_compute",
    "fetch_lineups_dynamic",
    "fetch_lineups_direct",
    "get_team_ratings_dynamic",
    "get_team_ratings_direct",
    "get_team_four_factors_dynamic",
    "get_team_four_factors_direct",
    "get_team_minutes_dynamic",
    "get_team_minutes_direct",
    "get_team_minutes_pergame",
    "get_team_dashboard_dynamic",
    "get_player_traditional_dynamic",
    "get_player_traditional_pergame",
    "effective_period",
    "person_display_name"
  )
  euro_block <- sql_block_between(
    sql,
    "euro_function_names constant text[] := ARRAY[",
    "];"
  )
  for (function_name in euro_functions) {
    expect_match(euro_block, sprintf("'%s'", function_name), fixed = TRUE)
  }

  audit_sql <- read_repo_txt("..", "sql", "security", "audit_app_access.sql")
  for (function_name in euro_functions) {
    expect_match(audit_sql, sprintf("('%s')", function_name), fixed = TRUE)
  }

  # Publication machinery mutates the schema and must never be app-callable.
  expect_false(grepl("refresh_", euro_block, fixed = TRUE))

  # The shadow schema uses a curated relation list, so a new relation is
  # closed to the app until it is added on purpose.
  expect_match(sql, "v_curated_relations := v_schema = ANY(curated_schemas)", fixed = TRUE)
  expect_match(
    sql_block_between(sql, "curated_schemas constant text[] := ARRAY[", "];"),
    "'euroleague'",
    fixed = TRUE
  )
  expect_match(
    sql,
    "ALTER DEFAULT PRIVILEGES IN SCHEMA %I REVOKE ALL PRIVILEGES ON TABLES FROM app_readonly",
    fixed = TRUE
  )
  expect_match(sql, "GRANT SELECT ON %I.%I TO app_readonly", fixed = TRUE)

  # It is also outside Supabase's managed surface.
  expect_match(
    sql,
    "v_grant_service_role := has_service_role AND v_schema <> ALL(private_schemas)",
    fixed = TRUE
  )
  expect_match(
    sql_block_between(sql, "private_schemas constant text[] := ARRAY[", "];"),
    "'euroleague'",
    fixed = TRUE
  )
})

test_that("hardening enforces a single RLS policy name", {
  sql <- read_repo_txt("..", "sql", "security", "enable_readonly_rls.sql")

  expect_match(sql, "ALTER TABLE %I.%I ENABLE ROW LEVEL SECURITY", fixed = TRUE)
  for (legacy_policy in c("rls_read_all", "rls_read_all_app_readonly", "app_readonly_select_all")) {
    expect_match(
      sql,
      sprintf("DROP POLICY IF EXISTS %s ON", legacy_policy),
      fixed = TRUE
    )
  }
  expect_match(sql, "CREATE POLICY app_readonly_select_all ON", fixed = TRUE)
})

test_that("read-only database privilege audit covers public exposure and app writes", {
  sql <- read_repo_txt("..", "sql", "security", "audit_app_access.sql")

  expect_match(sql, "untrusted_schema_usage", fixed = TRUE)
  expect_match(sql, "untrusted_relation_select", fixed = TRUE)
  expect_match(sql, "untrusted_routine_execute", fixed = TRUE)
  expect_match(sql, "app_schema_create", fixed = TRUE)
  expect_match(sql, "app_schema_usage_missing", fixed = TRUE)
  expect_match(sql, "app_relation_write", fixed = TRUE)
  expect_match(sql, "app_required_relation_select_missing", fixed = TRUE)
  expect_match(sql, "app_unexpected_routine_execute", fixed = TRUE)
  expect_match(sql, "app_required_routine_execute_missing", fixed = TRUE)
})

test_that("audit verifies RLS state from the catalog, not from the apply script", {
  sql <- read_repo_txt("..", "sql", "security", "audit_app_access.sql")

  expect_match(sql, "rls_disabled", fixed = TRUE)
  expect_match(sql, "rls_read_policy_missing", fixed = TRUE)
  expect_match(sql, "rls_unexpected_policy", fixed = TRUE)
  expect_match(sql, "NOT x.relrowsecurity", fixed = TRUE)
})

test_that("audit scopes allowlists per schema and covers the shadow schema", {
  sql <- read_repo_txt("..", "sql", "security", "audit_app_access.sql")

  expect_match(sql, "'euroleague'", fixed = TRUE)
  expect_match(sql, "app_unexpected_relation_select", fixed = TRUE)
  expect_match(sql, "service_role_unexpected_access", fixed = TRUE)
  expect_match(sql, "allowed_app_routines(schema_name, routine_name)", fixed = TRUE)
  expect_match(sql, "required_app_relations(schema_name, relation_name)", fixed = TRUE)

  euro_routine_block <- sql_block_between(
    sql,
    "euro_app_routines(routine_name) AS (",
    "\n),"
  )
  expect_match(euro_routine_block, "'fetch_lineups_dynamic'", fixed = TRUE)
  expect_false(grepl("refresh_", euro_routine_block, fixed = TRUE))
})

test_that("database security apply script is dry-run by default", {
  sql <- read_repo_txt("..", "scripts", "apply_db_security.R")

  expect_match(sql, 'Sys.getenv("CONFIRM_DB_SECURITY_APPLY", "0")', fixed = TRUE)
  expect_match(sql, "dbRollback(con)", fixed = TRUE)
  expect_match(sql, "dbCommit(con)", fixed = TRUE)
  expect_match(sql, "if (confirm_apply)", fixed = TRUE)
})

test_that("sourcing the security script defines the shared step and runs nothing", {
  sql <- read_repo_txt("..", "scripts", "apply_db_security.R")
  expect_match(sql, "run_db_security_hardening <- function(con, root = \".\")", fixed = TRUE)
  expect_match(sql, "if (sys.nframe() == 0L) {", fixed = TRUE)
  expect_match(sql, "run_db_security_hardening(con)", fixed = TRUE)

  # The guard is what lets apply_web_foundation.R reuse the step: sourcing
  # must define the function without connecting, reading env or applying.
  env <- new.env()
  sys.source(repo_file("..", "scripts", "apply_db_security.R"), envir = env)
  expect_true(is.function(env$run_db_security_hardening))
  expect_false(exists("confirm_apply", envir = env, inherits = FALSE))
  expect_false(exists("con", envir = env, inherits = FALSE))
})

# ---- Web foundation: app_web login role and app_api schema ------------------

# Quoted identifiers in one list block, in order.
sql_quoted_names <- function(block) {
  hits <- regmatches(block, gregexpr("'[A-Za-z0-9_]+'", block))[[1]]
  gsub("'", "", hits, fixed = TRUE)
}

test_that("web_role.sql creates app_web inside app_readonly with role-default timeouts", {
  sql <- read_repo_txt("..", "sql", "security", "web_role.sql")

  expect_match(sql, "CREATE ROLE app_web", fixed = TRUE)
  expect_match(sql, "IN ROLE app_readonly;", fixed = TRUE)
  # An existing role is pulled back into app_readonly, not left as found.
  expect_match(sql, "GRANT app_readonly TO app_web;", fixed = TRUE)
  expect_match(sql, "ALTER ROLE app_web SET statement_timeout = '3s';", fixed = TRUE)
  expect_match(
    sql,
    "ALTER ROLE app_web SET idle_in_transaction_session_timeout = '5s';",
    fixed = TRUE
  )
  for (attr in c("LOGIN", "INHERIT", "NOSUPERUSER", "NOCREATEDB", "NOCREATEROLE",
                 "NOREPLICATION", "NOBYPASSRLS")) {
    # Both branches: the CREATE and the ALTER of an existing role.
    expect_length(gregexpr(sprintf("\n      %s\n", attr), sql, fixed = TRUE)[[1]], 2L)
  }
  expect_length(
    gregexpr("CONNECTION LIMIT __APP_WEB_CONNECTION_LIMIT__", sql, fixed = TRUE)[[1]],
    2L
  )

  # No credential in the file, and no route to any other role.
  expect_false(grepl("PASSWORD\\s+'", sql, ignore.case = TRUE))
  expect_false(grepl("\\bENCRYPTED\\b", sql, ignore.case = TRUE))
  expect_false(grepl(
    "IN ROLE\\s+[^;]*\\b(anon|authenticated|service_role|postgres|authenticator)\\b",
    sql,
    ignore.case = TRUE
  ))
  expect_false(grepl(
    "GRANT\\s+(anon|authenticated|service_role|postgres|authenticator)\\b",
    sql,
    ignore.case = TRUE
  ))
})

test_that("hardening covers app_api through its own enumerated allowlist", {
  sql <- read_repo_txt("..", "sql", "security", "enable_readonly_rls.sql")

  expect_true("app_api" %in% sql_quoted_names(
    sql_block_between(sql, "target_schemas constant text[] := ARRAY[", "];")
  ))
  # Curated (no blanket SELECT) and private (no service_role), like euroleague.
  expect_true("app_api" %in% sql_quoted_names(
    sql_block_between(sql, "curated_schemas constant text[] := ARRAY[", "];")
  ))
  expect_true("app_api" %in% sql_quoted_names(
    sql_block_between(sql, "private_schemas constant text[] := ARRAY[", "];")
  ))
  expect_match(sql, "WHEN 'app_api' THEN app_api_function_names", fixed = TRUE)
  expect_match(sql, "WHEN 'app_api' THEN app_api_relations", fixed = TRUE)
  expect_match(sql, "FOREACH v_relation IN ARRAY v_relation_names", fixed = TRUE)

  # Writers never go on the web API's allowlist.
  app_api_block <- sql_block_between(
    sql,
    "app_api_function_names constant text[] := ARRAY[",
    "]::text[];"
  )
  expect_false(grepl("(refresh_|begin_publication|fence_publication|finish_publication)",
                     app_api_block))
})

test_that("audit checks app_api with its own allowlist", {
  sql <- read_repo_txt("..", "sql", "security", "audit_app_access.sql")

  expect_match(
    sql,
    "WHERE nspname IN ('basketball_test', 'basketball', 'euroleague', 'app_api')",
    fixed = TRUE
  )
  expect_match(sql, "WHEN 'app_api' THEN 'app_api'", fixed = TRUE)
  expect_match(sql, "JOIN app_api_routines r ON s.allowlist = 'app_api'", fixed = TRUE)
  expect_match(sql, "JOIN app_api_app_relations r ON s.allowlist = 'app_api'", fixed = TRUE)
  # app_api must not inherit another schema's lists through the old
  # curated/not-curated join.
  expect_false(grepl("JOIN \\w+_app_(routines|relations) r ON (NOT )?s\\.curated_relations", sql))
})

test_that("hardening and audit allowlists are twins", {
  hardening <- read_repo_txt("..", "sql", "security", "enable_readonly_rls.sql")
  audit <- read_repo_txt("..", "sql", "security", "audit_app_access.sql")

  pairs <- list(
    israeli_routines = list(
      c("app_function_names constant text[] := ARRAY[", "];"),
      c("israeli_app_routines(routine_name) AS (", "\n),")
    ),
    euro_routines = list(
      c("euro_function_names constant text[] := ARRAY[", "];"),
      c("euro_app_routines(routine_name) AS (", "\n),")
    ),
    app_api_routines = list(
      c("app_api_function_names constant text[] := ARRAY[", "]::text[];"),
      c("app_api_routines(routine_name) AS (", "\n),")
    ),
    euro_relations = list(
      c("euro_app_relations constant text[] := ARRAY[", "];"),
      c("euro_app_relations(relation_name) AS (", "\n),")
    ),
    app_api_relations = list(
      c("app_api_relations constant text[] := ARRAY[", "]::text[];"),
      c("app_api_app_relations(relation_name) AS (", "\n),")
    )
  )
  for (name in names(pairs)) {
    markers <- pairs[[name]]
    from_hardening <- sql_quoted_names(
      sql_block_between(hardening, markers[[1]][[1]], markers[[1]][[2]])
    )
    from_audit <- sql_quoted_names(
      sql_block_between(audit, markers[[2]][[1]], markers[[2]][[2]])
    )
    expect_identical(sort(from_hardening), sort(from_audit), info = name)
    expect_false(anyDuplicated(from_hardening) > 0L, info = name)
  }
})

test_that("app_api schema file grants USAGE only, never EXECUTE", {
  sql <- read_repo_txt("..", "sql", "app_api", "000_schema.sql")

  expect_match(sql, "CREATE SCHEMA IF NOT EXISTS app_api;", fixed = TRUE)
  expect_match(sql, "REVOKE ALL PRIVILEGES ON SCHEMA app_api FROM PUBLIC;", fixed = TRUE)
  expect_match(sql, "ARRAY['anon', 'authenticated']", fixed = TRUE)
  expect_match(sql, "REVOKE CREATE ON SCHEMA app_api FROM app_readonly;", fixed = TRUE)
  expect_match(sql, "GRANT USAGE ON SCHEMA app_api TO app_readonly;", fixed = TRUE)
  # Function grants come only from the hardening allowlist (review R9); a
  # grant here would vanish on the next re-apply and hide a missing entry.
  expect_false(grepl("GRANT\\s+EXECUTE", sql, ignore.case = TRUE))
})

# TRUE when one statement fragment grants something on app_api to a
# browser-facing role or PUBLIC.
grants_app_api_to_untrusted <- function(statement) {
  grepl("\\bGRANT\\b", statement, ignore.case = TRUE) &&
    grepl("\\bapp_api\\b", statement, ignore.case = TRUE) &&
    grepl("\\bTO\\b[^;]*\\b(anon|authenticated|public)\\b", statement, ignore.case = TRUE)
}

test_that("the app_api grant scan can fail", {
  expect_true(grants_app_api_to_untrusted("GRANT USAGE ON SCHEMA app_api TO anon"))
  expect_true(grants_app_api_to_untrusted(
    "grant execute on all functions in schema app_api to PUBLIC"
  ))
  expect_true(grants_app_api_to_untrusted(
    "GRANT EXECUTE ON FUNCTION app_api.version() TO app_readonly, authenticated"
  ))
  expect_false(grants_app_api_to_untrusted("GRANT USAGE ON SCHEMA app_api TO app_readonly"))
  expect_false(grants_app_api_to_untrusted("REVOKE ALL PRIVILEGES ON SCHEMA app_api FROM PUBLIC"))
})

test_that("no SQL file grants anything on app_api to anon, authenticated or PUBLIC", {
  sql_files <- list.files(repo_file("..", "sql"), pattern = "\\.sql$",
                          recursive = TRUE, full.names = TRUE)
  expect_true(any(grepl("/app_api/", sql_files, fixed = TRUE)))
  expect_true(any(grepl("/security/web_role.sql", sql_files, fixed = TRUE)))

  offenders <- unlist(lapply(sql_files, function(path) {
    text <- paste(readLines(path, warn = FALSE), collapse = "\n")
    statements <- strsplit(text, ";", fixed = TRUE)[[1]]
    hits <- Filter(grants_app_api_to_untrusted, statements)
    if (length(hits)) paste0(basename(path), ": ", trimws(hits)) else NULL
  }))
  expect_identical(offenders, NULL)

  # Inside sql/app_api nothing is granted to those roles at all.
  app_api_files <- sql_files[grepl("/app_api/", sql_files, fixed = TRUE)]
  for (path in app_api_files) {
    text <- paste(readLines(path, warn = FALSE), collapse = "\n")
    expect_false(
      grepl("GRANT[^;]*\\bTO\\b[^;]*\\b(anon|authenticated|public)\\b", text, ignore.case = TRUE),
      info = basename(path)
    )
  }
})

test_that("audit holds app_web to app_readonly's reach and its role defaults", {
  sql <- read_repo_txt("..", "sql", "security", "audit_app_access.sql")
  role_sql <- read_repo_txt("..", "sql", "security", "web_role.sql")

  for (violation in c("web_role_privileged_attribute",
                      "web_role_connection_limit_missing",
                      "web_role_app_readonly_missing",
                      "web_role_unexpected_membership",
                      "web_role_setting_missing")) {
    expect_match(sql, sprintf("'%s'", violation), fixed = TRUE)
  }

  # The settings the audit requires are the ones web_role.sql sets (twin pin).
  set_lines <- regmatches(
    role_sql,
    gregexpr("ALTER ROLE app_web SET [a-z_]+ = '[^']+';", role_sql)
  )[[1]]
  from_role <- sub("ALTER ROLE app_web SET ([a-z_]+) = '([^']+)';", "\\1=\\2", set_lines)
  expect_length(from_role, 2L)
  settings_block <- sql_block_between(sql, "web_role_required_settings(setting) AS (", "\n),")
  from_audit <- gsub(
    "'", "",
    regmatches(settings_block, gregexpr("'[a-z_]+=[0-9a-z]+'", settings_block))[[1]],
    fixed = TRUE
  )
  expect_setequal(from_audit, from_role)
})

test_that("web foundation apply script is dry-run by default and keeps the password out", {
  src <- read_repo_txt("..", "scripts", "apply_web_foundation.R")

  expect_match(src, 'Sys.getenv("CONFIRM_WEB_FOUNDATION_APPLY", "0")', fixed = TRUE)
  expect_match(src, "if (confirm_apply) {", fixed = TRUE)
  expect_match(src, "dbRollback(con)", fixed = TRUE)
  expect_match(src, "dbCommit(con)", fixed = TRUE)

  # Fails closed without a password; the password is only ever a bound value.
  expect_match(src, 'Sys.getenv("APP_WEB_PASSWORD", "")', fixed = TRUE)
  expect_match(src, "if (confirm_apply && !nzchar(app_web_password)) {", fixed = TRUE)
  expect_match(src, "params = list(app_web_password)", fixed = TRUE)
  expect_match(src, "format('ALTER ROLE app_web PASSWORD %L',", fixed = TRUE)
  uses <- regmatches(src, gregexpr("[A-Za-z_.]+\\([^()]*app_web_password", src))[[1]]
  expect_false(any(grepl("^(paste|paste0|sprintf|cat|message|print|gsub|sub)\\(", uses)))

  # Same transaction ends with the shared security re-apply and audit.
  expect_match(src, 'file.path("scripts", "apply_db_security.R")', fixed = TRUE)
  expect_match(src, "source(security_script, local = TRUE)", fixed = TRUE)
  expect_match(src, "run_db_security_hardening(con)", fixed = TRUE)
  expect_match(src, "__APP_WEB_CONNECTION_LIMIT__", fixed = TRUE)
  expect_match(src, 'Sys.getenv("APP_WEB_CONNECTION_LIMIT", "10")', fixed = TRUE)
})
