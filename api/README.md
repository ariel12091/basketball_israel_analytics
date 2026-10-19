# IBPL analytics API (`api/`)

A Plumber API over the same R functions the Shiny app runs. It holds no
basketball logic of its own: routes (`plumber.R`) call handlers
(`R/handlers_*.R`), handlers call the app's canonical lookups and the service
layer (`app/R/svc_*.R`). Design: `docs/superpowers/specs/2026-10-19-api-mcp-design.md`.
`frontend-v2/server` is the deprecated earlier API and is not a model for this one.

Not exposed publicly: access control is an open decision, so the server binds
to `127.0.0.1` by default.

## Run

```bash
RSCRIPT="/c/Program Files/R/R-4.4.2/bin/Rscript.exe"
PG_HOST=... PG_DB=postgres PG_USER=... PG_PASS=... PG_PORT=5432 "$RSCRIPT" api/run.R
```

| Variable | Default | |
|---|---|---|
| `PG_HOST`, `PG_DB`, `PG_USER`, `PG_PASS`, `PG_SSLMODE` | -- / `require` | read-only role, as the app |
| `PG_PORT` | **5432** | never 6543: the transaction pooler crosses concurrent prepared statements (CLAUDE.md) |
| `POOL_MAX`, `PG_STATEMENT_TIMEOUT_MS` | 3, 20000 | as the app |
| `API_HOST`, `API_PORT` | `127.0.0.1`, 3010 | |
| `IBPL_API_REPLAY_DIR` | unset | serve recorded query answers (golden fixtures) instead of a database -- smoke runs only |

`app/.Renviron` sets `PG_PORT=6543` for the app; do not load it for the API.

## Endpoints (`{league}` = `il` \| `euroleague` \| `eurocup`)

| Route | Answer |
|---|---|
| `GET /v1/meta/data-version` | `{il, euroleague}`: the ETL version each league's caches are keyed on |
| `GET /v1/{league}/seasons` | seasons with data, newest first (Israeli: season-ending year; EuroLeague: provider start year) |
| `GET /v1/{league}/teams?season=` | `fetch_teams_min()` / `euro_fetch_teams()` |
| `GET /v1/{league}/players?season=` | `fetch_players_basic()` / `euro_fetch_players_basic()` |
| `GET /v1/il/game-numbers?season=` | `fetch_gn_values()` |
| `GET /v1/il/game-types` | `GAME_TYPE_CHOICES_UI`; `game_type=` takes any code of a `value` |
| `GET /v1/{euroleague,eurocup}/phases?season=` | `euro_fetch_phases()` with labels |
| `GET /v1/{euroleague,eurocup}/rounds?season=` | `euro_fetch_round_values()` |
| `GET /v1/{league}/game-logs` | Tab 4 / Tab 11: one row per team-game (`svc_game_logs()`); `view=summary\|four_factors`, the Game Logs filters (`svc_game_logs_params()`), `sort=<col>:<asc\|desc>[,...]` (default date then GN / round, descending; NAs last; text columns order byte-wise, C locale, unlike the browser table's case-insensitive sort), `format=csv` (visible columns only, as Shiny exports) |

Data endpoints (`game-logs`) answer the design's response contract: `{rows, totals, meta}`, with `meta.columns` giving each key's label, definition, format, polarity, percentile key and whether Shiny hides it.

Every list endpoint answers `{rows: [...], meta: {league, competition, season}}`,
or the rows alone as CSV with `format=csv`. `season` defaults to the app's
default season for the league.

## Errors

| Status | When | Body |
|---|---|---|
| 400 | an unknown parameter, a malformed / out-of-range value, or bytes that do not decode to UTF-8 text (`param` is `"query"` when the name itself is empty or undecodable) | `{error, param}` naming the parameter |
| 404 | unknown league, or an endpoint the league does not have | `{error, param}` |
| 504 | the database statement timeout | `{error}` |
| 500 | anything else (no query text in the body; the detail is logged) | `{error}` |

Plumber is given no parsed query (`api_query_filter` in `R/params.R`, `api_router()` in `R/bootstrap.R`): handlers read the raw
query string through `api_query()`, so `%00` or `%FF` is a 400, not a 500.
Parameters are validated by the service layer (`app/R/svc_params.R`,
`svc_game_logs_params()`), fail closed: nothing is coerced or dropped.

Limits, each a 400 naming the parameter: the raw query string at most 4096 bytes (`API_MAX_QUERY_BYTES`,
param `"query"`), at most 10 `stat` filters (`GL_MAX_STAT`), 5 `sort` keys (`SVC_MAX_SORT`) and 50 ids in
`teams`, `opponents`, `game_type` or `phase` (`SVC_MAX_IDS`).

Shared secret: when the `plumber.sharedSecret` option or `PLUMBER_SHAREDSECRET` / `PLUMBER_SHARED_SECRET`
is set, a request without a matching `Plumber-Shared-Secret` header is a 400 `{error, param}`
(`api_shared_secret_filter`), as under Plumber's default filters.

CSV: text cells that would start a spreadsheet formula get a leading `'` (`csv_defuse_formula()`, the rule
Shiny's CSV buttons apply; `docs/shared_definitions.md`, Exports).

## Tests

```bash
"$RSCRIPT" -e "testthat::test_dir('api/tests/testthat')"   # API suite only (replay, no database)
RUN_DB_TESTS=0 "$RSCRIPT" scripts/test_all.R              # app suite, then API suite
```
