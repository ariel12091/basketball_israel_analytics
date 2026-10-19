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

Every list endpoint answers `{rows: [...], meta: {league, competition, season}}`,
or the rows alone as CSV with `format=csv`. `season` defaults to the app's
default season for the league.

## Errors

| Status | When | Body |
|---|---|---|
| 400 | an unknown parameter, or a malformed / out-of-range value | `{error, param}` naming the parameter |
| 404 | unknown league, or an endpoint the league does not have | `{error, param}` |
| 504 | the database statement timeout | `{error}` |
| 500 | anything else (no query text in the body; the detail is logged) | `{error}` |

Parameters are validated by the service layer (`app/R/svc_params.R`,
`svc_game_logs_params()`), fail closed: nothing is coerced or dropped.

## Tests

```bash
"$RSCRIPT" -e "testthat::test_dir('api/tests/testthat')"   # API suite only (replay, no database)
RUN_DB_TESTS=0 "$RSCRIPT" scripts/test_all.R              # app suite, then API suite
```
