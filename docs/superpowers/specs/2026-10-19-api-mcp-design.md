# API and MCP server for IBPL Analytics -- design (2026-10-19)

Companion: `2026-10-19-api-mcp-feature-inventory.md` (109 features with IDs, checked
against the live app). This document says how every one of them reaches the API.

## Intent

Give the app's numbers a programmatic front door, so that a new front end, an AI
assistant (through MCP) and the customer's own systems can ask what the Shiny app
answers -- and get **exactly** the numbers Shiny shows for the same filters. The
existing Plumber API in `frontend-v2/server` is deprecated and is not a model: it holds
its own copies of logic (e.g. its own lineup filter, D3) and drifted.

## Goals

1. Every feature in the inventory is reachable through the API: each filter is a
   parameter, each view is an endpoint or a `view` value, each derived number (ranks,
   tags, TOTAL rows, auto min-possessions, rank arrows, rate modes) is a response field.
2. One implementation per behaviour. Shiny, the API and the MCP server call the same R
   functions; nothing is re-derived in a second place (`docs/shared_definitions.md`
   rules R1, R4, R8).
3. Proven parity: for every feature ID, an automated test shows the API returns what
   Shiny returns for the same inputs.
4. MCP tools that let an assistant answer the app's questions with the app's numbers and
   the contract's definitions.

## Non-goals

- A new front end. The API returns what one needs; building it is a later project.
- New metrics or definitions. Anything not in the inventory is out.
- Retiring Shiny. It keeps running, on the shared functions.
- Access control and public exposure (decision #5, deferred). Until it is decided the
  API is not exposed publicly.

## Anti-goals (a technically working result that would still be a failure)

- An API number that differs from the Shiny number for the same filters.
- A second copy of a rule in the API "because it was simpler" -- that is how the
  deprecated API drifted.
- An endpoint that silently drops a filter it does not support (it must reject it).
- Moving logic out of Shiny and changing Shiny's output in the process.

## Decisions (agreed 2026-10-19)

| # | Decision | Status |
|---|---|---|
| 1 | Callers: a future front end, an AI assistant via MCP, and the customer's systems. The API returns everything a front end needs to rebuild every screen | Agreed (default) |
| 2 | Shiny moves onto the same shared functions | Agreed (default) |
| 3 | API in R (Plumber) | Agreed (default) |
| 4 | Host on Posit Connect Cloud next to the app if it serves Plumber; otherwise decide | Agreed, pending a check (see Open) |
| 5 | Access control | **Deferred** by the user |
| 6 | Both leagues, every tab, in phases; done = every inventory ID covered | Agreed (default) |
| 7 | MCP: one tool per tab with the same filters; definitions exposed as resources | Agreed (default) |
| 8 | Exact parity, one test per inventory ID | Agreed (default) |

## Architecture

```
            Shiny server (app/R/server_tab*.R) --+
                                                 |
 MCP server (thin) --> Plumber API (api/) -------+--> service layer (app/R/svc_*.R)
                                                 |       pure R functions, one per tab/view
                                                 |       call helpers.R + stored SQL functions
                                                 +--> PostgreSQL (stored functions and views)
```

### Service layer -- `app/R/svc_*.R` (new)

One pure function per tab view, e.g. `svc_onoff(params)`, `svc_lineups(params)`,
`svc_team_ratings(params)`, `svc_game_logs(params)`, `svc_player_stats(params)`,
`svc_compare_*(params)`, `svc_gameflow(params)`, `svc_home(params)`.

- **Input**: one validated parameter list, the same shape for every tab (league,
  competition, season, view, game-context filters X-05, clutch X-06, tab-specific
  controls). Validation is one function (`svc_params()`), fail-closed (Security rule 4).
- **Output**: `list(rows = data.frame, meta = list(...))` -- see Response contract.
- **No Shiny dependency**: no `input`, `session`, reactives or `showNotification`. Rate
  limiting and caching are passed in (or wrapped around by the caller).
- **How code gets there**: by moving the R-closure logic out of the server files,
  byte-identical, one move per commit, verified by reversing the move and diffing
  against `HEAD` (project rule: verify a move with a diff, not new tests). Shiny's
  reactives then call the service function. This is the main body of work: Tab 1 Shot
  Profile, Tab 2/10 lineup ranking and modal, Tab 3 Shot Profile / Traditional / rank
  arrows, Tab 4/11 filtering, Tab 5 pipeline, all of Compare, Gameflow readers.
- **Inline SQL** (the five places in the inventory) moves into one R reader function
  each, called by Shiny and the API alike. Moving it into stored functions is
  desirable under R1 but is a database change; it is listed as a follow-up, not a
  prerequisite.

### Plumber API -- `api/` (new, not `frontend-v2/server`)

- Sources `app/R/helpers.R`, `global.R` (connection pool, constants -- the UI-building
  parts are not needed), `global_euro.R`, the generated catalogues and `svc_*.R`.
- One endpoint per tab, `view` as a parameter where a tab has views:
  `GET /v1/{league}/onoff`, `/lineups`, `/lineups/{unit}/games`, `/team-ratings`,
  `/game-logs`, `/player-stats`, `/compare/{teams|lineups|players}`,
  `/compare/teams/{team}/detail`, `/compare/teams/{team}/players`,
  `/games/{game}/gameflow`, `/home/{team}` (IL), plus reference endpoints
  `/meta/seasons`, `/teams`, `/players`, `/game-numbers`, `/phases`, `/rounds`,
  `/game-types`, `/data-version`, `/definitions`.
- `{league}` = `il` | `euroleague` | `eurocup`. EuroLeague-only and Israeli-only views
  (Shot Profile, Traditional, Compare, Home) return 404 for the other league.
- Formats: JSON by default, `?format=csv` returns the same rows (X-14, and the On/Off
  CSV the Shiny button never delivered).
- Errors: unknown or unsupported parameters -> 400 naming the parameter; statement
  timeout -> 504 with the message (never a blank result, unlike the Compare detail
  view today).
- Caching keyed exactly like Shiny's (data version + league + season), so an ETL run
  invalidates both.

### MCP server -- `mcp/` (new, thin)

- One tool per tab (`onoff`, `lineups`, `lineup_games`, `team_ratings`, `game_logs`,
  `player_stats`, `compare_teams`, `compare_lineups`, `compare_players`, `gameflow`,
  `team_overview`) plus reference tools (`list_teams`, `list_players`, `seasons`).
  Each tool's parameters and descriptions are generated from the API's parameter
  schema, so the two cannot disagree.
- Resources: `docs/shared_definitions.md` (definitions), the glossary and column
  definitions (`COLUMN_TOOLTIPS`), so the assistant states numbers with their meaning.
- It calls the HTTP API and holds no basketball logic. Language is the builder's call
  (see "Left to the builder").

## Response contract (every data endpoint)

```
{
  "rows":   [ ... one object per table row, stable snake_case keys ... ],
  "totals": { ... } | null,            # TOTAL row where the tab has one (LD-10, PS-06)
  "meta": {
    "league", "competition", "season", "view", "data_version",
    "path": "fast" | "filtered",       # X-07, for transparency and tests
    "filters": { ...echo of the resolved filters, after defaults... },
    "auto_min_poss": {...} | null,     # see below
    "default_sort": [{"column","direction"}],
    "columns": [ { "key", "label", "definition", "format", "polarity",
                   "best_direction", "percentile_key", "group" } ],
    "warnings": [ ... ],               # e.g. rate-mode hidden players (PS-02), Gameflow health (GF-03)
    "rank_baseline": {...}             # population the percentiles were computed on
  }
}
```

Rules that keep the screens reproducible:

- **Percentiles** (`pr_*`) are always computed on the population Shiny ranks on (before
  team, player, min-possession and stat-filter narrowing) and returned on every row,
  with each column's polarity, so a client colours cells exactly as Shiny does
  (X-11, X-12).
- **Sorting** (X-13): `sort=` parameter; default = Shiny's default per view; nulls last;
  `best_direction` per column reproduces Shiny's first-click direction.
- **Auto min-possessions** (ON-06, LD-09, CMP-07): stateless. With `min_poss=auto` the
  API computes the bar with the same function Shiny uses and applies it; `meta`
  returns the value and its inputs. Tab 1/8's "lower only, undo its own lowering,
  capped at the baseline" rule becomes `auto = min(baseline, computed)` where the
  baseline is the default or the caller's `min_poss_baseline`. A numeric `min_poss`
  is manual and turns auto off, as moving the slider does.
- **Tags and annotations** are fields, not HTML: `luck_3pt_flag`, `luck_3pt_ours/theirs`,
  `net_padded`, `sample_flag` (ON-08/09), `ff_impact_pts` (ON-10), `rank`, `rank_delta`
  (TR-06/07).
- **Presentation-only items** (chips bar, reset, column drag, mobile layout, FLIP
  transitions, hover menus) are not API features; they are recorded in the inventory
  as X-10, X-15 and GF-04 and covered by returning the data they display.

## Parity proof (decision #8)

1. **Baseline first (Phase 0).** Before any code moves, capture what Shiny produces today
   for a fixed preset set per tab and view: the data frame each renderer receives (via
   `testServer`), saved as golden files. Presets cover every filter family in X-05/X-06,
   both paths (fast and filtered), both leagues, each group size, rate mode, side, and
   every Compare preset and mode. About 15-30 presets per tab.
2. **Extraction proves itself twice.** Each move is a byte-identical diff, and the golden
   files must still match after it.
3. **API parity.** For each preset, the API response (rows + totals + meta values) must
   equal the golden frame, value for value. One test file per tab; each test names the
   inventory IDs it covers.
4. **Coverage gate.** A test reads the inventory, collects every ID, and fails if any ID
   is not named by at least one passing parity test (rule R9: the check can fail).
5. Runs nightly with the DB-backed suite, on port 5432 (the 6543 pooler crosses
   concurrent prepared statements).

## Phases

| Phase | Content | Done when |
|---|---|---|
| 0 | Golden files of current Shiny output per tab/preset; coverage-gate test (failing) | Goldens committed; gate lists all 109 IDs as uncovered |
| 1 | Service layer, tab by tab (Tab 4/11 and 5 first: least closure logic; Compare last), Shiny switched to it | Goldens unchanged after each tab; full Shiny test suite green |
| 2 | Plumber skeleton: params validation, pool, cache keys, errors, CSV, reference endpoints | Reference endpoints match Shiny's dropdown data |
| 3 | Data endpoints per tab, both leagues | Parity tests green per tab; gate shows those IDs covered |
| 4 | MCP server over the API | Each tool answers one preset per tab identically to the API |
| 5 | Hosting (after the Connect Cloud check) and access control (after decision #5) | Deployed behind whatever #5 decides |
| 6 | The two broken features delivered working through the shared layer: On/Off CSV; Compare team detail under the starters presets (speed up `get_team_four_factors_dynamic` for starter filters, or route it like `get_team_ratings_dynamic` via the preset cache) | Both work in Shiny and the API; tests pin them |

Every phase follows the contract's change procedure: find the term in
`docs/shared_definitions.md`, change its owner, update twins, run the pins, record any
remaining deviation the same day.

## Constraints

- Shiny output must not change during extraction (except Phase 6's fixes, which are
  deliberate and separately tested).
- Parameterised SQL only; inputs untrusted; 20 s statement timeout honoured.
- Do not touch the user's local working tree or branch; build on short-lived branches
  per the repo's branching rules (`api/...`, `shiny/...` for extraction moves).
- EuroLeague reuses the Israeli implementation (rule R7); the league is a parameter,
  never a parallel `euro_` service.

## Open

1. **Does Posit Connect Cloud host Plumber APIs?** To check before Phase 5. If not, the
   host is the user's decision.
2. **Access control** (decision #5) -- the user will decide later; nothing is exposed
   publicly before then.

## Left to the builder (my calls, marked so they can be overruled)

- MCP server language: a small Python (FastMCP) or TypeScript process, since it holds no
  basketball logic; R MCP tooling is immature. *My call.*
- Endpoint naming and versioning (`/v1/...`) as above. *My call.*
- Golden-file format (Parquet or RDS) and the exact preset lists per tab. *My call.*
- Inline SQL moved to one R reader each now, stored functions later. *My call.*
- Order of tabs in Phase 1 beyond "simplest first, Compare last". *My call.*
