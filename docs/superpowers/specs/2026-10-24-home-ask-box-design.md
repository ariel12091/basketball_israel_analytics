# Home "Ask" box -- design (2026-10-24)

**Scope change (user, 2026-10-24):** a general chat box on every page, not only Home:
a navbar button opens a chat panel on every tab in all three competitions. Same
behaviour as below. It does **not** know the current page's filters (no page
context for now). Where this document says "Home box", read "chat panel".

A free-text question box in the React app (`web/`). A visitor asks a question
in Hebrew or English, gets a short written answer in the same language, and a link
to the app page that shows the full answer with the same filters applied. Follow-up
questions continue the conversation.

Status: approved by the user 2026-10-24 ("let's start").

Builds on `2026-10-19-api-mcp-design.md` (one tool per tab, definitions as
resources), but over the Next.js readers in `web/src/server/`, not the Plumber API.

## Why

Visitors arrive with a question ("top 5 lineups of Maccabi", "what is Hapoel good
at", "how does team x do against the top 4"), not with a tab and a filter set in
mind. The app already answers those questions; the box translates the question into
the right page and filters, and says the answer in a sentence. The link teaches the
visitor where the full answer lives.

## Goals

1. Answer questions the app's pages can answer, in the visitor's language
   (Hebrew or English, matching the question).
2. Every answer links to the page and filters it was computed from.
3. Follow-up questions in the same conversation ("and their bench?").
4. Choose the model by measurement: the cheapest model that passes the test set,
   across providers (Claude, OpenAI; others optional).

## Non-goals

- Questions outside the app's data (injuries, schedules, news, predictions).
  These get an honest "I can't answer that here" plus the closest page.
- Accounts, saved conversations, server-side conversation storage.
- New metrics or definitions of any kind.
- Replacing the question cards. They stay, and stay usable before anything loads.
- Shiny. The box exists only in the React app.
- Page context: the chat does not read the current page's filters or rows (user,
  2026-10-24, "for now").

## Anti-goals (a failure even if it works)

- **A wrong number.** The user's top concern. Any number shown must be exactly
  the number the corresponding page shows for the same filters.
- **Any database access outside the existing readers.** No SQL, no `db.ts` import,
  no new view or function. Hard constraint from the user.
- A second definition of a metric (an average, a difference, a rank computed by
  the model or by the box). Violates `docs/shared_definitions.md` rule 1.
- An answer that sounds certain about something no reader returned.

## Constraints

- Data only through the answer functions the `/api/v1/[league]/*` routes already
  call (table below). Each takes the route's query parameters and validates them
  with the route's own parser, so a tool input *is* the API query and the page's
  URL query.
- Enforced, not promised: a unit test fails if any file under `web/src/ask/`
  imports `server/db.ts`, `pg`, or any `*Repo.ts` internals other than the
  answer functions listed in one allowlist file.
- Compare is Israeli only (as in Shiny). EuroLeague/EuroCup questions that need it
  are told so and linked to the closest page.
- Default seasons follow the pages (Israeli 26-27, Euro provider 2026). A season
  named in the question overrides.
- Provider API keys are server-side Vercel environment variables only.
- Existing read-only DB role, deadlines and admission limits apply unchanged,
  because the readers enforce them.

## API mapping (decided: in-process, one tool per route)

Tools run in-process (no HTTP hop) but are mapped one-to-one onto the API: same
name, same parameters, same parser, same answer function. This is the same
mapping as `2026-10-19-api-mcp-design.md` (where MCP called the HTTP API), minus
the network hop. The user chose in-process on 2026-10-24.

| Tool | API route (`/api/v1/{league}/...`) | Answer function | Page | Inventory IDs |
|---|---|---|---|---|
| `onoff` | `onoff` | `onoffAnswer` (`server/onoffApi.ts`) | `/{league}/onoff` | ON-01..14, X-05, X-11 |
| `lineups` | `lineups` | `lineupsAnswer` (`server/lineupsApi.ts`) | `/{league}/lineups` | LD-01..11, LD-13, LD-14 |
| `lineup_games` | `lineup-games` | `lineupGames` (`server/lineupGamesRepo.ts`) | lineup modal on `/{league}/lineups` | LD-12 |
| `lineup_roster` | `lineup-roster` | `getLineupRoster` (see note) | player chips on `/{league}/lineups` | LD-03 |
| `team_ratings` | `team-ratings` | `teamRatingsAnswer` (`server/teamRatingsRepo.ts`) | `/{league}/team-ratings` | TR-01..10 |
| `game_logs` | `game-logs` | `gameLogsAnswer` (`server/gameLogsApi.ts`) | `/{league}/game-logs` | GL-01..07 |
| `player_stats` | `player-stats` | `playerStatsAnswer` (`server/playerStatsRepo.ts`) | `/{league}/player-stats` | PS-01..09 |
| `compare` (IL only) | `compare` | `compareAnswer` (`server/compareRepo.ts`) | `/il/compare` | CMP-01..17 |
| `team_overview` (IL hub) | `home` | `homeAnswer` (`server/homeRepo.ts`) | `/{league}/home` | HOME-01..07 |
| `gameflow` (link-only in v1) | `gameflow` | `gameflowAnswer` (`server/gameflowRepo.ts`) | `/{league}/gameflow` | GF-01..06 |
| `list_teams` | -- (no route) | generated `data/catalogues.json` | -- | X-04 |

Not tools, with the reason:

- `player-stat-filters`: an internal helper the On/Off page uses to apply its
  Player Stats chips. The `onoff` tool reaches it through `onoffAnswer`'s parameters.
- `version` (`/api/v1/version`): the data version. Every answer function already
  reports it, and the route queries `withSnapshot` directly, which the import
  allowlist forbids.

Note: the `lineup-roster` and `player-stat-filters` routes call parser + repo
inline. Before `lineup_roster` is wired, move that pair into a named answer
function (`lineupRosterAnswer` in `server/lineupsApi.ts`) and have the route call
it: a byte-identical move, verified by diff, so route and tool share one entry
point.

Enforced by tests:

- **Route coverage:** every directory under `app/api/v1/[league]/` is either a
  tool in the registry or listed in the "not tools" exclusions with its reason.
  A new route with neither fails the test.
- **Same entry point:** each tool's registry entry names the answer function the
  route imports (checked by reading the route file's import), so the two can't
  drift apart.
- **Parameter schema = route parser:** tool parameters are validated by calling
  the route's parser, not by a second schema. The schema shown to the model is
  derived from the parser's accepted keys; a test checks that every key the
  parser reads appears in the tool schema, and the reverse.
- **Inventory IDs:** each tool lists its IDs (table above); a test checks every
  listed ID exists in `2026-10-19-api-mcp-feature-inventory.md`.

## How it works (decided)

### 1. Tools = the app's pages

One tool per API route, defined once in `web/src/ask/tools.ts`; the full list and
its mapping are in "API mapping" above. The import allowlist is that table's
answer-function column.

- Tool input: `league` plus the page's own query parameters (zod schema derived
  from or checked against the page parser), plus presentation-only `sort`
  (a column the reader returns, direction) and `limit` (max 10 rows).
- The tool runs the answer function, applies the same default sort the page uses
  unless `sort` is given, and returns: a result id (`r1`, `r2`, ...), the page link
  (`/{league}/{path}?{query}`, built by the existing href helpers), the column
  definitions (label, format, polarity, definition text from the existing column
  tooltips), and at most `limit` rows with a row id each.
- Invalid parameters: the parser's error message goes back to the model as a tool
  error; it may retry once.
- The same registry can later be exposed as an MCP endpoint; not in v1 (see below).

### 2. Numbers are filled in by the app, never typed by the model

- The model writes text with markers: `{{r1.row2.net}}`, `{{r1.row1.team}}`.
- The app replaces each marker with the cell value, formatted with the page's
  formatter for that column (same rounding, sign, percent).
- **Check before showing** (`web/src/ask/verify.ts`):
  - every marker resolves to an existing result, row and column;
  - the final text contains no digit that did not come from a marker, except
    digits that appear verbatim in the visitor's current question ("top 5");
  - season labels come from a `{{season}}` marker.
- On failure: regenerate once with the error stated; on a second failure show
  "Here's the page that answers this" with the link and the evidence table, and
  log it.
- Residual risk, named: words without digits ("best", "worst", "הכי טוב"). The
  prompt requires a rank marker for any superlative; the test set checks it.
  Not machine-enforced in v1.

### 3. Evidence under every answer

Below the text: a small table of the exact rows used (the same cells the markers
point to, same formatting), and the page link as the primary call to action.

### 4. Conversation

- The conversation (questions, answers, tool calls and results) lives in the
  browser and is sent with each new question. Nothing stored server-side.
- Capped at 6 turns; then "start a new question".
- Follow-ups reuse earlier tool calls from the history, so "their bench" becomes
  the same Compare call with the Starters/Bench preset, and its link.
- Each turn's numbers come only from that turn's tool results.
- Order: ship single-question end to end first; turn follow-ups on once the test
  set passes.

### 5. Language

- Answer in the question's language. Hebrew answers render right-to-left; links,
  team names from the catalogue and the evidence table follow the existing page
  direction.
- Hebrew team names and nicknames: the model receives the season's team list
  (from `list_teams`) and maps names itself. No alias list typed into code
  (`shared_definitions.md` rule 2). If the test set shows the mapping fails,
  Hebrew display names belong in the `season_teams` catalogue -- a DB change that
  needs the user's approval, not part of v1.

### 6. Model choice by measurement

- `web/src/ask/providers/` holds one thin adapter per provider over its official
  SDK (`@anthropic-ai/sdk`, `openai`), behind one interface (messages + tools in,
  tool calls / text out, usage out). The model is a server setting.
- Candidates for the first run: Claude Haiku 5.5, Claude Sonnet 5.5, and one or two
  current OpenAI models (prices looked up at test time). Opus 5.5 as the quality
  ceiling.
- Test set `web/test/ask/cases.json`: 40-50 cases, about half Hebrew. Each case has
  the question (or turns), the expected tool(s) and required parameter subset, and
  an `unanswerable` flag where applicable. Covers: the user's three examples and
  variations, every tool, both leagues, seasons named and unnamed, Hebrew
  nicknames, out-of-scope questions, and 6-8 multi-turn cases.
- Scoring per case (`web/scripts/ask-eval.ts`): right tool and parameters;
  verification passed without fallback; correct language; unanswerable handled.
  Plus cost and time per answer from provider usage.
- **A model with any answer that fails verification on a second try, or states a
  number not from a marker, is disqualified.** Among the rest: the cheapest with
  at least 90% right tool and parameters wins. The user spot-checks 10 answers of
  the finalist for wording.
- The eval runs the real readers sequentially (same load profile as the existing
  live smoke checks), not in parallel.

### 7. Limits

- Hard monthly spend cap set in each provider's billing settings ($20 while
  testing). The only limit that cannot be bypassed.
- In-app: about 20 questions per hour per browser and per IP, max 6 turns, max
  question length 500 characters, max 4 tool calls per turn. On Vercel this
  limiter is per instance; acceptable while testing, and the spend cap is the
  backstop. Not a fix for the general X-20 gap.
- `ASK_ENABLED=false` hides the box; question cards are unaffected.

### 8. Logging

One structured line per turn in the server log: question text, tools and
parameters, verification pass/fail/fallback, model, tokens, cost, latency.
No IP, no identifiers. Real questions feed the test set.

## Interfaces

- `POST /api/v1/ask` (Node runtime): body `{ league, history, question }`;
  response streams status events ("reading Lineups...") then one final
  `{ text, results, links, fallback }`. The final text is assembled and verified
  server-side before it is sent; the browser never receives raw marker text.
- UI: a navbar button opening a chat panel on every page, in all three
  competitions, using existing Shiny styles and the phone-first width rules in
  `web/CLAUDE.md`. The conversation survives tab switches and the answer links.
  Its state lives in a provider in the root layout, which persists across client
  navigation, mirrored to `sessionStorage` behind try/catch so a reload keeps it.
  The league sent with a question is the current page's league; no other page
  state is sent.

## Settled after the builder review

- **Reader payloads differ per tab.** Each tool has a small adapter that flattens
  its answer function's payload into rows and columns. Column labels, formats,
  polarity and definitions come from the generated `web/src/data/*.json`
  (`shinyUi.json`, `compareUi.json`, `homeUi.json`), and cell text from the same
  pure formatters the tables use (`formatNumber` in `DataTable.tsx`,
  `formatCompare` in `rules/compare.ts`). Where a formatter sits in a component
  file, move it into a shared non-React module first (a byte-identical move).
  Each adapter gets a pin test: for a recorded golden, its formatted cells equal
  the page's rendered cells. *Claude's call.*
- **Sorting happens in the browser** (`DataTable`'s ordering rules and the
  column order data). The adapters reuse that same ordering function, exported
  rather than copied, and a pin test compares the adapter's top-N with the
  page's. Sorting is presentation; no new definition. *Claude's call.*
- **Admission and deadlines live in `db.ts` (`withSnapshot`), not in the routes**,
  so in-process calls go through them unchanged. Tool calls in one turn run
  sequentially; the ask route sets `maxDuration = 60` and stops calling tools
  after 40 s with whatever results it has. *Claude's call.*
- **Non-table results.** `team_overview` (Home hub) is flattened into named rows
  (record, ranks, key players, lineups, storylines). Compare detail sections
  become one row per metric. `gameflow` is link-only in v1: no numbers in the
  text. *Claude's call.*
- **Digit rule.** Western and Arabic-Indic digits count as digits. Hebrew-letter
  numerals are not machine-checked, because the same pattern is an ordinary
  abbreviation (ת״א); the prompt forbids them and the test set checks. Digits inside marker-filled values (team names, dates) are fine. Link
  text is built by the app. Numbers spelled out in words are not machine-checked;
  the prompt forbids them and the test set checks for them. *Claude's call.*
- **Browser history is untrusted.** The browser sends previous questions,
  answers and tool *parameters* only. The server re-runs any earlier tool call a
  follow-up relies on (the readers' 60-second cache makes this cheap). Numbers
  never come from client-supplied results. Re-runs count toward the 4-call cap.
  *Claude's call.*
- **`list_teams`** reads the generated catalogue `web/src/data/catalogues.json`
  (from `season_teams`), so it touches no database at all.
- **"vs top 4"** is Compare's existing `top_bottom_rank` preset.
- **Model ids.** Claude: `claude-haiku-5-5`, `claude-sonnet-5-5`,
  `claude-opus-5-5`. OpenAI ids and prices are looked up when the eval runs, and
  recorded in the eval report.

## Left to the builder

- Prompt wording, tool descriptions, marker syntax details.
- Whether to use the official SDK tool runners or a hand-written loop.
- Status event format; UI details within existing styles.
- Exact zod schemas, as long as they accept exactly what the page parsers accept.
- An MCP endpoint over the same registry (`/api/mcp`) after v1 -- one adapter, no
  new logic.

## Calls made by Claude (not the user)

- React Home only, not Shiny.
- In-process answer functions rather than HTTP calls to `/api/v1`.
- No Hebrew alias list; the model maps names from the team list.
- Digits allowed only from markers or the visitor's own question.
- 6 turns, 20 questions/hour, $20 cap, 90% pass bar, 10-answer spot check.
- Logging without identifiers.
- MCP endpoint deferred past v1.

## Order of work

Prerequisite (user, not yet done as of 2026-10-24): API accounts with Anthropic and
OpenAI, each with a key and a monthly spend cap, keys set as Vercel server env vars.
Steps 1-2 need no key: they are built and tested with a scripted fake provider
that returns fixed tool calls and marker text. Step 3 starts on whichever account
exists first; step 4's comparison needs both.

1. Tool registry over the answer functions, the `lineupRosterAnswer` move, and the
   mapping tests (import allowlist, route coverage, entry point, parameters,
   inventory IDs).
2. Marker filling and verification.
3. Single-question answers with one provider (Claude Haiku 5.5).
4. Test set and eval script; run all candidates; pick the model.
5. Follow-ups.
6. Limits and off switch.
7. Chat panel on every page (navbar button), Hebrew RTL, browser checks at
   390/1440 px.
