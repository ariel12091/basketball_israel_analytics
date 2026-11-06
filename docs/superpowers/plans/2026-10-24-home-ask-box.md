# Home Ask Box Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A general chat panel, opened from the navbar on every page, that answers Hebrew/English questions in a sentence, with every number filled in from the app's own answer functions, plus a link to the page that shows it.

**Architecture:** A tool registry maps one tool to each `/api/v1/[league]/*` route and calls that route's answer function in-process. A provider-neutral turn loop lets a language model choose tools; the app fills the model's `{{r1.row2.net}}` markers with page-formatted cells and rejects any answer with a stray digit. A POST route streams status, then the verified answer, to a chat panel that the navbar opens on every page. The panel has no page context.

**Tech Stack:** Next.js 16.4.2 / React 19.3.1 / TypeScript 5.9.3 / zod 4.1.12 / vitest 5.0.5 / Playwright 1.56.1 (all in `web/`); `@anthropic-ai/sdk` and `openai` added in Tasks 8-9.

**Spec:** `docs/superpowers/specs/2026-10-24-home-ask-box-design.md` (read it first; the "API mapping" and "Settled after the builder review" sections are normative).

## Global Constraints

- No database access outside the answer functions in the spec's mapping table: no `server/db.ts`, `pg` or SQL under `web/src/ask/` (Task 2's test enforces it).
- No metric computed by the box or the model: no averages, differences or ranks; sorting by a returned column is allowed (`docs/shared_definitions.md` rule 1).
- Any digit shown must come from a filled marker or appear verbatim in the visitor's current question.
- Answer in the question's language; Hebrew answers right-to-left.
- Limits: 6 turns, about 20 questions per hour per browser and per IP, 500-character question, 4 tool calls per turn, `maxDuration = 60`, no tool calls started after 40 s, 10 rows per tool result.
- `ASK_ENABLED=false` hides the box and makes `POST /api/v1/ask` return 404. `ASK_FOLLOWUPS` (default `false`) gates follow-ups until Task 10 passes.
- Provider keys only in server env: `ANTHROPIC_API_KEY`, `OPENAI_API_KEY`; model chosen by `ASK_PROVIDER` (`anthropic` | `openai` | `fake`) and `ASK_MODEL`.
- Claude model ids: `claude-haiku-5-5`, `claude-sonnet-5-5`, `claude-opus-5-5`. OpenAI ids are looked up at eval time.
- Log line per turn: question, tools + parameters, verification result, model, tokens, cost, latency. No IP or identifiers.
- Code moves are byte-identical and verified by diff, never rewritten (root CLAUDE.md).
- Existing style: `web/CLAUDE.md` (TabKit/DataTable reuse, width rules in `layoutClasses.ts`, Shiny CSS, npm through PowerShell `npm.cmd`).
- Commits: the user's rule is that app files are committed and pushed to `sql/web-foundation`; tests, fixtures and docs stay local and unstaged. Each "Commit" step stages only the task's non-test files.

## Review Focus

1. **A link that opens the page with different numbers than the answer.** Tool queries are API queries; the page must read the same keys. Pin: Task 3 `link round-trips through the page parser` (the page path's server component calls the same parser with the link's query and gets the same parsed params).
2. **A team named in Hebrew or by nickname ("מכבי", "הפועל ת״א").** The visitor expects that team, not a near miss. Pin: Task 10 cases `he-nickname-*` (at least 8), each with the expected `team` value.
3. **A season or league named in the question, or EuroLeague asked for Compare.** Expect that season or league, or an honest "Compare is Israeli League only" plus the closest page. Pin: Task 6 `compare on euroleague returns tool error naming the league`, plus Task 10 cases `season-*` and `euro-compare-*`.
4. **A reader that times out or returns nothing (early season, no qualifying lineup).** Expect a short "no data for that yet" plus the link, never invented text. Pin: Task 6 `empty rows -> answer says no rows and keeps the link` and `tool deadline error -> fallback with link`.
5. **Forged follow-up history.** Expect numbers only from server-run tool calls. Pin: Task 6 `client-supplied results are ignored and calls are re-run`.

---

### Task 1: Byte-identical moves the tools need

**Files:**
- Modify: `web/src/server/lineupsApi.ts` (add `lineupRosterAnswer`)
- Modify: `web/src/app/api/v1/[league]/lineup-roster/route.ts` (call it)
- Create: `web/src/rules/format.ts` (receives `formatNumber`)
- Create: `web/src/rules/order.ts` (receives `orderKey`)
- Modify: `web/src/components/shiny/DataTable.tsx` (import and re-export both)

**Interfaces:**
- Produces: `lineupRosterAnswer(league: League, search: URLSearchParams)` returning `getLineupRoster(parseLineups(search, league))`; `formatNumber(v: unknown, digits: number): string` from `@/rules/format`; `orderKey(v: unknown): number | string | undefined` from `@/rules/order`. `DataTable.tsx` keeps exporting `formatNumber` so existing imports are unchanged.

- [ ] **Step 1: Move the roster call into `lineupRosterAnswer`** and make the route call `lineupRosterAnswer(leagueParam(...), url.searchParams)`. The expression moved is `getLineupRoster(parseLineups(search, league))`.
- [ ] **Step 2: Move `formatNumber` (DataTable.tsx lines 80-89) to `rules/format.ts` and `orderKey` (the uncommitted version with `undefined` for missing/NaN) to `rules/order.ts`.** No `"use client"` in either new file.
- [ ] **Step 3: Verify the moves by diff.** Run `git diff -U0 -- src/components/shiny/DataTable.tsx` and confirm removed lines equal the new files' bodies (excluding export/import lines). Expected: identical text.
- [ ] **Step 4: Run** `npm.cmd test -- --maxWorkers=1` and `npm.cmd run typecheck`. Expected: all pass, same counts as before.
- [ ] **Step 5: Commit** the five app files: `git commit -m "web: shared roster answer, formatNumber and orderKey modules for the ask box"`.

### Task 2: Tool registry and mapping tests

**Files:**
- Create: `web/src/ask/registry.ts`
- Test: `web/src/ask/registry.test.ts`

**Interfaces:**
- Consumes: Task 1's `lineupRosterAnswer`.
- Produces:
  ```ts
  export type ToolName = "onoff" | "lineups" | "lineup_games" | "lineup_roster" | "team_ratings"
    | "game_logs" | "player_stats" | "compare" | "team_overview" | "gameflow" | "list_teams";
  export type ToolEntry = {
    name: ToolName;
    route: string | null;              // dir under app/api/v1/[league]/, null for list_teams
    answerName: string | null;         // the identifier the route imports
    answer: ((league: League, s: URLSearchParams) => Promise<unknown>) | null;
    page: string | null;               // path segment of the page the link opens
    leagues: League[];                 // compare and team_overview: ["il"]
    inventory: string[];               // feature inventory IDs
    numbers: boolean;                  // false for gameflow (link-only in v1)
  };
  export const TOOLS: Record<ToolName, ToolEntry>;
  export const NOT_TOOLS: Record<string, string>; // route dir -> reason
  ```
  Values copied from the spec's mapping table. `compareAnswer` and `homeAnswer` take no league; wrap them as `(_l, s) => compareAnswer(s)`. `team_overview.page` is `"home"`; `lineup_games` and `lineup_roster` open `"lineups"`.

- [ ] **Step 1: Write the failing tests** in `registry.test.ts`:
  - `every route dir is a tool or excluded`: `readdirSync("src/app/api/v1/[league]")` equals the sorted union of `TOOLS[*].route` (non-null) and `Object.keys(NOT_TOOLS)`, where `NOT_TOOLS = { "player-stat-filters": <reason> }`. The league-less `src/app/api/v1/` holds only `[league]`, `version` and the new `ask`; anything else there fails.
  - `each tool names the function its route imports`: for each tool with a route, the route file text contains `import { ${answerName} }`.
  - `inventory ids exist`: every ID in every `inventory` appears as `| ${id} |` in `../docs/superpowers/specs/2026-10-19-api-mcp-feature-inventory.md`.
  - `ask code never touches the database`: for every `.ts`/`.tsx` under `src/ask/`, no import of `server/db`, `"pg"`, any `*Repo` module other than the ones the registry imports, and no `/\bSELECT\b|\bFROM basketball/` text.
- [ ] **Step 2: Run** `npm.cmd test -- src/ask/registry.test.ts`. Expected: FAIL, module missing.
- [ ] **Step 3: Implement `registry.ts`.** Only `registry.ts` may import answer functions; other ask files import from it.
- [ ] **Step 4: Run** the test file. Expected: PASS (4 tests).
- [ ] **Step 5: Commit** `src/ask/registry.ts`.

### Task 3: Running a tool

**Files:**
- Create: `web/src/ask/run.ts`, `web/src/ask/flatten.ts`
- Test: `web/src/ask/run.test.ts`

**Interfaces:**
- Consumes: `TOOLS` (Task 2), `orderKey` (Task 1), `rawQueryGuard` and `ParamError` from `server/api.ts`, `ui.ilTeams`/catalogue data for `list_teams`.
- Produces:
  ```ts
  export type ToolCall = { tool: ToolName; league: League; query: Record<string, string | string[]>;
    sort?: { key: string; desc: boolean }; limit?: number };
  export type AskRow = { id: string; cells: Record<string, unknown> };   // id "row1".."rowN", "total"
  export type ToolResult = { id: string; call: ToolCall; link: string | null; view: string | null;
    rows: AskRow[]; total: AskRow | null; rowCount: number; version: string | null };
  export type ToolError = { id: string; call: ToolCall; error: string; param: string | null };
  export function runTool(id: string, call: ToolCall, deadlineAt: number): Promise<ToolResult | ToolError>;
  export function flatten(tool: ToolName, payload: unknown): { rows: Record<string, unknown>[]; total: Record<string, unknown> | null };
  ```
- Behaviour pinned by the spec:
  - Build `URLSearchParams` from `query`; run `rawQueryGuard` on the built URL.
  - Wrap it in a `RecordingParams extends URLSearchParams` that records names passed to `get`/`getAll`/`has`. Call the answer function. The parsers all read synchronously before their first `await`. Any query key not recorded by the time the call returns its promise is an unknown parameter: return `ToolError` naming it, without awaiting.
  - `ParamError` -> `ToolError` with its `param` and message; `DeadlineError`/`BusyError` -> `ToolError` "the data took too long".
  - A league not in `entry.leagues` -> `ToolError` "`compare` is available for the Israeli League only".
  - `flatten`: `payload.rows` for every table tool. Rows with `is_total === 0` or `is_multi_team_total` truthy are not ranked: lineups' `is_total === 0` row becomes `total`. `team_overview`: one row per named section of `payload.frames` (record, ranks, key players, lineups, storylines). `gameflow`: `rows: []`.
  - Sort by `sort.key` with `orderKey`, missing last in both directions; unknown `sort.key` -> `ToolError`. `limit` defaults to 5, max 10. `rowCount` is the full count before limiting.
  - `link`: `/${league}/${entry.page}?${query}` (null for `list_teams`). `view` from `payload.meta.view`; `version` from `payload.meta.version`.

- [ ] **Step 1: Write the failing tests** (answer functions replaced with `vi.mock("@/ask/registry", ...)` returning recorded payloads):
  - `unknown parameter is rejected before awaiting` (query `{season:"2027", bogus:"1"}` -> `ToolError` with `param: "bogus"`).
  - `ParamError becomes a tool error with its param`.
  - `compare on euroleague returns tool error naming the league`.
  - `sorts by key, missing last, both directions` (values `[3, null, 7]` -> desc `[7,3,null]`, asc `[3,7,null]`).
  - `limit defaults to 5 and caps at 10; rowCount is pre-limit`.
  - `lineups total row is split out`.
  - `link carries the query and page`: lineups IL `{season:"2027", team:"2", size:"5"}` -> `/il/lineups?season=2027&team=2&size=5`.
  - `the linked page reads its query with the tool's answer function` (Review Focus 1): for each tool with a page, `src/app/[league]/<page>/page.tsx` (or the server module it imports) contains `import { ${answerName} }` and passes the page's `searchParams` to it. So the page parses the link's query with the same parser and gets the same rows.
  - `list_teams returns the season catalogue` for IL 2027 and the Euro team list.
- [ ] **Step 2: Run** `npm.cmd test -- src/ask/run.test.ts`. Expected: FAIL.
- [ ] **Step 3: Implement `run.ts` and `flatten.ts`.**
- [ ] **Step 4: Run.** Expected: PASS.
- [ ] **Step 5: Commit** `src/ask/run.ts`, `src/ask/flatten.ts`.

### Task 4: Page-identical cell text

**Files:**
- Create: `web/src/ask/display.ts`
- Test: `web/src/ask/display.test.ts`
- Modify (only if vitest cannot load `.tsx`): `web/vitest.config.ts` add `esbuild: { jsx: "automatic" }`

**Interfaces:**
- Consumes: `formatNumber` (Task 1), `formatCompare` (`rules/compare.ts`), `ToolResult` (Task 3).
- Produces:
  ```ts
  export type ColumnSpec = { label: string; digits?: number; kind?: "text" | "number" | "signed" | "compare" };
  export function displaySpec(tool: ToolName, league: League, view: string | null): Record<string, ColumnSpec>;
  export function cellText(tool: ToolName, league: League, view: string | null, key: string, row: AskRow): string | null; // null = not a displayed column
  ```
  The spec is a twin of each page's column builder (`LineupTable.tsx`, `TeamRatingsTable.tsx`, `PlayerStatsTable.tsx`, `OnOffTables.tsx` `summaryColumns`/`ffColumns`/`spColumns`, `GameLogsTable.tsx`, `CompareData.tsx`/`CompareDetails.tsx`, `HomeHub.tsx`), carrying only plain text and number columns. Shot cells, heat colours and composite cells are excluded: a marker to them is invalid.

- [ ] **Step 1: Write the pin test** `cellText equals the page column text`: for each tool and view, build the page's own `Column[]` (import the column builder, or render the table component's column list with stub handlers) over recorded rows from the existing golden fixtures, and assert `cellText(...) === col.render(row).text` (or `String(row[col.id] ?? "")` when the column has no `render`) for every key in `displaySpec`. Also assert every `displaySpec` key exists as a page column id with the same label.
- [ ] **Step 2: Run** `npm.cmd test -- src/ask/display.test.ts`. Expected: FAIL.
- [ ] **Step 3: Implement `display.ts`.** Labels come from `data/shinyUi.json` / `compareUi.json` / `homeUi.json` where the tables take them from there; never retyped.
- [ ] **Step 4: Run.** Expected: PASS for every tool, view and league.
- [ ] **Step 5: Commit** `src/ask/display.ts` (and `vitest.config.ts` if changed).

### Task 5: Filling markers and checking the answer

**Files:**
- Create: `web/src/ask/markers.ts`
- Test: `web/src/ask/markers.test.ts`

**Interfaces:**
- Consumes: `ToolResult`, `cellText` (Tasks 3-4).
- Produces:
  ```ts
  export type Filled = { ok: true; text: string; used: { result: string; row: string; key: string }[] }
                     | { ok: false; problems: string[] };
  export function fillAndVerify(template: string, results: ToolResult[], question: string, season: string): Filled;
  ```
  Marker grammar: `{{<resultId>.<rowId>.<key>}}` and `{{season}}`; `rowId` is `row1`..`rowN` or `total`.
- Rules (spec section 2 + digit rule): every marker resolves to an existing result, row and displayed column (`cellText !== null`); a marker into a `numbers: false` tool is invalid. After removing markers, the template contains no digit (Unicode `\p{Nd}`, which covers Western and Arabic-Indic), except digit runs that appear verbatim in `question`. Hebrew-letter numerals are not machine-checked: the same pattern is ordinary abbreviation (ת״א is Tel Aviv). The prompt forbids them and Task 10 checks for them. Unresolved `{{`/`}}` is a problem.

- [ ] **Step 1: Write the failing tests:**
  - `fills a marker with the page text` (`{{r1.row1.net_rtg}}` with `net_rtg` `7.25`, digits 1 -> `"7.3"`; the expected string comes from `cellText`, not retyped).
  - `rejects a typed digit` ("Net 7.3" -> problem names `7.3`).
  - `allows digits from the question` (question "top 5 lineups", template "the top 5 ..." -> ok).
  - `rejects Arabic-Indic digits` ("٧") and `allows Hebrew abbreviations` ("הפועל ת״א" passes).
  - `rejects unknown result, row, key, and non-displayed column`.
  - `rejects markers into gameflow`.
  - `season marker uses the season label`.
- [ ] **Step 2: Run** `npm.cmd test -- src/ask/markers.test.ts`. Expected: FAIL.
- [ ] **Step 3: Implement `markers.ts`.**
- [ ] **Step 4: Run.** Expected: PASS.
- [ ] **Step 5: Commit** `src/ask/markers.ts`.

### Task 6: Provider interface, scripted provider and the turn loop

**Files:**
- Create: `web/src/ask/providers/types.ts`, `web/src/ask/providers/fake.ts`, `web/src/ask/prompt.ts`, `web/src/ask/turn.ts`
- Test: `web/src/ask/turn.test.ts`

**Interfaces:**
- Consumes: `runTool` (Task 3), `fillAndVerify` (Task 5), `TOOLS` (Task 2).
- Produces:
  ```ts
  // providers/types.ts
  export type ProviderMessage = { role: "user" | "assistant"; text?: string;
    toolCalls?: { id: string; call: ToolCall }[]; toolResults?: { id: string; content: unknown }[] };
  export type ProviderReply = { text: string | null; toolCalls: { id: string; call: ToolCall }[];
    usage: { inputTokens: number; outputTokens: number; costUsd: number } };
  export interface Provider { name: string; model: string;
    reply(system: string, tools: ToolSchema[], messages: ProviderMessage[], signal: AbortSignal): Promise<ProviderReply> }
  export type ToolSchema = { name: ToolName; description: string; parameters: object };
  // providers/fake.ts
  export function scriptedProvider(script: ProviderReply[]): Provider;   // replies in order
  // prompt.ts
  export function systemPrompt(league: League, today: string): string;   // stable text for caching
  export function toolSchemas(): ToolSchema[];                           // one per TOOLS entry, query as object of strings
  // turn.ts
  export type HistoryTurn = { question: string; answer: string; calls: ToolCall[] }; // from the browser; no results
  export type TurnEvent = { type: "status"; text: string } | { type: "final"; answer: AskAnswer };
  export type AskAnswer = { text: string; lang: "he" | "en"; results: ToolResult[];
    evidence: { result: string; row: string; key: string; label: string; text: string }[];
    links: string[]; fallback: boolean; usage: ProviderReply["usage"] };
  export function askTurn(input: { league: League; question: string; history: HistoryTurn[] },
    provider: Provider, opts: { now: () => number; followups: boolean }): AsyncGenerator<TurnEvent>;
  ```
- Loop rules: at most 4 tool calls per turn, counting re-runs of history calls. No new call after 40 s from start. History calls are re-run server-side by `runTool`; nothing from the browser is used as data. Status event per tool ("Reading Lineups..."). Final text through `fillAndVerify`. On failure, one regeneration with the problems sent back; on a second failure `fallback: true`, text = a fixed "Here's the page that answers this" in the question's language, links and evidence kept. Language: `he` when the question contains Hebrew letters, else `en`. `followups: false` -> history ignored. `systemPrompt` states: markers only, no typed numbers or number words, superlatives need a rank marker, no computed differences or averages, out-of-scope -> say so + closest page, Compare IL only, answer in the question's language.

- [ ] **Step 1: Write the failing tests** with `scriptedProvider` and mocked `runTool`:
  - `one tool call, then marker text -> final answer with link and evidence`.
  - `typed digit -> regenerates once, then succeeds`.
  - `two failures -> fallback with link and evidence`.
  - `fifth tool call is refused with a tool error`.
  - `no tool call starts after 40 s` (fake `now`).
  - `client-supplied results are ignored and calls are re-run` (Review Focus 5: history turn with a `results` field is stripped by the type guard; `runTool` is called for the reused call).
  - `followups disabled ignores history`.
  - `empty rows -> answer says no rows and keeps the link` (Review Focus 4).
  - `tool deadline error -> fallback with link` (Review Focus 4).
  - `compare on euroleague returns tool error naming the league` (Review Focus 3, passed through to the model as a tool result).
  - `Hebrew question -> lang he and Hebrew fallback text`.
- [ ] **Step 2: Run** `npm.cmd test -- src/ask/turn.test.ts`. Expected: FAIL.
- [ ] **Step 3: Implement** the four files.
- [ ] **Step 4: Run.** Expected: PASS.
- [ ] **Step 5: Commit** the four app files.

### Task 7: The ask route, limits, off switch and logging

**Files:**
- Create: `web/src/app/api/v1/ask/route.ts`, `web/src/ask/limits.ts`, `web/src/ask/log.ts`, `web/src/ask/provider.ts`
- Test: `web/src/ask/limits.test.ts`, `web/src/app/api/v1/ask/route.test.ts`

**Interfaces:**
- Consumes: `askTurn` (Task 6), `scriptedProvider`.
- Produces:
  - `POST /api/v1/ask`, `runtime = "nodejs"`, `maxDuration = 60`. Body (zod): `{ league: "il"|"euroleague"|"eurocup", question: string (1..500), history: HistoryTurn[] (max 5 entries), clientId: string (max 64) }`. Response `text/event-stream` (`data: <TurnEvent JSON>\n\n`).
  - `limits.ts`: `checkLimit(keys: string[], now: number): { ok: true } | { ok: false; retryAfterSec: number }`. Sliding window of 20 per hour per key; keys are `client:<clientId>` and `ip:<x-forwarded-for first hop>`. Process memory only (spec: per instance while testing).
  - `provider.ts`: `currentProvider(): Provider` from `ASK_PROVIDER`/`ASK_MODEL` (`fake` reads `ASK_FAKE_SCRIPT`, a JSON file path, for tests and offline e2e).
  - `log.ts`: `logTurn(entry)`, one `console.log(JSON.stringify({ kind: "ask_turn", ... }))` with the spec's fields only.
- [ ] **Step 1: Write the failing tests:** `21st request in an hour is limited with retryAfter`, `window slides`; route: `ASK_ENABLED=false -> 404`, `question over 500 chars -> 400`, `6th turn (history length 6) -> 400 "start a new question"`, `streams status then final with fake provider`, `log line has no ip or clientId`.
- [ ] **Step 2: Run** `npm.cmd test -- src/ask/limits.test.ts src/app/api/v1/ask/route.test.ts`. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** Expected: PASS. Also `npm.cmd run typecheck` and `npm.cmd run build`.
- [ ] **Step 5: Commit** the four app files.

### Task 8: Anthropic adapter (needs the user's Anthropic key for Step 4 only)

**Files:**
- Create: `web/src/ask/providers/anthropic.ts`
- Modify: `web/package.json` (add `@anthropic-ai/sdk`, exact version pinned, at least two weeks old)
- Test: `web/src/ask/providers/anthropic.test.ts`

**Interfaces:**
- Consumes: `Provider` types (Task 6). Read the claude-api skill's TypeScript `README.md` and `tool-use.md` before writing; do not guess SDK names.
- Produces: `anthropicProvider(model: string): Provider`. Manual loop shape, one `messages.create` per `reply`. `tool_choice: auto`, `strict: true` tools, top-level `cache_control: {type: "ephemeral"}`, `output_config.effort: "low"` (re-tuned in Task 10). `costUsd` from usage and the price table: Haiku 5.5 $0.10/$0.50, Sonnet 5.5 $2/$10, Opus 5.5 $4/$20 per million input/output tokens; cache reads at the model's cache-read rate. `stop_reason: "refusal"` -> fallback answer.
- [ ] **Step 1: Write the failing tests** against a stubbed SDK client: `maps tool_use blocks to toolCalls`, `maps tool results back as tool_result blocks in one user message`, `computes cost from usage`, `refusal becomes fallback`.
- [ ] **Step 2: Run.** Expected: FAIL. **Step 3: Implement. Step 4: Run.** Expected: PASS. Then, with a key, `ASK_PROVIDER=anthropic ASK_MODEL=claude-haiku-5-5` and one manual question against the local server on port 3100.
- [ ] **Step 5: Commit** `anthropic.ts`, `package.json`, `package-lock.json`.

### Task 9: OpenAI adapter (needs the user's OpenAI key for Step 4 only)

Same shape as Task 8: `web/src/ask/providers/openai.ts`, `openaiProvider(model: string): Provider`, official `openai` SDK with an exact pinned version; read its current docs for tool calling before writing. Prices come from OpenAI's pricing page on the day, written into a constant with the date in a comment. Same four tests in `openai.test.ts`. Commit `openai.ts` and the package files.

### Task 10: Test set, eval and model choice

**Files:**
- Create: `web/test/ask/cases.json`, `web/scripts/ask-eval.ts`
- Output: `output/ask/eval-<date>.json` (ignored), summary in `docs/ask_eval_<date>.md` (local)

**Interfaces:**
- Consumes: `askTurn`, `anthropicProvider`, `openaiProvider`, live readers through the local production server environment (`PG_PORT=5432`, read-only role).
- Case format: `{ id, lang: "he"|"en", turns: string[], expect: { tool: ToolName, league: League, query: Record<string,string> /* required subset */ }[] | "unanswerable" }`.
- Cases: 40-50 total, about half Hebrew. Must include the user's three examples (EN and HE), every tool, all three competitions, `season-*` (named and unnamed seasons), at least 8 `he-nickname-*`, `euro-compare-*`, out-of-scope questions, and 6-8 multi-turn cases.
- Scoring per case: right tools and query subset; verification passed without fallback; correct language; unanswerable handled. Plus cost and latency.
- Decision rule (spec section 6): any answer with a number not from a marker, or failing verification twice, disqualifies the model. Among the rest, the cheapest with at least 90% right tool and parameters wins.

- [ ] **Step 1: Write `cases.json`** and get the user to skim the Hebrew cases.
- [ ] **Step 2: Implement `ask-eval.ts`** (`--provider`, `--model`, `--cases`). Runs cases sequentially, one reader call at a time, as the live smoke checks do. Run it in a window the user approves.
- [ ] **Step 3: Run** `node node_modules/tsx/dist/cli.mjs scripts/ask-eval.ts --provider anthropic --model claude-haiku-5-5`, then Sonnet 5.5, Opus 5.5 and the chosen OpenAI models. Expected: a summary table per model.
- [ ] **Step 4: Write the summary** with the winner. The user spot-checks 10 of the winner's answers. Set the winner as the documented `ASK_PROVIDER`/`ASK_MODEL` defaults and turn on `ASK_FOLLOWUPS=true` once the multi-turn cases pass.
- [ ] **Step 5: Commit** only the default change in `provider.ts` (the eval script and cases stay local per the user's rule).

### Task 11: The chat panel on every page

**Files:**
- Create: `web/src/components/shiny/AskPanel.tsx` (panel UI), `web/src/components/AskProvider.tsx` (`"use client"` context: open state, conversation, `clientId`)
- Modify: `web/src/app/layout.tsx` (wrap children in `AskProvider`, passing `askEnabled` read from `ASK_ENABLED` on the server)
- Modify: `web/src/components/shiny/Navbar.tsx` (an "Ask" button that toggles the panel, rendered only when `askEnabled`; the navbar renders on every page through TabKit, HomeClient and GameflowClient)
- Test: `web/e2e/ask.spec.ts` (with `ASK_PROVIDER=fake` and a scripted file)

**Interfaces:**
- Consumes: `POST /api/v1/ask` (Task 7), `AskAnswer`/`TurnEvent` types (Task 6).
- Produces: `useAsk(): { enabled: boolean; open: boolean; toggle(): void; turns: AskTurnView[]; ask(question: string, league: League): Promise<void>; reset(): void }`.
- Behaviour:
  - The panel is a side drawer from 1200 px and a full-width bottom sheet below, following the width rules in `layoutClasses.ts`. Escape and the close button close it and return focus to the Ask button.
  - Textarea plus a submit button; Enter submits, Shift+Enter adds a newline. Status lines while streaming.
  - Answer text gets `dir="rtl"` for `lang: "he"`. The evidence table is built from `answer.evidence` with the existing DataTable styles, no sorting or paging. The first link is the primary button; following it navigates client-side and keeps the panel and its conversation.
  - Follow-up input only when follow-ups are on; after 6 turns, "Start a new question".
  - The league sent is the current route's `[league]` segment; nothing else from the page is sent.
  - Conversation and `clientId` are mirrored to `sessionStorage` behind try/catch, and the page works without it.
  - Errors (429 with retry time, 400, network) show inline and keep the question text.
  - The page underneath stays usable; no horizontal scroll at 390 px.

- [ ] **Step 1: Write the failing e2e tests** at 390 and 1440 px:
  - `ask button opens the panel on every page` (home, onoff, lineups, team-ratings, game-logs, player-stats, compare, gameflow).
  - `asks and shows answer, evidence and link`.
  - `link opens the page with the same filters and the conversation stays`.
  - `conversation survives a tab switch and a reload`.
  - `question from a EuroLeague page is sent with league euroleague`.
  - `Hebrew answer is rtl`.
  - `fallback shows the page link`.
  - `429 shows retry time and keeps the question`.
  - `Escape closes and focus returns to the Ask button`.
  - `no Ask button when ASK_ENABLED=false`.
  - `no horizontal overflow at 390`.
- [ ] **Step 2: Run** `node node_modules/playwright/cli.js test e2e/ask.spec.ts --workers=1`. Expected: FAIL.
- [ ] **Step 3: Implement** `AskPanel.tsx`, `AskProvider.tsx` and the two modifications.
- [ ] **Step 4: Run** the e2e file plus `e2e/home.spec.ts` and `e2e/selectors.spec.ts` (navbar regressions), `npm.cmd test -- --maxWorkers=1`, `npm.cmd run typecheck`, `npm.cmd run lint` and `npm.cmd run build`. Expected: all pass; lint shows only the two existing warnings. Inspect phone and desktop screenshots in both languages.
- [ ] **Step 5: Commit** the four app files and push `sql/web-foundation`.
