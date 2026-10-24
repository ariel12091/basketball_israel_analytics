# Ask chat panel — handoff (2026-10-24)

A chat box on every page of the React app (`web/`). A visitor asks in Hebrew or
English; a language model answers by calling **tools that are the app's own
`/api/v1/[league]/*` answer functions, run in-process**. It never touches the
database any other way. The model writes markers such as `{{r1.row1.net_rtg}}`;
the app fills them with the exact text the page shows, rejects any digit the
model typed itself, and shows the answer with the quoted cells and a link to
the page.

- Spec: `docs/superpowers/specs/2026-10-24-home-ask-box-design.md` (approved; scope
  changed to "every page, no page context")
- Plan: `docs/superpowers/plans/2026-10-24-home-ask-box.md`
- Ledger with every ruling and its cost if wrong:
  `.superpowers/sdd/2026-10-24-home-ask-box/progress.md` (gitignored, local only;
  the rulings that matter are repeated below)

## State

| Task | State |
|---|---|
| 1-8, 11 | Done, committed and pushed on `sql/web-foundation` (da5c2b4..aee8576) |
| 9 OpenAI adapter | Not started: user has no OpenAI account yet |
| 10 Model comparison | Cases and script written; not run (needs API keys) |
| Final whole-branch review | Not done; run after Tasks 9-10 |

**Off by default.** The panel and `POST /api/v1/ask` exist only when
`ASK_ENABLED=true`. With it on and no key, a real question fails with an error.

## Configuration (server env only)

| Variable | Meaning |
|---|---|
| `ASK_ENABLED` | `true` shows the panel and opens the route; anything else is off |
| `ASK_FOLLOWUPS` | `true` sends earlier turns; off until Task 10's multi-turn cases pass |
| `ASK_PROVIDER` | `anthropic`, `fake` (replays `ASK_FAKE_SCRIPT`, a JSON array of replies) |
| `ASK_MODEL` | defaults to `claude-haiku-5-5` |
| `ANTHROPIC_API_KEY` | needed for `anthropic`; set a monthly spend cap in the console |

## Code map (`web/src/`)

- `ask/registry.ts` — one tool per API route (the mapping table in the spec),
  plus `list_teams`. Table tools carry the page's request builder
  (`queryFromSearch` defaults + `lineupsPath`/`onoffPath`/...). Only this file
  imports answer functions.
- `ask/run.ts` — `runTool()`: builds the request exactly as the page does,
  rejects keys the page would ignore, requires `sort` on table tools, sorts
  (missing last), caps rows, builds the link (`linkFor`).
- `ask/pageRows.ts` — the rows a freshly opened link shows: each page's own table
  rules (Lineups auto minimum, Player Stats display mode, Team Ratings and On/Off
  table rows, On/Off first-load auto minimum).
- `ask/compareTool.ts` — Compare's route answers one side; the tool reads A and B
  and pairs them with `joinCompare`/`finalizeCompare`, like `CompareClient`.
- `ask/flatten.ts` — payload to rows; Home hub sections via `homeDashboard`.
- `ask/display.ts` — the quotable columns and the page formatter for each.
- `ask/markers.ts` — fill markers, reject typed numbers.
- `ask/turn.ts` — the loop: 4 tool calls, 40 s tool budget, one retry, fallback,
  history calls re-run server-side (browser results are never trusted).
- `ask/prompt.ts` — system prompt and tool descriptions (stable text, cacheable).
- `ask/providers/` — `types.ts`, `fake.ts`, `anthropic.ts` (SDK 0.133.0, manual
  loop, raw assistant content round-tripped, prompt caching, low effort).
- `ask/limits.ts`, `ask/log.ts`, `ask/provider.ts`, `app/api/v1/ask/route.ts`.
- `components/AskProvider.tsx` (state in the root layout + sessionStorage),
  `components/shiny/AskPanel.tsx`, styles at the end of `app/web.css`.

## Tests

- Unit (vitest): `ask/*.test.ts`, `ask/providers/*.test.ts`,
  `app/api/v1/ask/route.test.ts` — 87 tests. Key pins: `display.test.ts`
  renders the real page tables and requires identical cell text; `registry.test.ts`
  maps every route and bans DB access from `ask/` (mutation-checked).
- Browser: `e2e/ask.spec.ts` — needs a server with `ASK_ENABLED=true`
  (it mocks `/api/v1/ask` in the browser). 40/40 on 2026-10-24.
- Eval: `test/ask/cases.json` (44 cases, 21 Hebrew) and `scripts/ask-eval.ts`:
  `node --conditions=react-server --import tsx scripts/ask-eval.ts --provider anthropic --model claude-haiku-5-5`
  from `web/`. Real readers, sequential, `PG_PORT=5432`. Spends money.

## Rulings worth knowing

- Tools must reproduce the page's request and table rules, not just call the
  answer function: answer functions return the population and pages narrow it
  in the browser. The checkpoint review's Critical finding was exactly this.
- The Lineups page's automatic minimum is 0 possessions for a single team, so
  the page itself ranks tiny samples first; the tool mirrors it. Use
  `team_overview` for best/worst lineup with 100+ possessions.
- Compare in the chat is the league table only (no detail or players views);
  Players mode is not in v1. `gameflow` is link-only.
- Hebrew-letter numerals and number words are not machine-checked (ת״א is an
  abbreviation); the eval flags number words.
- The Ask button floats bottom-right instead of in the navbar (no spare navbar
  width, see root CLAUDE.md). After a full reload the conversation is kept but
  the panel starts closed.
- Deferred minors: display pins cover only the Israeli league and Per Game mode;
  Compare team names come from the catalogue (the page uses its refs).

## Next steps

1. With an Anthropic key: run the eval for Haiku 5.5, Sonnet 5.5 (Opus 5.5 as the
   ceiling); the user skims the Hebrew cases first.
2. With an OpenAI key: write `providers/openai.ts` from its current docs, run the eval.
3. Pick the model (any failed-twice answer or number word disqualifies; then the
   cheapest with at least 90% right tool and parameters), set the default, turn on
   `ASK_FOLLOWUPS` if the multi-turn cases pass.
4. Final whole-branch review on the most capable model, then fix Critical/Important.
