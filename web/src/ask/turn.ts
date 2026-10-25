import "server-only";
// One question: the model chooses tools, the app runs them, and the answer is
// shown only after its markers are filled and checked (spec sections 2 and 4).
import ui from "@/data/shinyUi.json";
import type { League } from "@/rules/gameLogs";
import { cellText, columnDefinition, displaySpec } from "./display";
import { fillAndVerify } from "./markers";
import { systemPrompt, toolSchemas } from "./prompt";
import { TOOLS } from "./registry";
import { linkFor, runTool, type ToolCall, type ToolError, type ToolResult } from "./run";
import type { Provider, ProviderMessage, Usage } from "./providers/types";
import { turnLifetime } from "./lifetime";

export type HistoryTurn = { question: string; answer: string; calls: ToolCall[] };
export type Evidence = { result: string; row: string; key: string; label: string; text: string };
export type ToolProblem = { tool: string; param: string | null; error: string };
export type AskAnswer = {
  text: string;
  lang: "he" | "en";
  results: ToolResult[];
  /** This turn's own successful lookups (not the re-run history): what the
   * browser sends back as this turn's calls on the next question. */
  calls: ToolCall[];
  evidence: Evidence[];
  links: string[];
  fallback: boolean;
  usage: Usage;
  /** Tool calls the app rejected this turn (for the log and the eval). */
  errors: ToolProblem[];
  /** Answers the number check rejected, with its reasons. */
  rejected: { text: string; problems: string[] }[];
};
export type TurnEvent = { type: "status"; text: string } | { type: "final"; answer: AskAnswer };

// A two-player Lineups comparison needs four (teams, roster, two reads);
// six leaves room for one detour.
export const MAX_CALLS = 6;
const MAX_RERUNS = 6;
const MAX_RETRIES = 2;
const TOOL_BUDGET_MS = 40_000;
const MAX_STEPS = 8;
const FALLBACK = {
  en: "Here's the page that answers this.",
  he: "הנה העמוד שעונה על השאלה.",
};
const TOOL_LABELS: Record<string, string> = {
  onoff: "On/Off",
  lineups: "Lineups",
  lineup_games: "the lineup's games",
  lineup_roster: "rosters",
  team_ratings: "Team Ratings",
  game_logs: "Game Logs",
  player_stats: "Player Stats",
  compare: "Compare",
  team_overview: "the team overview",
  gameflow: "Gameflow",
  list_teams: "the team list",
};

const isResult = (r: ToolResult | ToolError): r is ToolResult => "rows" in r;
/** The app's Hebrew basketball terms: possessions are פוזשנים, not החזקות.
 * Applied to the final text so it holds whatever the model wrote. Only where
 * the word must be the noun: חזקה/חזקות alone is also "strong" ("הגנה חזקה"). */
function hebrewTerms(text: string): string {
  return text
    // After a count, or "number of / per / total": "188 החזקות", "מספר החזקות".
    .replace(/(\p{Nd}\s*|(?<!\p{L})(?:מספר|לכל|סך)\s+)ה?חזקות(?!\p{L})/gu, "$1פוזשנים")
    // With the article on the noun itself: "ההחזקות".
    .replace(/(?<!\p{L})([ובלמשכ]?)ההחזקות(?!\p{L})/gu, "$1הפוזשנים")
    .replace(/(?<!\p{L})([ובלמשכ]?)ההחזקה(?!\p{L})/gu, "$1הפוזשן")
    // "בהפסד הפסד 90-93": the filled result already says ניצחון/הפסד.
    .replace(/(ב?)(הפסד|ניצחון)\s+\2(?!\p{L})/gu, "$1$2");
}

/** Columns the model needs to make its next lookup, shown even when not quotable. */
const ID_KEYS = ["team_id", "player_id", "game_id", "sub_lineup_hash", "identity_id"];
/** Result-level markers ({{r1.count}}, {{r1.side_a}}) as the model sees them. */
const EXTRA_LABELS: Record<string, string> = {
  count: "Rows the page lists",
  side_a: "Compare side A heading",
  side_b: "Compare side B heading",
  min_poss: "Minimum possessions the rows were filtered by",
  entity: "The team or lineup the Compare detail is about",
  context_a: "Side A games, record and possessions",
  context_b: "Side B games, record and possessions",
};

/** What the model sees of a result: ids, quotable columns and their raw values. */
function forModel(r: ToolResult | ToolError) {
  if (!isResult(r)) return { id: r.id, error: r.error, param: r.param };
  const spec = displaySpec(r.call.tool, r.call.league, r.view);
  const keys = Object.keys(spec);
  // The quotable columns, plus the identifiers the next lookup needs.
  const shown = keys.length ? [...ID_KEYS, ...keys] : null;
  const pick = (cells: Record<string, unknown>) =>
    Object.fromEntries(
      (shown ?? Object.keys(cells)).filter((k) => k in cells).map((k) => [k, cells[k]]),
    );
  return {
    id: r.id,
    query: r.call.query,
    link: r.link,
    view: r.view,
    rowCount: r.rowCount,
    quotable: TOOLS[r.call.tool].numbers
      ? Object.fromEntries(Object.entries(spec).map(([k, c]) => [k, columnDefinition(c.label)]))
      : {},
    rows: r.rows.map((row) => ({ id: row.id, ...pick(row.cells) })),
    ...(r.total ? { total: { id: "total", ...pick(r.total.cells) } } : {}),
    // Quote these as {{<id>.<key>}}, e.g. {{r1.count}} or {{r1.side_a}}.
    values: Object.fromEntries(
      Object.entries(r.extras ?? {}).map(([k, v]) => [
        k,
        `${EXTRA_LABELS[k] ?? k}: ${v} (quote as {{${r.id}.${k}}})`,
      ]),
    ),
  };
}

/** {{season}}: the season the answer's lookups used, in their own league's
 * convention; the section's league first, else the league the question was
 * about (a EuroLeague question asked from the Israeli section). */
function seasonLabel(league: League, calls: ToolCall[]): string {
  const withSeason = calls.filter((c) => typeof c.query.season === "string");
  const c = withSeason.find((x) => x.league === league) ?? withSeason[0];
  return c
    ? seasonLabelFor(c.league, c.query.season as string)
    : seasonLabelFor(calls[0]?.league ?? league, undefined);
}

/** A tool call as the app can run and store it: the model's input is
 * untrusted, so query values become strings, limit an integer within 1-10 and
 * sort a key with a direction; anything else is a fixable parameter error. */
function normalizeCall(call: ToolCall): ToolCall | { error: string; param: string } {
  const raw = call as unknown as Record<string, unknown>;
  const q = raw.query ?? {};
  if (typeof q !== "object" || Array.isArray(q) || q === null)
    return { error: "query must be an object of parameter values", param: "query" };
  const query: Record<string, string | string[]> = {};
  for (const [k, v] of Object.entries(q as Record<string, unknown>)) {
    if (v == null) continue;
    if (Array.isArray(v)) query[k] = v.map(String);
    else if (typeof v === "object") return { error: `Give ${k} as text`, param: k };
    else query[k] = String(v);
  }
  let sort: ToolCall["sort"];
  if (raw.sort != null) {
    const s = raw.sort as Record<string, unknown>;
    if (typeof s !== "object" || typeof s.key !== "string")
      return { error: "sort must be { key, desc }", param: "sort" };
    sort = { key: s.key, desc: Boolean(s.desc) };
  }
  const n = Math.trunc(Number(raw.limit));
  return {
    tool: call.tool,
    league: call.league,
    query,
    ...(sort ? { sort } : {}),
    ...(raw.limit != null && Number.isFinite(n) ? { limit: Math.min(Math.max(1, n), 10) } : {}),
  };
}

function seasonLabelFor(league: League, raw: string | undefined): string {
  const y = Number(raw ?? (league === "il" ? ui.defaultGameYear : ui.euroDefaultSeason));
  const start = league === "il" ? y - 1 : y;
  return `${start}-${String(start + 1).slice(2)}`;
}

/** Only the fields a history turn may carry; results from the browser are dropped. */
function cleanHistory(history: HistoryTurn[]): HistoryTurn[] {
  return history.map((h) => ({
    question: String(h.question ?? ""),
    answer: String(h.answer ?? ""),
    calls: (h.calls ?? []).map((c) => ({
      tool: c.tool,
      league: c.league,
      query: c.query,
      ...(c.sort ? { sort: c.sort } : {}),
      ...(c.limit ? { limit: c.limit } : {}),
    })),
  }));
}

export async function* askTurn(
  input: { league: League; question: string; history: HistoryTurn[] },
  provider: Provider,
  opts: { now: () => number; followups: boolean; signal?: AbortSignal },
): AsyncGenerator<TurnEvent> {
  const lifetime = turnLifetime(TOOL_BUDGET_MS, opts.signal);
  try {
    yield* turnEvents(input, provider, opts, lifetime);
  } finally {
    lifetime.dispose();
  }
}

async function* turnEvents(
  input: { league: League; question: string; history: HistoryTurn[] },
  provider: Provider,
  opts: { now: () => number; followups: boolean },
  lifetime: ReturnType<typeof turnLifetime>,
): AsyncGenerator<TurnEvent> {
  lifetime.signal.throwIfAborted();
  const deadline = opts.now() + TOOL_BUDGET_MS;
  const lang = /[֐-׿]/.test(input.question) ? "he" : "en";
  const usage: Usage = { inputTokens: 0, outputTokens: 0, costUsd: 0 };
  const all: (ToolResult | ToolError)[] = [];
  const messages: ProviderMessage[] = [];
  let calls = 0;

  // Re-running earlier turns' lookups has its own budget, so a follow-up always
  // keeps its MAX_CALLS lookups (it used to inherit the earlier turn's four).
  // The same lookup twice in one question returns the first result (same id)
  // instead of spending another of the question's lookups.
  const done = new Map<string, { result: ToolResult; limit: number }>();
  const own: ToolCall[] = [];
  async function* execute(id: string, requested: ToolCall, rerun = false) {
    const normal = normalizeCall(requested);
    if ("error" in normal) {
      // Read nothing, so it uses up no lookup; MAX_STEPS bounds the retries.
      const err: ToolError = { id, call: requested, error: normal.error, param: normal.param };
      all.push(err);
      return err;
    }
    const call = normal;
    // Same tool, league, filters (in any order) and sort; the limit only
    // matters when this call asks for more rows than the earlier one got.
    const key = JSON.stringify([
      call.tool,
      call.league,
      Object.entries(call.query).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)),
      call.sort ?? null,
    ]);
    const limit = call.limit ?? 5;
    const prior = done.get(key);
    if (!rerun && prior && prior.limit >= limit) return prior.result;
    if (!rerun && calls >= MAX_CALLS) {
      const err: ToolError = { id, call, error: "Tool call limit reached for this question", param: null };
      all.push(err);
      return err;
    }
    if (!rerun) calls++;
    yield { type: "status" as const, text: `Reading ${TOOL_LABELS[call.tool] ?? call.tool}…` };
    const r = await lifetime.wait(() => runTool(id, call, deadline));
    all.push(r);
    // A call rejected for a bad parameter read nothing, so it does not use up
    // one of the question's lookups (MAX_STEPS still bounds the retries).
    if (!rerun && !isResult(r) && r.param != null) calls--;
    // Only successes are reused: a failed lookup (a timeout) may be retried.
    if (!rerun && isResult(r)) {
      done.set(key, { result: r, limit });
      own.push(r.call);
    }
    return r;
  }

  const rejected: { text: string; problems: string[] }[] = [];
  const finish = (text: string | null, used: Evidence[], fallback: boolean): TurnEvent => {
    const results = all.filter(isResult);
    const usedIds = new Set(used.map((u) => u.result));
    const links = [
      ...new Set(
        (fallback || !usedIds.size
          ? all.map((r) => (isResult(r) ? r.link : linkFor(r.call)))
          : results.filter((r) => usedIds.has(r.id)).map((r) => r.link)
        ).filter((l): l is string => !!l),
      ),
    ];
    return {
      type: "final",
      answer: {
        text: text ?? FALLBACK[lang],
        lang,
        results,
        calls: own,
        evidence: used,
        links,
        fallback,
        usage,
        errors: all
          .filter((r): r is ToolError => !isResult(r))
          .map((r) => ({ tool: r.call.tool, param: r.param, error: r.error })),
        rejected,
      },
    };
  };

  try {
    // Earlier turns: their questions and answers, and their tool calls re-run
    // here so every number comes from this server, never from the browser.
    if (opts.followups) {
      const history = cleanHistory(input.history);
      // Only the most recent MAX_RERUNS earlier lookups are re-run; older turns
      // keep their question and answer text.
      const total = history.reduce((n, h) => n + h.calls.length, 0);
      let seen = 0;
      for (const h of history) {
        messages.push({ role: "user", text: h.question });
        const reran: { id: string; content: unknown }[] = [];
        const toolCalls = [];
        for (const call of h.calls) {
          if (seen++ < total - MAX_RERUNS) continue;
          const id = `r${all.length + 1}`;
          const r = yield* execute(id, call, true);
          toolCalls.push({ id, call });
          reran.push({ id, content: forModel(r) });
        }
        if (toolCalls.length) {
          messages.push({ role: "assistant", toolCalls });
          messages.push({ role: "user", toolResults: reran });
        }
        messages.push({ role: "assistant", text: h.answer });
      }
    }
    // The visitor's question, with its language spelled out for the model.
    messages.push({
      role: "user",
      text: `${input.question}\n\n(Answer in ${lang === "he" ? "Hebrew" : "English"}.)`,
    });

    const system = systemPrompt(input.league),
      tools = toolSchemas();
    let retries = 0;
    for (let step = 0; step < MAX_STEPS; step++) {
      const reply = await lifetime.wait(() => provider.reply(system, tools, messages, lifetime.signal));
      usage.inputTokens += reply.usage.inputTokens;
      usage.outputTokens += reply.usage.outputTokens;
      usage.costUsd += reply.usage.costUsd;
      if (reply.refused) break;
      if (reply.toolCalls.length) {
        messages.push({
          role: "assistant",
          text: reply.text ?? undefined,
          toolCalls: reply.toolCalls,
          raw: reply.raw,
        });
        const out: { id: string; content: unknown }[] = [];
        for (const tc of reply.toolCalls) {
          const r = yield* execute(`r${all.length + 1}`, tc.call);
          out.push({ id: tc.id, content: forModel(r) });
        }
        messages.push({ role: "user", toolResults: out });
        continue;
      }
      // The panel shows plain text: markdown emphasis would appear as asterisks.
      const text = (reply.text ?? "").replace(/\*\*|__/g, "");
      const results = all.filter(isResult);
      let filled = fillAndVerify(
        text,
        results,
        // Numbers the visitor used anywhere in the conversation ("top 5" two
        // questions ago) may be repeated.
        [...(opts.followups ? input.history.map((h) => String(h.question ?? "")) : []), input.question].join("\n"),
        seasonLabel(input.league, results.map((r) => r.call)),
        lang,
      );
      // A turn whose every tool failed has nothing to stand on: show the page.
      if (all.length && !results.length) break;
      // Checks on the answer's own words (markers removed), reported together with
      // the number check so one retry can fix everything.
      const words = text.replace(/\{\{[^{}]*\}\}/g, " ");
      const extra: string[] = [];
      if (/[֐-׿]/.test(words) !== (lang === "he"))
        extra.push(`Answer in ${lang === "he" ? "Hebrew" : "English"}, the language of the question`);
      // Links come from the app only; the answer text never carries an address.
      if (/https?:\/\/|www\.|[\w.-]+@[\w-]+\.\w|\b[\w-]+\.(?:com|org|net|io|co|il|app|dev|me|ly)\b/i.test(words))
        extra.push("Remove the web address or email; the app adds the page link");
      if (extra.length)
        filled = { ok: false, problems: [...(filled.ok ? [] : filled.problems), ...extra] };
      if (filled.ok) {
        const byId = new Map(results.map((r) => [r.id, r]));
        const evidence = filled.used.map((u) => {
          const r = byId.get(u.result)!;
          if (u.row === "")
            return { ...u, label: EXTRA_LABELS[u.key] ?? u.key, text: r.extras![u.key] };
          const row = u.row === "total" ? r.total! : r.rows.find((x) => x.id === u.row)!;
          return {
            ...u,
            label: displaySpec(r.call.tool, r.call.league, r.view)[u.key].label,
            text: cellText(r.call.tool, r.call.league, r.view, u.key, row, lang)!,
          };
        });
        yield finish(lang === "he" ? hebrewTerms(filled.text) : filled.text, evidence, false);
        return;
      }
      rejected.push({ text, problems: filled.problems });
      if (retries >= MAX_RETRIES) break;
      retries++;
      messages.push({ role: "assistant", text, raw: reply.raw });
      messages.push({
        role: "user",
        text: `Your answer was rejected: ${filled.problems.join("; ")}. Rewrite it with markers for every number and name, and nothing computed. Reply with the corrected answer only, with no comment about the rewrite.`,
      });
    }
    yield finish(null, [], true);
  } catch (e) {
    // Out of time (not a cancelled request): show the page instead of an error.
    if ((lifetime.signal.reason as Error | undefined)?.name !== "TimeoutError") throw e;
    yield finish(null, [], true);
  }
}
