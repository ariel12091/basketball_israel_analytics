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

export type HistoryTurn = { question: string; answer: string; calls: ToolCall[] };
export type Evidence = { result: string; row: string; key: string; label: string; text: string };
export type ToolProblem = { tool: string; param: string | null; error: string };
export type AskAnswer = {
  text: string;
  lang: "he" | "en";
  results: ToolResult[];
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

const MAX_CALLS = 4;
const MAX_RERUNS = 6;
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
/** Result-level markers ({{r1.count}}, {{r1.side_a}}) as the model sees them. */
const EXTRA_LABELS: Record<string, string> = {
  count: "Rows the page lists",
  side_a: "Compare side A heading",
  side_b: "Compare side B heading",
  min_poss: "Minimum possessions the rows were filtered by",
};

/** What the model sees of a result: ids, quotable columns and their raw values. */
function forModel(r: ToolResult | ToolError) {
  if (!isResult(r)) return { id: r.id, error: r.error, param: r.param };
  const spec = displaySpec(r.call.tool, r.call.league, r.view);
  const keys = Object.keys(spec);
  const pick = (cells: Record<string, unknown>) =>
    Object.fromEntries(
      (keys.length ? keys : Object.keys(cells)).filter((k) => k in cells).map((k) => [k, cells[k]]),
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
    resultValues: Object.fromEntries(
      Object.entries(r.extras ?? {}).map(([k, v]) => [k, `${EXTRA_LABELS[k] ?? k}: ${v}`]),
    ),
  };
}

function seasonLabel(league: League, calls: ToolCall[]): string {
  const raw = calls.find((c) => c.league === league && typeof c.query.season === "string")
    ?.query.season as string | undefined;
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
  opts: { now: () => number; followups: boolean },
): AsyncGenerator<TurnEvent> {
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
  const done = new Map<string, ToolResult | ToolError>();
  async function* execute(id: string, call: ToolCall, rerun = false) {
    const key = JSON.stringify(call);
    if (!rerun && done.has(key)) return done.get(key)!;
    if (!rerun && calls >= MAX_CALLS) {
      const err: ToolError = { id, call, error: "Tool call limit reached for this question", param: null };
      all.push(err);
      return err;
    }
    if (!rerun) calls++;
    yield { type: "status" as const, text: `Reading ${TOOL_LABELS[call.tool] ?? call.tool}…` };
    const r = await runTool(id, call, deadline);
    all.push(r);
    // Only successes are reused: a failed lookup (a timeout) may be retried.
    if (!rerun && isResult(r)) done.set(key, r);
    return r;
  }

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
  messages.push({ role: "user", text: input.question });

  const system = systemPrompt(input.league),
    tools = toolSchemas();
  const ctrl = new AbortController();
  let retried = false;
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

  for (let step = 0; step < MAX_STEPS; step++) {
    const reply = await provider.reply(system, tools, messages, ctrl.signal);
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
    const text = reply.text ?? "";
    const results = all.filter(isResult);
    let filled = fillAndVerify(text, results, input.question, seasonLabel(input.league, results.map((r) => r.call)));
    // A turn whose every tool failed has nothing to stand on: show the page.
    if (all.length && !results.length) break;
    // The answer's own words (markers removed) must be in the visitor's language.
    const words = text.replace(/\{\{[^{}]*\}\}/g, " ");
    const hebrew = /[֐-׿]/.test(words);
    if (filled.ok && hebrew !== (lang === "he"))
      filled = {
        ok: false,
        problems: [`Answer in ${lang === "he" ? "Hebrew" : "English"}, the language of the question`],
      };
    // Links come from the app only; the answer text never carries an address.
    if (filled.ok && /https?:\/\/|www\.|[\w.-]+@[\w-]+\.\w|\b[\w-]+\.(?:com|org|net|io|co|il|app|dev|me|ly)\b/i.test(words))
      filled = { ok: false, problems: ["Remove the web address or email; the app adds the page link"] };
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
          text: cellText(r.call.tool, r.call.league, r.view, u.key, row)!,
        };
      });
      yield finish(filled.text, evidence, false);
      return;
    }
    rejected.push({ text, problems: filled.problems });
    if (retried) break;
    retried = true;
    messages.push({ role: "assistant", text, raw: reply.raw });
    messages.push({
      role: "user",
      text: `Your answer was rejected: ${filled.problems.join("; ")}. Rewrite it with markers for every number and name, and nothing computed. Reply with the corrected answer only, with no comment about the rewrite.`,
    });
  }
  yield finish(null, [], true);
}
