// One server log line per answered question (spec section 8): which tools
// ran, whether the answer passed, and what it cost. Not the question itself
// (2026-11-19, user): a visitor's words are stored only when they flag an
// answer and agree to send it (/api/v1/ask/feedback). No IP or ids.
import type { AskAnswer } from "./turn";

export function logTurn(entry: {
  questionChars: number;
  provider: string;
  model: string;
  answer: AskAnswer | null;
  ms: number;
  error?: string;
}) {
  const a = entry.answer;
  console.log(
    JSON.stringify({
      kind: "ask_turn",
      questionChars: entry.questionChars,
      provider: entry.provider,
      model: entry.model,
      tools: a?.results.map((r) => ({ tool: r.call.tool, league: r.call.league, query: r.call.query })) ?? [],
      toolErrors: a?.errors ?? [],
      rejected: a?.rejected ?? [],
      fallback: a?.fallback ?? null,
      lang: a?.lang ?? null,
      inputTokens: a?.usage.inputTokens ?? 0,
      outputTokens: a?.usage.outputTokens ?? 0,
      costUsd: a?.usage.costUsd ?? 0,
      ms: entry.ms,
      ...(entry.error ? { error: entry.error } : {}),
    }),
  );
}
