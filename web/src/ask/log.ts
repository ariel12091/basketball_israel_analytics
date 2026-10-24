// One server log line per answered question (spec section 8): what was asked,
// which tools ran, whether the answer passed, and what it cost. No IP or ids.
import type { AskAnswer } from "./turn";

export function logTurn(entry: {
  question: string;
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
      question: entry.question,
      provider: entry.provider,
      model: entry.model,
      tools: a?.results.map((r) => ({ tool: r.call.tool, league: r.call.league, query: r.call.query })) ?? [],
      toolErrors: a?.errors ?? [],
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
