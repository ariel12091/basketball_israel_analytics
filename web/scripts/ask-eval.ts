// The ask box's model comparison (spec section 6). Runs every case in
// test/ask/cases.json through the real loop and readers, one question after
// another (the same load profile as the live smoke checks), and scores it.
//
//   node --conditions=react-server --import tsx scripts/ask-eval.ts \
//     --provider anthropic --model claude-haiku-5-5
// (tsx as a loader in this process, so "server-only" resolves for the server.)
//
// Needs ../app/.Renviron (read-only DB role, PG_PORT=5432) and the provider's
// key in the environment. Spends real money: Haiku about $0.003 per question.
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { performance } from "node:perf_hooks";
import { askTurn, type AskAnswer, type HistoryTurn } from "../src/ask/turn";
import { anthropicProvider } from "../src/ask/providers/anthropic";
import type { Provider } from "../src/ask/providers/types";
import type { ToolCall } from "../src/ask/run";

type Expect = { tool: string; league: string; query: Record<string, string> };
type Case = { id: string; lang: "he" | "en"; turns: string[]; expect: Expect[] | "unanswerable" };

const arg = (name: string, fallback?: string) => {
  const i = process.argv.indexOf(`--${name}`);
  return i > 0 ? process.argv[i + 1] : fallback;
};
// Spelled-out numbers and ordinals the digit check cannot see (Task 10 scores them).
const NUMBER_WORDS =
  /\b(one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|thirteen|fourteen|fifteen|twenty|thirty|hundred|first|second|third|fourth|fifth|sixth|seventh|eighth|ninth|tenth|half)\b|(?:^|[\s,.])(אחד|אחת|שתיים|שניים|שלוש|שלושה|ארבע|ארבעה|חמש|חמישה|שש|שישה|שבע|שבעה|שמונה|תשע|תשעה|עשר|עשרה|ראשון|ראשונה|שני|שנייה|שלישי|שלישית|רביעי|רביעית|חמישי|חמישית|חצי)(?=$|[\s,.])/iu;

function provider(): Provider {
  const name = arg("provider", "anthropic"),
    model = arg("model", "claude-haiku-5-5")!;
  if (name === "anthropic") return anthropicProvider(model);
  throw new Error(`Provider ${name} has no adapter yet`);
}
const matches = (c: ToolCall, e: Expect) =>
  c.tool === e.tool &&
  c.league === e.league &&
  Object.entries(e.query).every(([k, v]) => String(c.query[k] ?? "") === v);

async function main() {
  const env = new URL("../../app/.Renviron", import.meta.url);
  if (existsSync(env)) process.loadEnvFile(env);
  process.env.PG_PORT = "5432";
  const p = provider();
  const cases = JSON.parse(readFileSync(arg("cases", "test/ask/cases.json")!, "utf8")) as Case[];
  const rows = [];
  for (const c of cases) {
    const history: HistoryTurn[] = [];
    const turns = [];
    for (const [i, question] of c.turns.entries()) {
      const t0 = performance.now();
      let answer: AskAnswer | null = null,
        error: string | null = null;
      try {
        for await (const e of askTurn({ league: "il", question, history }, p, { now: Date.now, followups: true }))
          if (e.type === "final") answer = e.answer;
      } catch (e) {
        error = e instanceof Error ? e.message : String(e);
      }
      const ms = Math.round(performance.now() - t0);
      const calls = answer?.results.map((r) => r.call) ?? [];
      const expect = c.expect === "unanswerable" ? null : c.expect[i];
      const text = answer?.text ?? "";
      turns.push({
        question,
        text,
        calls,
        ms,
        error,
        costUsd: answer?.usage.costUsd ?? 0,
        toolOk: expect ? calls.some((call) => matches(call, expect)) : (answer?.evidence.length ?? 0) === 0,
        verified: !!answer && !answer.fallback,
        // Two failed verifications with data in hand: the disqualifying case.
        failedTwice: !!answer && answer.fallback && answer.results.length > 0,
        langOk: /[֐-׿]/.test(text) === (c.lang === "he"),
        numberWord: NUMBER_WORDS.test(text),
      });
      if (answer) history.push({ question, answer: answer.text, calls });
    }
    rows.push({ id: c.id, turns });
    const last = turns.at(-1)!;
    console.log(`${c.id.padEnd(30)} tool ${turns.every((t) => t.toolOk) ? "ok " : "BAD"} verified ${turns.every((t) => t.verified) ? "ok " : "no "} ${last.ms}ms $${last.costUsd.toFixed(4)}`);
  }
  const all = rows.flatMap((r) => r.turns);
  const summary = {
    provider: p.name,
    model: p.model,
    cases: rows.length,
    turns: all.length,
    toolOkPct: Math.round((100 * all.filter((t) => t.toolOk).length) / all.length),
    verifiedPct: Math.round((100 * all.filter((t) => t.verified).length) / all.length),
    langOkPct: Math.round((100 * all.filter((t) => t.langOk).length) / all.length),
    disqualified: all.some((t) => t.failedTwice || t.numberWord),
    failedTwice: all.filter((t) => t.failedTwice).length,
    numberWords: all.filter((t) => t.numberWord).length,
    costUsd: Math.round(all.reduce((s, t) => s + t.costUsd, 0) * 1e4) / 1e4,
    medianMs: all.map((t) => t.ms).sort((a, b) => a - b)[Math.floor(all.length / 2)],
  };
  console.log(JSON.stringify(summary, null, 2));
  mkdirSync("../output/ask", { recursive: true });
  const file = `../output/ask/eval-${new Date().toISOString().slice(0, 10)}-${p.model}.json`;
  writeFileSync(file, JSON.stringify({ summary, rows }, null, 2));
  console.log(`wrote ${file}`);
}
main().catch((e) => {
  console.error(e);
  process.exit(1);
});
