// The ask box's model comparison (spec section 6), run against the DEPLOYED
// chat: every case in test/ask/cases.json goes through POST /api/v1/ask on a
// Vercel deployment, one question after another, and is scored from the
// streamed final answer.
//
//   node --import tsx scripts/ask-eval.ts --base https://<preview-url>
//
// VERCEL_AUTOMATION_BYPASS_SECRET (Vercel: Deployment Protection, Protection
// Bypass for Automation) comes from the environment or app/.Renviron.
//
// The deployment chooses the model (ASK_PROVIDER / ASK_MODEL in Vercel); to
// compare models, change ASK_MODEL, redeploy, run again. Set ASK_RATE_LIMIT on
// that deployment above the number of questions (about 50). Spends real money.
import { spawnSync } from "node:child_process";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { NUMBER_WORD, NUMBER, bare, digitWords, statCountWords, withoutTerms } from "../src/ask/numberWords";
import type { AskAnswer, HistoryTurn } from "../src/ask/turn";
import type { ToolCall } from "../src/ask/run";

type Expect = { tool: string; league: string; query: Record<string, string> };
/** One turn's acceptable answers: a tool call, "unanswerable", or any of several. */
type Accept = Expect | "unanswerable" | (Expect | "unanswerable")[];
type Case = {
  id: string;
  lang: "he" | "en";
  turns: string[];
  expect: Accept[] | "unanswerable";
  /** Hostile cases: text that must never appear in the answer. */
  forbid?: string[];
};
type Final = { type: "final"; answer: AskAnswer; provider: string; model: string };

const arg = (name: string, fallback?: string) => {
  const i = process.argv.indexOf(`--${name}`);
  return i > 0 ? process.argv[i + 1] : fallback;
};
/** Number words the visitor did not use. `stat` is the answer check's rule
 * (a word counting a stat: disqualifying); `any` is every other one, a flag
 * only (src/ask/numberWords.ts). */
const numberWords = (text: string, question: string) => {
  const asked = new Set([
    ...(question.match(NUMBER_WORD) ?? []).map(bare),
    ...(question.match(NUMBER) ?? []).flatMap(digitWords),
  ]);
  const own = withoutTerms(text),
    fresh = (w: string) => !asked.has(bare(w));
  return {
    stat: statCountWords(own).some(fresh),
    any: (own.match(NUMBER_WORD) ?? []).filter(fresh),
  };
};

const matches = (c: ToolCall, e: Expect) =>
  c.tool === e.tool &&
  c.league === e.league &&
  Object.entries(e.query).every(([k, v]) => String(c.query[k] ?? "") === v);

/** POST through the bypass secret when there is one, else through `vercel curl`
 * (the logged-in Vercel CLI passes Deployment Protection). */
async function post(base: string, body: string): Promise<string> {
  const bypass = process.env.VERCEL_AUTOMATION_BYPASS_SECRET;
  if (bypass) {
    const res = await fetch(new URL("/api/v1/ask", base), {
      method: "POST",
      headers: { "content-type": "application/json", "x-vercel-protection-bypass": bypass },
      body,
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}: ${(await res.text()).slice(0, 200)}`);
    return res.text();
  }
  const file = path.join(tmpdir(), `ask-eval-${process.pid}.json`);
  writeFileSync(file, body);
  const r = spawnSync(
    "vercel.cmd",
    ["curl", "/api/v1/ask", "--deployment", base, "--", "-sS", "-X", "POST", "-H", '"content-type: application/json"', "--data-binary", `@${file}`, "-m", "120"],
    { shell: true, encoding: "utf8", maxBuffer: 64 * 1024 * 1024 },
  );
  if (r.status !== 0 && !r.stdout) throw new Error(r.stderr.slice(-300));
  return r.stdout;
}

async function ask(base: string, question: string, history: HistoryTurn[], clientId: string) {
  const text = await post(base, JSON.stringify({ league: "il", question, history, clientId }));
  if (!text.includes("data: ")) throw new Error(text.slice(0, 200));
  const events = text
    .split(/\r?\n/)
    .filter((l) => l.startsWith("data: "))
    .map((l) => JSON.parse(l.slice(6)));
  const final = events.find((e) => e.type === "final") as Final | undefined;
  if (!final) throw new Error(events.find((e) => e.type === "error")?.error ?? "No final answer");
  return final;
}

async function main() {
  // The bypass secret may live in app/.Renviron (gitignored credentials file).
  const env = new URL("../../app/.Renviron", import.meta.url);
  if (existsSync(env)) process.loadEnvFile(env);
  const base = arg("base");
  if (!base) throw new Error("Give --base https://<deployment>");
  const cases = JSON.parse(readFileSync(arg("cases", "test/ask/cases.json")!, "utf8")) as Case[];
  const clientId = `eval-${Date.now()}`;
  const rows = [];
  let model = "?";
  for (const c of cases) {
    const history: HistoryTurn[] = [];
    const turns = [];
    for (const [i, question] of c.turns.entries()) {
      const t0 = performance.now();
      let answer: AskAnswer | null = null,
        error: string | null = null;
      try {
        const final = await ask(base, question, history, clientId);
        answer = final.answer;
        model = `${final.provider}/${final.model}`;
      } catch (e) {
        error = e instanceof Error ? e.message : String(e);
      }
      const ms = Math.round(performance.now() - t0);
      // This turn's own lookups (results also hold the re-run history calls).
      const calls = answer?.calls ?? answer?.results.map((r) => r.call) ?? [];
      const accept = c.expect === "unanswerable" ? ["unanswerable" as const] : [c.expect[i]].flat();
      const text = answer?.text ?? "";
      turns.push({
        question,
        text,
        calls,
        ms,
        error,
        costUsd: answer?.usage.costUsd ?? 0,
        toolErrors: answer?.errors ?? [],
        rejected: answer?.rejected ?? [],
        toolOk:
          !!answer &&
          accept.some((e) =>
            // A refusal may still name teams from the team list it looked up.
            e === "unanswerable"
              ? answer!.evidence.length === 0 || calls.every((call) => call.tool === "list_teams")
              // A follow-up may answer from an earlier turn's re-run lookup.
              : [...calls, ...(answer!.results ?? []).map((r) => r.call)].some((call) => matches(call, e)),
          ),
        verified: !!answer && !answer.fallback,
        // Two failed verifications with data in hand: the disqualifying case.
        failedTwice: !!answer && answer.fallback && answer.results.length > 0,
        langOk: !!answer && /[֐-׿]/.test(text) === (c.lang === "he"),
        numberWord: numberWords(text, question).stat,
        otherNumberWords: numberWords(text, question).any,
        // Hostile cases: text that must never appear in the answer.
        forbidden: (c.forbid ?? []).filter((f) => text.toLowerCase().includes(f.toLowerCase())),
      });
      if (answer) history.push({ question, answer: answer.text, calls });
    }
    rows.push({ id: c.id, turns });
    const last = turns.at(-1)!;
    console.log(
      `${c.id.padEnd(30)} tool ${turns.every((t) => t.toolOk) ? "ok " : "BAD"} verified ${turns.every((t) => t.verified) ? "ok " : "no "} ${last.ms}ms $${last.costUsd.toFixed(4)}${last.error ? ` ERROR ${last.error}` : ""}${turns.some((t) => t.forbidden.length) ? " FORBIDDEN" : ""}`,
    );
  }
  const all = rows.flatMap((r) => r.turns);
  const pct = (f: (t: (typeof all)[number]) => boolean) => Math.round((100 * all.filter(f).length) / all.length);
  const summary = {
    base,
    model,
    cases: rows.length,
    turns: all.length,
    errors: all.filter((t) => t.error).length,
    toolOkPct: pct((t) => t.toolOk),
    verifiedPct: pct((t) => t.verified),
    langOkPct: pct((t) => t.langOk),
    disqualified: all.some((t) => t.failedTwice || t.numberWord || t.forbidden.length > 0),
    forbiddenHits: all.filter((t) => t.forbidden.length > 0).length,
    failedTwice: all.filter((t) => t.failedTwice).length,
    numberWords: all.filter((t) => t.numberWord).length,
    // Not disqualifying: number words that do not count a stat ("top ten").
    otherNumberWords: all.filter((t) => t.otherNumberWords.length).length,
    costUsd: Math.round(all.reduce((s, t) => s + t.costUsd, 0) * 1e4) / 1e4,
    medianMs: all.map((t) => t.ms).sort((a, b) => a - b)[Math.floor(all.length / 2)],
  };
  console.log(JSON.stringify(summary, null, 2));
  mkdirSync("../output/ask", { recursive: true });
  const file = `../output/ask/eval-${new Date().toISOString().slice(0, 10)}-${model.replace(/\W+/g, "_")}.json`;
  writeFileSync(file, JSON.stringify({ summary, rows }, null, 2));
  console.log(`wrote ${file}`);
}
main().catch((e) => {
  console.error(e);
  process.exit(1);
});
