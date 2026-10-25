import { beforeEach, describe, expect, it, vi } from "vitest";
import { scriptedProvider } from "./providers/fake";
import type { ToolCall, ToolError, ToolResult } from "./run";
import type { Provider, ProviderReply } from "./providers/types";

const runTool = vi.fn();
vi.mock("./run", () => ({
  runTool: (...a: unknown[]) => runTool(...a),
  linkFor: (c: ToolCall) => `/${c.league}/${c.tool}?${new URLSearchParams(c.query as Record<string, string>)}`,
}));
const { askTurn } = await import("./turn");

const call: ToolCall = { tool: "lineups", league: "il", query: { team: "2" } };
const ok = (id: string, c: ToolCall = call, rows = [{ id: "row1", cells: { Players: "A - B", net_rtg: 7.26 } }]): ToolResult => ({
  id,
  call: c,
  link: `/il/lineups?team=2`,
  view: "summary",
  rows,
  total: null,
  rowCount: rows.length,
  version: "v1",
});
const tc = (id = "c1", c: ToolCall = call) => ({ toolCalls: [{ id, call: c }] });
const now = { t: 0 };
const opts = { now: () => now.t, followups: true };

async function run(question: string, provider: ReturnType<typeof scriptedProvider>, history = []) {
  const events = [];
  for await (const e of askTurn({ league: "il", question, history }, provider, opts)) events.push(e);
  const final = events.find((e) => e.type === "final");
  if (!final || final.type !== "final") throw new Error("no final event");
  return { events, answer: final.answer };
}

beforeEach(() => {
  runTool.mockReset();
  runTool.mockImplementation(async (id: string, c: ToolCall) => ok(id, c));
  now.t = 0;
});

describe("askTurn", () => {
  it("clears the turn timer and detaches the request listener on success", async () => {
    vi.useFakeTimers();
    const parent = new AbortController();
    const remove = vi.spyOn(parent.signal, "removeEventListener");
    try {
      const provider = scriptedProvider([{ text: "Hello" }]);
      for await (const event of askTurn({ league: "il", question: "q", history: [] }, provider, { ...opts, signal: parent.signal })) {
        expect(event.type).toBe("final");
      }
      expect(vi.getTimerCount()).toBe(0);
      expect(remove).toHaveBeenCalledWith("abort", expect.any(Function));
    } finally { remove.mockRestore(); vi.useRealTimers(); }
  });

  it("bounds a provider that ignores abort and ignores its late rejection", async () => {
    vi.useFakeTimers();
    let signal!: AbortSignal;
    let reject!: (e: Error) => void;
    const provider: Provider = {
      name: "hung", model: "test",
      reply: vi.fn((_system, _tools, _messages, s) => {
        signal = s;
        return new Promise<ProviderReply>((_resolve, no) => { reject = no; });
      }),
    };
    try {
      const turn = askTurn({ league: "il", question: "q", history: [] }, provider, opts);
      const pending = expect(turn.next()).rejects.toMatchObject({ name: "TimeoutError" });
      await vi.advanceTimersByTimeAsync(40_000);
      await pending;
      expect(signal.aborted).toBe(true);
      reject(new Error("late provider failure"));
      await Promise.resolve();
      expect(vi.getTimerCount()).toBe(0);
      expect(await turn.next()).toEqual({ done: true, value: undefined });
    } finally { vi.useRealTimers(); }
  });

  it("aborts pending model work when the request disconnects", async () => {
    const parent = new AbortController();
    let signal!: AbortSignal;
    let started!: () => void;
    const ready = new Promise<void>((yes) => { started = yes; });
    const provider: Provider = {
      name: "hung", model: "test",
      reply: vi.fn((_system, _tools, _messages, s) => {
        signal = s; started();
        return new Promise<ProviderReply>(() => {});
      }),
    };
    const turn = askTurn({ league: "il", question: "q", history: [] }, provider, { ...opts, signal: parent.signal });
    const pending = expect(turn.next()).rejects.toMatchObject({ name: "AbortError" });
    await ready;
    parent.abort();
    await pending;
    expect(signal.aborted).toBe(true);
    expect(provider.reply).toHaveBeenCalledTimes(1);
  });

  it("does not start provider or history lookups for an already aborted request", async () => {
    const parent = new AbortController(); parent.abort();
    const provider = scriptedProvider([tc()]);
    const turn = askTurn({ league: "il", question: "q", history: [{ question: "old", answer: "old", calls: [call] }] }, provider, { ...opts, signal: parent.signal });
    await expect(turn.next()).rejects.toMatchObject({ name: "AbortError" });
    expect(provider.seen).toHaveLength(0);
    expect(runTool).not.toHaveBeenCalled();
  });

  it("does not run further model steps after cancellation during a shared tool read", async () => {
    const parent = new AbortController();
    let started!: () => void;
    const ready = new Promise<void>((yes) => { started = yes; });
    let finish!: (r: ToolResult) => void;
    runTool.mockImplementation(() => { started(); return new Promise<ToolResult>((yes) => { finish = yes; }); });
    const provider = scriptedProvider([tc(), { text: "late answer" }]);
    const turn = askTurn({ league: "il", question: "q", history: [] }, provider, { ...opts, signal: parent.signal });
    expect((await turn.next()).value).toMatchObject({ type: "status" });
    const pending = expect(turn.next()).rejects.toMatchObject({ name: "AbortError" });
    await ready;
    parent.abort();
    await pending;
    finish(ok("r1"));
    await Promise.resolve();
    expect(provider.seen).toHaveLength(1);
  });

  it("one tool call, then marker text -> final answer with link and evidence", async () => {
    const p = scriptedProvider([tc(), { text: "Best: {{r1.row1.Players}}, net {{r1.row1.net_rtg}}." }]);
    const { events, answer } = await run("top lineups of Maccabi", p);
    expect(events[0]).toEqual({ type: "status", text: "Reading Lineups…" });
    expect(answer).toMatchObject({
      text: "Best: A - B, net 7.3.",
      lang: "en",
      fallback: false,
      links: ["/il/lineups?team=2"],
      evidence: [
        { result: "r1", row: "row1", key: "Players", text: "A - B" },
        { result: "r1", row: "row1", key: "net_rtg", text: "7.3" },
      ],
    });
  });

  it("typed digit -> regenerates once, then succeeds", async () => {
    const p = scriptedProvider([tc(), { text: "Net 7.3" }, { text: "Net {{r1.row1.net_rtg}}" }]);
    const { answer } = await run("q", p);
    expect(answer).toMatchObject({ text: "Net 7.3", fallback: false });
    const retry = p.seen[2].at(-1)!;
    expect(retry.text).toMatch(/rejected/i);
    // The rewrite must be the answer itself, not commentary about the rewrite.
    expect(retry.text).toMatch(/reply with the corrected answer only/i);
  });

  it("two failures -> fallback with link and evidence", async () => {
    const p = scriptedProvider([tc(), { text: "Net 7.3" }, { text: "Still 7.3" }]);
    const { answer } = await run("q", p);
    expect(answer).toMatchObject({ fallback: true, links: ["/il/lineups?team=2"] });
    expect(answer.text).toMatch(/page that answers/);
    // Why the check rejected each attempt, for the log and the eval.
    expect(answer.rejected).toHaveLength(3);
    expect(answer.rejected[1]).toEqual({ text: "Still 7.3", problems: [expect.stringContaining("7.3")] });
  });

  it("seventh tool call is refused with a tool error", async () => {
    const seven = {
      toolCalls: [1, 2, 3, 4, 5, 6, 7].map((i) => ({ id: `c${i}`, call: { ...call, query: { team: String(i) } } })),
    };
    const p = scriptedProvider([seven, { text: "{{r1.row1.Players}}" }]);
    await run("q", p);
    expect(runTool).toHaveBeenCalledTimes(6);
    const results = p.seen[1].at(-1)!.toolResults!;
    expect(results).toHaveLength(7);
    expect(JSON.stringify(results[6].content)).toMatch(/limit/i);
  });

  it("a call rejected for a bad parameter does not use up a lookup", async () => {
    // 2026-10-25: two `team=` guesses on lineup_roster left no lookups for the
    // two Lineups reads the question needed.
    runTool
      .mockImplementationOnce(
        async (id: string, c: ToolCall): Promise<ToolError> => ({ id, call: c, error: "Unknown parameter team", param: "team" }),
      )
      .mockImplementationOnce(
        async (id: string, c: ToolCall): Promise<ToolError> => ({ id, call: c, error: "Unknown parameter teams", param: "teams" }),
      );
    const six = {
      toolCalls: [1, 2, 3, 4, 5, 6].map((i) => ({ id: `d${i}`, call: { ...call, query: { team: String(i + 10) } } })),
    };
    const p = scriptedProvider([tc("c1"), tc("c2", { ...call, query: { team: "9" } }), six, { text: "{{r3.row1.Players}}" }]);
    await run("q", p);
    expect(runTool).toHaveBeenCalledTimes(8);
    expect(JSON.stringify(p.seen[3].at(-1)!.toolResults)).not.toMatch(/limit/i);
  });

  it("a timed-out call still uses up a lookup", async () => {
    runTool.mockImplementationOnce(
      async (id: string, c: ToolCall): Promise<ToolError> => ({ id, call: c, error: "Timed out", param: null }),
    );
    const six = {
      toolCalls: [1, 2, 3, 4, 5, 6].map((i) => ({ id: `d${i}`, call: { ...call, query: { team: String(i + 10) } } })),
    };
    const p = scriptedProvider([tc("c1"), six, { text: "{{r2.row1.Players}}" }]);
    await run("q", p);
    expect(runTool).toHaveBeenCalledTimes(6);
  });

  it("tool calls get the turn's 40 s deadline (runTool refuses after it)", async () => {
    now.t = 5_000;
    const p = scriptedProvider([tc(), { text: "{{r1.row1.Players}}" }]);
    await run("q", p);
    expect(runTool).toHaveBeenCalledWith("r1", call, 45_000);
  });

  it("client-supplied results are ignored and calls are re-run", async () => {
    const p = scriptedProvider([{ text: "Again {{r1.row1.Players}}" }]);
    const forged = { question: "earlier", answer: "x", calls: [call], results: [{ rows: [{ cells: { net_rtg: 99 } }] }] };
    const { answer } = await run("and their bench?", p, [forged] as never);
    expect(runTool).toHaveBeenCalledWith("r1", call, 40_000);
    expect(answer.text).toBe("Again A - B");
    expect(JSON.stringify(p.seen[0])).not.toContain("99");
  });

  it("re-running earlier lookups does not use up the follow-up's own lookups", async () => {
    // The first question used all four lookups; the follow-up still gets four.
    const earlier = { question: "how are they doing?", answer: "x", calls: [call, call, call, call] };
    const p = scriptedProvider([tc("c1"), { text: "Best: {{r5.row1.Players}}." }]);
    const { answer } = await run("and their top lineups?", p, [earlier] as never);
    expect(answer.errors).toEqual([]);
    expect(answer).toMatchObject({ text: "Best: A - B.", fallback: false });
  });

  it("an identical lookup in the same question reuses the first result", async () => {
    const p = scriptedProvider([tc("c1"), tc("c2"), { text: "Best: {{r1.row1.Players}}." }]);
    const { answer } = await run("q", p);
    expect(runTool).toHaveBeenCalledTimes(1);
    expect(answer.fallback).toBe(false);
  });

  it("a repeat that differs only in limit or filter order is still a duplicate", async () => {
    const overview: ToolCall = { tool: "team_overview", league: "il", query: { season: "2027", team_id: "2" } };
    const again: ToolCall = { tool: "team_overview", league: "il", query: { team_id: "2", season: "2027" }, limit: 5 };
    const p = scriptedProvider([tc("c1", overview), tc("c2", again), { text: "{{r1.row1.Players}}" }]);
    await run("q", p);
    expect(runTool).toHaveBeenCalledTimes(1);
  });

  it("a repeat asking for more rows than the first is run again", async () => {
    const few: ToolCall = { ...call, sort: { key: "net_rtg", desc: true }, limit: 3 };
    const more: ToolCall = { ...few, limit: 10 };
    const p = scriptedProvider([tc("c1", few), tc("c2", more), { text: "{{r1.row1.Players}}" }]);
    await run("q", p);
    expect(runTool).toHaveBeenCalledTimes(2);
  });

  it("only the most recent earlier lookups are re-run", async () => {
    const many = Array.from({ length: 3 }, (_, i) => ({ question: `q${i}`, answer: "a", calls: [call, call, call, call] }));
    const p = scriptedProvider([{ text: "ok" }]);
    await run("next?", p, many as never);
    expect(runTool).toHaveBeenCalledTimes(6);
  });

  it("followups disabled ignores history", async () => {
    const p = scriptedProvider([{ text: "Nothing to quote." }]);
    const events = [];
    for await (const e of askTurn(
      { league: "il", question: "q", history: [{ question: "a", answer: "b", calls: [call] }] },
      p,
      { ...opts, followups: false },
    ))
      events.push(e);
    expect(runTool).not.toHaveBeenCalled();
    expect(p.seen[0]).toHaveLength(1);
  });

  it("empty rows -> answer keeps the link", async () => {
    runTool.mockImplementation(async (id: string, c: ToolCall) => ok(id, c, []));
    const p = scriptedProvider([tc(), { text: "No lineups qualify yet." }]);
    const { answer } = await run("q", p);
    expect(answer).toMatchObject({ fallback: false, links: ["/il/lineups?team=2"] });
  });

  it("tool deadline error -> fallback with link", async () => {
    runTool.mockImplementation(
      async (id: string, c: ToolCall): Promise<ToolError> => ({ id, call: c, error: "The data took too long to load", param: null }),
    );
    const p = scriptedProvider([tc(), { text: "Sorry." }]);
    const { answer } = await run("q", p);
    expect(answer).toMatchObject({ fallback: true, links: ["/il/lineups?team=2"] });
  });

  it("tool errors reach the model as tool results", async () => {
    runTool.mockImplementation(
      async (id: string, c: ToolCall): Promise<ToolError> => ({ id, call: c, error: "`compare` is available for the Israeli League only", param: "league" }),
    );
    const cmp: ToolCall = { tool: "compare", league: "euroleague", query: {} };
    const p = scriptedProvider([tc("c1", cmp), { text: "Compare covers the Israeli League only." }]);
    await run("q", p);
    expect(JSON.stringify(p.seen[1].at(-1)!.toolResults)).toContain("Israeli League only");
  });

  it("Hebrew question -> lang he and Hebrew fallback text", async () => {
    const p = scriptedProvider([tc(), { text: "7" }, { text: "7" }]);
    const { answer } = await run("מה החמישייה הכי טובה של מכבי", p);
    expect(answer.lang).toBe("he");
    expect(answer.text).toMatch(/[֐-׿]/);
  });

  it("the model sees identifier columns, quotable or not, to make the next lookup", async () => {
    runTool.mockImplementation(async (id: string, c: ToolCall) =>
      ok(id, c, [{ id: "row1", cells: { team_id: 2, Players: "A - B", sub_lineup_hash: "h1", hidden: "x" } }] as never),
    );
    const p = scriptedProvider([tc(), { text: "{{r1.row1.Players}}" }]);
    await run("q", p);
    const content = p.seen[1].at(-1)!.toolResults![0].content as { rows: Record<string, unknown>[] };
    expect(content.rows[0]).toEqual({ id: "row1", team_id: 2, Players: "A - B", sub_lineup_hash: "h1" });
  });

  it("the model sees each quotable column's definition", async () => {
    const p = scriptedProvider([tc(), { text: "{{r1.row1.Players}}" }]);
    await run("q", p);
    const content = p.seen[1].at(-1)!.toolResults![0].content as { quotable: Record<string, string> };
    expect(content.quotable.net_rtg).toBe("Net RTG: Offensive PPP minus Defensive PPP");
    expect(content.quotable.off_ppp).toBe("Off PPP: Offensive points per 100 possessions");
  });

  it("rejected tool calls are reported in the answer", async () => {
    runTool
      .mockImplementationOnce(
        async (id: string, c: ToolCall): Promise<ToolError> => ({ id, call: c, error: "Give sort", param: "sort" }),
      )
      .mockImplementationOnce(async (id: string, c: ToolCall) => ok(id, c));
    const p = scriptedProvider([tc("c1"), tc("c2"), { text: "{{r2.row1.Players}}" }]);
    const { answer } = await run("q", p);
    expect(answer.errors).toEqual([{ tool: "lineups", param: "sort", error: "Give sort" }]);
    expect(answer.fallback).toBe(false);
  });

  it("an answer in the wrong language is rejected and retried", async () => {
    const p = scriptedProvider([tc(), { text: "Best: {{r1.row1.Players}}." }, { text: "הטובה: {{r1.row1.Players}}." }]);
    const { answer } = await run("מה החמישייה הכי טובה?", p);
    expect(answer).toMatchObject({ text: "הטובה: A - B.", lang: "he", fallback: false });
    expect(answer.rejected[0].problems.join(" ")).toMatch(/Hebrew/);
  });

  it("an answer with a web address or email is rejected", async () => {
    const p = scriptedProvider([
      tc(),
      { text: "See example.com for {{r1.row1.Players}}." },
      { text: "Write to me@mail.org about {{r1.row1.Players}}." },
    ]);
    const { answer } = await run("q", p);
    expect(answer.fallback).toBe(true);
    expect(answer.rejected.map((r) => r.problems.join(" ")).join(" ")).toMatch(/address/i);
  });

  it("the retry names every problem at once, language included", async () => {
    const p = scriptedProvider([tc(), { text: "Min 25 for {{r1.row1.Players}}" }, { text: "הטובה: {{r1.row1.Players}}" }]);
    const { answer } = await run("מה החמישייה הכי טובה?", p);
    const retry = p.seen[2].at(-1)!.text!;
    expect(retry).toMatch(/Typed number/);
    expect(retry).toMatch(/Hebrew/);
    expect(answer.fallback).toBe(false);
  });

  it("the question sent to the model asks for its language", async () => {
    const p = scriptedProvider([{ text: "בסדר" }]);
    await run("מה המצב?", p);
    expect(p.seen[0].at(-1)!.text).toMatch(/Answer in Hebrew/);
  });

  it("markdown emphasis is removed from the answer", async () => {
    const p = scriptedProvider([tc(), { text: "**Best:** {{r1.row1.Players}}" }]);
    const { answer } = await run("q", p);
    expect(answer.text).toBe("Best: A - B");
  });

  it("Hebrew answers say פוזשנים for possessions", async () => {
    const p = scriptedProvider([tc(), { text: "{{r1.row1.Players}}: מינימום החזקות, החזקה אחת, ב-חזקות" }]);
    const { answer } = await run("מה החמישייה?", p);
    expect(answer.text).toBe("A - B: מינימום פוזשנים, פוזשן אחת, ב-פוזשנים");
  });

  it("numbers from earlier questions in the conversation may be repeated", async () => {
    const earlier = { question: "top 5 2-man lineups of Maccabi", answer: "x", calls: [] };
    const p = scriptedProvider([tc(), { text: "Of those five, {{r1.row1.Players}} stands out." }]);
    const { answer } = await run("what are they good at?", p, [earlier] as never);
    expect(answer.fallback).toBe(false);
  });

  it("an incomplete marker gets a retry message showing the full form", async () => {
    const p = scriptedProvider([tc(), { text: "{{r1.row1}} leads" }, { text: "{{r1.row1.Players}} leads" }]);
    const { answer } = await run("q", p);
    expect(p.seen[2].at(-1)!.text).toMatch(/\{\{r1\.row1\.<column>\}\}/);
    expect(answer.fallback).toBe(false);
  });

  it("two retries before the fallback", async () => {
    const p = scriptedProvider([tc(), { text: "7" }, { text: "8" }, { text: "{{r1.row1.Players}}" }]);
    const { answer } = await run("q", p);
    expect(answer).toMatchObject({ fallback: false, text: "A - B" });
    expect(answer.rejected).toHaveLength(2);
  });

  it("a doubled result word in Hebrew is collapsed", async () => {
    const p = scriptedProvider([tc(), { text: "{{r1.row1.Players}} הסתיים בהפסד הפסד" }]);
    const { answer } = await run("איך היה המשחק?", p);
    expect(answer.text).toContain("בהפסד");
    expect(answer.text).not.toContain("הפסד הפסד");
  });

  it("a refusal becomes the fallback", async () => {
    const p = scriptedProvider([{ refused: true }]);
    const { answer } = await run("q", p);
    expect(answer.fallback).toBe(true);
  });
});
