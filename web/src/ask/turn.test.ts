import { beforeEach, describe, expect, it, vi } from "vitest";
import { scriptedProvider } from "./providers/fake";
import type { ToolCall, ToolError, ToolResult } from "./run";

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
  });

  it("two failures -> fallback with link and evidence", async () => {
    const p = scriptedProvider([tc(), { text: "Net 7.3" }, { text: "Still 7.3" }]);
    const { answer } = await run("q", p);
    expect(answer).toMatchObject({ fallback: true, links: ["/il/lineups?team=2"] });
    expect(answer.text).toMatch(/page that answers/);
  });

  it("fifth tool call is refused with a tool error", async () => {
    const five = { toolCalls: [1, 2, 3, 4, 5].map((i) => ({ id: `c${i}`, call })) };
    const p = scriptedProvider([five, { text: "{{r1.row1.Players}}" }]);
    await run("q", p);
    expect(runTool).toHaveBeenCalledTimes(4);
    const results = p.seen[1].at(-1)!.toolResults!;
    expect(results).toHaveLength(5);
    expect(JSON.stringify(results[4].content)).toMatch(/limit/i);
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

  it("a refusal becomes the fallback", async () => {
    const p = scriptedProvider([{ refused: true }]);
    const { answer } = await run("q", p);
    expect(answer.fallback).toBe(true);
  });
});
