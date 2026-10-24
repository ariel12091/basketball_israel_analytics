import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("@/ask/run", async (orig) => ({
  ...(await orig<typeof import("@/ask/run")>()),
  runTool: async (id: string, call: unknown) => ({
    id,
    call,
    link: "/il/lineups?team=2",
    view: "summary",
    rows: [{ id: "row1", cells: { Players: "A - B", net_rtg: 7.26 } }],
    total: null,
    rowCount: 1,
    version: "v1",
  }),
}));
const { POST } = await import("./route");

const script = path.join(mkdtempSync(path.join(tmpdir(), "ask-")), "script.json");
writeFileSync(
  script,
  JSON.stringify([
    { toolCalls: [{ id: "c1", call: { tool: "lineups", league: "il", query: { team: "2" }, sort: { key: "net_rtg", desc: true } } }] },
    { text: "Best: {{r1.row1.Players}} ({{r1.row1.net_rtg}})." },
  ]),
);
const body = (b: Record<string, unknown> = {}) => ({
  league: "il",
  question: "top lineups of Maccabi",
  history: [],
  clientId: "c-1",
  ...b,
});
const post = (b: unknown, ip = "1.2.3.4") =>
  POST(
    new Request("http://x/api/v1/ask", {
      method: "POST",
      headers: { "content-type": "application/json", "x-forwarded-for": `${ip}, 10.0.0.1` },
      body: JSON.stringify(b),
    }),
  );

let logs: string[] = [];
beforeEach(() => {
  vi.stubEnv("ASK_ENABLED", "true");
  vi.stubEnv("ASK_PROVIDER", "fake");
  vi.stubEnv("ASK_FAKE_SCRIPT", script);
  logs = [];
  vi.spyOn(console, "log").mockImplementation((s: string) => void logs.push(String(s)));
});
afterEach(() => {
  vi.unstubAllEnvs();
  vi.restoreAllMocks();
});

describe("POST /api/v1/ask", () => {
  it("ASK_ENABLED unset or false -> 404", async () => {
    vi.stubEnv("ASK_ENABLED", "false");
    expect((await post(body())).status).toBe(404);
    vi.stubEnv("ASK_ENABLED", "");
    expect((await post(body())).status).toBe(404);
  });

  it("question over 500 chars -> 400", async () => {
    expect((await post(body({ question: "x".repeat(501) }))).status).toBe(400);
  });

  it("6th turn -> 400 start a new question", async () => {
    const h = { question: "q", answer: "a", calls: [] };
    const r = await post(body({ history: Array(6).fill(h) }));
    expect(r.status).toBe(400);
    expect(await r.json()).toMatchObject({ error: expect.stringMatching(/new question/i) });
  });

  it("streams status then final with the scripted provider", async () => {
    const r = await post(body(), "9.9.9.1");
    expect(r.headers.get("content-type")).toMatch(/text\/event-stream/);
    const events = (await r.text())
      .split("\n\n")
      .filter(Boolean)
      .map((e) => JSON.parse(e.replace(/^data: /, "")));
    expect(events[0]).toMatchObject({ type: "status" });
    expect(events.at(-1)).toMatchObject({
      type: "final",
      answer: { text: "Best: A - B (7.3).", links: ["/il/lineups?team=2"], fallback: false },
    });
  });

  it("log line has no ip or clientId", async () => {
    await (await post(body({ clientId: "secret-client" }), "7.7.7.7")).text();
    const line = logs.find((l) => l.includes("ask_turn"))!;
    expect(JSON.parse(line)).toMatchObject({ kind: "ask_turn", question: "top lineups of Maccabi", fallback: false });
    expect(line).not.toContain("secret-client");
    expect(line).not.toContain("7.7.7.7");
  });

  it("21st question in an hour from one browser -> 429 with Retry-After", async () => {
    let last: Response | undefined;
    for (let i = 0; i < 21; i++) {
      last = await post(body({ clientId: "burst" }), `5.5.5.${i}`);
      if (last.status === 200) await last.text();
    }
    expect(last!.status).toBe(429);
    expect(Number(last!.headers.get("retry-after"))).toBeGreaterThan(0);
  });
});
