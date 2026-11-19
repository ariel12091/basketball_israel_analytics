import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const calls: { sql: string; params: unknown[]; options: unknown }[] = [];
let stored: string | null = "7";
vi.mock("@/server/db", async (orig) => ({
  ...(await orig<typeof import("@/server/db")>()),
  withSnapshot: async (_d: unknown, fn: (q: unknown) => Promise<unknown>, options: unknown) =>
    fn(async (sql: string, params: unknown[]) => {
      calls.push({ sql, params, options });
      return [{ id: stored }];
    }),
}));
const { POST } = await import("./route");

const body = (b: Record<string, unknown> = {}) => ({
  league: "il",
  question: "who led Maccabi in assists",
  answer: "Lorenzo Brown, 6.1 per game.",
  calls: [{ tool: "player_stats", league: "il", query: { team: "2" } }],
  note: "  he was injured  ",
  clientId: "c-1",
  ...b,
});
const post = (b: unknown, headers: Record<string, string> = {}) =>
  POST(
    new Request("http://x/api/v1/ask/feedback", {
      method: "POST",
      headers: { "content-type": "application/json", "x-forwarded-for": "1.2.3.4", ...headers },
      body: JSON.stringify(b),
    }),
  );

let logs: string[] = [];
beforeEach(() => {
  vi.stubEnv("ASK_ENABLED", "true");
  vi.stubEnv("ASK_PROVIDER", "fake");
  calls.length = 0;
  stored = "7";
  logs = [];
  vi.spyOn(console, "log").mockImplementation((s: string) => void logs.push(String(s)));
  vi.spyOn(console, "error").mockImplementation((s: string) => void logs.push(String(s)));
});
afterEach(() => {
  vi.unstubAllEnvs();
  vi.restoreAllMocks();
});

describe("POST /api/v1/ask/feedback", () => {
  it("stores through the one function, in a writing transaction, and logs no content", async () => {
    const r = await post(body({ clientId: "store" }), { "x-forwarded-for": "9.9.9.1" });
    expect(r.status).toBe(200);
    expect(calls).toHaveLength(1);
    expect(calls[0].sql).toBe(
      "SELECT app_feedback.submit_ask_feedback($1, $2, $3, $4::jsonb, $5, $6) AS id",
    );
    expect(calls[0].options).toEqual({ write: true });
    const [league, question, answer, json, note] = calls[0].params;
    expect([league, question, answer, note]).toEqual([
      "il", "who led Maccabi in assists", "Lorenzo Brown, 6.1 per game.", "  he was injured  ",
    ]);
    expect(JSON.parse(String(json))).toEqual(body().calls);
    // Nothing identifying the visitor reaches the database.
    expect(JSON.stringify(calls[0].params)).not.toMatch(/store|9\.9\.9\.1/);
    expect(logs).toEqual([JSON.stringify({ kind: "ask_feedback", stored: true })]);
  });

  it("is off with the chat, and refuses another site's page", async () => {
    vi.stubEnv("ASK_ENABLED", "false");
    expect((await post(body())).status).toBe(404);
    vi.stubEnv("ASK_ENABLED", "true");
    expect((await post(body(), { origin: "https://evil.example" })).status).toBe(403);
    expect(calls).toHaveLength(0);
  });

  it("refuses a malformed or oversized flag before the database", async () => {
    for (const b of [
      body({ question: "" }),
      body({ question: "x".repeat(501) }),
      body({ answer: "x".repeat(4001) }),
      body({ note: "x".repeat(501) }),
      body({ calls: Array(7).fill({ tool: "x" }) }),
      body({ calls: [{ q: "x".repeat(8000) }] }),
      body({ league: "nba" }),
    ])
      expect((await post(b, { "x-forwarded-for": "9.9.9.2" })).status).toBe(400);
    expect(calls).toHaveLength(0);
  });

  it("says so when the database's hourly cap refuses", async () => {
    stored = null;
    const r = await post(body({ clientId: "full" }), { "x-forwarded-for": "9.9.9.3" });
    expect(r.status).toBe(503);
    expect(await r.json()).toMatchObject({ error: expect.stringMatching(/full/) });
  });

  it("allows five flags an hour per browser", async () => {
    for (let i = 0; i < 5; i++)
      expect((await post(body({ clientId: "five" }), { "x-forwarded-for": `9.9.8.${i}` })).status).toBe(200);
    expect((await post(body({ clientId: "five" }), { "x-forwarded-for": "9.9.8.9" })).status).toBe(429);
  });
});
