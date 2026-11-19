import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ParamError, handleError } from "./api";
import { BusyError, DeadlineError } from "./deadline";
import { failureOf, leagueOfPath, logged, pageFailed, SLOW_MS } from "./requestLog";

let lines: { level: string; line: Record<string, unknown> }[] = [];
beforeEach(() => {
  lines = [];
  for (const level of ["log", "warn", "error"] as const)
    vi.spyOn(console, level).mockImplementation((s: string) => void lines.push({ level, line: JSON.parse(s) }));
});
afterEach(() => vi.restoreAllMocks());

const pgError = (code: string) => Object.assign(new Error("relation x does not exist: SELECT secret"), { code });

describe("request log lines", () => {
  it("names a known league, never arbitrary path text", () => {
    expect(leagueOfPath("/api/v1/euroleague/lineups")).toBe("euroleague");
    expect(leagueOfPath("/api/v1/<script>/lineups")).toBe("other");
    expect(leagueOfPath("/api/v1/version")).toBeNull();
  });

  it("classifies failures; a database error carries its SQLSTATE, never its message", () => {
    expect(failureOf(new ParamError("team", "Invalid team")).reason).toBe("param");
    expect(failureOf(new BusyError()).reason).toBe("busy");
    expect(failureOf(new DeadlineError()).reason).toBe("timeout");
    expect(failureOf(pgError("57014"))).toMatchObject({ reason: "timeout", code: "57014" });
    expect(failureOf(pgError("42P01"))).toMatchObject({ reason: "database", code: "42P01" });
    expect(failureOf(Object.assign(new Error("x"), { code: "not a sqlstate" }))).toMatchObject({ reason: "error" });
    expect(JSON.stringify(failureOf(pgError("42P01")))).not.toContain("secret");
  });

  it("an API error line names the route and league from the wrapped request", async () => {
    const get = logged("lineups", async (_r: Request) => handleError(pgError("42P01")));
    const res = await get(new Request("http://x/api/v1/eurocup/lineups?team=SECRET"));
    expect(res.status).toBe(503);
    expect(lines).toHaveLength(1);
    expect(lines[0].level).toBe("error");
    expect(lines[0].line).toMatchObject({
      kind: "api_error", route: "lineups", league: "eurocup", status: 503, reason: "database", code: "42P01",
    });
    expect(JSON.stringify(lines)).not.toMatch(/SECRET|secret/);
  });

  it("a refused parameter is logged without its name or value; busy and timeouts are warnings", async () => {
    const run = (e: unknown) => logged("onoff", async (_r: Request) => handleError(e))(new Request("http://x/api/v1/il/onoff"));
    expect((await run(new ParamError("typed-by-visitor", "Unknown parameter"))).status).toBe(400);
    expect((await run(new BusyError())).status).toBe(503);
    expect((await run(new DeadlineError())).status).toBe(504);
    expect(lines.map((l) => [l.level, l.line.reason, l.line.status])).toEqual([
      ["log", "param", 400], ["warn", "busy", 503], ["warn", "timeout", 504],
    ]);
    expect(JSON.stringify(lines)).not.toContain("typed-by-visitor");
  });

  it("writes slow_request only past the threshold", async () => {
    let now = 1000;
    vi.spyOn(performance, "now").mockImplementation(() => now);
    const slow = logged("team-ratings", async (_r: Request) => {
      now += SLOW_MS;
      return new Response("{}");
    });
    const quick = logged("team-ratings", async (_r: Request) => new Response("{}"));
    await quick(new Request("http://x/api/v1/il/team-ratings"));
    expect(lines).toHaveLength(0);
    await slow(new Request("http://x/api/v1/il/team-ratings"));
    expect(lines).toEqual([
      { level: "warn", line: { kind: "slow_request", route: "team-ratings", league: "il", status: 200, ms: SLOW_MS } },
    ]);
  });

  it("a page's failed first load is logged and still resolves to null", async () => {
    await expect(Promise.reject(pgError("08006")).catch(pageFailed("home", "il"))).resolves.toBeNull();
    expect(lines[0]).toEqual({
      level: "error",
      line: { kind: "page_load_failed", page: "home", league: "il", reason: "database", code: "08006", name: "Error" },
    });
  });
});
