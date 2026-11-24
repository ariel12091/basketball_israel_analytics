import { describe, it, expect } from "vitest";
import { readWinProb, WIN_PROB_TIMEOUT_MS } from "./winProbRepo";
import type { Query } from "./db";

// A fake snapshot query: records every statement, and fails the
// game_win_probability call when asked to.
function fake(fail?: Error) {
  const seen: string[] = [];
  const q = (async (sql: string, params: unknown[]) => {
    seen.push(params.length ? `${sql} ${JSON.stringify(params)}` : sql);
    if (sql.includes("game_win_probability")) {
      if (fail) throw fail;
      return [{ possession_no: 1 }];
    }
    return [];
  }) as Query;
  return { q, seen };
}

describe("readWinProb", () => {
  it("reads under its own short timeout and leaves the transaction as it was", async () => {
    const { q, seen } = fake();
    expect(await readWinProb(q, "il", 673)).toEqual([{ possession_no: 1 }]);
    expect(seen[0]).toBe("SAVEPOINT win_prob");
    expect(seen[1]).toBe(`SET LOCAL statement_timeout = '${WIN_PROB_TIMEOUT_MS}ms'`);
    expect(seen[2]).toContain('game_win_probability($1::text, $2::int4) ["il",673]');
    // Rolled back on success too: undoes the SET LOCAL for the page's later reads.
    expect(seen[3]).toBe("ROLLBACK TO SAVEPOINT win_prob");
  });
  it("gives up on a slow or failing read without failing the page", async () => {
    const timeout = Object.assign(new Error("canceling statement due to statement timeout"), { code: "57014" });
    const { q, seen } = fake(timeout);
    expect(await readWinProb(q, "euro", 1147)).toEqual([]);
    expect(seen.at(-1)).toBe("ROLLBACK TO SAVEPOINT win_prob");
  });
  it("keeps the timeout well inside the request's SQL budget", () => {
    expect(WIN_PROB_TIMEOUT_MS).toBeLessThanOrEqual(5000);
  });
});
