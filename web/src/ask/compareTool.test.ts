import { describe, expect, it, vi } from "vitest";
import { ParamError } from "@/server/api";
import { compareTable } from "./compareTool";

const rowsA = [
  { team_id: 2, team_name: "A", net_rtg: 8, off_poss: 300 },
  { team_id: 3, team_name: "B", net_rtg: 1, off_poss: 300 },
];
const rowsB = [
  { team_id: 2, team_name: "A", net_rtg: -2, off_poss: 280 },
  { team_id: 3, team_name: "B", net_rtg: 4, off_poss: 5 },
];
const side = () =>
  vi.fn(async (s: URLSearchParams) => ({
    rows: s.get("opp_rank_side") === "bottom" ? rowsB : rowsA,
    meta: { version: "v1" },
  }));

describe("compareTable: the Compare page's paired table", () => {
  it("pairs A and B with the page's rules: gap, min poss, rank", async () => {
    const answer = side();
    const t = await compareTable(
      answer,
      new URLSearchParams("season=2026&preset=top_bottom_rank"),
    );
    expect(answer).toHaveBeenCalledTimes(2);
    const [a, b] = answer.mock.calls.map(([s]) => s);
    expect([a.get("opp_rank_side"), b.get("opp_rank_side")]).toEqual(["top", "bottom"]);
    // Team 3's B side has 5 possessions, under the default minimum of 10.
    expect(t.rows).toMatchObject([
      { team_id: 2, metric_a: 8, metric_b: -2, gap: 10, rank: 1 },
    ]);
    expect(t.meta).toMatchObject({ version: "v1", view: "Teams" });
  });

  it("team_id pins the team on both sides", async () => {
    const answer = side();
    await compareTable(
      answer,
      new URLSearchParams("season=2026&preset=top_bottom_rank&team_id=2"),
    );
    expect(answer.mock.calls.map(([s]) => s.get("teams"))).toEqual(["2", "2"]);
  });

  it("rejects unknown parameters and the Players mode", async () => {
    await expect(compareTable(side(), new URLSearchParams("bogus=1"))).rejects.toMatchObject({
      param: "bogus",
    });
    await expect(
      compareTable(side(), new URLSearchParams("view=Players")),
    ).rejects.toBeInstanceOf(ParamError);
  });

  it("rejects views the tool does not quote, and prototype keys", async () => {
    for (const q of ["cmp_view=detail", "cmp_view=players", "open_detail=true", "toString=1", "drill_player=5"])
      await expect(compareTable(side(), new URLSearchParams(q)), q).rejects.toBeInstanceOf(ParamError);
    await expect(compareTable(side(), new URLSearchParams("cmp_view=league&open_detail=false"))).resolves.toBeTruthy();
  });

  it("refuses to pair sides from different data versions", async () => {
    let n = 0;
    const answer = vi.fn(async () => ({ rows: rowsA, meta: { version: `v${n++}` } }));
    await expect(compareTable(answer, new URLSearchParams(""))).rejects.toThrow(/changed/);
  });
});
