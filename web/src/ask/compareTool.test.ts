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
    // The page's own side headings, for the answer's wording.
    expect(t.meta.sides).toEqual({ a: "vs Top 4", b: "vs Bottom 4" });
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
    for (const q of [
      "cmp_view=detail", // no entity
      "cmp_view=players",
      "view=Lineups&cmp_view=players&entity=abc",
      "open_detail=true", // needs a preset and team_id, as Home's links send
      "cmp_view=bogus&entity=2",
      "toString=1",
      "drill_player=5",
    ])
      await expect(compareTable(side(), new URLSearchParams(q)), q).rejects.toBeInstanceOf(ParamError);
    await expect(compareTable(side(), new URLSearchParams("cmp_view=league&open_detail=false"))).resolves.toBeTruthy();
  });

  it("detail: the page's sections for one team, from ratings, four factors and shooting", async () => {
    const read = vi.fn(async (s: URLSearchParams) => {
      const win = s.get("outcome") === "win";
      const kind = s.get("kind");
      const rows =
        kind === "ratings"
          ? [
              { team_id: 3, team_name: "B", net_rtg: 0 },
              {
                team_id: 10,
                team_name: "GALIL",
                games_played: win ? 6 : 4,
                wins: win ? 6 : 0,
                losses: win ? 0 : 4,
                off_poss: win ? 420 : 300,
                net_rtg: win ? 12.04 : -8.5,
                off_ppp: win ? 115 : 100,
                def_ppp: win ? 103 : 108.5,
              },
            ]
          : kind === "four_factors"
            ? [{ team_id: 10, off_efg: win ? 55 : 50, off_tov: win ? 12 : 15 }]
            : [{ team_id: 10, off_fg2_made: 10, off_fg2_att: 20, off_fg3_made: 3, off_fg3_att: 10 }];
      return { rows, meta: { version: "v1" } };
    });
    const t = await compareTable(
      read,
      new URLSearchParams("season=2026&preset=win_loss&team_id=10&open_detail=true"),
    );
    // Ratings, four factors and shooting, side A then side B each.
    expect(read.mock.calls.map(([s]) => `${s.get("kind")}:${s.get("outcome")}`)).toEqual([
      "ratings:win",
      "ratings:loss",
      "four_factors:win",
      "four_factors:loss",
      "shooting:win",
      "shooting:loss",
    ]);
    expect(read.mock.calls.slice(4).map(([s]) => s.get("teams"))).toEqual(["10", "10"]);
    expect(t.meta).toMatchObject({ view: "detail", sides: { a: "Win", b: "Loss" } });
    const by = (label: string) => t.rows.find((r) => r.label === label)!;
    expect(by("Net Rtg")).toMatchObject({ section: "Ratings", a: 12.04, b: -8.5, fmt: "net", better: "Win" });
    // Def Rtg is better lower: the loss side's 108.5 is worse.
    expect(by("Def Rtg")).toMatchObject({ a: 103, b: 108.5, better: "Win" });
    expect(by("TOV%")).toMatchObject({ section: "Offensive Four Factors", better: "Win" });
    expect(by("Win%")).toMatchObject({ a: 100, b: 0 });
    // Neutral metrics name no better side.
    expect(by("2PT Freq").better).toBeNull();
    expect(t.rows).toHaveLength(32);
    expect(t.meta.values).toMatchObject({
      entity: "GALIL ELION", // the catalogue name the page heads the detail with
      context_a: "6 GP | 6-0 | 420 Poss",
      context_b: "4 GP | 0-4 | 300 Poss",
    });
  });

  it("players: the team's players under both conditions, as the page joins them", async () => {
    const read = vi.fn(async (s: URLSearchParams) => ({
      rows: [
        {
          team_id: 10,
          team_name: "GALIL",
          player_id: 7,
          player_name: "P. One",
          gp: 3,
          poss_on_floor: 200,
          pts: s.get("outcome") === "win" ? 60 : 30,
        },
      ],
      meta: { version: "v1" },
    }));
    const t = await compareTable(
      read,
      new URLSearchParams("season=2026&preset=win_loss&team_id=10&cmp_view=players&entity=10"),
    );
    expect(read.mock.calls.map(([s]) => [s.get("kind"), s.get("teams")])).toEqual([
      ["players", "10"],
      ["players", "10"],
    ]);
    expect(t.meta).toMatchObject({ view: "players" });
    expect(t.rows).toMatchObject([{ player_name: "P. One", metric_a: 20, metric_b: 10, gap: 10, rank: 1 }]);
  });

  it("refuses to pair sides from different data versions", async () => {
    let n = 0;
    const answer = vi.fn(async () => ({ rows: rowsA, meta: { version: `v${n++}` } }));
    await expect(compareTable(answer, new URLSearchParams(""))).rejects.toThrow(/changed/);
  });
});
