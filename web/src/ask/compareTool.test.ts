import { describe, expect, it, vi } from "vitest";
import { ParamError } from "@/server/api";
import { compareLink, compareTable } from "./compareTool";

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

  describe("filters combine with presets instead of being dropped", () => {
    const sidesOf = async (q: string) => {
      const answer = side();
      const t = await compareTable(answer, new URLSearchParams(q));
      return { a: answer.mock.calls[0][0], b: answer.mock.calls[1][0], t };
    };

    it("game types on their own sides, named in the labels", async () => {
      const { a, b, t } = await sidesOf("a_game_type=5&b_game_type=10,34");
      expect([a.get("game_type"), b.get("game_type")]).toEqual(["5", "10,34"]);
      expect(t.meta.sides).toEqual({ a: "Regular", b: "W.Cup" });
    });

    it("a filter without a side reaches both, on top of the preset", async () => {
      const { a, b, t } = await sidesOf("preset=home_away&game_type=10,34");
      expect([a.get("home_away"), b.get("home_away")]).toEqual(["home", "away"]);
      expect([a.get("game_type"), b.get("game_type")]).toEqual(["10,34", "10,34"]);
      expect(t.meta.sides).toEqual({ a: "Home | W.Cup", b: "Away | W.Cup" });
    });

    it("a one-sided filter mirrors under a preset, as the page's edit does", async () => {
      const { a, b } = await sidesOf("preset=home_away&a_game_type=5");
      expect([a.get("game_type"), b.get("game_type")]).toEqual(["5", "5"]);
    });

    it("stays one-sided with no preset", async () => {
      const { a, b } = await sidesOf("a_game_type=10,34");
      expect([a.get("game_type"), b.get("game_type")]).toEqual(["10,34", null]);
    });

    it("the top/bottom preset takes another N", async () => {
      const { a, b } = await sidesOf("preset=top_bottom_rank&opp_rank_n=6");
      expect([a.get("opp_rank_n"), b.get("opp_rank_n")]).toEqual(["6", "6"]);
      expect(a.get("opp_rank_side")).toBe("top");
    });

    it("team_id pins the team with no preset", async () => {
      const { a, b } = await sidesOf("a_game_type=5&b_game_type=10,34&team_id=2");
      expect([a.get("teams"), b.get("teams")]).toEqual(["2", "2"]);
    });

    it("open_detail opens one team's Detail with no preset", async () => {
      const { t } = await sidesOf("a_game_type=5&b_game_type=10,34&team_id=2&open_detail=true");
      expect(t.meta.view).toBe("detail");
    });

    it("last N vs season: the preset's N, last_n meaning side A", async () => {
      for (const q of ["preset=last_n_season&a_last_n=5", "preset=last_n_season&last_n=5"]) {
        const { a, b, t } = await sidesOf(q);
        expect([a.get("last_n"), b.get("last_n")]).toEqual(["5", null]);
        expect(t.meta.sides.b).toBe("Season");
      }
    });

    it("refuses only what cannot exist", async () => {
      for (const q of [
        "preset=home_away&home_away=home",
        "preset=win_loss&a_outcome=loss",
        "preset=last_n_season&b_last_n=5",
        "a_last_n=5&min_gn=3",
        "preset=gn_split&a_last_n=5",
        "phase=x",
        "team_id=abc",
      ])
        await expect(compareTable(side(), new URLSearchParams(q)), q).rejects.toBeInstanceOf(ParamError);
      await expect(
        compareTable(side(), new URLSearchParams("preset=home_away&a_home_away=home")),
      ).resolves.toBeTruthy();
    });

    it("the link is the query the numbers were read with", () => {
      const params = (q: string) =>
        Object.fromEntries(new URL(compareLink(Object.fromEntries(new URLSearchParams(q)))!, "http://x").searchParams);
      expect(params("preset=home_away&season=2027&game_type=10,34")).toEqual({
        preset: "home_away",
        preset_applied: "true",
        season: "2027",
        a_home_away: "home",
        b_home_away: "away",
        a_game_type: "10,34",
        b_game_type: "10,34",
      });
      expect(params("a_game_type=5&b_game_type=10,34&team_id=2")).toMatchObject({
        a_teams: "2",
        b_teams: "2",
      });
      expect(compareLink({ preset: "home_away", home_away: "home" })).toBeNull();
    });
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
      "open_detail=true", // no team to open
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
